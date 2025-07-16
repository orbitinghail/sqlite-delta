"""
Three-Phase CDC Pattern for SQLite

This module provides a Change Data Capture (CDC) implementation using a three-phase
versioning system. The pattern maintains up to three versions of each row across
different phases (0, 1, 2) to enable atomic changeset generation and delta compression.

This code is not intended for production use, but serves as a demonstration of
the pattern and tests its correctness.
"""

import random
import sqlite3
import sys
import time
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterator, List, Literal, Union

import fossil_delta

# Add parent directory to path to import testlib
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from testlib import compute_hash_from_rows, compute_table_hash, sqlite3_test_db


@dataclass
class InsertOp:
    """Represents an insert operation in a changeset."""

    id: int
    data: str


@dataclass
class UpdateOp:
    """Represents an update operation with delta compression in a changeset."""

    id: int
    delta: bytes


@dataclass
class DeleteOp:
    """Represents a delete operation in a changeset."""

    id: int


# Type alias for changeset operations
ChangesetOp = Union[InsertOp, UpdateOp, DeleteOp]

# Type alias for a changeset, mapping table names to lists of operations
Changeset = Dict[str, List[ChangesetOp]]


def setup_three_phase_table(conn: sqlite3.Connection, table_name: str) -> str:
    """
    Create a three-phase table with phase and deleted columns.

    This creates a simple table with:
    - id INTEGER: primary key
    - data BLOB: single data column
    - phase/deleted: three-phase pattern columns

    Args:
        conn: SQLite database connection
        table_name: Name of the table to create

    Returns:
        The name of the created table.
    """
    sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            -- Application columns
            id INTEGER,
            data BLOB,

            -- Three-phase pattern columns:
            -- phase represents the current phase of the row
            -- 0: new row, not seen by any changeset
            -- 1: row version used by an in-progress changeset
            -- 2: stable row version
            phase INTEGER NOT NULL DEFAULT 0,

            -- deleted indicates whether the row is logically deleted
            -- the row will be removed after reaching phase 2
            deleted BOOL NOT NULL DEFAULT 0,

            -- The primary key must include phase, as there are now up to 3 copies
            -- of each row depending on snapshot state
            PRIMARY KEY (id, phase)
        )
    """
    conn.execute(sql)
    return table_name


def setup_regular_table(conn: sqlite3.Connection, table_name: str) -> str:
    """
    Create a regular table without three-phase columns.

    This creates a simple table with:
    - id INTEGER: primary key
    - data BLOB: single data column

    Args:
        conn: SQLite database connection
        table_name: Name of the table to create

    Returns:
        The name of the created table.
    """
    sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id INTEGER PRIMARY KEY,
            data BLOB
        )
    """
    conn.execute(sql)
    return table_name


def insert_or_update(conn: sqlite3.Connection, table_name: str, row_id: int, data: str) -> None:
    """
    Insert or update a row using upsert operation targeting phase 0.

    Args:
        conn: SQLite database connection
        table_name: Name of the table
        row_id: Primary key value
        data: Data value to insert/update
    """
    sql = f"""
        INSERT INTO {table_name} (id, data)
        VALUES (?, ?)
        ON CONFLICT (id, phase) DO UPDATE SET data = excluded.data
    """
    conn.execute(sql, (row_id, data))


def logical_delete(conn: sqlite3.Connection, table_name: str, row_id: int) -> None:
    """
    Logically delete a row by setting deleted=1 with phase=0.

    Note: We can clear the data here to save space, but it is not strictly
    necessary for correctness. The row will be removed once it reaches phase=2.

    Args:
        conn: SQLite database connection
        table_name: Name of the table
        row_id: Primary key value to delete
    """
    sql = f"""
        INSERT INTO {table_name} (id, deleted)
        VALUES (?, 1)
        ON CONFLICT (id, phase) DO UPDATE SET
            deleted = excluded.deleted,
            data = NULL
    """
    conn.execute(sql, (row_id,))


def read_latest(conn: sqlite3.Connection, table_name: str, row_id: int) -> str | None:
    """
    Read the latest version of a row (lowest phase, not deleted).

    Args:
        conn: SQLite database connection
        table_name: Name of the table
        row_id: Primary key value to read

    Returns:
        Data value, or None if not found/deleted
    """
    sql = f"""
        SELECT data FROM (
            SELECT * FROM {table_name}
            WHERE id = ?
            ORDER BY phase ASC
            LIMIT 1
        ) WHERE deleted = 0
    """
    cursor = conn.execute(sql, (row_id,))
    row = cursor.fetchone()

    if row is None:
        return None

    return row[0]


@contextmanager
def changeset(conn: sqlite3.Connection, table_name: str) -> Iterator[List[ChangesetOp]]:
    """
    Context manager for atomic changeset generation with delta compression.

    This context manager:
    1. Transitions all phase 0 rows to phase 1
    2. Generates changeset by comparing phase 1 and phase 2 rows
    3. Computes deltas for updates using fossil delta algorithm
    4. Automatically cleans up processed changes

    Args:
        conn: SQLite database connection
        table_name: Name of the table to generate changeset for

    Yields:
        List of changeset operations (InsertOp, UpdateOp, DeleteOp)
    """
    operations = []

    # Step 1: Transition phase 0 rows to phase 1
    #
    # Note: If any rows exist in phase 1 then the last changeset operation failed to complete
    # In this case we have to recover
    with conn:
        # Remove any rows in phase 1 which were overwritten in phase 0
        conn.execute(f"""
            DELETE FROM {table_name}
            WHERE phase = 1 AND id IN (SELECT id FROM {table_name} WHERE phase = 0)
        """)

        # Finally migrate phase 0 rows to phase 1
        conn.execute(f"UPDATE {table_name} SET phase = 1 WHERE phase = 0")

    # Step 2: Generate changeset using single query per SQL pattern
    with conn:
        changeset_sql = f"""
            SELECT
                IFNULL(before.id, after.id) as id,
                before.data as data_before,
                after.data as data_after,
                after.deleted as deleted
            FROM
                (SELECT * FROM {table_name} WHERE phase = 2) as before
                RIGHT JOIN
                (SELECT * FROM {table_name} WHERE phase = 1) as after
                USING (id)
        """

        cursor = conn.execute(changeset_sql)
        rows = cursor.fetchall()

        for row in rows:
            row_id, data_before, data_after, deleted = row

            # Determine operation type
            if deleted:
                operations.append(DeleteOp(id=row_id))
            elif data_before is None:
                # New row (insert) - no corresponding phase 2 row
                operations.append(InsertOp(id=row_id, data=data_after))
            else:
                # Updated row - compute delta
                before_bytes = (
                    data_before
                    if isinstance(data_before, bytes)
                    else str(data_before).encode("utf-8")
                )
                after_bytes = (
                    data_after if isinstance(data_after, bytes) else str(data_after).encode("utf-8")
                )
                delta = fossil_delta.create_delta(before_bytes, after_bytes)

                operations.append(UpdateOp(id=row_id, delta=delta))

    try:
        yield operations
    except:
        # If we crash while yielding the changeset do nothing - we don't want to
        # lose changes
        raise
    else:
        # Step 3: Cleanup: remove deleted rows and then move all alive rows to phase=2
        with conn:
            # First we need to remove three classes of rows:
            #   1. deleted phase=2 rows
            #   2. deleted phase=1 rows
            #   3. phase=2 rows which are being updated/deleted by a phase=1 row
            conn.execute(f"""
                DELETE FROM {table_name} as outer
                WHERE
                    (
                        phase = 2 AND
                        (
                            -- First case: deleted phase=2 rows
                            deleted = 1

                            -- Third case: phase=2 rows which are in phase=1
                            OR EXISTS (
                                SELECT * FROM {table_name} as inner
                                WHERE outer.id = inner.id AND phase = 1
                            )
                        )
                    ) OR (
                        -- Second case: deleted phase=1 rows
                        phase = 1 AND deleted = 1
                    )
            """)

            # Migrate phase 1 rows to phase 2
            conn.execute(f"UPDATE {table_name} SET phase = 2 WHERE phase = 1")


def compact(conn: sqlite3.Connection, table_name: str) -> None:
    """
    Compact a three-phase table down to only phase=2 rows.

    This operation keeps the latest version of each row and sets its phase to 2.
    It should be used to take periodic checkpoints to prevent delta histories
    from growing too large.

    Args:
        conn: SQLite database connection
        table_name: Name of the table to compact
    """
    with conn:
        # First delete all rows which are either logically deleted or not the latest version
        conn.execute(f"""
            DELETE FROM {table_name}
            WHERE (id, phase) IN (
                SELECT id, phase FROM (
                    SELECT id, phase, deleted, ROW_NUMBER() OVER (PARTITION BY id ORDER BY phase ASC) AS rn
                    FROM {table_name}
                )
                WHERE (rn = 1 AND deleted = 1) OR (rn > 1)
            )
        """)

        # Then update the remaining rows to phase 2
        conn.execute(f"UPDATE {table_name} SET phase = 2")


def compute_id_data_hash(conn: sqlite3.Connection, table_name: str, debug: bool = False) -> str:
    """
    Compute a SHA256 hash of only the id and data columns from a table.

    This function can be used to compare tables that have different schemas
    but should contain the same id/data pairs.

    Args:
        conn: SQLite database connection
        table_name: Name of the table to hash
        debug: Whether to print debug information

    Returns:
        Hex string of the SHA256 hash
    """
    cursor = conn.execute(f"SELECT id, data FROM {table_name} ORDER BY id")
    return compute_hash_from_rows(cursor, debug)


def generate_random_workload(max_id: int, seed: int) -> Iterator[tuple]:
    """
    Generate a random workload of insert, update, and delete operations.

    This generator produces a stream of operation tuples that randomly
    insert, update, and delete rows with IDs in the range [1, max_id].
    The bounded ID space ensures the dataset doesn't grow unbounded.

    Args:
        max_id: Maximum ID value to use (keeps dataset bounded)
        seed: Random seed for reproducible workloads

    Yields:
        tuple: (operation_type, row_id, data) where operation_type is "insert", "update", or "delete"
    """
    if seed is not None:
        random.seed(seed)

    # Track which IDs exist to make deletes and updates more meaningful
    existing_ids = set()

    while True:
        operation_type = random.choice(["insert", "update", "delete"])

        if operation_type == "insert" and len(existing_ids) <= max_id:
            # Create a new row
            row_id = random.randint(1, max_id)
            data = f"data_{row_id}_v{random.randint(1, 1000)}"
            existing_ids.add(row_id)
            yield ("upsert", row_id, data)

        elif operation_type == "update" and existing_ids:
            # Update an existing row
            target_id = random.choice(list(existing_ids))
            data = f"updated_data_{target_id}_v{random.randint(1, 1000)}"
            yield ("upsert", target_id, data)

        elif operation_type == "delete" and existing_ids:
            # Delete an existing row
            target_id = random.choice(list(existing_ids))
            existing_ids.discard(target_id)
            yield ("delete", target_id, "")


def apply_changeset_operation(
    conn: sqlite3.Connection, table_name: str, operation: ChangesetOp
) -> None:
    """
    Apply a single changeset operation directly to phase=2.

    This function is used for replication scenarios where changesets are applied
    on top of a clean checkpoint without intermediate operations.

    Args:
        conn: SQLite database connection
        table_name: Name of the table
        operation: The operation to apply
    """
    if isinstance(operation, InsertOp):
        # Insert directly as phase=2
        sql = f"""
            INSERT INTO {table_name} (id, data, phase)
            VALUES (?, ?, 2)
        """
        conn.execute(sql, (operation.id, operation.data))
    elif isinstance(operation, UpdateOp):
        # Read current data and apply delta, then update phase=2 row
        current_data = read_latest(conn, table_name, operation.id)
        if current_data is not None:
            current_bytes = (
                current_data
                if isinstance(current_data, bytes)
                else str(current_data).encode("utf-8")
            )
            updated_bytes = fossil_delta.apply_delta(current_bytes, operation.delta)
            updated_data = (
                updated_bytes.decode("utf-8") if isinstance(updated_bytes, bytes) else updated_bytes
            )
            # Update the phase=2 row directly
            sql = f"UPDATE {table_name} SET data = ? WHERE id = ? AND phase = 2"
            conn.execute(sql, (updated_data, operation.id))
    elif isinstance(operation, DeleteOp):
        # Remove the phase=2 row completely for deletes
        sql = f"DELETE FROM {table_name} WHERE id = ? AND phase = 2"
        conn.execute(sql, (operation.id,))


def apply_operation_to_regular_table(
    conn: sqlite3.Connection,
    table_name: str,
    operation: Literal["upsert", "delete"],
    rowid: int,
    data: str = "",
) -> None:
    """
    Apply an operation to a regular table (without three-phase columns).

    Args:
        conn: SQLite database connection
        table_name: Name of the regular table
        operation: The operation to apply
        rowid: Primary key value for the operation
        data: Data value for insert/update operations (default empty string)
    """
    if operation == "upsert":
        # Insert or update (upsert)
        sql = f"""
            INSERT INTO {table_name} (id, data)
            VALUES (?, ?)
            ON CONFLICT (id) DO UPDATE SET data = excluded.data
        """
        conn.execute(sql, (rowid, data))
    elif operation == "delete":
        # Delete the row
        sql = f"DELETE FROM {table_name} WHERE id = ?"
        conn.execute(sql, (rowid,))


def test_random_workload(
    seed: int | None = None,
    duration_seconds: float = 3.0,
    max_id: int = 100,
    replication_probability: float = 0.1,
) -> None:
    """
    Test the three-phase pattern with a random workload.

    This test:
    1. Applies a random workload stream to a three-phase table
    2. Applies the same stream to a regular table (without three-phase columns)
    3. Replicates the three-phase table to a second database using changesets
    4. Sends changesets to the replica with some probability after each operation
    5. Runs for a configurable duration
    6. Compacts both three-phase tables after the workload
    7. Compares the hash of all three tables

    Args:
        seed: Random seed for reproducible results
        duration_seconds: How long to run the workload
        max_id: Maximum ID value to use in workload
        replication_probability: Probability of sending changeset to replica after each op
    """

    if seed is None:
        seed = random.randint(0, 2**32 - 1)

    print(f"  Running random workload test for {duration_seconds} seconds...")
    print(
        f"  Using max_id={max_id}, replication_probability={replication_probability}, seed={seed}"
    )

    # Create databases
    with (
        sqlite3_test_db() as writer_conn,
        sqlite3_test_db() as replica_conn,
        sqlite3_test_db() as regular_conn,
    ):
        # Set up tables
        writer_table = setup_three_phase_table(writer_conn, "WorkloadTable")
        replica_table = setup_three_phase_table(replica_conn, "WorkloadTable")
        regular_table = setup_regular_table(regular_conn, "WorkloadTable")

        # Counters
        operation_count = 0
        changeset_count = 0

        # Generate workload and apply it
        start_time = time.time()
        workload_gen = generate_random_workload(max_id, seed)

        while time.time() - start_time < duration_seconds:
            # Get next operation
            operation_type, row_id, data = next(workload_gen)
            operation_count += 1

            # Apply to three-phase table
            if operation_type == "upsert":
                insert_or_update(writer_conn, writer_table, row_id, data)
            elif operation_type == "delete":
                logical_delete(writer_conn, writer_table, row_id)

            # Apply operation to regular table
            apply_operation_to_regular_table(
                regular_conn, regular_table, operation_type, row_id, data
            )

            # Randomly decide whether to replicate
            if random.random() < replication_probability:
                changeset_count += 1
                # Generate changeset and apply to replica
                with changeset(writer_conn, writer_table) as changeset_ops:
                    for op in changeset_ops:
                        apply_changeset_operation(replica_conn, replica_table, op)

        # Final replication to ensure replica is up to date
        with changeset(writer_conn, writer_table) as final_changeset:
            changeset_count += 1
            for op in final_changeset:
                apply_changeset_operation(replica_conn, replica_table, op)

        print(f"  Applied {operation_count} operations in {time.time() - start_time:.2f} seconds")
        print(f"  Applied {changeset_count} changesets to replica")

        # Compact both three-phase tables
        compact(writer_conn, writer_table)
        compact(replica_conn, replica_table)

        # Compute hashes of all three tables (comparing only id+data columns)
        writer_hash = compute_id_data_hash(writer_conn, writer_table)
        replica_hash = compute_id_data_hash(replica_conn, replica_table)
        regular_hash = compute_id_data_hash(regular_conn, regular_table)

        print(f"  Writer hash:  {writer_hash}")
        print(f"  Replica hash: {replica_hash}")
        print(f"  Regular hash: {regular_hash}")

        # All tables should have the same id+data content
        assert writer_hash == replica_hash, (
            f"Writer and replica hashes don't match: {writer_hash} != {replica_hash}"
        )
        assert writer_hash == regular_hash, (
            f"Writer and regular hashes don't match: {writer_hash} != {regular_hash}"
        )

        print("  ✓ All table hashes match! Random workload test passed.")


def run_example() -> None:
    """
    Demonstrate the three-phase CDC pattern with a complete example.
    """
    with sqlite3_test_db() as conn:
        # Create example table
        table_name = setup_three_phase_table(conn, "AppTable")

        # Insert initial data
        initial_data = [
            (1, "data 1; revision 1"),
            (2, "data 2; revision 1"),
            (3, "data 3; revision 1"),
            (4, "data 4; revision 1"),
            (5, "data 5; revision 1"),
        ]

        for row_id, data_value in initial_data:
            insert_or_update(conn, table_name, row_id, data_value)

        print("Initial data inserted")

        # Create initial changeset to establish phase 2 baseline
        with changeset(conn, table_name) as ops:
            print(f"Initial changeset created with {len(ops)} inserts")

        # Make some modifications
        insert_or_update(conn, table_name, 1, "data 1; revision 2")
        insert_or_update(conn, table_name, 3, "data 3; revision 2")
        insert_or_update(conn, table_name, 5, "data 5; revision 2")
        insert_or_update(conn, table_name, 6, "data 6; revision 1")
        insert_or_update(conn, table_name, 7, "data 7; revision 1")
        logical_delete(conn, table_name, 2)

        print("Modifications made")

        # Generate changeset
        with changeset(conn, table_name) as operations:
            print(f"\nGenerated changeset with {len(operations)} operations:")
            for op in operations:
                if isinstance(op, InsertOp):
                    print(f"  Insert id={op.id}: {op.data}")
                elif isinstance(op, UpdateOp):
                    print(f"  Update id={op.id}: delta={len(op.delta)} bytes")
                elif isinstance(op, DeleteOp):
                    print(f"  Delete id={op.id}")

        print("\nChangeset completed and cleanup performed")

        # Demonstrate compaction
        print("\nBefore compaction:")
        cursor = conn.execute(f"SELECT COUNT(*) FROM {table_name}")
        row_count = cursor.fetchone()[0]
        print(f"Total rows: {row_count}")

        compact(conn, table_name)

        print("After compaction:")
        cursor = conn.execute(f"SELECT COUNT(*) FROM {table_name}")
        row_count = cursor.fetchone()[0]
        print(f"Total rows: {row_count}")

        cursor = conn.execute(f"SELECT COUNT(*) FROM {table_name} WHERE phase = 2")
        phase2_count = cursor.fetchone()[0]
        print(f"Phase 2 rows: {phase2_count}")

        print("Table compacted to latest versions only")


def test_pattern():
    """
    Test suite focused on three-phase pattern correctness.
    """

    def test_basic_operations():
        """Test basic insert, update, and delete operations."""
        with sqlite3_test_db() as conn:
            table_name = setup_three_phase_table(conn, "AppTable")

            # Test insert
            insert_or_update(conn, table_name, 1, "test data")
            result = read_latest(conn, table_name, 1)
            assert result == "test data", f"Expected 'test data', got {result}"

            # Test update
            insert_or_update(conn, table_name, 1, "updated data")
            result = read_latest(conn, table_name, 1)
            assert result == "updated data", f"Expected 'updated data', got {result}"

            # Test delete
            logical_delete(conn, table_name, 1)
            result = read_latest(conn, table_name, 1)
            assert result is None, f"Expected None after delete, got {result}"

            print("✓ Basic operations test passed")

    def test_changeset_generation():
        """Test changeset generation with mixed operations."""
        with sqlite3_test_db() as conn:
            table_name = setup_three_phase_table(conn, "AppTable")

            # Insert initial data
            insert_or_update(conn, table_name, 1, "data1")
            insert_or_update(conn, table_name, 2, "data2")
            insert_or_update(conn, table_name, 3, "data3")

            # Create first changeset to establish phase 2 rows
            with changeset(conn, table_name) as ops:
                assert len(ops) == 3, f"Expected 3 initial operations, got {len(ops)}"

            # Make changes
            insert_or_update(conn, table_name, 1, "updated1")  # Update
            insert_or_update(conn, table_name, 4, "data4")  # Insert
            logical_delete(conn, table_name, 3)  # Delete

            # Generate changeset
            with changeset(conn, table_name) as operations:
                assert len(operations) == 3, f"Expected 3 operations, got {len(operations)}"

                # Check operation types
                inserts = [op for op in operations if isinstance(op, InsertOp)]
                updates = [op for op in operations if isinstance(op, UpdateOp)]
                deletes = [op for op in operations if isinstance(op, DeleteOp)]

                assert len(inserts) == 1, f"Expected 1 insert, got {len(inserts)}"
                assert len(updates) == 1, f"Expected 1 update, got {len(updates)}"
                assert len(deletes) == 1, f"Expected 1 delete, got {len(deletes)}"

                # Verify specific operations
                insert_op = inserts[0]
                assert insert_op.id == 4, f"Expected insert id=4, got {insert_op.id}"
                assert insert_op.data == "data4"

                update_op = updates[0]
                assert update_op.id == 1, f"Expected update id=1, got {update_op.id}"

                delete_op = deletes[0]
                assert delete_op.id == 3, f"Expected delete id=3, got {delete_op.id}"

            print("✓ Changeset generation test passed")

    def test_replication():
        """Test replication between writer and replica using changesets and compaction."""
        # Create writer and replica databases
        with sqlite3_test_db() as writer_conn, sqlite3_test_db() as replica_conn:
            writer_table = setup_three_phase_table(writer_conn, "AppTable")
            replica_table = setup_three_phase_table(replica_conn, "AppTable")

            # === CHECKPOINT 1: Initial data ===
            print("  Checkpoint 1: Initial data")
            initial_data = [
                (1, "data1_v1"),
                (2, "data2_v1"),
                (3, "data3_v1"),
                (4, "data4_v1"),
            ]

            for row_id, data_value in initial_data:
                insert_or_update(writer_conn, writer_table, row_id, data_value)

            # Create checkpoint and replicate
            with changeset(writer_conn, writer_table) as operations:
                for op in operations:
                    apply_changeset_operation(replica_conn, replica_table, op)

            # Verify tables match
            compact(writer_conn, writer_table)
            compact(replica_conn, replica_table)
            writer_hash = compute_table_hash(writer_conn, writer_table)
            replica_hash = compute_table_hash(replica_conn, replica_table)
            assert writer_hash == replica_hash, "Tables don't match after checkpoint 1"

            # === CHECKPOINT 2: Updates and new data ===
            print("  Checkpoint 2: Updates and new data")
            insert_or_update(writer_conn, writer_table, 1, "data1_v2")  # Update
            insert_or_update(writer_conn, writer_table, 2, "data2_v2")  # Update
            insert_or_update(writer_conn, writer_table, 5, "data5_v1")  # Insert
            logical_delete(writer_conn, writer_table, 4)  # Delete

            with changeset(writer_conn, writer_table) as operations:
                for op in operations:
                    apply_changeset_operation(replica_conn, replica_table, op)

            # Verify tables match
            compact(writer_conn, writer_table)
            compact(replica_conn, replica_table)
            writer_hash = compute_table_hash(writer_conn, writer_table)
            replica_hash = compute_table_hash(replica_conn, replica_table)
            assert writer_hash == replica_hash, "Tables don't match after checkpoint 2"

            # === CHECKPOINT 3: Replicate multiple changesets ===
            print("  Checkpoint 3: Complex changes")
            insert_or_update(writer_conn, writer_table, 1, "data1_v3")  # Update again
            insert_or_update(writer_conn, writer_table, 6, "data6_v1")  # Insert
            insert_or_update(writer_conn, writer_table, 7, "data7_v1")  # Insert

            with changeset(writer_conn, writer_table) as operations:
                for op in operations:
                    apply_changeset_operation(replica_conn, replica_table, op)

            logical_delete(writer_conn, writer_table, 3)  # Delete
            insert_or_update(writer_conn, writer_table, 8, "data8_v1")  # Insert
            logical_delete(writer_conn, writer_table, 7)  # Delete what we just inserted

            with changeset(writer_conn, writer_table) as operations:
                for op in operations:
                    apply_changeset_operation(replica_conn, replica_table, op)

            # Final verification
            compact(writer_conn, writer_table)
            compact(replica_conn, replica_table)
            writer_hash = compute_table_hash(writer_conn, writer_table)
            replica_hash = compute_table_hash(replica_conn, replica_table)
            assert writer_hash == replica_hash, "Tables don't match after checkpoint 3"

            # Verify final state manually
            expected_data = {
                1: "data1_v3",  # Updated twice
                2: "data2_v2",  # Updated once
                5: "data5_v1",  # Inserted
                6: "data6_v1",  # Inserted
                8: "data8_v1",  # Inserted
                # 3, 4, 7 were deleted
            }

            for row_id, expected_value in expected_data.items():
                writer_result = read_latest(writer_conn, writer_table, row_id)
                replica_result = read_latest(replica_conn, replica_table, row_id)
                assert writer_result == expected_value, (
                    f"Writer row {row_id}: expected {expected_value}, got {writer_result}"
                )
                assert replica_result == expected_value, (
                    f"Replica row {row_id}: expected {expected_value}, got {replica_result}"
                )

            # Verify deleted rows
            for deleted_id in [3, 4, 7]:
                assert read_latest(writer_conn, writer_table, deleted_id) is None
                assert read_latest(replica_conn, replica_table, deleted_id) is None

            print("✓ Replication test passed")

    def test_phase_isolation():
        """Test that concurrent writes don't interfere with changeset generation."""
        with sqlite3_test_db() as conn:
            table_name = setup_three_phase_table(conn, "AppTable")

            # Insert initial data
            insert_or_update(conn, table_name, 1, "data1")

            # Create baseline changeset
            with changeset(conn, table_name):
                pass

            # Make a change
            insert_or_update(conn, table_name, 1, "changed")

            # Start changeset generation (this transitions phase 0 -> 1)
            with conn:
                conn.execute(f"UPDATE {table_name} SET phase = 1 WHERE phase = 0")

                # Simulate concurrent write (should go to phase 0)
                insert_or_update(conn, table_name, 1, "concurrent change")
                insert_or_update(conn, table_name, 2, "new row")

                # Verify phase isolation
                cursor = conn.execute(
                    f"SELECT phase, data FROM {table_name} WHERE id = 1 ORDER BY phase"
                )
                rows = cursor.fetchall()

                # Should have phase 0 (concurrent) and phase 1 (changeset) versions
                phases = [row[0] for row in rows]
                assert 0 in phases, "Expected phase 0 row from concurrent write"
                assert 1 in phases, "Expected phase 1 row from changeset"

            print("✓ Phase isolation test passed")

    def test_compact():
        """Test table compaction functionality."""
        with sqlite3_test_db() as conn:
            table_name = setup_three_phase_table(conn, "AppTable")

            # Insert initial data
            insert_or_update(conn, table_name, 1, "data1")
            insert_or_update(conn, table_name, 2, "data2")
            insert_or_update(conn, table_name, 3, "data3")

            # Create baseline changeset
            with changeset(conn, table_name):
                pass

            # Make modifications to create multiple phases
            insert_or_update(conn, table_name, 1, "updated1")  # Update
            insert_or_update(conn, table_name, 4, "data4")  # Insert
            logical_delete(conn, table_name, 3)  # Delete

            # Before compact - should have multiple copies of rows
            cursor = conn.execute(f"SELECT COUNT(*) FROM {table_name}")
            before_count = cursor.fetchone()[0]
            assert before_count > 4, f"Expected more than 4 rows before compact, got {before_count}"

            # Compact the table
            compact(conn, table_name)

            # verify the table only contains the latest version of non-deleted rows
            cursor = conn.execute(f"SELECT COUNT(*) FROM {table_name}")
            after_count = cursor.fetchone()[0]
            # Should have ID 1 (updated1), ID 2 (data2), ID 4 (data4) = 3 rows
            # ID 3 is completely removed because its latest version was deleted
            assert after_count == 3, f"Expected 3 rows after compact, got {after_count}"

            # All remaining rows should be phase=2
            cursor = conn.execute(f"SELECT COUNT(*) FROM {table_name} WHERE phase = 2")
            phase2_count = cursor.fetchone()[0]
            assert phase2_count == 3, f"Expected 3 phase=2 rows, got {phase2_count}"

            # Verify data integrity - latest versions are preserved
            assert read_latest(conn, table_name, 1) == "updated1"  # Kept latest version
            assert read_latest(conn, table_name, 2) == "data2"  # Kept (single, non-deleted)
            assert read_latest(conn, table_name, 3) is None  # Removed (latest was deleted)
            assert read_latest(conn, table_name, 4) == "data4"  # Kept (single, non-deleted)

            print("✓ Compact test passed")

    def test_crash_safety():
        """Test that changes are not lost when application crashes during changeset generation."""
        with sqlite3_test_db() as writer_conn, sqlite3_test_db() as replica_conn:
            writer_table = setup_three_phase_table(writer_conn, "AppTable")
            replica_table = setup_three_phase_table(replica_conn, "AppTable")

            # === SETUP: Initial data ===
            print("  Setup: Creating baseline data")
            initial_data = [
                (1, "data1_v1"),
                (2, "data2_v1"),
                (3, "data3_v1"),
                (4, "data4_v1"),
                (5, "data5_v1"),
            ]

            for row_id, data_value in initial_data:
                insert_or_update(writer_conn, writer_table, row_id, data_value)

            # Create baseline changeset and replicate
            with changeset(writer_conn, writer_table) as operations:
                for op in operations:
                    apply_changeset_operation(replica_conn, replica_table, op)

            # Verify baseline state
            compact(writer_conn, writer_table)
            compact(replica_conn, replica_table)
            baseline_hash = compute_table_hash(writer_conn, writer_table)
            replica_baseline_hash = compute_table_hash(replica_conn, replica_table)
            assert baseline_hash == replica_baseline_hash, "Baseline mismatch"

            # === BATCH 1: Make changes before crash ===
            print("  Batch 1: Making changes before simulated crash")
            insert_or_update(writer_conn, writer_table, 1, "data1_v2")  # Update
            insert_or_update(writer_conn, writer_table, 4, "data4_v2")  # Update
            insert_or_update(writer_conn, writer_table, 6, "data6_v1")  # Insert
            logical_delete(writer_conn, writer_table, 2)  # Delete

            # === SIMULATE CRASH: Start changeset generation but crash during yield ===
            print("  Simulating crash during changeset generation")

            try:
                with changeset(writer_conn, writer_table) as operations:
                    # Simulate application crash during processing
                    raise RuntimeError("Simulated application crash")
            except RuntimeError as e:
                if "Simulated application crash" not in str(e):
                    raise
                # Expected crash - continue with test
                pass

            # Verify that phase 1 rows still exist (no cleanup happened due to crash)
            cursor = writer_conn.execute(f"SELECT COUNT(*) FROM {writer_table} WHERE phase = 1")
            phase1_count = cursor.fetchone()[0]
            assert phase1_count == 4, f"Expected 4 phase=1 rows after crash, got {phase1_count}"

            # === BATCH 2: Make more changes after crash ===
            print("  Batch 2: Making additional changes after crash")
            insert_or_update(writer_conn, writer_table, 1, "data1_v3")  # Update
            insert_or_update(writer_conn, writer_table, 3, "data3_v2")  # Update
            insert_or_update(writer_conn, writer_table, 7, "data7_v1")  # Insert
            logical_delete(writer_conn, writer_table, 5)  # Delete
            logical_delete(writer_conn, writer_table, 6)  # Delete

            # === RECOVERY: Generate final changeset (should capture ALL changes) ===
            print("  Recovery: Generating changeset to capture all changes")

            # The next changeset should include:
            # - Any changes from batch 1 that weren't overwritten by batch 2
            # - The new operations from batch 2
            with changeset(writer_conn, writer_table) as recovery_operations:
                # Apply all operations to replica
                for op in recovery_operations:
                    apply_changeset_operation(replica_conn, replica_table, op)

                # Verify we captured all expected changes
                # Should have:
                #   2 surviving operation from batch 1
                # + 5 operations from batch 2
                # = 7 total
                assert len(recovery_operations) == 7, (
                    f"Expected 7 total operations, got {len(recovery_operations)}"
                )

                # Count operation types
                inserts = [op for op in recovery_operations if isinstance(op, InsertOp)]
                updates = [op for op in recovery_operations if isinstance(op, UpdateOp)]
                deletes = [op for op in recovery_operations if isinstance(op, DeleteOp)]

                assert len(inserts) == 1, f"Expected 1 insert, got {len(inserts)}"  # 7
                assert len(updates) == 3, f"Expected 3 updates, got {len(updates)}"  # 1, 3, 4
                assert len(deletes) == 3, f"Expected 3 deletes, got {len(deletes)}"  # 2, 5, 6

            # === VERIFICATION: Final state should match expected ===
            print("  Verification: Checking final state")

            # Verify tables match
            compact(writer_conn, writer_table)
            compact(replica_conn, replica_table)
            final_writer_hash = compute_table_hash(writer_conn, writer_table)
            final_replica_hash = compute_table_hash(replica_conn, replica_table)
            assert final_writer_hash == final_replica_hash, "Final state mismatch"

            # Verify specific data values
            expected_final_state = {
                1: "data1_v3",  # Updated in batch 2
                # 2 was deleted in batch 1
                3: "data3_v2",  # Updated in batch 2
                4: "data4_v2",  # Updated in batch 1
                # 5 was deleted in batch 2
                # 6 was deleted in batch 2
                7: "data7_v1",  # Inserted in batch 2
            }

            for row_id, expected_value in expected_final_state.items():
                writer_result = read_latest(writer_conn, writer_table, row_id)
                replica_result = read_latest(replica_conn, replica_table, row_id)

                assert writer_result == expected_value, (
                    f"Writer row {row_id}: expected {expected_value}, got {writer_result}"
                )
                assert replica_result == expected_value, (
                    f"Replica row {row_id}: expected {expected_value}, got {replica_result}"
                )

            # Verify deleted row
            for rid in [2, 5, 6]:
                # These should be logically deleted
                assert read_latest(writer_conn, writer_table, rid) is None
                assert read_latest(replica_conn, replica_table, rid) is None

            print("✓ Crash safety test passed")

    # Run all tests
    print("Running three-phase CDC pattern tests...")
    test_basic_operations()
    test_changeset_generation()
    test_replication()
    test_phase_isolation()
    test_compact()
    test_crash_safety()
    test_random_workload()
    print("✅ All tests passed!")


if __name__ == "__main__":
    # Run tests first
    test_pattern()
    print("\n" + "=" * 50 + "\n")

    # Then run example
    print("Running example demonstration...")
    run_example()
