"""
Trigger-lite CDC Pattern for SQLite

This module provides a Change Data Capture (CDC) implementation using SQLite triggers.
The pattern tracks changes to application tables using a central changes table and
provides functionality to generate changesets for replication or synchronization.

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
from typing import Any, Dict, Iterator, List, Literal, Union

# Add parent directory to path to import testlib
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from testlib import (
    assert_tables_equal,
    compute_table_hash,
    generate_random_workload,
    sqlite3_test_db,
)


@dataclass
class UpsertOp:
    """Represents an upsert operation in a changeset."""

    rowid: int
    data: Dict[str, Any]


@dataclass
class DeleteOp:
    """Represents a delete operation in a changeset."""

    rowid: int


# Type alias for changeset operations
ChangesetOp = Union[UpsertOp, DeleteOp]

# Type alias for a changeset, mapping table names to lists of operations
Changeset = Dict[str, List[ChangesetOp]]


def setup_changes_tables(conn: sqlite3.Connection) -> None:
    """
    Create the changes and changes_gsn tables for tracking modifications.

    The changes table stores:
    - tid: unique identifier for the application table
    - rid: row identifier of the changed row in the application table
    - gsn: Global Sequence Number for ordering changes

    The changes_gsn table stores:
    - max_gsn: The maximum GSN seen so far, incremented by each change
    """
    conn.execute("""
        CREATE TABLE IF NOT EXISTS changes (
            -- tid is a unique identifier for the application table
            tid INTEGER NOT NULL,

            -- rid is the row identifier of the changed row in the application table
            rid INTEGER NOT NULL,

            -- Global Sequence Number (GSN) is a unique number representing when the
            -- change occurred relative to all other changes
            gsn INTEGER NOT NULL,

            PRIMARY KEY (tid, rid)
        ) WITHOUT ROWID
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS changes_gsn (
            -- Global Sequence Number (GSN) is a monotonically increasing number used to sequence changes.
            max_gsn INTEGER NOT NULL
        )
    """)
    conn.execute("INSERT INTO changes_gsn (max_gsn) VALUES (0)")


def setup_triggers(conn: sqlite3.Connection, table_name: str, table_id: int) -> None:
    """
    Set up INSERT, UPDATE, and DELETE triggers for a table.

    Creates three triggers that automatically track changes:
    - INSERT trigger: records new rows
    - UPDATE trigger: records modified rows
    - DELETE trigger: records deleted rows

    All triggers use ON CONFLICT DO UPDATE to ensure the latest GSN
    is recorded for each row.

    For additional performance and storage efficiency, we use application
    defined Table IDs rather than strings.
    """
    # Create INSERT trigger
    insert_trigger_sql = f"""
        CREATE TRIGGER IF NOT EXISTS trg_{table_name}_insert
        AFTER INSERT ON {table_name}
        BEGIN
            UPDATE changes_gsn SET max_gsn = max_gsn + 1;
            INSERT INTO changes(tid, rid, gsn)
            VALUES ({table_id}, new.rowid, (SELECT max_gsn FROM changes_gsn))
            ON CONFLICT DO UPDATE SET gsn = excluded.gsn;
        END
    """
    conn.execute(insert_trigger_sql)

    # Create UPDATE trigger
    update_trigger_sql = f"""
        CREATE TRIGGER IF NOT EXISTS trg_{table_name}_update
        AFTER UPDATE ON {table_name}
        BEGIN
            UPDATE changes_gsn SET max_gsn = max_gsn + 1;
            INSERT INTO changes(tid, rid, gsn)
            VALUES ({table_id}, new.rowid, (SELECT max_gsn FROM changes_gsn))
            ON CONFLICT DO UPDATE SET gsn = excluded.gsn;
        END
    """
    conn.execute(update_trigger_sql)

    # Create DELETE trigger
    delete_trigger_sql = f"""
        CREATE TRIGGER IF NOT EXISTS trg_{table_name}_delete
        AFTER DELETE ON {table_name}
        BEGIN
            UPDATE changes_gsn SET max_gsn = max_gsn + 1;
            INSERT INTO changes(tid, rid, gsn)
            VALUES ({table_id}, old.rowid, (SELECT max_gsn FROM changes_gsn))
            ON CONFLICT DO UPDATE SET gsn = excluded.gsn;
        END
    """
    conn.execute(delete_trigger_sql)


@contextmanager
def changeset(conn: sqlite3.Connection, table_mapping: Dict[str, int]) -> Iterator[Changeset]:
    """
    Context manager for atomic changeset generation and cleanup.

    This context manager:
    1. Creates a changeset and captures the latest GSN in a read transaction
    2. Yields the changeset for processing or saving
    3. Automatically cleans up processed changes

    Yields:
        Changeset containing UpsertOp and DeleteOp operations for each table.
    """

    # Query template for changeset generation
    # This query joins each application table with the changes table to either
    # acquire the new row version or report the row as deleted.

    changeset_data = {}
    snapshot_gsn = 0

    # Generate changeset in read transaction
    with conn:
        # Snapshot the global sequence number
        cursor = conn.execute("SELECT max_gsn FROM changes_gsn")
        snapshot_gsn = cursor.fetchone()[0]

        for table_name, table_id in table_mapping.items():
            sql = f"""
                SELECT
                    IFNULL(base.rowid, changes.rid) AS rowid,
                    CASE
                        WHEN base.rowid IS NULL THEN 'delete'
                        ELSE 'upsert'
                    END AS operation,
                    base.*
                FROM
                    (SELECT * FROM changes WHERE tid = {table_id}) AS changes
                    LEFT JOIN {table_name} AS base ON base.rowid = changes.rid
            """.strip()

            cursor = conn.execute(sql)
            columns = [description[0] for description in cursor.description]
            rows = cursor.fetchall()

            operations = []
            for row in rows:
                row_dict = dict(zip(columns, row))
                rowid = row_dict.pop("rowid")
                operation = row_dict.pop("operation")

                if operation == "delete":
                    operations.append(DeleteOp(rowid=rowid))
                else:
                    operations.append(UpsertOp(rowid=rowid, data=row_dict))

            changeset_data[table_name] = operations

    try:
        yield changeset_data
    except:
        # If we crash while yielding the changeset do nothing - we don't want to
        # lose changes
        raise
    else:
        # Clean up processed changes if the yield was successful
        conn.execute("DELETE FROM changes WHERE gsn <= ?", (snapshot_gsn,))


def apply_changeset_operation(
    conn: sqlite3.Connection, table_name: str, operation: ChangesetOp
) -> None:
    """
    Apply a single changeset operation to a table.

    This function is used for replication scenarios where changesets are applied
    to a replica database.

    Args:
        conn: SQLite database connection
        table_name: Name of the table
        operation: The operation to apply
    """
    if isinstance(operation, UpsertOp):
        # Build column list and values dynamically based on operation data
        columns = list(operation.data.keys())
        placeholders = ", ".join(["?"] * len(columns))
        column_names = ", ".join(columns)
        values = [operation.data[col] for col in columns]

        sql = f"""
            INSERT OR REPLACE INTO {table_name} (rowid, {column_names})
            VALUES (?, {placeholders})
        """
        conn.execute(sql, [operation.rowid] + values)
    elif isinstance(operation, DeleteOp):
        sql = f"DELETE FROM {table_name} WHERE rowid = ?"
        conn.execute(sql, (operation.rowid,))


def apply_changeset(conn: sqlite3.Connection, changeset_data: Changeset) -> None:
    """
    Apply a complete changeset to a database connection.

    This function wraps the changeset application in a transaction and temporarily
    disables foreign key constraint validation to allow operations to be applied
    in any order. This is necessary because changesets may contain operations that
    would violate foreign key constraints if applied immediately (e.g., deleting
    a parent row before its child rows, or inserting child rows before parent rows).

    Args:
        conn: SQLite database connection
        changeset_data: Complete changeset containing operations for all tables
    """
    with conn:
        conn.execute("PRAGMA defer_foreign_keys = ON")
        for table_name, operations in changeset_data.items():
            for operation in operations:
                apply_changeset_operation(conn, table_name, operation)


def create_example_tables(conn: sqlite3.Connection) -> Dict[str, int]:
    """
    Create example application tables for demonstration.

    Application tables must have an explicit rowid. To do this you have two choices:
    1. Use an INTEGER PRIMARY KEY column, which will be the rowid.
    2. Use a separate column for the rowid, which must be unique and indexed.
    This is demonstrated in the `AppTable` and `AppTableExplicitRowid` tables below.

    We require an explicit rowid to ensure that the rowid's used are stable. If
    implicit rowids are used, they may be changed by the SQLite vacuum operation.

    Returns:
        Dictionary mapping table names to their table IDs for changeset generation
    """
    # Standard table with INTEGER PRIMARY KEY
    conn.execute("""
        CREATE TABLE IF NOT EXISTS AppTable(
            id INTEGER PRIMARY KEY,
            t TEXT
        )
    """)

    # Table with explicit rowid column
    conn.execute("""
        CREATE TABLE IF NOT EXISTS AppTableExplicitRowid(
            id BLOB UNIQUE,
            t TEXT,

            -- Explicit rowid column
            rid INTEGER PRIMARY KEY
        )
    """)

    # Return table mapping for changeset operations
    return {"AppTable": 1, "AppTableExplicitRowid": 2}


def setup_workload_table(conn: sqlite3.Connection, table_name: str) -> str:
    """
    Create a simple table for workload testing.

    This creates a simple table with:
    - id INTEGER: primary key
    - data TEXT: single data column

    Args:
        conn: SQLite database connection
        table_name: Name of the table to create

    Returns:
        The name of the created table.
    """
    sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id INTEGER PRIMARY KEY,
            data TEXT
        )
    """
    conn.execute(sql)
    return table_name


def apply_operation_to_workload_table(
    conn: sqlite3.Connection,
    table_name: str,
    operation: Literal["upsert", "delete"],
    rowid: int,
    data: str = "",
) -> None:
    """
    Apply an operation to a workload table.

    Args:
        conn: SQLite database connection
        table_name: Name of the table
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
    duration_seconds: float = 2.0,
    max_id: int = 100,
    replication_probability: float = 0.1,
) -> None:
    """
    Test the trigger-lite pattern with a random workload.

    This test:
    1. Applies a random workload stream to a trigger-lite table
    2. Applies the same stream to a regular table (without trigger-lite tracking)
    3. Replicates the trigger-lite table to a second database using changesets
    4. Sends changesets to the replica with some probability after each operation
    5. Runs for a configurable duration
    6. Compares the hash of all three tables

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
        # Set up changes table for writer and replica
        setup_changes_tables(writer_conn)
        setup_changes_tables(replica_conn)

        # Set up tables
        writer_table = setup_workload_table(writer_conn, "WorkloadTable")
        setup_triggers(writer_conn, writer_table, table_id=1)
        replica_table = setup_workload_table(replica_conn, "WorkloadTable")
        setup_triggers(replica_conn, replica_table, table_id=1)
        regular_table = setup_workload_table(regular_conn, "WorkloadTable")

        # Table mapping for changesets
        table_mapping = {"WorkloadTable": 1}

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

            # Apply to trigger-lite table
            apply_operation_to_workload_table(
                writer_conn, writer_table, operation_type, row_id, data
            )

            # Apply operation to regular table
            apply_operation_to_workload_table(
                regular_conn, regular_table, operation_type, row_id, data
            )

            # Randomly decide whether to replicate
            if random.random() < replication_probability:
                changeset_count += 1
                # Generate changeset and apply to replica
                with changeset(writer_conn, table_mapping) as changeset_ops:
                    for op in changeset_ops["WorkloadTable"]:
                        apply_changeset_operation(replica_conn, replica_table, op)

        # Final replication to ensure replica is up to date
        with changeset(writer_conn, table_mapping) as final_changeset:
            changeset_count += 1
            for op in final_changeset["WorkloadTable"]:
                apply_changeset_operation(replica_conn, replica_table, op)

        print(f"  Applied {operation_count} operations in {time.time() - start_time:.2f} seconds")
        print(f"  Applied {changeset_count} changesets to replica")

        # Verify all three tables are equal
        assert_tables_equal(
            "writer and replica tables mismatch",
            writer_conn,
            writer_table,
            replica_conn,
            replica_table,
        )
        assert_tables_equal(
            "writer and regular tables mismatch",
            writer_conn,
            writer_table,
            regular_conn,
            regular_table,
        )

        print("  ✓ All table hashes match! Random workload test passed.")


def run_example() -> None:
    """
    Demonstrate the trigger-lite CDC pattern with a complete example.
    """
    with sqlite3_test_db() as conn:
        # Set up the changes tracking infrastructure
        setup_changes_tables(conn)

        # Create example tables and get table mapping
        table_mapping = create_example_tables(conn)

        # Set up triggers for both tables
        setup_triggers(conn, "AppTable", table_id=table_mapping["AppTable"])
        setup_triggers(
            conn,
            "AppTableExplicitRowid",
            table_id=table_mapping["AppTableExplicitRowid"],
        )

        # Insert some initial data
        conn.executemany(
            "INSERT INTO AppTable (id, t) VALUES (?, ?)",
            [
                (1, "data 1; revision 1"),
                (2, "data 2; revision 1"),
                (3, "data 3; revision 1"),
                (4, "data 4; revision 1"),
                (5, "data 5; revision 1"),
            ],
        )

        conn.executemany(
            "INSERT INTO AppTableExplicitRowid (id, t) VALUES (?, ?)",
            [
                ("id1", "data 1; revision 1"),
                ("id2", "data 2; revision 1"),
                ("id3", "data 3; revision 1"),
            ],
        )

        # Clear changes table to simulate a prior changeset was already taken
        # after the initial state
        conn.execute("DELETE FROM changes")

        # Make some updates
        conn.execute("UPDATE AppTable SET t = 'data 1; revision 2' WHERE id = 1")
        conn.execute("UPDATE AppTableExplicitRowid SET t = 'data 1; revision 2' WHERE id = 'id1'")

        # Delete a row
        conn.execute("DELETE FROM AppTable WHERE id = 5")

        # Get changes count
        cursor = conn.execute("SELECT COUNT(*) FROM changes")
        changes_count = cursor.fetchone()[0]
        print(f"Changes pending: {changes_count}")

        # Create changeset with automatic cleanup
        with changeset(conn, table_mapping) as changeset_data:
            # Display changeset
            for table_name, operations in changeset_data.items():
                print(f"\nChanges for {table_name}:")
                for op in operations:
                    if isinstance(op, UpsertOp):
                        print(f"  Upsert rowid={op.rowid}: {op.data}")
                    elif isinstance(op, DeleteOp):
                        print(f"  Delete rowid={op.rowid}")

        # Check remaining changes
        cursor = conn.execute("SELECT COUNT(*) FROM changes")
        remaining_changes = cursor.fetchone()[0]
        print(f"\nRemaining changes after cleanup: {remaining_changes}")


def test_pattern():
    """
    Test suite focused on changeset correctness.
    """

    def test_basic_changeset():
        """Test basic changeset generation with mixed operations."""
        with sqlite3_test_db() as conn:
            setup_changes_tables(conn)

            # Create test table and define table mapping
            conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)")
            table_mapping = {"users": 1}
            setup_triggers(conn, "users", table_id=table_mapping["users"])

            # Insert test data
            conn.execute(
                "INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@example.com')"
            )
            conn.execute("INSERT INTO users (id, name, email) VALUES (2, 'Bob', 'bob@example.com')")
            conn.execute(
                "INSERT INTO users (id, name, email) VALUES (3, 'Charlie', 'charlie@example.com')"
            )

            # clear changes to ensure that only the following changes are tracked
            conn.execute("DELETE FROM changes")

            # Make changes
            conn.execute("UPDATE users SET email = 'alice.updated@example.com' WHERE id = 1")
            conn.execute("DELETE FROM users WHERE id = 3")
            conn.execute(
                "INSERT INTO users (id, name, email) VALUES (4, 'David', 'david@example.com')"
            )

            # Generate changeset
            with changeset(conn, table_mapping) as changeset_data:
                operations = changeset_data["users"]

                # Should have operations for all modified rows
                assert len(operations) == 3, f"Expected 3 operations, got {len(operations)}"

                # Check operation types
                upserts = [op for op in operations if isinstance(op, UpsertOp)]
                deletes = [op for op in operations if isinstance(op, DeleteOp)]

                assert len(upserts) == 2, f"Expected 2 upserts, got {len(upserts)}"
                assert len(deletes) == 1, f"Expected 1 delete, got {len(deletes)}"

                # Verify delete operation
                delete_op = deletes[0]
                assert delete_op.rowid == 3, f"Expected delete rowid=3, got {delete_op.rowid}"

                # Verify upsert data includes all columns
                alice_op = next(op for op in upserts if op.data.get("id") == 1)
                assert alice_op.data["email"] == "alice.updated@example.com"

            print("✓ Basic changeset test passed")

    def test_multiple_tables():
        """Test changeset generation across multiple tables."""
        with sqlite3_test_db() as conn:
            setup_changes_tables(conn)

            # Create multiple tables and define table mapping
            conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
            conn.execute("CREATE TABLE posts (id INTEGER PRIMARY KEY, title TEXT, user_id INTEGER)")
            table_mapping = {"users": 1, "posts": 2}

            setup_triggers(conn, "users", table_id=table_mapping["users"])
            setup_triggers(conn, "posts", table_id=table_mapping["posts"])

            # Insert data
            conn.execute("INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob')")
            conn.execute(
                "INSERT INTO posts (id, title, user_id) VALUES (1, 'Post 1', 1), (2, 'Post 2', 2)"
            )

            # clear changes to ensure that only the following changes are tracked
            conn.execute("DELETE FROM changes")

            # Make changes to both tables
            conn.execute("UPDATE users SET name = 'Alice Updated' WHERE id = 1")
            conn.execute("DELETE FROM posts WHERE id = 2")

            # Generate changeset
            with changeset(conn, table_mapping) as changeset_data:
                assert "users" in changeset_data, "Users table missing from changeset"
                assert "posts" in changeset_data, "Posts table missing from changeset"

                # Check users changes
                users_ops = changeset_data["users"]
                assert len(users_ops) == 1, f"Expected 1 user operation, got {len(users_ops)}"

                # Check posts changes
                posts_ops = changeset_data["posts"]
                assert len(posts_ops) == 1, f"Expected 1 post operation, got {len(posts_ops)}"

                # Verify we have a delete operation for posts
                post_deletes = [op for op in posts_ops if isinstance(op, DeleteOp)]
                assert len(post_deletes) == 1, "Expected 1 post delete operation"

            print("✓ Multiple tables test passed")

    def test_changeset_cleanup():
        """Test that changeset context manager cleans up processed changes."""
        with sqlite3_test_db() as conn:
            setup_changes_tables(conn)

            # Create test table and define table mapping
            conn.execute("CREATE TABLE test_table (id INTEGER PRIMARY KEY, data TEXT)")
            table_mapping = {"test_table": 1}
            setup_triggers(conn, "test_table", table_id=table_mapping["test_table"])

            # Create initial changes
            conn.execute("INSERT INTO test_table (id, data) VALUES (1, 'data1')")
            conn.execute("INSERT INTO test_table (id, data) VALUES (2, 'data2')")

            # Verify changes exist
            cursor = conn.execute("SELECT COUNT(*) FROM changes")
            initial_count = cursor.fetchone()[0]
            assert initial_count == 2, f"Expected 2 initial changes, got {initial_count}"

            # Generate changeset (should clean up)
            with changeset(conn, table_mapping) as changeset_data:
                assert len(changeset_data["test_table"]) == 2, "Expected 2 operations in changeset"

            # Verify changes were cleaned up
            cursor = conn.execute("SELECT COUNT(*) FROM changes")
            remaining_count = cursor.fetchone()[0]
            assert remaining_count == 0, f"Expected 0 remaining changes, got {remaining_count}"

            # Add new changes after cleanup
            conn.execute("INSERT INTO test_table (id, data) VALUES (3, 'data3')")
            conn.execute("DELETE FROM test_table WHERE id = 1")

            # Verify new changes are tracked
            cursor = conn.execute("SELECT COUNT(*) FROM changes")
            new_count = cursor.fetchone()[0]
            assert new_count == 2, f"Expected 2 new changes, got {new_count}"

            print("✓ Changeset cleanup test passed")

    def test_explicit_rowid_table():
        """Test changeset generation with explicit rowid tables."""
        with sqlite3_test_db() as conn:
            setup_changes_tables(conn)

            # Create table with explicit rowid and define table mapping
            conn.execute("""
                CREATE TABLE explicit_table (
                    id BLOB UNIQUE,
                    data TEXT,
                    rid INTEGER PRIMARY KEY
                )
            """)
            table_mapping = {"explicit_table": 1}

            setup_triggers(conn, "explicit_table", table_id=table_mapping["explicit_table"])

            # Insert data
            conn.execute(
                "INSERT INTO explicit_table (id, data) VALUES (?, ?)",
                (b"key1", "value1"),
            )
            conn.execute(
                "INSERT INTO explicit_table (id, data) VALUES (?, ?)",
                (b"key2", "value2"),
            )

            # clear changes to ensure that only the following changes are tracked
            conn.execute("DELETE FROM changes")

            # Update and delete
            conn.execute(
                "UPDATE explicit_table SET data = 'updated_value1' WHERE id = ?",
                (b"key1",),
            )
            conn.execute("DELETE FROM explicit_table WHERE id = ?", (b"key2",))

            # Generate changeset
            with changeset(conn, table_mapping) as changeset_data:
                operations = changeset_data["explicit_table"]
                assert len(operations) == 2, f"Expected 2 operations, got {len(operations)}"

                # Check for upsert and delete
                upserts = [op for op in operations if isinstance(op, UpsertOp)]
                deletes = [op for op in operations if isinstance(op, DeleteOp)]

                assert len(upserts) == 1, f"Expected 1 upsert, got {len(upserts)}"
                assert len(deletes) == 1, f"Expected 1 delete, got {len(deletes)}"

                # Verify upsert data
                upsert_op = upserts[0]
                assert upsert_op.data["data"] == "updated_value1"

            print("✓ Explicit rowid table test passed")

    def test_replication():
        """Test replication between writer and replica using changesets."""
        # Create writer and replica databases
        with sqlite3_test_db() as writer_conn, sqlite3_test_db() as replica_conn:
            # Setup both databases with identical schema
            setup_changes_tables(writer_conn)
            setup_changes_tables(replica_conn)

            writer_conn.execute("CREATE TABLE AppTable (id INTEGER PRIMARY KEY, data TEXT)")
            replica_conn.execute("CREATE TABLE AppTable (id INTEGER PRIMARY KEY, data TEXT)")

            table_mapping = {"AppTable": 1}
            setup_triggers(writer_conn, "AppTable", table_id=table_mapping["AppTable"])
            setup_triggers(replica_conn, "AppTable", table_id=table_mapping["AppTable"])

            # === CHECKPOINT 1: Initial data ===
            print("  Checkpoint 1: Initial data")
            writer_conn.executemany(
                "INSERT INTO AppTable (id, data) VALUES (?, ?)",
                [
                    (1, "data1_v1"),
                    (2, "data2_v1"),
                    (3, "data3_v1"),
                    (4, "data4_v1"),
                ],
            )

            # Generate changeset and apply to replica
            with changeset(writer_conn, table_mapping) as changeset_data:
                apply_changeset(replica_conn, changeset_data)

            # Verify tables match using hash comparison
            writer_hash = compute_table_hash(writer_conn, "AppTable")
            replica_hash = compute_table_hash(replica_conn, "AppTable")
            assert writer_hash == replica_hash, "Tables don't match after checkpoint 1"

            # === CHECKPOINT 2: Updates and new data ===
            print("  Checkpoint 2: Updates and new data")
            writer_conn.execute("UPDATE AppTable SET data = 'data1_v2' WHERE id = 1")  # Update
            writer_conn.execute("UPDATE AppTable SET data = 'data2_v2' WHERE id = 2")  # Update
            writer_conn.execute("INSERT INTO AppTable (id, data) VALUES (5, 'data5_v1')")  # Insert
            writer_conn.execute("DELETE FROM AppTable WHERE id = 4")  # Delete

            with changeset(writer_conn, table_mapping) as changeset_data:
                apply_changeset(replica_conn, changeset_data)

            # Verify tables match using hash comparison
            writer_hash = compute_table_hash(writer_conn, "AppTable")
            replica_hash = compute_table_hash(replica_conn, "AppTable")
            assert writer_hash == replica_hash, "Tables don't match after checkpoint 2"

            # === CHECKPOINT 3: Complex changes ===
            print("  Checkpoint 3: Complex changes")
            writer_conn.execute("UPDATE AppTable SET data = 'data1_v3' WHERE id = 1")
            writer_conn.executemany(
                "INSERT INTO AppTable (id, data) VALUES (?, ?)",
                [
                    (6, "data6_v1"),
                    (7, "data7_v1"),
                ],
            )

            with changeset(writer_conn, table_mapping) as changeset_data:
                apply_changeset(replica_conn, changeset_data)

            writer_conn.execute("DELETE FROM AppTable WHERE id = 3")  # Delete
            writer_conn.execute("INSERT INTO AppTable (id, data) VALUES (8, 'data8_v1')")  # Insert
            writer_conn.execute("DELETE FROM AppTable WHERE id = 7")  # Delete what we just inserted

            with changeset(writer_conn, table_mapping) as changeset_data:
                apply_changeset(replica_conn, changeset_data)

            # Final verification using hash comparison
            writer_hash = compute_table_hash(writer_conn, "AppTable")
            replica_hash = compute_table_hash(replica_conn, "AppTable")
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
                writer_result = writer_conn.execute(
                    "SELECT data FROM AppTable WHERE id = ?", (row_id,)
                ).fetchone()
                replica_result = replica_conn.execute(
                    "SELECT data FROM AppTable WHERE id = ?", (row_id,)
                ).fetchone()

                writer_value = writer_result[0] if writer_result else None
                replica_value = replica_result[0] if replica_result else None

                assert writer_value == expected_value, (
                    f"Writer row {row_id}: expected {expected_value}, got {writer_value}"
                )
                assert replica_value == expected_value, (
                    f"Replica row {row_id}: expected {expected_value}, got {replica_value}"
                )

            # Verify deleted rows
            for deleted_id in [3, 4, 7]:
                writer_result = writer_conn.execute(
                    "SELECT data FROM AppTable WHERE id = ?", (deleted_id,)
                ).fetchone()
                replica_result = replica_conn.execute(
                    "SELECT data FROM AppTable WHERE id = ?", (deleted_id,)
                ).fetchone()
                assert writer_result is None, f"Writer should not have deleted row {deleted_id}"
                assert replica_result is None, f"Replica should not have deleted row {deleted_id}"

            print("✓ Replication test passed")

    def test_foreign_key_cascade():
        """Test replication with foreign key CASCADE DELETE."""
        # Create writer and replica databases
        with sqlite3_test_db() as writer_conn, sqlite3_test_db() as replica_conn:
            # Enable foreign key constraints
            writer_conn.execute("PRAGMA foreign_keys = ON")
            replica_conn.execute("PRAGMA foreign_keys = ON")

            # Setup both databases with identical schema
            setup_changes_tables(writer_conn)
            setup_changes_tables(replica_conn)

            # Create parent and child tables with foreign key relationship
            schema_queries = [
                """
                CREATE TABLE authors (
                    id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL
                )
                """,
                """
                CREATE TABLE articles (
                    id INTEGER PRIMARY KEY,
                    title TEXT NOT NULL,
                    author_id INTEGER NOT NULL,
                    FOREIGN KEY (author_id) REFERENCES authors(id) ON DELETE CASCADE
                )
                """,
            ]

            for conn in [writer_conn, replica_conn]:
                for query in schema_queries:
                    conn.execute(query)

            # Define table mapping
            table_mapping = {"authors": 1, "articles": 2}

            # Setup triggers on both databases
            setup_triggers(writer_conn, "authors", table_id=table_mapping["authors"])
            setup_triggers(writer_conn, "articles", table_id=table_mapping["articles"])
            setup_triggers(replica_conn, "authors", table_id=table_mapping["authors"])
            setup_triggers(replica_conn, "articles", table_id=table_mapping["articles"])

            # === CHECKPOINT 1: Initial data ===
            print("  Checkpoint 1: Initial data with foreign key relationships")

            # Insert authors
            writer_conn.executemany(
                "INSERT INTO authors (id, name) VALUES (?, ?)",
                [(1, "Alice"), (2, "Bob"), (3, "Charlie")],
            )

            # Insert articles
            writer_conn.executemany(
                "INSERT INTO articles (id, title, author_id) VALUES (?, ?, ?)",
                [
                    (1, "Article 1 by Alice", 1),
                    (2, "Article 2 by Alice", 1),
                    (3, "Article 3 by Bob", 2),
                    (4, "Article 4 by Charlie", 3),
                    (5, "Article 5 by Charlie", 3),
                ],
            )

            # Generate changeset and apply to replica
            with changeset(writer_conn, table_mapping) as changeset_data:
                apply_changeset(replica_conn, changeset_data)

            # Verify tables match
            for table_name in ["authors", "articles"]:
                writer_hash = compute_table_hash(writer_conn, table_name)
                replica_hash = compute_table_hash(replica_conn, table_name)
                assert writer_hash == replica_hash, (
                    f"Tables {table_name} don't match after checkpoint 1"
                )

            # === CHECKPOINT 2: CASCADE DELETE test ===
            print("  Checkpoint 2: Testing cascade delete")

            # Delete author with id=1, which should cascade delete articles 1 and 2
            writer_conn.execute("DELETE FROM authors WHERE id = 1")

            # Verify cascade worked on writer
            remaining_articles = writer_conn.execute(
                "SELECT COUNT(*) FROM articles WHERE author_id = 1"
            ).fetchone()[0]
            assert remaining_articles == 0, "Cascade delete did not work on writer"

            # Apply changeset to replica with deferred foreign key constraints
            with changeset(writer_conn, table_mapping) as changeset_data:
                apply_changeset(replica_conn, changeset_data)

            # Verify tables match after cascade delete
            for table_name in ["authors", "articles"]:
                writer_hash = compute_table_hash(writer_conn, table_name)
                replica_hash = compute_table_hash(replica_conn, table_name)
                assert writer_hash == replica_hash, (
                    f"Tables {table_name} don't match after cascade delete"
                )

            # === CHECKPOINT 3: Mixed operations with more cascade deletes ===
            print("  Checkpoint 3: Mixed operations with multiple cascade deletes")

            # Add new author and articles
            writer_conn.execute("INSERT INTO authors (id, name) VALUES (4, 'David')")
            writer_conn.executemany(
                "INSERT INTO articles (id, title, author_id) VALUES (?, ?, ?)",
                [
                    (6, "Article 6 by David", 4),
                    (7, "Article 7 by David", 4),
                    (8, "Article 8 by Bob", 2),
                ],
            )

            # Update existing article
            writer_conn.execute("UPDATE articles SET title = 'Updated Article 3' WHERE id = 3")

            # Delete another author (Bob), which should cascade delete articles 3 and 8
            writer_conn.execute("DELETE FROM authors WHERE id = 2")

            # Apply changeset to replica with deferred foreign key constraints
            with changeset(writer_conn, table_mapping) as changeset_data:
                apply_changeset(replica_conn, changeset_data)

            # Final verification
            for table_name in ["authors", "articles"]:
                writer_hash = compute_table_hash(writer_conn, table_name)
                replica_hash = compute_table_hash(replica_conn, table_name)
                assert writer_hash == replica_hash, (
                    f"Tables {table_name} don't match after final checkpoint"
                )

            # Verify final state manually
            final_authors = writer_conn.execute(
                "SELECT id, name FROM authors ORDER BY id"
            ).fetchall()
            expected_authors = [(3, "Charlie"), (4, "David")]
            assert final_authors == expected_authors, (
                f"Expected {expected_authors}, got {final_authors}"
            )

            final_articles = writer_conn.execute(
                "SELECT id, title, author_id FROM articles ORDER BY id"
            ).fetchall()
            expected_articles = [
                (4, "Article 4 by Charlie", 3),
                (5, "Article 5 by Charlie", 3),
                (6, "Article 6 by David", 4),
                (7, "Article 7 by David", 4),
            ]
            assert final_articles == expected_articles, (
                f"Expected {expected_articles}, got {final_articles}"
            )

            # Verify replica matches
            replica_authors = replica_conn.execute(
                "SELECT id, name FROM authors ORDER BY id"
            ).fetchall()
            replica_articles = replica_conn.execute(
                "SELECT id, title, author_id FROM articles ORDER BY id"
            ).fetchall()
            assert replica_authors == expected_authors, (
                f"Replica authors don't match: {replica_authors}"
            )
            assert replica_articles == expected_articles, (
                f"Replica articles don't match: {replica_articles}"
            )

            print("✓ Foreign key cascade test passed")

    # Run all tests
    print("Running trigger-lite CDC pattern tests...")
    test_basic_changeset()
    test_multiple_tables()
    test_changeset_cleanup()
    test_explicit_rowid_table()
    test_replication()
    test_foreign_key_cascade()
    test_random_workload()
    print("✅ All tests passed!")


if __name__ == "__main__":
    # Run tests first
    test_pattern()
    print("\n" + "=" * 50 + "\n")

    # Then run example
    print("Running example demonstration...")
    run_example()
