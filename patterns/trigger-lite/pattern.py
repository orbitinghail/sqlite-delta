"""
Trigger-lite CDC Pattern for SQLite

This module provides a Change Data Capture (CDC) implementation using SQLite triggers.
The pattern tracks changes to application tables using a central changes table and
provides functionality to generate changesets for replication or synchronization.

Key Features:
- Automatic change tracking via triggers
- Global Sequence Number (GSN) for ordering changes
- Changeset generation with automatic cleanup
- Support for tables with explicit rowid columns
- Context manager for atomic changeset operations

Usage:
    # Set up the pattern (autocommit enabled)
    conn = sqlite3.connect(":memory:", autocommit=True)

    setup_changes_table(conn)
    setup_triggers(conn, "users", table_id=1)

    # Generate changeset with automatic cleanup
    with changeset(conn, {"users": 1}) as changeset_data:
        # Process changeset
        for table_name, operations in changeset_data.items():
            for op in operations:
                if isinstance(op, UpsertOp):
                    print(f"Upsert: {op.rowid} -> {op.data}")
                elif isinstance(op, DeleteOp):
                    print(f"Delete: {op.rowid}")
"""

import contextlib
import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Dict, Iterator, List, Union

from jinja2 import Template


@contextmanager
def sqlite3_test_db() -> Iterator[sqlite3.Connection]:
    """Create an in-memory SQLite database with autocommit enabled."""
    with contextlib.closing(sqlite3.connect(":memory:", autocommit=True)) as conn:
        yield conn


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


def setup_changes_table(conn: sqlite3.Connection) -> None:
    """
    Create the changes table for tracking modifications.

    The changes table stores:
    - tid: unique identifier for the application table
    - rid: row identifier of the changed row in the application table
    - gsn: Global Sequence Number for ordering changes
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
    insert_trigger = Template("""
        CREATE TRIGGER IF NOT EXISTS trg_{{ table_name }}_insert
        AFTER INSERT ON {{ table_name }}
        BEGIN
            INSERT INTO changes(tid, rid, gsn)
            VALUES ({{ table_id }}, new.rowid, (SELECT IFNULL(MAX(gsn), 0) + 1 FROM changes))
            ON CONFLICT DO UPDATE SET gsn = excluded.gsn;
        END
    """)
    conn.execute(insert_trigger.render(table_name=table_name, table_id=table_id))

    # Create UPDATE trigger
    update_trigger = Template("""
        CREATE TRIGGER IF NOT EXISTS trg_{{ table_name }}_update
        AFTER UPDATE ON {{ table_name }}
        BEGIN
            INSERT INTO changes(tid, rid, gsn)
            VALUES ({{ table_id }}, new.rowid, (SELECT IFNULL(MAX(gsn), 0) + 1 FROM changes))
            ON CONFLICT DO UPDATE SET gsn = excluded.gsn;
        END
    """)
    conn.execute(update_trigger.render(table_name=table_name, table_id=table_id))

    # Create DELETE trigger
    delete_trigger = Template("""
        CREATE TRIGGER IF NOT EXISTS trg_{{ table_name }}_delete
        AFTER DELETE ON {{ table_name }}
        BEGIN
            INSERT INTO changes(tid, rid, gsn)
            VALUES ({{ table_id }}, old.rowid, (SELECT IFNULL(MAX(gsn), 0) + 1 FROM changes))
            ON CONFLICT DO UPDATE SET gsn = excluded.gsn;
        END
    """)
    conn.execute(delete_trigger.render(table_name=table_name, table_id=table_id))


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

    # Template for changeset query
    # This query joins each application table with the changes table to either
    # acquire the new row version or report the row as deleted.
    changeset_template = Template("""
    SELECT
        IFNULL(base.rowid, changes.rid) AS rowid,
        CASE
            WHEN base.rowid IS NULL THEN 'delete'
            ELSE 'upsert'
        END AS operation,
        base.*
    FROM
        (SELECT * FROM changes WHERE tid = {{ table_id }}) AS changes
        LEFT JOIN {{ table_name }} AS base ON base.rowid = changes.rid
    """)

    changeset_data = {}
    snapshot_gsn = 0

    # Generate changeset in read transaction
    with conn:
        # Snapshot the global sequence number
        cursor = conn.execute("SELECT IFNULL(MAX(gsn), 0) FROM changes")
        snapshot_gsn = cursor.fetchone()[0]

        for table_name, table_id in table_mapping.items():
            sql = changeset_template.render(table_name=table_name, table_id=table_id).strip()

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


def run_example() -> None:
    """
    Demonstrate the trigger-lite CDC pattern with a complete example.
    """
    with sqlite3_test_db() as conn:
        # Set up the changes tracking infrastructure
        setup_changes_table(conn)

        # Create example tables and get table mapping
        table_mapping = create_example_tables(conn)

        # Set up triggers for both tables
        setup_triggers(conn, "AppTable", table_id=table_mapping["AppTable"])
        setup_triggers(
            conn, "AppTableExplicitRowid", table_id=table_mapping["AppTableExplicitRowid"]
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
            setup_changes_table(conn)

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
            setup_changes_table(conn)

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
            setup_changes_table(conn)

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
            setup_changes_table(conn)

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
            conn.execute("INSERT INTO explicit_table (id, data) VALUES (?, ?)", (b"key1", "value1"))
            conn.execute("INSERT INTO explicit_table (id, data) VALUES (?, ?)", (b"key2", "value2"))

            # clear changes to ensure that only the following changes are tracked
            conn.execute("DELETE FROM changes")

            # Update and delete
            conn.execute(
                "UPDATE explicit_table SET data = 'updated_value1' WHERE id = ?", (b"key1",)
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

    # Run all tests
    print("Running trigger-lite CDC pattern tests...")
    test_basic_changeset()
    test_multiple_tables()
    test_changeset_cleanup()
    test_explicit_rowid_table()
    print("✅ All tests passed!")


if __name__ == "__main__":
    # Run tests first
    test_pattern()
    print("\n" + "=" * 50 + "\n")

    # Then run example
    print("Running example demonstration...")
    run_example()
