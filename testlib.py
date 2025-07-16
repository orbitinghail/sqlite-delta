"""
Shared test library for SQLite Delta patterns.

This module provides common testing utilities and functions shared across
different CDC patterns to avoid code duplication.
"""

import contextlib
import hashlib
import random
import sqlite3
import struct
from contextlib import contextmanager
from typing import Iterator


@contextmanager
def sqlite3_test_db() -> Iterator[sqlite3.Connection]:
    """Create an in-memory SQLite database with autocommit enabled."""
    with contextlib.closing(sqlite3.connect(":memory:", autocommit=True)) as conn:
        yield conn


def compute_hash_from_rows(rows: Iterator[tuple], debug: bool = False) -> str:
    """
    Compute a SHA256 hash from an iterator of row tuples.

    Hashes each cell with an explicit type prefix to ensure deterministic output.

    Args:
        rows: Iterator of row tuples
        debug: Whether to print debug information

    Returns:
        Hex string of the SHA256 hash
    """
    hasher = hashlib.sha256()

    for row in rows:
        if debug:
            print(f"Row: {row}")

        for cell in row:
            if cell is None:
                hasher.update(b"n")
            elif isinstance(cell, int):
                hasher.update(b"i")
                hasher.update(struct.pack("<q", cell))
            elif isinstance(cell, float):
                hasher.update(b"f")
                hasher.update(struct.pack("<d", cell))
            elif isinstance(cell, str):
                encoded = cell.encode("utf-8")
                hasher.update(b"s")
                hasher.update(struct.pack("<I", len(encoded)))
                hasher.update(encoded)
            elif isinstance(cell, bytes):
                hasher.update(b"b")
                hasher.update(struct.pack("<I", len(cell)))
                hasher.update(cell)
            else:
                raise TypeError(f"Unsupported type in table: {type(cell)}")

    return hasher.hexdigest()


def compute_table_hash(conn: sqlite3.Connection, table_name: str, debug: bool = False) -> str:
    """
    Compute a SHA256 hash of a table's contents for verification.

    Scans the table in default row order (typically primary key order), and
    hashes each cell with an explicit type prefix to ensure deterministic output.

    Args:
        conn: SQLite database connection
        table_name: Name of the table to hash
        debug: Whether to print debug information

    Returns:
        Hex string of the SHA256 hash
    """
    # Get ordered list of column names
    schema_cursor = conn.execute(f"PRAGMA table_info({table_name})")
    pk_columns = []
    for col in schema_cursor.fetchall():
        _, name, _, _, _, pk = col
        # pk is the position in primary key (1-based), 0 means not part of PK
        if pk > 0:
            pk_columns.append((pk, name))

    # Sort PK columns by their position in the primary key
    pk_columns.sort(key=lambda x: x[0])
    order_by = ", ".join(name for _, name in pk_columns) if pk_columns else "ROWID"

    # Rely on SQLite's default row order (by PK or ROWID)
    cursor = conn.execute(f"SELECT * FROM {table_name} ORDER BY {order_by}")

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
        tuple: (operation_type, row_id, data) where operation_type is "upsert" or "delete"
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
