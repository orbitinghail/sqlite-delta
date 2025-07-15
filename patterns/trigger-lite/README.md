# Trigger-lite CDC Pattern

The trigger-lite pattern provides automatic Change Data Capture (CDC) for SQLite databases using triggers and a central changes table. It's designed for general-purpose replication and change tracking scenarios.

## How it works

The pattern consists of three main components:

### 1. Changes Table

A central table that tracks all modifications across your application tables:

```sql
CREATE TABLE changes (
    tid INTEGER NOT NULL,  -- table identifier
    rid INTEGER NOT NULL,  -- row identifier (SQLite rowid)
    gsn INTEGER NOT NULL,  -- Global Sequence Number for ordering
    PRIMARY KEY (tid, rid)
) WITHOUT ROWID;
```

See [`setup_changes_table()`](pattern.py) for the implementation.

### 2. Database Triggers

Automatic triggers on each tracked table that record changes to the changes table:

- **INSERT trigger**: Records new rows
- **UPDATE trigger**: Records modified rows
- **DELETE trigger**: Records deleted rows

Each trigger assigns a unique **Global Sequence Number (GSN)** to maintain change ordering across all tables.

See [`setup_triggers()`](pattern.py) for the implementation.

### 3. Changeset Generation

A context manager that atomically:

1. Captures the current GSN
2. Generates changesets by joining application tables with the changes table
3. Returns typed operations (`UpsertOp` for inserts/updates, `DeleteOp` for deletes)
4. Automatically cleans up processed changes

See [`changeset()`](pattern.py) for the implementation.

## Requirements

- **Explicit rowid**: Tables must use either `INTEGER PRIMARY KEY` or an explicit rowid column
- **WAL mode**: Recommended for concurrent access during changeset generation
- **Table IDs**: Use integer identifiers for tables (more efficient than strings)

## Best suited for

- General purpose replication or change tracking
- Scenarios where storing entire row data in changesets is acceptable

## Implementation Details

See [`run_example()`](pattern.py) for a complete working example and the test functions for comprehensive usage patterns.

## Tweaks

- **Primary key tracking**: Instead of SQLite rowid, use table primary keys(requires extending SQLite with a custom function to encode multi-column PKs)
- **String table names**: Use table names instead of IDs in the changes table (less efficient but more readable; if you do this consider removing `WITHOUT ROWID`)
- **Custom cleanup**: Implement custom changeset retention policies instead of immediate cleanup (will require keeping around the GSN to start the next changeset from)
