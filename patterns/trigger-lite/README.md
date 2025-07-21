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

See `setup_changes_table()` in [pattern.py] for the implementation.

### 2. Database Triggers

Automatic triggers on each tracked table that record changes to the changes table:

- **INSERT trigger**: Records new rows
- **UPDATE trigger**: Records modified rows
- **DELETE trigger**: Records deleted rows

Each trigger assigns a unique **Global Sequence Number (GSN)** to maintain change ordering across all tables.

See `setup_triggers()` in [pattern.py] for the implementation.

### 3. Changeset Generation

A context manager that atomically:

1. Captures the current GSN
2. Generates changesets by joining application tables with the changes table
3. Returns typed operations (`UpsertOp` for inserts/updates, `DeleteOp` for deletes)
4. Automatically cleans up processed changes

See `changeset()` in [pattern.py] for the implementation.

### 4. Checkpoint Generation

Periodically a Checkpoint should be created to allow the history of changesets to restart. This ensures that the history doesn't get too long, as well as serving as a backup should something go wrong.

Before Checkpointing the database, you must truncate the `changes` table. This effectively resets the change history allowing it to start from scratch.

After running compaction and generating a Checkpoint, the database history is fully reset. The resulting Checkpoint needs to be sent to any replicas in entirely before replicating changes can resume.

## Requirements

- **Explicit rowid**: Tables must use either `INTEGER PRIMARY KEY` or an explicit rowid column
- **WAL mode**: Recommended for concurrent access during changeset generation
- **Table IDs**: Use integer identifiers for tables (more efficient than strings)

## Why Explicit Row IDs?

This pattern uses explicit row IDs (`rid`) for change tracking rather than table primary keys. This design choice offers several advantages:

### Compact Changes Table

The changes table uses only three integers per tracked change:

- `tid` (table identifier)
- `rid` (row identifier)
- `gsn` (global sequence number)

This keeps the changes table extremely small and efficient, regardless of the underlying table's primary key structure (single column, composite, string-based, etc.).

### Primary Key Agnostic

By using SQLite's internal row ID, the pattern works identically across different primary key designs:

- Simple integer PKs
- Composite primary keys
- String primary keys
- Tables with no explicit primary key

### Stable Row Identifiers

The explicit `rid integer primary key` ensures row IDs remain stable across database operations like `VACUUM`. Without this, SQLite might reassign rowids during vacuum operations, breaking change tracking.

### Identical Performance Characteristics

From SQLite's perspective, a table with:

```sql
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    name TEXT
);
```

Has identical B-tree layout and performance to:

```sql
CREATE TABLE users (
    rid INTEGER PRIMARY KEY,
    id INTEGER UNIQUE,
    name TEXT
);
```

The explicit rowid doesn't add overhead - it just makes the internal rowid visible and stable.

### Alternative: Primary Key Tracking

If you prefer tracking primary keys explicitly, the pattern will work with the following modifications:

- change the type of the `rid` column to blob (or just remove the type if you are ok with polymorphism in your database)
- extend SQLite with a function which encodes any composite primary keys into a single value
- update the triggers to write the primary key into the changes table (possibly calling the function you defined in the last step)

This will increase the storage overhead of the changes table as well as the performance overhead of triggers. You may also want to remove the `WITHOUT ROWID` optimization on the changes table.

## Best suited for

- General purpose replication or change tracking
- Scenarios where storing entire row data in changesets is acceptable

## Implementation Details

See `run_example()` in [pattern.py] for a complete working example and the test functions for comprehensive usage patterns.

## Tweaks

- **String table names**: Use table names instead of IDs in the changes table (less efficient but more readable; if you do this consider removing `WITHOUT ROWID`)
- **Custom cleanup**: Implement custom changeset retention policies instead of immediate cleanup (will require keeping around the GSN to start the next changeset from)

[pattern.py]: pattern.py
