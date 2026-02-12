# Three-Phase CDC Pattern

The three-phase pattern provides Change Data Capture (CDC) for SQLite tables using a triple-buffering approach with delta compression. It's designed for scenarios where delta compression is beneficial and atomic changeset generation is required.

## How it works

The pattern uses a versioning system with up to three phases (0, 1, 2) of each row to enable atomic changeset generation and delta compression.

### 1. Three-Phase Table Structure

Tables are augmented with phase and deleted columns to support versioning:

```sql
CREATE TABLE IF NOT EXISTS AppTable (
    -- Application columns
    id INTEGER,
    data BLOB,

    -- Three-phase pattern columns
    phase INTEGER NOT NULL DEFAULT 0,  -- 0: new, 1: in-progress, 2: stable
    deleted BOOL NOT NULL DEFAULT 0,   -- logical deletion flag

    PRIMARY KEY (id, phase)  -- Multiple versions per logical row
)
```

See `setup_three_phase_table()` in [pattern.py] for the implementation.

### 2. Phase Management

- **Phase 0**: New rows and updates from application operations
- **Phase 1**: Rows being processed by an active changeset
- **Phase 2**: Stable rows that have been through changeset processing

Application operations always target phase 0, while changeset generation works with phases 1 and 2.

> [!TIP]
> One way to reason about the three phases is to think of the rows compacting into phase 2 over time. It's loosely similar to how LSM trees compact tuples into lower-level layers.

See `insert_or_update()` and `logical_delete()` in [pattern.py] for how to safely modify a three-phase table.

### 3. Changeset Generation

Generating a changeset must perform the following operations across three transactions:

**Transaction 1**: Transition phase 0 rows to phase 1

- delete any rows in phase 1 which have been updated by phase 0
- update any rows in phase 0 to phase 1

**Transaction 2 (readonly)**: Build the changeset

- compare rows in phase 2 and phase 1
- for rows which have changed (i.e. aren't inserts or deletes) use a delta-compression method to compute the delta
- store inserts and deletes as is in the changeset

**Transaction 3**: Cleanup

First we need to remove two kinds of rows:

1. deleted phase 1 rows
2. phase 2 rows which are being updated/deleted by a phase 1 row

Then we update any rows left in phase 1 to phase 2.

See `changeset()` in [pattern.py] for an example implementation which uses the fossil delta algorithm to compute differences.

### 4. Checkpoint Generation

Before checkpointing the entire database, you should first compact any three-phase tables. To do this you run the following two operations in a transaction:

1. delete all rows which are logically deleted or are not the latest version
2. update all rows to phase=2

After running compaction you may checkpoint the database. The resulting Checkpoint needs to be sent to any replicas in entirely before replicating changes can resume.

See `compact()` in [pattern.py] for the implementation.

## Requirements

- **Phase column**: Primary key must include the phase column
- **WAL mode**: Recommended for concurrent access during changeset generation

## Best suited for

- Tables with very wide rows that would benefit from delta compression
- Key/value workloads where the additional complexity of multi-version rows is easy to handle

## Implementation Details

See `run_example()` in [pattern.py] for a complete working example and the test functions for comprehensive usage patterns including crash recovery scenarios.

## Tweaks

- **Inline delta computation**: Extend SQLite with a function to perform delta computation during changeset queries (may avoid extra data copies)
- **Alternative delta algorithms**: Replace fossil delta with other compression algorithms suited to your data

[pattern.py]: pattern.py
