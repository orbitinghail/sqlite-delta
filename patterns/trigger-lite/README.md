# Trigger-lite CDC pattern

## SQLite configuration

This pattern depends on holding a read transaction open on the database while building the changeset. Thus, WAL mode is recommended when using this pattern.

```
pragma journal_mode=wal;
```

## Best suited for

- general purpose replication or change tracking.
- when you are ok with storing the entire row in the changeset whenever the row changes.

## Tweaks

- Instead of using the SQLite rowid, use the row's primary key to track changes. This may require extending SQLite with a function which can encode multi-column PKs into a single canonical value.
