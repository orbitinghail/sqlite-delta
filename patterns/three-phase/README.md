# Three-phase CDC pattern

## Best suited for

- tables with very wide rows that would benefit from delta compression.
- key/value workloads; the additional phase column increases the cost and complexity of table scans.

## Tweaks

- Inline delta computation: extend SQLite with a function which performs the delta computation between two row versions during the checkpoint query. This may avoid extra data copies depending on how you are running the checkpoint query.
