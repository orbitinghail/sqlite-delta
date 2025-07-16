<p align="center">
  <h1 align="center"><code>sqlite-delta</code></h1>
  <p align="center"><em>Lightweight CDC patterns for SQLite</em></p>
</p>

---

`sqlite-delta` is a collection of tested change-data-capture (CDC) patterns for SQLite databases. **This repository is educational** - the patterns are designed to be studied, understood, and adapted for your specific use case rather than used directly in production.

The provided implementations serve as working examples with comprehensive tests to demonstrate the concepts. Users should read and understand the patterns, then re-implement them in their own codebase tailored to their specific tables and requirements.

## Patterns

- [trigger-lite](./patterns/trigger-lite): keep track of changed rows since the last diff, requires all tables have a rowid.
- [three-phase](./patterns/three-phase): triple-buffer rows in a table to enable delta compression with minimal extra data movement.

## Glossary

- **Changeset**: A Changeset is a set of operations which encode the difference between two versions of a SQLite database or table.
- **Checkpoint**: A canonical full dump of a SQLite database or table.

## Dependencies

To run scripts and tests in this project, you'll need to install [uv](https://docs.astral.sh/uv/). Then run the following command to install all dependencies:

```sh
uv sync
```

## Acknowledgments

This project was developed in collaboration with [Shapr3D](https://www.shapr3d.com/). Special thanks to:

- [Richárd Szabó](https://github.com/richardsabow) - Collaborated on the design of the trigger-lite and three-phase patterns
- [Dávid Mentler](https://github.com/mentlerd) - Collaborated on the design of the trigger-lite and three-phase patterns

## Contributing

Thank you for your interest in contributing your time and expertise to the project. Please [read our contribution guide] to learn more about the process.

[read our contribution guide]: https://github.com/orbitinghail/sqlite-delta/blob/main/CONTRIBUTING.md

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE] or https://www.apache.org/licenses/LICENSE-2.0)
- MIT license ([LICENSE-MIT] or https://opensource.org/licenses/MIT)

at your option.

[LICENSE-APACHE]: https://github.com/orbitinghail/sqlite-delta/blob/main/LICENSE-APACHE
[LICENSE-MIT]: https://github.com/orbitinghail/sqlite-delta/blob/main/LICENSE-MIT
