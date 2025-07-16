<p align="center">
  <h1 align="center"><code>sqlite-delta</code></h1>
  <p align="center"><em>Lightweight CDC patterns for SQLite</em></p>
</p>

---

`sqlite-delta` is a collection of tested change-data-capture (CDC) patterns which can be used with SQLite. Patterns can be found in [./patterns], and each one includes a README documenting usage along with sample SQL files and tests.

## Patterns

- [trigger-lite](./patterns/trigger-lite): keep track of changed rows since the last diff, requires all tables have a rowid.
- [three-phase](./patterns/three-phase): triple-buffer rows in a table to enable delta compression with minimal extra data movement.

## Glossary

- **Changeset**: A Changeset is a set of operations which encode the difference between two versions of a SQLite database or table.
- **Checkpoint**: A canonical full dump of a SQLite database or table.

## Dependencies

To run scripts and tests in this project, you'll need the following dependencies installed:

- [uv](https://docs.astral.sh/uv/)
- [sqlite3](https://www.sqlite.org/)

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
