# Contributing to `sqlite-delta`

Welcome to the orbitinghail `sqlite-delta` repo! We are so excited you are here. Thank you for your interest in contributing your time and expertise to the project. The following document details contribution guidelines.

Whether you're addressing an open issue (or filing a new one), fixing a typo in our documentation, adding to core capabilities of the project, or introducing a new use case, all kinds of contributions are welcome.

## Gaining consensus

Before working on `sqlite-delta`, it's important to gain consensus on what you want to change or build. This will streamline the PR review process and make sure that your work is aligned with the projects goals. This can be done in a number of ways:

- [File an issue]: best for bug reports and concrete feature requests
- [Start a discussion]: best for ideas and more abstract topics
- [Join the Discord]: best for real-time collaboration

[File an issue]: https://github.com/orbitinghail/sqlite-delta/issues/new
[Start a discussion]: https://github.com/orbitinghail/sqlite-delta/discussions/new/choose
[Join the Discord]: https://discord.gg/etFk2N9nzC

## Pull Request (PR) process

To ensure your contribution is reviewed, all pull requests must be made against the `main` branch.

PRs must include a brief summary of what the change is, any issues associated with the change, and any fixes the change addresses. Please include the relevant link(s) for any fixed issues.

Pull requests do not have to pass all automated checks before being opened, but all checks must pass before merging. This can be useful if you need help figuring out why a required check is failing.

Our automated PR checks verify that:

- The code has been formatted correctly, according to `uv run poe fmt`.
- There are no linting errors, according to `uv run poe lint`.

## Licensing

`sqlite-delta` is licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE] or https://www.apache.org/licenses/LICENSE-2.0)
- MIT license ([LICENSE-MIT] or https://opensource.org/licenses/MIT)

[LICENSE-APACHE]: https://github.com/orbitinghail/graft/blob/main/LICENSE-APACHE
[LICENSE-MIT]: https://github.com/orbitinghail/graft/blob/main/LICENSE-MIT

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in the work by you shall be dual licensed as above, without any additional terms or conditions.

All submissions are bound by the [Developer's Certificate of Origin 1.1](https://developercertificate.org/) and shall be dual licensed as above, without any additional terms or conditions.

```
Developer's Certificate of Origin 1.1

By making a contribution to this project, I certify that:

(a) The contribution was created in whole or in part by me and I
    have the right to submit it under the open source license
    indicated in the file; or

(b) The contribution is based upon previous work that, to the best
    of my knowledge, is covered under an appropriate open source
    license and I have the right under that license to submit that
    work with modifications, whether created in whole or in part
    by me, under the same open source license (unless I am
    permitted to submit under a different license), as indicated
    in the file; or

(c) The contribution was provided directly to me by some other
    person who certified (a), (b) or (c) and I have not modified
    it.

(d) I understand and agree that this project and the contribution
    are public and that a record of the contribution (including all
    personal information I submit with it, including my sign-off) is
    maintained indefinitely and may be redistributed consistent with
    this project or the open source license(s) involved.
```
