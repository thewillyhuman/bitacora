# Pinned tools

Programs that tests run to check what is not Rust: shell scripts and deployment files. Each is one
release, fetched by Bazel for the platform at hand and verified by its checksum, so a test gives the
same answer on every machine and needs nothing installed.

| Target | Program | Checks |
| --- | --- | --- |
| `//hack/tools:shellcheck` | shellcheck | Every shell script of the repository |

The releases and their checksums are in `//:MODULE.bazel`; the aliases in `BUILD.bazel` pick the right
one for macOS arm64, Linux x86-64 and Linux arm64. To add a platform, add its archive there and its
line here. To move to a new release, change the URLs and checksums of all three platforms together.

`defs.bzl` has the test rules built on these programs:

- `shellcheck_test(name, srcs)` lints shell scripts. Every package that has scripts declares one.
