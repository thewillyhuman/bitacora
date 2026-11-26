# Pinned tools

Programs that tests run to check what is not Rust: shell scripts and deployment files. Each is one
release, fetched by Bazel for the platform at hand and verified by its checksum, so a test gives the
same answer on every machine and needs nothing installed.

| Target | Program | Checks |
| --- | --- | --- |
| `//hack/tools:shellcheck` | shellcheck | Every shell script of the repository |
| `//hack/tools:hadolint` | hadolint | The Dockerfile of the container images |
| `//hack/tools:helm` | helm | The Helm chart: lints it and renders it |

The releases and their checksums are in `//:MODULE.bazel`; the aliases in `BUILD.bazel` pick the right
one for macOS arm64, Linux x86-64 and Linux arm64. To add a platform, add its archive there and its
line here. To move to a new release, change the URLs and checksums of all three platforms together.

hadolint is the exception to "nothing installed": its release for macOS on arm64 is 103 MB, so on a
Mac the 23 MB release for x86-64 is used, which needs Rosetta
(`softwareupdate --install-rosetta --agree-to-license`). The test that uses it fails with that advice,
and does not pass unchecked, on a Mac without Rosetta.

`defs.bzl` has the test rules built on these programs:

- `shellcheck_test(name, srcs)` lints shell scripts. Every package that has scripts declares one.
- `rejection_test(name, checker, args, data, findings)` tests a checker script: it runs the checker on
  an input that was broken on purpose and requires it to fail and to name every mistake. Every checker
  of deployment files has one, because a checker that accepts everything looks exactly like one that
  works.
