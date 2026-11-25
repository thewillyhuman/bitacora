"""Test rules built on the pinned tools of this package."""

load("@rules_shell//shell:sh_test.bzl", "sh_test")

def shellcheck_test(name, srcs):
    """Declares a test that lints shell scripts with the pinned shellcheck.

    Args:
        name: the test target name.
        srcs: the shell scripts to lint, as labels of files.
    """
    sh_test(
        name = name,
        size = "small",
        srcs = ["//hack/tools:shellcheck_test.sh"],
        args = ["$(rootpath {})".format(script) for script in srcs],
        data = srcs + ["//hack/tools:shellcheck"],
        env = {"SHELLCHECK": "$(rootpath //hack/tools:shellcheck)"},
    )
