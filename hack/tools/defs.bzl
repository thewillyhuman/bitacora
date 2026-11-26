"""Test rules built on the pinned tools of this package."""

load("@bazel_skylib//rules:write_file.bzl", "write_file")
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

def rejection_test(name, checker, args, data, findings, env = {}):
    """Declares a test that a checker fails on a bad input and names every mistake in it.

    A checker of deployment files is only worth what it refuses. This runs
    the checker on an input that was broken on purpose and requires a
    non-zero exit and one line of its report for each mistake.

    Args:
        name: the test target name.
        checker: the checker script, as the label of a file.
        args: the arguments that hand the checker the broken input; `$(rootpath ...)` is expanded.
        data: the files the checker reads.
        findings: one phrase per mistake; each must appear in what the checker prints.
        env: the environment the checker needs; `$(rootpath ...)` is expanded.
    """
    write_file(
        name = name + "_findings",
        out = name + ".findings",
        content = findings + [""],
        testonly = True,
    )
    sh_test(
        name = name,
        size = "small",
        srcs = ["//hack/tools:rejection_test.sh"],
        args = [
            "$(rootpath :{}_findings)".format(name),
            "$(rootpath {})".format(checker),
        ] + args,
        data = data + [checker, ":{}_findings".format(name)],
        env = env,
    )
