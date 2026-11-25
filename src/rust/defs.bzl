"""Macros that declare a bitacora crate the same way in every package.

Cargo.toml stays the source of truth for third-party dependencies and lints;
these macros read both from it, so a BUILD file only names what Cargo cannot
tell Bazel: the first-party crates a target depends on.
"""

load("@crates//:defs.bzl", "aliases", "all_crate_deps")
load("@rules_rust//cargo:defs.bzl", "extract_cargo_lints")
load("@rules_rust//rust:defs.bzl", "rust_binary", "rust_library", "rust_test")

_WORKSPACE_MANIFEST = "//src/rust:Cargo.toml"
_LINTS = ":cargo_lints"

def _declare_lints():
    if not native.existing_rule("cargo_lints"):
        extract_cargo_lints(
            name = "cargo_lints",
            manifest = "Cargo.toml",
            workspace = _WORKSPACE_MANIFEST,
        )

def bitacora_library(name, deps = [], test_deps = [], test_data = [], test_size = "small", **kwargs):
    """Declares a library crate and the test target for its unit tests.

    Args:
        name: the crate name with underscores, for example `bitacora_types`.
        deps: first-party crates the library depends on.
        test_deps: first-party crates only the unit tests depend on.
        test_data: files the unit tests read at run time.
        test_size: Bazel test size of the unit tests.
        **kwargs: passed through to `rust_library`.
    """
    _declare_lints()
    rust_library(
        name = name,
        srcs = native.glob(["src/**/*.rs"], exclude = ["src/bin/**", "src/main.rs"]),
        aliases = aliases(),
        lint_config = _LINTS,
        proc_macro_deps = all_crate_deps(proc_macro = True),
        visibility = ["//visibility:public"],
        deps = deps + all_crate_deps(normal = True),
        **kwargs
    )
    rust_test(
        name = name + "_test",
        size = test_size,
        aliases = aliases(
            normal_dev = True,
            proc_macro_dev = True,
        ),
        crate = ":" + name,
        data = test_data,
        lint_config = _LINTS,
        proc_macro_deps = all_crate_deps(proc_macro_dev = True),
        deps = test_deps + all_crate_deps(normal_dev = True),
    )

def bitacora_binary(name, srcs, deps = [], **kwargs):
    """Declares a binary crate.

    Args:
        name: the binary name as typed at a shell, for example `bita-replica`.
        srcs: the binary's source files.
        deps: first-party crates the binary depends on.
        **kwargs: passed through to `rust_binary`.
    """
    _declare_lints()
    rust_binary(
        name = name,
        srcs = srcs,
        aliases = aliases(),
        lint_config = _LINTS,
        proc_macro_deps = all_crate_deps(proc_macro = True),
        visibility = ["//visibility:public"],
        deps = deps + all_crate_deps(normal = True),
        **kwargs
    )

def bitacora_integration_test(name, srcs, deps = [], size = "small", **kwargs):
    """Declares a test that uses a crate only through its public interface.

    Args:
        name: the test target name.
        srcs: the test's source files.
        deps: first-party crates the test depends on.
        size: Bazel test size.
        **kwargs: passed through to `rust_test`.
    """
    _declare_lints()
    rust_test(
        name = name,
        size = size,
        srcs = srcs,
        aliases = aliases(
            normal = True,
            normal_dev = True,
            proc_macro = True,
            proc_macro_dev = True,
        ),
        lint_config = _LINTS,
        proc_macro_deps = all_crate_deps(
            proc_macro = True,
            proc_macro_dev = True,
        ),
        deps = deps + all_crate_deps(
            normal = True,
            normal_dev = True,
        ),
        **kwargs
    )
