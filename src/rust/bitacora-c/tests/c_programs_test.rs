//! The C programs of `src/c`, run against a cluster of real processes.
//!
//! # Why this test exists
//!
//! The unit tests of this crate stop where the network begins, and they call the interface from Rust. A
//! C program that embeds the library sees neither: it sees a header, an archive, and a cluster. This
//! test is the one place where all three are real. The programs are compiled by a C compiler against the
//! checked-in header, linked against the static library, and run against etcd, three `bita-replica`
//! daemons and a `bita-controller` daemon.
//!
//! # What is covered
//!
//! - `src/c/tests/end_to_end.c`: the life of a stream as a database lives it. It creates a stream,
//!   appends single records and a batch, reads them back byte for byte, follows the tail and is told
//!   "nothing new", has its writer fenced by a second one, reads below the low-water mark after a
//!   truncation and is told so with the mark, and deletes the stream. The program checks every step
//!   itself; this test checks that it exits with 0.
//! - `src/c/examples/quickstart.c`: the example of `docs/clients/c.md` works, and prints what the
//!   document says it prints.
//! - The program shown in `docs/clients/c.md` is, byte for byte, the example that is compiled and run.
//!
//! # How the fixtures work
//!
//! `bitacora-clusterkit` starts the cluster on loopback ports of its own in a temporary directory and
//! stops it when the test ends. The test writes the cluster's configuration to a TOML file, the way an operator
//! would hand it to a database, and passes its path to the C program.
//!
//! Both programs run against one cluster, one after the other, each on a stream of its own. A cluster
//! is five processes and a dozen ports; starting a second one at the same moment would cost more than
//! it tells.
//!
//! The daemons and the C programs come from environment variables that the Bazel target sets. Cargo
//! does not build C programs, so under Cargo the test that needs them passes without running, and says
//! so on standard error. The comparisons of the document with the example run in both.

// The test writes a configuration file and reads a document and a source file: there is no injected
// disk to go through here.
#![allow(clippy::disallowed_methods)]

use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

use bitacora::{ClusterConfig, ObjectStoreConfig};
use bitacora_clusterkit::{Binaries, ChildProcess, RealCluster};

/// How many replica daemons the cluster runs: what a stream with three replicas needs.
const REPLICA_COUNT: usize = 3;

/// Far longer than a C program takes on an idle machine; one that runs this long has failed.
const GENEROUS_WAIT: Duration = Duration::from_secs(60);

/// The environment variable that names the compiled `src/c/tests/end_to_end.c`.
const END_TO_END_PROGRAM_VARIABLE: &str = "BITA_C_END_TO_END_BINARY";

/// The environment variable that names the compiled `src/c/examples/quickstart.c`.
const QUICKSTART_PROGRAM_VARIABLE: &str = "BITA_C_QUICKSTART_BINARY";

/// The environment variable that names the source of the example.
const QUICKSTART_SOURCE_VARIABLE: &str = "BITA_C_QUICKSTART_SOURCE";

/// The environment variable that names the document that shows the example.
const DOCUMENT_VARIABLE: &str = "BITA_C_DOCUMENT";

/// The variable Bazel sets in every test it runs.
const BAZEL_TEST_VARIABLE: &str = "BAZEL_TEST";

/// What the example prints when it runs against a stream nobody has written to.
const QUICKSTART_OUTPUT: &str = "\
appended \"first\" as record 1
appended \"second\" as record 2
appended \"third\" as record 3
record 1: first
record 2: second
record 3: third
";

/// Finds a compiled C program through its environment variable.
///
/// # Returns
///
/// The absolute path of the program, or `None` under Cargo, which does not build it; the test should
/// then pass without running.
///
/// # Panics
///
/// Panics when the variable is not set under Bazel, which always provides the program: there, a
/// missing one means the build files are broken.
fn c_program(variable: &str) -> Option<PathBuf> {
    let Some(program) = std::env::var_os(variable) else {
        assert!(
            std::env::var_os(BAZEL_TEST_VARIABLE).is_none(),
            "{variable} must be set: the test target must pass the C program"
        );
        eprintln!("{variable} is not set: the C program is built by Bazel only, so this test does not run");
        return None;
    };
    let program = PathBuf::from(program);
    if program.is_absolute() {
        return Some(program);
    }
    let working_directory = std::env::current_dir().expect("the working directory can be read");
    Some(working_directory.join(program))
}

/// A file of the repository that a test reads: named by Bazel through `variable`, or found from this
/// crate's directory under Cargo.
fn repository_file(variable: &str, path_from_the_crate: &str) -> PathBuf {
    match std::env::var_os(variable) {
        Some(path) => PathBuf::from(path),
        None => Path::new(env!("CARGO_MANIFEST_DIR")).join(path_from_the_crate),
    }
}

/// The cluster configuration as the TOML file every bitacora program reads.
///
/// # Panics
///
/// Panics for an S3 object store, which the cluster kit never configures, and if the text written does
/// not read back as the same configuration.
fn cluster_configuration_text(config: &ClusterConfig) -> String {
    let mut quoted_endpoints = Vec::new();
    for endpoint in &config.etcd_endpoints {
        quoted_endpoints.push(format!("{endpoint:?}"));
    }
    let object_store_table = match &config.object_store {
        ObjectStoreConfig::None => String::new(),
        ObjectStoreConfig::LocalDirectory { path } => {
            let path = path.to_str().expect("a temporary directory has a UTF-8 path");
            format!("\n[object_store]\nkind = \"local_directory\"\npath = {path:?}\n")
        }
        ObjectStoreConfig::S3 { .. } => panic!("the cluster kit does not configure S3"),
    };
    let text = format!(
        "etcd_endpoints = [{}]\netcd_key_prefix = {:?}\n{object_store_table}",
        quoted_endpoints.join(", "),
        config.etcd_key_prefix
    );
    let read_back: ClusterConfig = toml::from_str(&text).expect("the configuration written is valid TOML");
    assert_eq!(&read_back, config, "the configuration file says what the cluster is");
    text
}

/// What a C program left behind when it ended.
struct ProgramRun {
    /// The exit code, or `None` if the program was ended by a signal.
    exit_code: Option<i32>,
    /// Everything the program wrote to standard output and standard error.
    output: String,
}

/// Runs a C program against `cluster` until it exits by itself.
///
/// The program gets one argument: the path of a file with the cluster's configuration.
///
/// # Panics
///
/// Panics if the program is still running after [`GENEROUS_WAIT`], with what it wrote so far.
async fn run_against(cluster: &RealCluster, program_name: &str, program: &Path) -> ProgramRun {
    let directory = tempfile::tempdir().expect("a temporary directory can be created");
    let configuration_path = directory.path().join("cluster.toml");
    std::fs::write(
        &configuration_path,
        cluster_configuration_text(cluster.cluster_config()),
    )
    .expect("the configuration file can be written");

    let mut command = Command::new(program);
    command.arg(&configuration_path);
    let mut process = ChildProcess::spawn(program_name, &mut command, &directory.path().join("output"));
    let Some(exit_status) = process.wait_for_exit(GENEROUS_WAIT).await else {
        panic!(
            "{program_name} is still running after {GENEROUS_WAIT:?}; it wrote:\n{}",
            process.log()
        );
    };
    ProgramRun {
        exit_code: exit_status.code(),
        output: process.log(),
    }
}

/// The contents of every fenced block of `language` in a Markdown document, each with a newline after
/// its last line.
fn fenced_blocks(document: &str, language: &str) -> Vec<String> {
    let opening_fence = format!("```{language}");
    let mut blocks = Vec::new();
    let mut open_block: Option<String> = None;
    for line in document.lines() {
        if let Some(mut block) = open_block.take() {
            if line == "```" {
                blocks.push(block);
            } else {
                block.push_str(line);
                block.push('\n');
                open_block = Some(block);
            }
        } else if line == opening_fence {
            open_block = Some(String::new());
        }
    }
    blocks
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn c_programs_pass_against_a_real_cluster() {
    let (Some(binaries), Some(quickstart), Some(end_to_end)) = (
        Binaries::from_environment(),
        c_program(QUICKSTART_PROGRAM_VARIABLE),
        c_program(END_TO_END_PROGRAM_VARIABLE),
    ) else {
        return;
    };
    let cluster = RealCluster::start(&binaries, REPLICA_COUNT).await;

    let quickstart_run = run_against(&cluster, "quickstart", &quickstart).await;
    let end_to_end_run = run_against(&cluster, "end_to_end", &end_to_end).await;

    assert_eq!(
        quickstart_run.exit_code,
        Some(0),
        "quickstart failed; it wrote:\n{}",
        quickstart_run.output
    );
    assert_eq!(quickstart_run.output, QUICKSTART_OUTPUT);
    assert_eq!(
        end_to_end_run.exit_code,
        Some(0),
        "end_to_end failed; it wrote:\n{}",
        end_to_end_run.output
    );
    assert_eq!(end_to_end_run.output, "end_to_end: every step passed\n");
}

#[test]
fn documented_program_is_the_example_that_is_compiled() {
    let document = std::fs::read_to_string(repository_file(DOCUMENT_VARIABLE, "../../../docs/clients/c.md"))
        .expect("the document can be read");
    let example = std::fs::read_to_string(repository_file(
        QUICKSTART_SOURCE_VARIABLE,
        "../../c/examples/quickstart.c",
    ))
    .expect("the example can be read");

    let c_blocks = fenced_blocks(&document, "c");

    assert!(
        c_blocks.contains(&example),
        "docs/clients/c.md must show src/c/examples/quickstart.c exactly, in a ```c block"
    );
}

#[test]
fn documented_output_is_what_the_example_prints() {
    let document = std::fs::read_to_string(repository_file(DOCUMENT_VARIABLE, "../../../docs/clients/c.md"))
        .expect("the document can be read");

    let text_blocks = fenced_blocks(&document, "text");

    assert!(
        text_blocks.contains(&QUICKSTART_OUTPUT.to_owned()),
        "docs/clients/c.md must show the output of the example exactly, in a ```text block"
    );
}
