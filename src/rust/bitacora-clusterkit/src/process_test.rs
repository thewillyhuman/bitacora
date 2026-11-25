//! Tests of the child process wrapper.
//!
//! # What is covered
//!
//! - A child's exit status and output reach the test.
//! - `terminate` and `kill` end a running child, and do nothing to one that has exited.
//! - Dropping the wrapper kills the child.
//! - Free ports are different from each other and can be bound.
//!
//! # How the fixtures work
//!
//! The children are the shell and `sleep`, which every machine that builds this repository has. Logs go
//! to a temporary directory.

use tempfile::TempDir;

use super::*;

/// Far longer than a shell takes to start and exit.
const GENEROUS_WAIT: Duration = Duration::from_secs(20);

/// Starts `sh -c <script>` as a child called `child`.
///
/// # Returns
///
/// The directory that holds the child's log, which must be kept alive, and the child.
fn spawn_shell(script: &str) -> (TempDir, ChildProcess) {
    let directory = TempDir::new().expect("a temporary directory can be created");
    let mut command = Command::new("sh");
    command.arg("-c").arg(script);
    let child = ChildProcess::spawn("child", &mut command, &directory.path().join("child.log"));
    (directory, child)
}

/// Whether the operating system still knows a process with this identifier.
fn is_running(process_id: u32) -> bool {
    Command::new("kill")
        .arg("-0")
        .arg(process_id.to_string())
        .stderr(Stdio::null())
        .status()
        .expect("the kill program can be run")
        .success()
}

#[tokio::test]
async fn exit_code_of_a_child_is_reported() {
    let (_directory, mut child) = spawn_shell("exit 3");

    let exit_status = child.wait_for_exit(GENEROUS_WAIT).await;

    assert_eq!(exit_status.and_then(|status| status.code()), Some(3));
}

#[tokio::test]
async fn standard_output_of_a_child_goes_to_its_log() {
    let (_directory, mut child) = spawn_shell("echo to-standard-output");

    child.wait_for_exit(GENEROUS_WAIT).await;

    assert!(
        child.log().contains("to-standard-output"),
        "{}",
        child.log()
    );
}

#[tokio::test]
async fn standard_error_of_a_child_goes_to_its_log() {
    let (_directory, mut child) = spawn_shell("echo to-standard-error >&2");

    child.wait_for_exit(GENEROUS_WAIT).await;

    assert!(child.log().contains("to-standard-error"), "{}", child.log());
}

#[tokio::test]
async fn child_that_keeps_running_is_reported_as_running() {
    let (_directory, mut child) = spawn_shell("sleep 100");

    let exit_status = child.wait_for_exit(Duration::from_millis(50)).await;

    assert_eq!(exit_status, None);
}

#[tokio::test]
async fn terminated_child_exits() {
    let (_directory, mut child) = spawn_shell("exec sleep 100");

    child.terminate();
    let exit_status = child.wait_for_exit(GENEROUS_WAIT).await;

    assert!(exit_status.is_some());
}

#[tokio::test]
async fn child_stopped_by_a_signal_has_no_exit_code() {
    let (_directory, mut child) = spawn_shell("exec sleep 100");

    child.terminate();
    let exit_status = child.wait_for_exit(GENEROUS_WAIT).await;

    assert_eq!(exit_status.and_then(|status| status.code()), None);
}

#[test]
fn killed_child_is_gone_when_kill_returns() {
    let (_directory, mut child) = spawn_shell("exec sleep 100");

    child.kill();

    assert!(child.has_exited());
}

#[tokio::test]
async fn terminating_a_child_that_has_exited_does_nothing() {
    let (_directory, mut child) = spawn_shell("exit 0");
    child.wait_for_exit(GENEROUS_WAIT).await;

    child.terminate();

    assert!(child.has_exited());
}

#[test]
fn dropping_the_wrapper_kills_the_child() {
    let (_directory, child) = spawn_shell("exec sleep 100");
    let process_id = child.id();

    drop(child);

    assert!(!is_running(process_id));
}

#[test]
fn child_is_known_by_the_name_it_was_given() {
    let (_directory, child) = spawn_shell("exit 0");

    assert_eq!(child.name(), "child");
}

#[test]
fn free_ports_are_different_from_each_other() {
    let mut ports = free_local_ports(8);

    ports.sort_unstable();
    ports.dedup();

    assert_eq!(ports.len(), 8);
}

#[test]
fn free_port_can_be_bound() {
    let ports = free_local_ports(1);

    let bound = TcpListener::bind((Ipv4Addr::LOCALHOST, ports[0]));

    assert!(bound.is_ok(), "{bound:?}");
}
