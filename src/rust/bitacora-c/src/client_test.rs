//! Tests of connecting a client from C.
//!
//! # What is covered
//!
//! - Connecting from text and from a file, with and without options.
//! - Every way the arguments can be wrong: null pointers, a configuration that is not valid or can never
//!   work, options out of range or with their size not set, text that is not UTF-8.
//! - What the caller is left with after a failure: a null handle and a message.
//! - Waiting on the client's threads, with and without a limit.
//! - That every handle can be shared between threads, which the header promises.
//!
//! # How the fixtures work
//!
//! Connecting sends nothing, so every test connects to an etcd endpoint nobody listens on. Options are
//! built with their size set, as a C program would, by `options_with`.

use std::ffi::{CStr, CString};

use super::*;
use crate::reader::{FollowerHandle, ReaderHandle};
use crate::records::RecordsHandle;
use crate::test_support::{ABSENT_CLUSTER_CONFIGURATION, TestClient, last_message};
use crate::writer::WriterHandle;

/// Options of this version of the structure, with their size set.
fn options_with(worker_threads: u32, writer_identity: *const c_char) -> ClientOptions {
    ClientOptions {
        struct_size: u32::try_from(size_of::<ClientOptions>()).expect("a small structure"),
        worker_threads,
        writer_identity,
    }
}

/// Connects with `configuration` and `options` and frees the client if one was made.
fn connect_status(configuration: &CStr, options: *const ClientOptions) -> BitacoraStatus {
    let mut client = std::ptr::null_mut();
    // SAFETY: the configuration is NUL-terminated, the options are null or a live structure with its size
    // set, and the output is a live pointer.
    let status = unsafe { bitacora_client_connect_text(configuration.as_ptr(), options, &raw mut client) };
    // SAFETY: the handle is null or was just returned, and is freed once.
    unsafe { bitacora_client_free(client) };
    status
}

/// Checks at compile time that a type can be used from several threads at once.
fn assert_shareable_between_threads<Handle: Send + Sync>() {}

#[test]
fn every_handle_can_be_shared_between_threads() {
    assert_shareable_between_threads::<ClientHandle>();
    assert_shareable_between_threads::<WriterHandle>();
    assert_shareable_between_threads::<ReaderHandle>();
    assert_shareable_between_threads::<FollowerHandle>();
    assert_shareable_between_threads::<RecordsHandle>();
}

#[test]
fn connecting_with_text_and_default_options_succeeds() {
    let status = connect_status(ABSENT_CLUSTER_CONFIGURATION, std::ptr::null());

    assert_eq!(status, BitacoraStatus::Ok);
}

#[test]
fn connecting_hands_out_a_client() {
    let mut client = std::ptr::null_mut();

    // SAFETY: the configuration is NUL-terminated, null options mean the defaults, and the output is a
    // live pointer.
    unsafe { bitacora_client_connect_text(ABSENT_CLUSTER_CONFIGURATION.as_ptr(), std::ptr::null(), &raw mut client) };

    let was_handed_out = !client.is_null();
    // SAFETY: the handle was just returned and is freed once.
    unsafe { bitacora_client_free(client) };
    assert!(was_handed_out);
}

#[test]
fn connecting_with_options_of_this_version_succeeds() {
    let writer_identity = CString::new("compute-1").expect("no NUL inside");
    let options = options_with(1, writer_identity.as_ptr());

    let status = connect_status(ABSENT_CLUSTER_CONFIGURATION, &raw const options);

    assert_eq!(status, BitacoraStatus::Ok, "{}", last_message());
}

#[test]
fn connecting_without_a_configuration_is_refused_by_name() {
    let mut client = std::ptr::null_mut();

    // SAFETY: null pointers are allowed, and the output is a live pointer.
    let status = unsafe { bitacora_client_connect_text(std::ptr::null(), std::ptr::null(), &raw mut client) };

    assert_eq!(status, BitacoraStatus::InvalidArgument);
    assert_eq!(last_message(), "configuration_text is null");
}

#[test]
fn connecting_without_a_place_for_the_client_is_refused_by_name() {
    // SAFETY: the configuration is NUL-terminated and null pointers are allowed.
    let status = unsafe {
        bitacora_client_connect_text(
            ABSENT_CLUSTER_CONFIGURATION.as_ptr(),
            std::ptr::null(),
            std::ptr::null_mut(),
        )
    };

    assert_eq!(status, BitacoraStatus::InvalidArgument);
    assert_eq!(last_message(), "client_out is null");
}

#[test]
fn failed_connect_leaves_a_null_client() {
    let mut client = std::ptr::NonNull::<ClientHandle>::dangling().as_ptr();

    // SAFETY: the text is NUL-terminated, null options mean the defaults, and the output is a live
    // pointer; its dangling content is overwritten, never read.
    unsafe { bitacora_client_connect_text(c"this is not TOML".as_ptr(), std::ptr::null(), &raw mut client) };

    assert!(client.is_null());
}

#[test]
fn configuration_that_is_not_toml_is_an_invalid_argument() {
    let status = connect_status(c"this is not TOML", std::ptr::null());

    assert_eq!(status, BitacoraStatus::InvalidArgument);
}

#[test]
fn configuration_with_a_misspelled_setting_is_an_invalid_argument() {
    let status = connect_status(c"etcd_endpoints = []\netcd_key_prefx = \"/staging/\"", std::ptr::null());

    assert_eq!(status, BitacoraStatus::InvalidArgument);
}

#[test]
fn configuration_without_endpoints_is_an_invalid_argument() {
    let status = connect_status(c"etcd_endpoints = []", std::ptr::null());

    assert_eq!(status, BitacoraStatus::InvalidArgument);
}

#[test]
fn more_worker_threads_than_allowed_are_an_invalid_argument() {
    let options = options_with(MAX_WORKER_THREADS + 1, std::ptr::null());

    let status = connect_status(ABSENT_CLUSTER_CONFIGURATION, &raw const options);

    assert_eq!(status, BitacoraStatus::InvalidArgument);
}

#[test]
fn options_whose_size_was_not_set_are_an_invalid_argument() {
    let options = ClientOptions {
        struct_size: 0,
        ..options_with(1, std::ptr::null())
    };

    let status = connect_status(ABSENT_CLUSTER_CONFIGURATION, &raw const options);

    assert_eq!(status, BitacoraStatus::InvalidArgument);
}

#[test]
fn writer_identity_that_is_not_utf8_is_an_invalid_argument() {
    let not_utf8: [u8; 3] = [0xFF, 0xFE, 0x00];
    let options = options_with(1, not_utf8.as_ptr().cast::<c_char>());

    let status = connect_status(ABSENT_CLUSTER_CONFIGURATION, &raw const options);

    assert_eq!(status, BitacoraStatus::InvalidArgument);
}

#[test]
fn connecting_with_a_configuration_file_succeeds() {
    let directory = tempfile::tempdir().expect("a temporary directory");
    let configuration_path = directory.path().join("cluster.toml");
    // A test writes the file it then asks the library to read; there is no injected disk to go through.
    #[allow(clippy::disallowed_methods)]
    std::fs::write(&configuration_path, ABSENT_CLUSTER_CONFIGURATION.to_bytes()).expect("the file is written");
    let c_path = CString::new(configuration_path.to_str().expect("a UTF-8 path")).expect("no NUL inside");
    let mut client = std::ptr::null_mut();

    // SAFETY: the path is NUL-terminated, null options mean the defaults, and the output is a live pointer.
    let status = unsafe { bitacora_client_connect_file(c_path.as_ptr(), std::ptr::null(), &raw mut client) };

    // SAFETY: the handle is null or was just returned, and is freed once.
    unsafe { bitacora_client_free(client) };
    assert_eq!(status, BitacoraStatus::Ok, "{}", last_message());
}

#[test]
fn configuration_file_that_does_not_exist_is_an_invalid_argument() {
    let mut client = std::ptr::null_mut();

    // SAFETY: the path is NUL-terminated, null options mean the defaults, and the output is a live pointer.
    let status = unsafe {
        bitacora_client_connect_file(
            c"/nonexistent/bitacora/cluster.toml".as_ptr(),
            std::ptr::null(),
            &raw mut client,
        )
    };

    assert_eq!(status, BitacoraStatus::InvalidArgument);
}

#[test]
fn connecting_without_a_configuration_path_is_refused_by_name() {
    let mut client = std::ptr::null_mut();

    // SAFETY: null pointers are allowed, and the output is a live pointer.
    let status = unsafe { bitacora_client_connect_file(std::ptr::null(), std::ptr::null(), &raw mut client) };

    assert_eq!(status, BitacoraStatus::InvalidArgument);
    assert_eq!(last_message(), "configuration_path is null");
}

#[test]
fn freeing_a_null_client_does_nothing() {
    // SAFETY: a null pointer is allowed.
    unsafe { bitacora_client_free(std::ptr::null_mut()) };
}

#[test]
fn work_of_a_call_runs_on_a_thread_of_the_client() {
    let client = TestClient::connect();
    let core = client.core();

    let name_of_the_working_thread = core.wait_for(async { std::thread::current().name().map(str::to_owned) });

    assert_eq!(name_of_the_working_thread.as_deref(), Some(WORKER_THREAD_NAME));
}

#[test]
fn work_of_a_call_does_not_run_on_the_calling_thread() {
    let client = TestClient::connect();
    let core = client.core();

    let working_thread = core.wait_for(async { std::thread::current().id() });

    assert_ne!(working_thread, std::thread::current().id());
}

#[test]
fn threads_started_later_are_started_by_the_clients_threads() {
    let client = TestClient::connect();
    let core = client.core();

    // Work that blocks gets a thread of its own, started when it is first needed. It is named like the
    // others because the client's runtime starts it, from a thread of the client.
    let name_of_the_late_thread = core.wait_for(async {
        tokio::task::spawn_blocking(|| std::thread::current().name().map(str::to_owned))
            .await
            .expect("the blocking work does not panic")
    });

    assert_eq!(name_of_the_late_thread.as_deref(), Some(WORKER_THREAD_NAME));
}

#[test]
fn panic_in_the_work_is_reported_by_the_call_that_waited_for_it() {
    let client = TestClient::connect();
    let core = client.core();

    let status = crate::guard::run_guarded(|| {
        core.wait_for(async { panic!("an invariant was broken") });
        Ok(())
    });

    assert_eq!(status, BitacoraStatus::Internal);
    assert_eq!(last_message(), "internal error in bitacora: an invariant was broken");
}

#[test]
fn client_still_works_after_its_work_panicked() {
    let client = TestClient::connect();
    let core = client.core();
    crate::guard::run_guarded(|| {
        core.wait_for(async { panic!("an invariant was broken") });
        Ok(())
    });

    let outcome = core.wait_for(async { 5 });

    assert_eq!(outcome, 5);
}

#[test]
fn work_without_a_limit_is_waited_for() {
    let client = TestClient::connect();
    let core = client.core();

    let outcome = core.wait_at_most(BITACORA_WAIT_WITHOUT_LIMIT, async { 5 });

    assert_eq!(outcome, Some(5));
}

#[test]
fn work_that_finishes_within_its_limit_is_waited_for() {
    let client = TestClient::connect();
    let core = client.core();

    let outcome = core.wait_at_most(60_000, async { 5 });

    assert_eq!(outcome, Some(5));
}

#[test]
fn work_that_outlasts_its_limit_is_given_up() {
    let client = TestClient::connect();
    let core = client.core();

    let outcome = core.wait_at_most(10, std::future::pending::<u32>());

    assert_eq!(outcome, None);
}
