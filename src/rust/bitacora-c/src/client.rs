//! The client handle: connecting to a cluster from C.
//!
//! # Why this file exists
//!
//! The Rust client is asynchronous and built from injected interfaces. A C program wants one call that
//! takes a configuration and gives back something to open writers and readers from, and it wants every
//! later call to block until it has an answer. This file is that first call, and the piece every other
//! handle shares to turn the client's asynchronous work into blocking calls.
//!
//! # What it contains
//!
//! - [`ClientHandle`]: what C holds as `bitacora_client`.
//! - [`ClientOptions`]: how the client runs, as `bitacora_client_options`.
//! - [`bitacora_client_connect_file`], [`bitacora_client_connect_text`], [`bitacora_client_free`].
//! - `ClientCore`: the Rust client with the threads that run it, shared by every handle opened from one
//!   client.
//!
//! # How it works
//!
//! A cluster is described by the same configuration every bitacora program reads: the TOML form of the
//! cluster configuration, with the etcd endpoints, the key prefix and the object store. C passes it as a
//! file path or as text. That keeps the C interface out of the business of mirroring every configuration
//! field as a structure member, and a new kind of object store needs no change here.
//!
//! Each client owns a Tokio runtime with a few worker threads. A call from C hands its work to that
//! runtime as a task and blocks the calling thread until the task has an answer. The calling thread
//! checks arguments, waits, and writes results; it never runs the client's own code. The same holds for
//! the background work of the client: heartbeats, lease renewals, sending records to replicas.
//!
//! That split is what makes the library's threads predictable for a program that cares about them. Every
//! thread is started by the thread that connects or by a thread descended from it, so all of them
//! inherit the signal mask the connecting thread had at that moment, and no thread is ever started by a
//! later call from whatever thread happens to make it.
//!
//! Writers, readers and followers keep the core alive: freeing the client handle while they exist is
//! allowed, and the threads stop when the last handle is freed.
//!
//! ```
//! use bitacora_c::{BitacoraStatus, bitacora_client_connect_text, bitacora_client_free};
//!
//! // Connecting sends nothing, so it succeeds while etcd is down; requests fail until it is back.
//! let configuration = c"etcd_endpoints = [\"http://127.0.0.1:1\"]";
//! let mut client = std::ptr::null_mut();
//! // SAFETY: the text is NUL-terminated, null options mean the defaults, and the output is a live pointer.
//! let status = unsafe { bitacora_client_connect_text(configuration.as_ptr(), std::ptr::null(), &raw mut client) };
//! assert_eq!(status, BitacoraStatus::Ok);
//! // SAFETY: the handle was just returned and is freed exactly once.
//! unsafe { bitacora_client_free(client) };
//! ```
//!
//! # Invariants
//!
//! - The work of a call runs on the client's threads. The calling thread only waits for it.
//! - A runtime is never dropped from one of its own threads: handles are freed by C, on C's threads, and
//!   no task holds a share of the core.
//! - No call into this library is ever made from a runtime thread, because the library never calls back
//!   into the caller's code.
//! - A client must not be used in a child process after `fork`: the child has the handles but none of the
//!   threads. Connect again in the child.
//!
//! # See also
//!
//! - `writer.rs`, `reader.rs`, `admin.rs`: what is opened from a client.
//! - `src/rust/bitacora/src/cluster.rs`: the cluster configuration and what connecting does.

use std::ffi::c_char;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use bitacora::{BitacoraClient, ClientSettings, ClusterConfig};
use tokio::runtime::{Builder, Runtime};
use tokio::task::JoinError;

use crate::arguments::{
    OutputSlot, SizedStructure, free_handle, into_handle_pointer, output_slot, read_sized_structure, text_argument,
};
use crate::guard::run_guarded;
use crate::status::{BitacoraStatus, Failure};

/// How many threads run a client's work when the caller does not say.
///
/// The work is waiting on the network, not computing, so two threads serve many streams; the second one
/// keeps heartbeats going while the first is busy.
const DEFAULT_WORKER_THREADS: u32 = 2;

/// The most threads a caller may ask for. More than this is a mistake, not a tuning choice.
const MAX_WORKER_THREADS: u32 = 256;

/// The name of the threads that run a client's work, as a debugger or `top` shows it.
const WORKER_THREAD_NAME: &str = "bitacora-client";

/// Pass this as a timeout to wait for as long as the call takes.
pub const BITACORA_WAIT_WITHOUT_LIMIT: u64 = 0;

/// How a client runs. Pass a null pointer to take every default.
///
/// Set `struct_size` to `sizeof(bitacora_client_options)` and every field that is not wanted to zero. The
/// size lets a later version add fields without breaking programs compiled against this one.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct ClientOptions {
    /// `sizeof(bitacora_client_options)`, as the caller's compiler knows the structure.
    pub struct_size: u32,
    /// How many threads run the client's work in the background. 0 takes the default, which is 2.
    pub worker_threads: u32,
    /// A name for this program's writers, recorded with their lease so that an operator can see who holds
    /// a stream. A NUL-terminated UTF-8 string; it is copied, so it need not outlive the call. Null takes
    /// the default, `bitacora-writer`.
    pub writer_identity: *const c_char,
}

// SAFETY: `ClientOptions` is `#[repr(C)]`, begins with the `u32` size, consists of two `u32` and one raw
// pointer, which leave no padding on any target, and every byte pattern is a valid value of each.
unsafe impl SizedStructure for ClientOptions {
    fn with_defaults() -> Self {
        Self {
            struct_size: 0,
            worker_threads: 0,
            writer_identity: std::ptr::null(),
        }
    }
}

/// The Rust client together with the threads that run it.
///
/// Every handle opened from one client holds a share of this, so the threads live as long as anything
/// that needs them.
pub(crate) struct ClientCore {
    /// The client. Declared before the runtime so that it is dropped first, while the runtime that its
    /// connections and tasks live on still exists.
    client: BitacoraClient,
    /// The threads that run the client's asynchronous work.
    runtime: Runtime,
}

/// Runs `work` as a task on the threads of `runtime` and blocks the calling thread until it is done.
///
/// # Arguments
///
/// * `runtime` - The threads that do the work.
/// * `work` - The asynchronous work of one call. It owns everything it uses, because it runs elsewhere.
///
/// # Returns
///
/// What the work produced.
///
/// # Panics
///
/// Panics with the work's own panic if the work panicked, so that the boundary of the entry point reports
/// it like any other. Panics if called from one of the runtime's own threads, which cannot happen
/// through the C interface: it never runs the caller's code on them.
fn run_on<Work>(runtime: &Runtime, work: Work) -> Work::Output
where
    Work: Future + Send + 'static,
    Work::Output: Send + 'static,
{
    let running_work = runtime.spawn(work);
    match runtime.block_on(running_work) {
        Ok(outcome) => outcome,
        Err(join_error) => continue_panic_of(join_error),
    }
}

/// Carries on, on the calling thread, the panic that ended a task.
///
/// # Panics
///
/// Always. With the task's own panic if it panicked; otherwise because the task was cancelled, which only
/// a runtime that is shutting down does, and a runtime does not shut down while a handle is in use.
fn continue_panic_of(join_error: JoinError) -> ! {
    match join_error.try_into_panic() {
        Ok(panic_payload) => std::panic::resume_unwind(panic_payload),
        Err(cancelled) => panic!("the client's threads stopped while a call was waiting for them: {cancelled}"),
    }
}

impl ClientCore {
    /// The Rust client, to open writers, readers and the stream administration from.
    pub(crate) const fn client(&self) -> &BitacoraClient {
        &self.client
    }

    /// Runs `work` on the client's threads and blocks the calling thread until it is done.
    ///
    /// # Arguments
    ///
    /// * `work` - The asynchronous work of one call. It owns everything it uses, because it runs on
    ///   another thread.
    ///
    /// # Returns
    ///
    /// What the work produced.
    ///
    /// # Panics
    ///
    /// Panics with the work's own panic if the work panicked.
    pub(crate) fn wait_for<Work>(&self, work: Work) -> Work::Output
    where
        Work: Future + Send + 'static,
        Work::Output: Send + 'static,
    {
        run_on(&self.runtime, work)
    }

    /// Runs `work` like [`ClientCore::wait_for`], but gives up waiting after `timeout_milliseconds`.
    ///
    /// Giving up drops the work where it stands. Whether that undoes anything is the business of the
    /// work: an append that was already queued stays queued.
    ///
    /// # Arguments
    ///
    /// * `timeout_milliseconds` - The longest to wait, or [`BITACORA_WAIT_WITHOUT_LIMIT`].
    /// * `work` - The asynchronous work of one call.
    ///
    /// # Returns
    ///
    /// `Some` with what the work produced, or `None` if the wait ended first.
    pub(crate) fn wait_at_most<Work>(&self, timeout_milliseconds: u64, work: Work) -> Option<Work::Output>
    where
        Work: Future + Send + 'static,
        Work::Output: Send + 'static,
    {
        if timeout_milliseconds == BITACORA_WAIT_WITHOUT_LIMIT {
            return Some(self.wait_for(work));
        }
        let longest_wait = Duration::from_millis(timeout_milliseconds);
        self.wait_for(async move { tokio::time::timeout(longest_wait, work).await.ok() })
    }
}

/// A connection to one bitacora cluster.
///
/// Every function that takes a client may be called from several threads at once with the same client.
pub struct ClientHandle {
    /// The client and its threads, shared with everything opened from this handle.
    core: Arc<ClientCore>,
}

impl ClientHandle {
    /// A share of the client and its threads, for a handle that is opened from this one.
    pub(crate) fn core(&self) -> Arc<ClientCore> {
        Arc::clone(&self.core)
    }
}

/// Starts a client for the cluster that `configuration_text` describes.
///
/// # Errors
///
/// Returns an invalid-argument [`Failure`] for a configuration that is not valid or can never work and
/// for options out of range, and an unavailable one if the threads or the object store cannot be opened.
fn connect(
    configuration_text: &str,
    options: ClientOptions,
    writer_identity: Option<&str>,
) -> Result<ClientHandle, Failure> {
    let cluster_config: ClusterConfig = match toml::from_str(configuration_text) {
        Ok(cluster_config) => cluster_config,
        Err(parse_error) => {
            return Err(Failure::invalid_argument(format!(
                "the cluster configuration is not valid: {parse_error}"
            )));
        }
    };
    if options.worker_threads > MAX_WORKER_THREADS {
        return Err(Failure::invalid_argument(format!(
            "options.worker_threads is {}; at most {MAX_WORKER_THREADS} are allowed",
            options.worker_threads
        )));
    }
    let worker_threads = if options.worker_threads == 0 {
        DEFAULT_WORKER_THREADS
    } else {
        options.worker_threads
    };
    let mut settings = ClientSettings::default();
    if let Some(writer_identity) = writer_identity {
        writer_identity.clone_into(&mut settings.writer.writer_identity);
    }

    let built_runtime = Builder::new_multi_thread()
        .worker_threads(usize::try_from(worker_threads).expect("at most 256 threads"))
        .thread_name(WORKER_THREAD_NAME)
        .enable_all()
        .build();
    let runtime = match built_runtime {
        Ok(runtime) => runtime,
        Err(io_error) => {
            return Err(Failure::new(
                BitacoraStatus::Unavailable,
                format!("cannot start the client's threads: {io_error}"),
            ));
        }
    };
    let client = run_on(
        &runtime,
        async move { bitacora::connect(&cluster_config, settings).await },
    )?;
    Ok(ClientHandle {
        core: Arc::new(ClientCore { client, runtime }),
    })
}

/// Reads the options and their text, starts the client and hands it to the caller.
///
/// # Safety
///
/// `options` must be null or point to a `bitacora_client_options` whose `struct_size` is set and whose
/// `writer_identity` is null or a NUL-terminated string.
unsafe fn connect_and_hand_out(
    configuration_text: &str,
    options: *const ClientOptions,
    client_out: OutputSlot<*mut ClientHandle>,
) -> Result<(), Failure> {
    // SAFETY: the caller guarantees that options which are not null are a sized structure.
    let options = unsafe { read_sized_structure(options, "options") }?;
    // SAFETY: the caller guarantees that an identity which is not null is a NUL-terminated string.
    let writer_identity =
        unsafe { crate::arguments::optional_text_argument(options.writer_identity, "options.writer_identity") }?;
    let client = connect(configuration_text, options, writer_identity)?;
    client_out.write(into_handle_pointer(client));
    Ok(())
}

/// Connects to the cluster described by a configuration file.
///
/// The file is the cluster configuration every bitacora program reads, in TOML: the etcd endpoints, the
/// key prefix and the object store. For example:
///
/// ```toml
/// etcd_endpoints = ["http://10.0.0.1:2379", "http://10.0.0.2:2379", "http://10.0.0.3:2379"]
///
/// [object_store]
/// kind = "local_directory"
/// path = "/var/lib/bitacora/objects"
/// ```
///
/// Connecting starts the client's threads and sends nothing: a cluster that is down is not an error
/// here. Calls fail with `BITACORA_STATUS_UNAVAILABLE` until it is back.
///
/// # Arguments
///
/// * `configuration_path` - The path of the configuration file, NUL-terminated UTF-8.
/// * `options` - How the client runs, or null for every default.
/// * `client_out` - Where the new client is written. It is set to null if the call fails. Free the client
///   with `bitacora_client_free`.
///
/// # Returns
///
/// `BITACORA_STATUS_OK`, or:
///
/// * `BITACORA_STATUS_INVALID_ARGUMENT` - A pointer is null, the file cannot be read, it is not a valid
///   cluster configuration, or an option is out of range.
/// * `BITACORA_STATUS_UNAVAILABLE` - The threads could not be started, or the object store could not be
///   opened.
///
/// # Safety
///
/// `configuration_path` must be null or a NUL-terminated string. `options` must be null or point to a
/// `bitacora_client_options` with `struct_size` set. `client_out` must be null or point to a pointer that
/// can be written.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn bitacora_client_connect_file(
    configuration_path: *const c_char,
    options: *const ClientOptions,
    client_out: *mut *mut ClientHandle,
) -> BitacoraStatus {
    run_guarded(|| {
        // SAFETY: the caller guarantees that `client_out` is null or a writable pointer.
        let client_out = unsafe { output_slot(client_out, "client_out") }?;
        client_out.write(std::ptr::null_mut());
        // SAFETY: the caller guarantees that the path is null or a NUL-terminated string.
        let configuration_path = unsafe { text_argument(configuration_path, "configuration_path") }?;
        // This crate is the edge of the process: reading the file the caller names is its job.
        let configuration_text = match std::fs::read_to_string(configuration_path) {
            Ok(configuration_text) => configuration_text,
            Err(io_error) => {
                return Err(Failure::invalid_argument(format!(
                    "cannot read the cluster configuration {configuration_path:?}: {io_error}"
                )));
            }
        };
        // SAFETY: the caller guarantees what `connect_and_hand_out` asks of `options`.
        unsafe { connect_and_hand_out(&configuration_text, options, client_out) }
    })
}

/// Connects to the cluster described by configuration text.
///
/// The same as `bitacora_client_connect_file`, for a program that keeps its settings elsewhere and builds
/// the TOML itself. The smallest configuration is one line:
///
/// ```toml
/// etcd_endpoints = ["http://127.0.0.1:2379"]
/// ```
///
/// # Arguments
///
/// * `configuration_text` - The cluster configuration in TOML, NUL-terminated UTF-8. It is copied.
/// * `options` - How the client runs, or null for every default.
/// * `client_out` - Where the new client is written. It is set to null if the call fails. Free the client
///   with `bitacora_client_free`.
///
/// # Returns
///
/// `BITACORA_STATUS_OK`, or:
///
/// * `BITACORA_STATUS_INVALID_ARGUMENT` - A pointer is null, the text is not a valid cluster
///   configuration, or an option is out of range.
/// * `BITACORA_STATUS_UNAVAILABLE` - The threads could not be started, or the object store could not be
///   opened.
///
/// # Safety
///
/// `configuration_text` must be null or a NUL-terminated string. `options` must be null or point to a
/// `bitacora_client_options` with `struct_size` set. `client_out` must be null or point to a pointer that
/// can be written.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn bitacora_client_connect_text(
    configuration_text: *const c_char,
    options: *const ClientOptions,
    client_out: *mut *mut ClientHandle,
) -> BitacoraStatus {
    run_guarded(|| {
        // SAFETY: the caller guarantees that `client_out` is null or a writable pointer.
        let client_out = unsafe { output_slot(client_out, "client_out") }?;
        client_out.write(std::ptr::null_mut());
        // SAFETY: the caller guarantees that the text is null or a NUL-terminated string.
        let configuration_text = unsafe { text_argument(configuration_text, "configuration_text") }?;
        // SAFETY: the caller guarantees what `connect_and_hand_out` asks of `options`.
        unsafe { connect_and_hand_out(configuration_text, options, client_out) }
    })
}

/// Frees a client.
///
/// Writers, readers and followers opened from it keep working: they hold what they need, and the
/// client's threads stop when the last of them is freed. A null pointer is ignored.
///
/// # Arguments
///
/// * `client` - The client to free, or null.
///
/// # Safety
///
/// `client` must be null or a client returned by this library that has not been freed. No other call may
/// be using it, and it must not be used afterwards.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn bitacora_client_free(client: *mut ClientHandle) {
    crate::guard::run_guarded_value((), || {
        // SAFETY: the caller guarantees what `free_handle` asks for.
        unsafe { free_handle(client) };
    });
}

#[cfg(test)]
#[path = "client_test.rs"]
mod tests;
