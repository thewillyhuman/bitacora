//! The metrics registry of a daemon and the metric types daemons record with.
//!
//! # Why this file exists
//!
//! Every bitacora process exposes Prometheus metrics (design, section 14, "Observability"). Metric names,
//! labels and units are an interface: dashboards and alert rules are written against them and break when
//! they change. This file is where the naming scheme is enforced and where the metrics library is kept
//! behind a small surface, so that no other crate depends on it and a metric cannot be registered with a
//! name that does not follow the scheme.
//!
//! # What it contains
//!
//! - [`MetricsRegistry`]: every metric of one process, and their text exposition.
//! - [`Counter`], [`Gauge`], [`DurationHistogram`]: label-free metrics a daemon registers for itself.
//! - `RequestMetrics`: the three metric families every instrumented interface records, used by the
//!   decorators of this crate.
//!
//! # How it works
//!
//! The naming scheme, which `docs/operations/metrics.md` lists metric by metric:
//!
//! - Every name starts with `bitacora_`, then the subsystem (`disk`, `replica`, `metadata_store`,
//!   `object_store`, `controller`), then what is measured.
//! - Units are base units and part of the name: `_seconds`, `_bytes`.
//! - Counters end in `_total`. The metrics library appends that suffix, and the unit, when it writes the
//!   exposition, so the names passed to this file's functions carry neither.
//! - A label is a small closed set of values known at compile time: an operation, an error kind, a
//!   direction. Nothing a client chooses (stream names, keys, addresses) is ever a label, so the number of
//!   time series of a process is bounded.
//!
//! Every interface that a daemon reaches the outside world through (replica requests, the metadata
//! store, the object store) records the same three families, named after the subsystem:
//!
//! - `bitacora_<subsystem>_requests_total{operation}`
//! - `bitacora_<subsystem>_request_duration_seconds{operation}`
//! - `bitacora_<subsystem>_request_failures_total{operation,error}`
//!
//! Durations are histograms with buckets from 100 microseconds to about 100 seconds, doubling each time.
//! That one set covers a disk flush as well as the upload of a segment.
//!
//! # Invariants
//!
//! - Recording a metric never blocks on I/O and never fails: it is a few atomic operations.
//! - A metric that was registered is exposed for the life of the process, also while its value is zero.
//!
//! # See also
//!
//! - `docs/operations/metrics.md`: the reference of every metric, for people who write dashboards.
//! - `http_endpoint.rs`: serves the exposition at `/metrics`.
//! - `instrumented_*.rs`: the decorators that record request metrics.

use std::future::Future;
use std::sync::{Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use prometheus_client::encoding::text::encode;
use prometheus_client::metrics::counter::Counter as LibraryCounter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge as LibraryGauge;
use prometheus_client::metrics::histogram::{Histogram, exponential_buckets};
use prometheus_client::registry::{Metric, Registry, Unit};
use tokio::time::Instant;

/// The prefix of every metric name.
const METRIC_NAME_PREFIX: &str = "bitacora";

/// The upper bound of the first duration bucket: 100 microseconds, about the fastest disk flush.
const SHORTEST_DURATION_BUCKET_SECONDS: f64 = 0.000_1;

/// How much wider each duration bucket is than the one before.
const DURATION_BUCKET_FACTOR: f64 = 2.0;

/// How many duration buckets there are. Twenty-one doublings from 100 microseconds end at about 105
/// seconds, beyond the longest request any daemon waits for.
const DURATION_BUCKET_COUNT: u16 = 21;

/// The label that names the operation of a request, for example `append` or `get_range`.
const OPERATION_LABEL: &str = "operation";

/// The label that names the kind of a failure, for example `unavailable`.
const ERROR_LABEL: &str = "error";

/// The label set of a metric that is split by operation.
type OperationLabels = [(&'static str, &'static str); 1];

/// The label set of a failure counter: the operation that failed and the kind of the failure.
type FailureLabels = [(&'static str, &'static str); 2];

/// A family of duration histograms that all use the same buckets.
type DurationFamily<Labels> = Family<Labels, Histogram, fn() -> Histogram>;

/// Creates an empty histogram with the duration buckets described in the file header.
fn new_duration_histogram() -> Histogram {
    Histogram::new(exponential_buckets(
        SHORTEST_DURATION_BUCKET_SECONDS,
        DURATION_BUCKET_FACTOR,
        DURATION_BUCKET_COUNT,
    ))
}

/// Every metric of one process.
///
/// A daemon creates one registry at start, hands it to the decorators and to its own metrics, and serves
/// it with [`crate::serve_observability`].
///
/// # Examples
///
/// ```
/// use bitacora_process::MetricsRegistry;
///
/// let registry = MetricsRegistry::new();
/// let rounds = registry.counter("controller_rounds", "Rounds of the controller's loop");
/// rounds.add(1);
///
/// assert!(registry.encode().contains("bitacora_controller_rounds_total 1"));
/// ```
#[derive(Debug)]
pub struct MetricsRegistry {
    /// The metrics library's registry. Behind a lock because registering needs exclusive access, while
    /// metrics are registered from several places during start.
    registry: Mutex<Registry>,
}

impl Default for MetricsRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl MetricsRegistry {
    /// Creates a registry without metrics. Every metric registered in it is named `bitacora_...`.
    #[must_use]
    pub fn new() -> Self {
        Self {
            registry: Mutex::new(Registry::with_prefix(METRIC_NAME_PREFIX)),
        }
    }

    /// Registers a counter: a number that only goes up, such as how many rounds were run.
    ///
    /// # Arguments
    ///
    /// * `name` - The name without the `bitacora_` prefix and without `_total`, for example
    ///   `controller_rounds`.
    /// * `help` - One sentence, without a full stop, that says what is counted.
    ///
    /// # Returns
    ///
    /// The counter, exposed as `bitacora_<name>_total`.
    pub fn counter(&self, name: &str, help: &str) -> Counter {
        let counter = LibraryCounter::default();
        self.lock().register(name, help, counter.clone());
        Counter { counter }
    }

    /// Registers a gauge: a number that goes up and down, such as how many segments miss copies.
    ///
    /// # Arguments
    ///
    /// * `name` - The name without the `bitacora_` prefix, for example `controller_leading`.
    /// * `help` - One sentence, without a full stop, that says what the value is.
    ///
    /// # Returns
    ///
    /// The gauge, exposed as `bitacora_<name>`.
    pub fn gauge(&self, name: &str, help: &str) -> Gauge {
        let gauge = LibraryGauge::default();
        self.lock().register(name, help, gauge.clone());
        Gauge { gauge }
    }

    /// Registers a gauge whose value is a number of bytes.
    ///
    /// # Arguments
    ///
    /// * `name` - The name without the `bitacora_` prefix and without `_bytes`, for example `disk_free`.
    /// * `help` - One sentence, without a full stop, that says what the value is.
    ///
    /// # Returns
    ///
    /// The gauge, exposed as `bitacora_<name>_bytes`.
    pub fn bytes_gauge(&self, name: &str, help: &str) -> Gauge {
        let gauge = LibraryGauge::default();
        self.lock()
            .register_with_unit(name, help, Unit::Bytes, gauge.clone());
        Gauge { gauge }
    }

    /// Registers a counter of bytes.
    ///
    /// # Arguments
    ///
    /// * `name` - The name without the `bitacora_` prefix and without `_bytes_total`, for example
    ///   `disk_written`.
    /// * `help` - One sentence, without a full stop, that says which bytes are counted.
    ///
    /// # Returns
    ///
    /// The counter, exposed as `bitacora_<name>_bytes_total`.
    pub fn bytes_counter(&self, name: &str, help: &str) -> Counter {
        let counter = LibraryCounter::default();
        self.lock()
            .register_with_unit(name, help, Unit::Bytes, counter.clone());
        Counter { counter }
    }

    /// Registers a histogram of durations, with the buckets described in the file header.
    ///
    /// # Arguments
    ///
    /// * `name` - The name without the `bitacora_` prefix and without `_seconds`, for example
    ///   `controller_pass_duration`.
    /// * `help` - One sentence, without a full stop, that says what takes that long.
    ///
    /// # Returns
    ///
    /// The histogram, exposed as `bitacora_<name>_seconds`.
    pub fn duration_histogram(&self, name: &str, help: &str) -> DurationHistogram {
        let histogram = new_duration_histogram();
        self.lock()
            .register_with_unit(name, help, Unit::Seconds, histogram.clone());
        DurationHistogram { histogram }
    }

    /// Registers a metric of the metrics library as it is: a family split by labels, for the decorators
    /// of this crate. The name follows the same rules as for [`MetricsRegistry::counter`].
    pub(crate) fn register_family(&self, name: &str, help: &str, family: impl Metric) {
        self.lock().register(name, help, family);
    }

    /// Like [`MetricsRegistry::register_family`], for a family that counts bytes. The name follows the
    /// same rules as for [`MetricsRegistry::bytes_counter`].
    pub(crate) fn register_bytes_family(&self, name: &str, help: &str, family: impl Metric) {
        self.lock()
            .register_with_unit(name, help, Unit::Bytes, family);
    }

    /// Writes every registered metric in the `OpenMetrics` text format, which Prometheus scrapes.
    ///
    /// # Returns
    ///
    /// The exposition: one `# HELP` and one `# TYPE` line per metric, one line per time series, and a
    /// final `# EOF` line.
    #[must_use]
    pub fn encode(&self) -> String {
        let mut exposition = String::new();
        encode(&mut exposition, &self.lock())
            .expect("writing to a string cannot fail, and no metric of this crate fails to encode");
        exposition
    }

    /// Locks the library's registry. A panic while it was held cannot leave it half-changed in a way
    /// that matters: registering only appends.
    fn lock(&self) -> MutexGuard<'_, Registry> {
        self.registry.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// A number that only goes up. Clones share the same value.
#[derive(Clone, Debug)]
pub struct Counter {
    /// The library's counter.
    counter: LibraryCounter,
}

impl Counter {
    /// Adds `amount` to the counter.
    pub fn add(&self, amount: u64) {
        self.counter.inc_by(amount);
    }

    /// The counter's current value.
    #[must_use]
    pub fn value(&self) -> u64 {
        self.counter.get()
    }
}

/// A number that goes up and down. Clones share the same value.
#[derive(Clone, Debug)]
pub struct Gauge {
    /// The library's gauge.
    gauge: LibraryGauge,
}

impl Gauge {
    /// Sets the gauge to `value`.
    pub fn set(&self, value: i64) {
        self.gauge.set(value);
    }

    /// Sets the gauge to a count or a size. A value beyond what a gauge can hold is stored as the largest
    /// value it can hold, which no real count or size reaches.
    pub fn set_unsigned(&self, value: u64) {
        self.gauge.set(i64::try_from(value).unwrap_or(i64::MAX));
    }

    /// Sets the gauge to 1 for `true` and 0 for `false`, the convention for a state that holds or not.
    pub fn set_flag(&self, is_set: bool) {
        self.gauge.set(i64::from(is_set));
    }

    /// The gauge's current value.
    #[must_use]
    pub fn value(&self) -> i64 {
        self.gauge.get()
    }
}

/// A distribution of durations. Clones share the same observations.
#[derive(Clone, Debug)]
pub struct DurationHistogram {
    /// The library's histogram, which holds seconds.
    histogram: Histogram,
}

impl DurationHistogram {
    /// Records that something took `duration`.
    pub fn observe(&self, duration: Duration) {
        self.histogram.observe(duration.as_secs_f64());
    }
}

/// The metrics every instrumented interface records: how many requests of each operation it served, how
/// long they took, and how many failed with which kind of error.
///
/// Clones share the same metrics.
#[derive(Clone, Debug)]
pub(crate) struct RequestMetrics {
    /// Requests by operation, counted when they finish.
    requests: Family<OperationLabels, LibraryCounter>,
    /// How long requests took, by operation, failed ones included.
    durations: DurationFamily<OperationLabels>,
    /// Failed requests by operation and kind of failure.
    failures: Family<FailureLabels, LibraryCounter>,
}

impl RequestMetrics {
    /// Registers the three request families of one subsystem.
    ///
    /// # Arguments
    ///
    /// * `registry` - The registry of the process.
    /// * `subsystem` - The part of the metric names after `bitacora_`: `replica`, `metadata_store` or
    ///   `object_store`.
    /// * `description` - What the requests are, for the help texts, for example `Requests to the replica`.
    pub(crate) fn register(registry: &MetricsRegistry, subsystem: &str, description: &str) -> Self {
        let requests = Family::<OperationLabels, LibraryCounter>::default();
        let durations =
            DurationFamily::<OperationLabels>::new_with_constructor(new_duration_histogram);
        let failures = Family::<FailureLabels, LibraryCounter>::default();
        let mut library_registry = registry.lock();
        library_registry.register(
            format!("{subsystem}_requests"),
            format!("{description}, by operation"),
            requests.clone(),
        );
        library_registry.register_with_unit(
            format!("{subsystem}_request_duration"),
            format!("How long they took: {description}, by operation"),
            Unit::Seconds,
            durations.clone(),
        );
        library_registry.register(
            format!("{subsystem}_request_failures"),
            format!("{description} that failed, by operation and kind of error"),
            failures.clone(),
        );
        Self {
            requests,
            durations,
            failures,
        }
    }

    /// Runs one request and records its outcome.
    ///
    /// The duration is measured from the first poll of `request` to its completion, on the runtime's
    /// clock, so it is virtual in tests that pause time.
    ///
    /// # Arguments
    ///
    /// * `operation` - The value of the `operation` label, for example `append`.
    /// * `request` - The call to the wrapped interface.
    /// * `kind_of_failure` - Names the kind of a failure, for the `error` label.
    ///
    /// # Returns
    ///
    /// What `request` returned, unchanged.
    pub(crate) async fn observe<Response, Failure>(
        &self,
        operation: &'static str,
        request: impl Future<Output = Result<Response, Failure>>,
        kind_of_failure: fn(&Failure) -> &'static str,
    ) -> Result<Response, Failure> {
        let started_at = Instant::now();
        let outcome = request.await;
        let operation_labels = [(OPERATION_LABEL, operation)];
        self.requests.get_or_create(&operation_labels).inc();
        self.durations
            .get_or_create(&operation_labels)
            .observe(started_at.elapsed().as_secs_f64());
        if let Err(failure) = &outcome {
            let failure_labels = [
                (OPERATION_LABEL, operation),
                (ERROR_LABEL, kind_of_failure(failure)),
            ];
            self.failures.get_or_create(&failure_labels).inc();
        }
        outcome
    }
}

#[cfg(test)]
#[path = "metrics_test.rs"]
mod tests;
