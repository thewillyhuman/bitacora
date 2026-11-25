//! Keeping the metrics reference true.
//!
//! # Why this file exists
//!
//! `docs/operations/metrics.md` lists every metric a daemon exposes, and people write dashboards and
//! alert rules from it. A reference that is maintained by hand next to the code falls behind the first
//! time somebody adds a metric in a hurry. This file is the check that makes that a test failure: every
//! crate that registers metrics compares what it really exposes with what the reference lists.
//!
//! # What it contains
//!
//! - [`undocumented_metrics`]: the metrics of an exposition that a reference text does not mention.
//!
//! # How it works
//!
//! The exposition has one `# TYPE <name> <type>` line per registered metric, whether or not it has a
//! value yet. The name people query differs from the one on that line for counters, whose samples end in
//! `_total`, so that suffix is added back. A metric counts as documented if the reference contains its
//! queried name between backticks, which is how the reference writes every name.
//!
//! The check goes one way only: a name in the reference that no code exposes is not found, because no
//! single crate exposes all of them.
//!
//! # Invariants
//!
//! - Pure: it reads no file. The caller reads the reference, since where it lies depends on the build.
//!
//! # See also
//!
//! - `metrics.rs`: where metrics are registered and exposed.
//! - `docs/operations/metrics.md`: the reference.

/// The start of the exposition line that declares a metric and its type.
const TYPE_LINE_PREFIX: &str = "# TYPE ";

/// The suffix the exposition format adds to the samples of a counter.
const COUNTER_SUFFIX: &str = "_total";

/// Lists the metrics of `exposition` that `reference` does not mention.
///
/// # Arguments
///
/// * `exposition` - What [`crate::MetricsRegistry::encode`] returned, after every metric of interest
///   was registered.
/// * `reference` - The text of the metrics reference.
///
/// # Returns
///
/// The names as they are queried (counters with `_total`), in the order of the exposition. Empty when
/// the reference mentions every one of them between backticks.
///
/// # Examples
///
/// ```
/// use bitacora_process::{MetricsRegistry, undocumented_metrics};
///
/// let registry = MetricsRegistry::new();
/// let _rounds = registry.counter("controller_rounds", "Rounds of the controller's loop");
/// let _leading = registry.gauge("controller_leading", "Whether this controller leads");
/// let reference = "| `bitacora_controller_rounds_total` | counter | Rounds of the loop |";
///
/// assert_eq!(
///     undocumented_metrics(&registry.encode(), reference),
///     ["bitacora_controller_leading"]
/// );
/// ```
#[must_use]
pub fn undocumented_metrics(exposition: &str, reference: &str) -> Vec<String> {
    let mut undocumented_names = Vec::new();
    for line in exposition.lines() {
        let Some(declaration) = line.strip_prefix(TYPE_LINE_PREFIX) else {
            continue;
        };
        let mut declaration_words = declaration.split_whitespace();
        let (Some(declared_name), Some(metric_type)) = (declaration_words.next(), declaration_words.next()) else {
            continue;
        };
        let queried_name = if metric_type == "counter" {
            format!("{declared_name}{COUNTER_SUFFIX}")
        } else {
            declared_name.to_owned()
        };
        if !reference.contains(&format!("`{queried_name}`")) {
            undocumented_names.push(queried_name);
        }
    }
    undocumented_names
}

#[cfg(test)]
#[path = "metrics_reference_test.rs"]
mod tests;
