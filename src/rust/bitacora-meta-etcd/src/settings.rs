//! What a store needs to know to reach its etcd cluster.
//!
//! # Why this file exists
//!
//! Every process that opens the metadata store (replica, controller, client
//! library) has to say where etcd is and how long it is willing to wait for
//! it. Collecting those values in one plain type keeps them out of function
//! signatures and gives each one a single documented meaning and default.
//!
//! # What it contains
//!
//! - [`EtcdSettings`]: endpoints, key prefix and the two timeouts.
//! - [`InvalidEtcdSettings`]: why a set of values was refused.
//!
//! # How it works
//!
//! The settings are plain data with public fields. [`EtcdSettings::new`]
//! fills in the defaults, and a caller overrides the fields it cares about.
//! Nothing is checked until [`crate::EtcdKeyValueStore::connect`] is called;
//! that is the one place where a wrong value is reported, so a caller cannot
//! forget to validate.
//!
//! # See also
//!
//! - `store.rs`: the store that is built from these settings.

use std::time::Duration;

use thiserror::Error;

/// The key prefix used unless the caller chooses another one.
const DEFAULT_KEY_PREFIX: &str = "/bitacora/";

/// How long one connection attempt may take by default. The same value as
/// the dial timeout of etcd's own command line client.
const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(2);

/// How long one request may take by default. The same value as the command
/// timeout of etcd's own command line client.
const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(5);

/// Where the etcd cluster is and how long to wait for it.
///
/// Build one with [`EtcdSettings::new`] and change the public fields that
/// should differ from the defaults. The values are checked when they are
/// handed to [`crate::EtcdKeyValueStore::connect`].
///
/// # Examples
///
/// ```
/// use std::time::Duration;
///
/// use bitacora_meta_etcd::EtcdSettings;
///
/// let mut settings = EtcdSettings::new(vec!["http://127.0.0.1:2379".to_owned()]);
/// settings.key_prefix = "/bitacora-staging/".to_owned();
/// settings.request_timeout = Duration::from_secs(2);
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EtcdSettings {
    /// The client URLs of the etcd members, for example
    /// `http://10.0.0.1:2379`. At least one is required. Requests are spread
    /// over the members that are reachable, so listing all of them lets the
    /// store survive the loss of one.
    pub endpoints: Vec<String>,

    /// The text put in front of every key before it reaches etcd, and
    /// removed again from every key that comes back. Two stores with
    /// different prefixes share one etcd cluster without seeing each other,
    /// provided neither prefix is the beginning of the other: end it with a
    /// `/`. An empty prefix means the store owns the whole key space.
    pub key_prefix: String,

    /// The longest one attempt to open a connection to an endpoint may take.
    /// Must be longer than zero.
    pub connect_timeout: Duration,

    /// The longest a `get`, `list` or `transact` may take, connecting
    /// included, before it fails as unavailable. Must be longer than zero.
    pub request_timeout: Duration,
}

impl EtcdSettings {
    /// Returns settings for the given endpoints with every other value at
    /// its default: key prefix `/bitacora/`, two seconds to connect and five
    /// seconds per request.
    ///
    /// # Arguments
    ///
    /// * `endpoints` - The client URLs of the etcd members. The list is
    ///   stored as given; an empty one is refused later, by
    ///   [`crate::EtcdKeyValueStore::connect`].
    #[must_use]
    pub fn new(endpoints: Vec<String>) -> Self {
        Self {
            endpoints,
            key_prefix: DEFAULT_KEY_PREFIX.to_owned(),
            connect_timeout: DEFAULT_CONNECT_TIMEOUT,
            request_timeout: DEFAULT_REQUEST_TIMEOUT,
        }
    }

    /// Checks the values that this crate can judge without calling etcd.
    ///
    /// The endpoints themselves are parsed by the etcd client when the store
    /// connects, so only their presence is checked here.
    ///
    /// # Errors
    ///
    /// Fails with [`InvalidEtcdSettings`] when there is no endpoint or when
    /// one of the timeouts is zero, which would make every request fail.
    pub(crate) fn validate(&self) -> Result<(), InvalidEtcdSettings> {
        if self.endpoints.is_empty() {
            return Err(InvalidEtcdSettings::because("there is no endpoint"));
        }
        if self.connect_timeout.is_zero() {
            return Err(InvalidEtcdSettings::because("the connect timeout is zero"));
        }
        if self.request_timeout.is_zero() {
            return Err(InvalidEtcdSettings::because("the request timeout is zero"));
        }
        Ok(())
    }
}

/// The settings handed to [`crate::EtcdKeyValueStore::connect`] cannot work.
///
/// This is a configuration mistake, not an outage: retrying with the same
/// settings fails the same way, so a process should report it and stop.
#[derive(Clone, Debug, PartialEq, Eq, Error)]
#[error("etcd settings are invalid: {reason}")]
pub struct InvalidEtcdSettings {
    /// Which value is wrong and why, for the operator who has to fix it.
    pub reason: String,
}

impl InvalidEtcdSettings {
    /// Builds the error from the reason the settings were refused.
    pub(crate) fn because(reason: impl Into<String>) -> Self {
        Self { reason: reason.into() }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Returns settings that are valid, for a test to break in one place.
    fn settings() -> EtcdSettings {
        EtcdSettings::new(vec!["http://127.0.0.1:2379".to_owned()])
    }

    #[test]
    fn default_settings_are_valid() {
        assert_eq!(settings().validate(), Ok(()));
    }

    #[test]
    fn settings_without_endpoints_are_refused() {
        let settings = EtcdSettings::new(Vec::new());

        let verdict = settings.validate();

        assert_eq!(verdict, Err(InvalidEtcdSettings::because("there is no endpoint")));
    }

    #[test]
    fn zero_connect_timeout_is_refused() {
        let mut settings = settings();
        settings.connect_timeout = Duration::ZERO;

        let verdict = settings.validate();

        assert_eq!(
            verdict,
            Err(InvalidEtcdSettings::because("the connect timeout is zero"))
        );
    }

    #[test]
    fn zero_request_timeout_is_refused() {
        let mut settings = settings();
        settings.request_timeout = Duration::ZERO;

        let verdict = settings.validate();

        assert_eq!(
            verdict,
            Err(InvalidEtcdSettings::because("the request timeout is zero"))
        );
    }
}
