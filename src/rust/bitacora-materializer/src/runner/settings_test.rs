//! Tests of the default pacing of a runner.
//!
//! # What is covered
//!
//! - The defaults of [`RunnerSettings::for_materializer`], one test per value: they are what an operator gets
//!   without configuring anything, and the design names two of them.
//!
//! # How the fixtures work
//!
//! There are none: the settings are a plain value.

use super::*;

#[test]
fn default_settings_carry_the_name_they_were_asked_for() {
    let settings = RunnerSettings::for_materializer("word-count");

    assert_eq!(settings.materializer, "word-count");
}

#[test]
fn default_checkpoint_comes_after_64_mebibytes() {
    let settings = RunnerSettings::for_materializer("word-count");

    assert_eq!(settings.checkpoint_after_bytes, 64 * 1024 * 1024);
}

#[test]
fn default_checkpoint_comes_after_a_minute() {
    let settings = RunnerSettings::for_materializer("word-count");

    assert_eq!(settings.checkpoint_after, Duration::from_secs(60));
}

#[test]
fn default_retry_comes_after_a_second() {
    let settings = RunnerSettings::for_materializer("word-count");

    assert_eq!(settings.retry_interval, Duration::from_secs(1));
}
