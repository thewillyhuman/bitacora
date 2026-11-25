//! The bitacora controller.
//!
//! A separate, leader-elected process that reads and writes the metadata
//! store (design, section 8). It is responsible for placement, failure
//! detection, repair, tiering, retention and reconciliation, and it is
//! never on the append path: if it is down, existing streams keep appending
//! and reading, and rolls continue until standby segments run out.

mod controller;

#[cfg(test)]
mod tests;

pub use controller::{Controller, ControllerSettings, PassReport};
