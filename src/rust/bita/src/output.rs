//! How results are shown to a person.
//!
//! # Why this file exists
//!
//! Every command returns a value, and that value is printed either as JSON for scripts or as text for a
//! person. JSON comes straight from `serde`. The text needs a few building blocks that all commands
//! share, so that tables line up the same way everywhere and a size, a time or a payload always looks the
//! same. This file holds those blocks.
//!
//! # What it contains
//!
//! - [`OutputFormat`]: the two ways a result can be printed.
//! - [`Table`]: rows under headers, with columns padded to line up.
//! - [`key_value_lines`]: a list of named values, with the values lined up.
//! - [`format_timestamp`], [`format_age`], [`format_bytes`], [`format_payload`]: one notation each for
//!   moments, lengths of time, sizes and record payloads.
//!
//! # How it works
//!
//! Everything here is a pure function from values to text. A table measures the widest cell of each
//! column in characters and pads the others with spaces; the last column is never padded, so lines carry
//! no trailing spaces. Timestamps are rendered in UTC with a calendar computed here, to avoid a date
//! library for one format.
//!
//! # Invariants
//!
//! - No function reads the clock, the terminal or the environment: the same value always gives the same
//!   text, which is what lets the renderers be tested as plain functions.
//! - Rendered text ends with a newline, so results can be printed as they are.
//!
//! # See also
//!
//! - `commands.rs`: the results and which of these blocks each one uses.

use std::fmt::Write as _;
use std::time::Duration;

use bitacora_types::Timestamp;
use clap::ValueEnum;

/// How a command's result is printed.
#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
pub(crate) enum OutputFormat {
    /// Aligned text for a person at a terminal.
    Table,
    /// JSON with stable field names, for scripts.
    Json,
}

/// Rows of cells under a row of headers.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Table {
    /// The name of each column, in upper case by convention.
    headers: Vec<String>,
    /// The rows. Every row has one cell per header.
    rows: Vec<Vec<String>>,
}

impl Table {
    /// Creates an empty table with the given column names.
    ///
    /// # Arguments
    ///
    /// * `headers` - The name of each column, in the order the columns are shown.
    pub(crate) fn new(headers: &[&str]) -> Self {
        let mut owned_headers = Vec::with_capacity(headers.len());
        for header in headers {
            owned_headers.push((*header).to_owned());
        }
        Self {
            headers: owned_headers,
            rows: Vec::new(),
        }
    }

    /// Adds one row.
    ///
    /// # Arguments
    ///
    /// * `cells` - One cell per column. A row with fewer cells is padded with empty ones, and cells beyond
    ///   the last column are dropped, so a table always renders.
    pub(crate) fn push_row(&mut self, mut cells: Vec<String>) {
        cells.resize(self.headers.len(), String::new());
        self.rows.push(cells);
    }

    /// Renders the table as text: the headers, then one line per row, columns separated by two spaces.
    ///
    /// # Returns
    ///
    /// The lines, each ended by a newline. A table without rows renders its headers alone, so that an
    /// empty result is visibly empty and not a blank screen.
    pub(crate) fn render(&self) -> String {
        let mut column_widths = Vec::with_capacity(self.headers.len());
        for (column, header) in self.headers.iter().enumerate() {
            let mut width = header.chars().count();
            for row in &self.rows {
                width = width.max(row[column].chars().count());
            }
            column_widths.push(width);
        }
        let mut rendered = String::new();
        render_line(&mut rendered, &self.headers, &column_widths);
        for row in &self.rows {
            render_line(&mut rendered, row, &column_widths);
        }
        rendered
    }
}

/// Appends one table line to `rendered`: the cells padded to their column widths, without trailing spaces.
fn render_line(rendered: &mut String, cells: &[String], column_widths: &[usize]) {
    let mut line = String::new();
    for (column, cell) in cells.iter().enumerate() {
        if column > 0 {
            line.push_str("  ");
        }
        line.push_str(cell);
        let padding = column_widths[column].saturating_sub(cell.chars().count());
        for _space in 0..padding {
            line.push(' ');
        }
    }
    rendered.push_str(line.trim_end());
    rendered.push('\n');
}

/// Renders named values one per line, with the values lined up.
///
/// # Arguments
///
/// * `pairs` - Each name with its value, in the order they are shown.
///
/// # Returns
///
/// One line per pair, `name:` padded to the longest name, then the value.
pub(crate) fn key_value_lines(pairs: &[(&str, String)]) -> String {
    let mut longest_name = 0;
    for (name, _value) in pairs {
        longest_name = longest_name.max(name.chars().count());
    }
    let mut rendered = String::new();
    for (name, value) in pairs {
        let padding = longest_name - name.chars().count();
        let _ = writeln!(rendered, "{name}:{} {value}", " ".repeat(padding));
    }
    rendered
}

/// How many days lie between 1 January of year 0 of the proleptic Gregorian calendar and 1970-01-01, in
/// the day numbering of [`civil_date`].
const DAYS_BEFORE_UNIX_EPOCH: u64 = 719_468;
/// How many days one 400-year cycle of the Gregorian calendar has.
const DAYS_PER_ERA: u64 = 146_097;
/// How many milliseconds one day has.
const MILLISECONDS_PER_DAY: u64 = 86_400_000;

/// The calendar date of a day, counted in days since 1970-01-01.
///
/// This is the well-known "civil from days" computation: it shifts the year to start in March, so that
/// the leap day is the last day of the year and month lengths follow one formula.
fn civil_date(days_since_unix_epoch: u64) -> (u64, u64, u64) {
    let shifted_days = days_since_unix_epoch + DAYS_BEFORE_UNIX_EPOCH;
    let era = shifted_days / DAYS_PER_ERA;
    let day_of_era = shifted_days % DAYS_PER_ERA;
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let shifted_month = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * shifted_month + 2) / 5 + 1;
    let month = if shifted_month < 10 {
        shifted_month + 3
    } else {
        shifted_month - 9
    };
    let year = year_of_era + era * 400 + u64::from(month <= 2);
    (year, month, day)
}

/// Renders a moment in UTC, to the second, as `2026-11-25T22:03:44Z`.
pub(crate) fn format_timestamp(timestamp: Timestamp) -> String {
    let unix_millis = timestamp.unix_millis();
    let (year, month, day) = civil_date(unix_millis / MILLISECONDS_PER_DAY);
    let seconds_of_day = (unix_millis % MILLISECONDS_PER_DAY) / 1_000;
    let hour = seconds_of_day / 3_600;
    let minute = (seconds_of_day % 3_600) / 60;
    let second = seconds_of_day % 60;
    format!("{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}Z")
}

/// Renders a length of time in its largest whole unit: `45s`, `12m`, `3h` or `9d`. Less than a second is
/// shown in milliseconds.
pub(crate) fn format_age(age: Duration) -> String {
    let seconds = age.as_secs();
    if seconds == 0 {
        return format!("{}ms", age.as_millis());
    }
    if seconds < 60 {
        return format!("{seconds}s");
    }
    if seconds < 3_600 {
        return format!("{}m", seconds / 60);
    }
    if seconds < 86_400 {
        return format!("{}h", seconds / 3_600);
    }
    format!("{}d", seconds / 86_400)
}

/// The binary units a size is shown in, smallest first.
const SIZE_UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];

/// Renders a size in the largest binary unit that keeps it at one or more, with one decimal: `1.5 GiB`.
/// Sizes below 1 KiB are shown in whole bytes.
pub(crate) fn format_bytes(size_bytes: u64) -> String {
    let mut unit = 0;
    let mut whole = size_bytes;
    let mut remainder = 0;
    while whole >= 1_024 && unit + 1 < SIZE_UNITS.len() {
        remainder = whole % 1_024;
        whole /= 1_024;
        unit += 1;
    }
    if unit == 0 {
        return format!("{whole} B");
    }
    let tenths = remainder * 10 / 1_024;
    format!("{whole}.{tenths} {}", SIZE_UNITS[unit])
}

/// Renders a record payload as text on one line.
///
/// Payloads are opaque bytes. Bytes that are not valid UTF-8 are shown as the replacement character, and
/// control characters, a newline included, are shown as escapes, so that one record is always one line.
pub(crate) fn format_payload(payload: &[u8]) -> String {
    let text = String::from_utf8_lossy(payload);
    let mut rendered = String::with_capacity(text.len());
    for character in text.chars() {
        if character.is_control() {
            rendered.extend(character.escape_default());
        } else {
            rendered.push(character);
        }
    }
    rendered
}

/// Renders a payload as lower-case hexadecimal digits, two per byte: every byte survives, whatever it is.
pub(crate) fn hex_of(payload: &[u8]) -> String {
    let mut rendered = String::with_capacity(payload.len() * 2);
    for byte in payload {
        let _ = write!(rendered, "{byte:02x}");
    }
    rendered
}

#[cfg(test)]
#[path = "output_test.rs"]
mod tests;
