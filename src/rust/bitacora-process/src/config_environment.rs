//! Environment references in configuration files.
//!
//! # Why this file exists
//!
//! A configuration file is written once for many processes, but a few settings differ for each of them:
//! a replica's identity, the address it registers under, where it runs. On a virtual machine a
//! configuration management tool renders one file per machine. On Kubernetes every pod of a set mounts
//! the same file, and what differs is only known inside the pod, as environment variables. Without a way
//! to refer to them, each deployment would need a wrapper script that rewrites the file before the
//! daemon starts. This file lets a setting take its value from the environment instead.
//!
//! # What it contains
//!
//! - [`expand_environment`]: replaces every `${NAME}` inside a double-quoted string of a TOML text by the
//!   value of that variable.
//! - [`ExpansionError`]: a referenced variable is not set.
//!
//! # How it works
//!
//! ```toml
//! replica_id = "${POD_NAME}"
//! advertised_address = "${POD_NAME}.replicas.example.svc:7000"
//! failure_domains = ["${NODE_ZONE}", "${NODE_NAME}"]
//! ```
//!
//! The text is walked once, keeping track of whether the position is inside a double-quoted string, a
//! single-quoted string, a comment, or none of them. Only inside double-quoted strings, single-line or
//! multi-line, is `${NAME}` a reference. A name is ASCII letters, digits and `_`, not starting with a
//! digit. `$${` writes a literal `${`.
//!
//! The value is inserted as data, not as text: characters that mean something inside a TOML string (the
//! quote, the backslash, line breaks and other control characters) are escaped on the way in. Whatever a
//! variable holds, the setting ends up holding exactly that, and it can never close the string and add
//! settings of its own. Line breaks being escaped also means that every line of the file keeps its
//! number, so the positions in the TOML parser's errors still point into the file the operator wrote.
//!
//! Single-quoted strings are TOML's literal strings and stay literal here. Comments are left alone, so
//! that a comment can show a reference without the variable having to be set.
//!
//! # Invariants
//!
//! - Pure: the variables are passed in. Reading the process environment is the caller's business.
//! - A text without `${` inside a double-quoted string comes back unchanged.
//! - Expansion happens once: a value that itself looks like a reference is not expanded again.
//! - The number of lines never changes.
//!
//! # See also
//!
//! - `config_file.rs`: reads the file, expands it with the process environment, and parses it.

use std::collections::BTreeMap;
use std::fmt::Write as _;

use thiserror::Error;

/// What starts a reference to an environment variable.
const REFERENCE_OPENING: &str = "${";

/// What ends a reference to an environment variable.
const REFERENCE_CLOSING: char = '}';

/// How a literal `${` is written inside a double-quoted string.
const ESCAPED_REFERENCE_OPENING: &str = "$${";

/// The quotes around a multi-line double-quoted string.
const MULTI_LINE_BASIC_QUOTES: &str = "\"\"\"";

/// The quotes around a multi-line single-quoted string.
const MULTI_LINE_LITERAL_QUOTES: &str = "'''";

/// Why the environment references of a configuration text could not be replaced.
#[derive(Clone, Debug, PartialEq, Eq, Error)]
pub(crate) enum ExpansionError {
    /// The text refers to a variable that is not set.
    #[error("line {line_number}: environment variable `{variable_name}` is not set")]
    UnsetVariable {
        /// The name between `${` and `}`.
        variable_name: String,
        /// The line of the reference, counted from 1.
        line_number: usize,
    },
}

/// Where in a TOML text the walk currently is, as far as references are concerned.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Context {
    /// Outside strings and comments: keys, numbers, punctuation.
    Plain,
    /// After a `#`, until the end of the line.
    Comment,
    /// Inside a double-quoted string, where references are replaced.
    BasicString {
        /// Whether the string was opened with three quotes and may span lines.
        is_multi_line: bool,
    },
    /// Inside a single-quoted string, which is literal.
    LiteralString {
        /// Whether the string was opened with three quotes and may span lines.
        is_multi_line: bool,
    },
}

/// The state of one walk over a configuration text.
struct Expansion<'input> {
    /// What of the text has not been looked at yet.
    remaining: &'input str,
    /// The text produced so far.
    expanded: String,
    /// The line the walk is on, counted from 1.
    line_number: usize,
    /// The variables a reference may name.
    variables: &'input BTreeMap<String, String>,
}

/// Replaces the environment references of a TOML text by the values of the variables they name.
///
/// See the file header for what a reference is and where it is replaced. The text is walked once; the
/// result is a new string.
///
/// # Arguments
///
/// * `file_text` - The text of a configuration file, as the operator wrote it.
/// * `variables` - The environment: every variable a reference may name, by name.
///
/// # Returns
///
/// The text with every reference inside a double-quoted string replaced. It has the same number of
/// lines as `file_text`.
///
/// # Errors
///
/// Returns [`ExpansionError::UnsetVariable`] for the first reference whose variable is not in
/// `variables`.
pub(crate) fn expand_environment(
    file_text: &str,
    variables: &BTreeMap<String, String>,
) -> Result<String, ExpansionError> {
    let mut expansion = Expansion {
        remaining: file_text,
        expanded: String::with_capacity(file_text.len()),
        line_number: 1,
        variables,
    };
    let mut context = Context::Plain;
    while !expansion.remaining.is_empty() {
        context = match context {
            Context::Plain => expansion.step_through_plain_text(),
            Context::Comment => expansion.step_through_a_comment(),
            Context::BasicString { is_multi_line } => expansion.step_through_a_basic_string(is_multi_line)?,
            Context::LiteralString { is_multi_line } => expansion.step_through_a_literal_string(is_multi_line),
        };
    }
    Ok(expansion.expanded)
}

impl Expansion<'_> {
    /// Moves the next `byte_count` bytes of the text to the result unchanged, counting the lines they end.
    ///
    /// `byte_count` must end on a character boundary within what remains.
    fn copy(&mut self, byte_count: usize) {
        let (copied, rest) = self.remaining.split_at(byte_count);
        self.line_number += copied.matches('\n').count();
        self.expanded.push_str(copied);
        self.remaining = rest;
    }

    /// Moves the next character of the text to the result unchanged. Does nothing at the end of the text.
    fn copy_one_character(&mut self) {
        if let Some(character) = self.remaining.chars().next() {
            self.copy(character.len_utf8());
        }
    }

    /// Drops the next `byte_count` bytes of the text without copying them. They hold no line break.
    fn skip(&mut self, byte_count: usize) {
        self.remaining = &self.remaining[byte_count..];
    }

    /// Takes one step outside strings and comments, and says where the walk is afterwards.
    fn step_through_plain_text(&mut self) -> Context {
        if self.remaining.starts_with(MULTI_LINE_BASIC_QUOTES) {
            self.copy(MULTI_LINE_BASIC_QUOTES.len());
            return Context::BasicString { is_multi_line: true };
        }
        if self.remaining.starts_with(MULTI_LINE_LITERAL_QUOTES) {
            self.copy(MULTI_LINE_LITERAL_QUOTES.len());
            return Context::LiteralString { is_multi_line: true };
        }
        let next_context = match self.remaining.chars().next() {
            Some('"') => Context::BasicString { is_multi_line: false },
            Some('\'') => Context::LiteralString { is_multi_line: false },
            Some('#') => Context::Comment,
            _anything_else => Context::Plain,
        };
        self.copy_one_character();
        next_context
    }

    /// Takes one step inside a comment, which ends with its line.
    fn step_through_a_comment(&mut self) -> Context {
        let ends_the_line = self.remaining.starts_with('\n');
        self.copy_one_character();
        if ends_the_line {
            Context::Plain
        } else {
            Context::Comment
        }
    }

    /// Takes one step inside a single-quoted string, where nothing is replaced.
    fn step_through_a_literal_string(&mut self, is_multi_line: bool) -> Context {
        let closing_quotes = if is_multi_line { MULTI_LINE_LITERAL_QUOTES } else { "'" };
        if self.remaining.starts_with(closing_quotes) {
            self.copy(closing_quotes.len());
            return Context::Plain;
        }
        // A single-line string cannot span lines. If the quote is missing, the parser will say so; the
        // walk goes on as it would have after it.
        let ends_an_unclosed_string = !is_multi_line && self.remaining.starts_with('\n');
        self.copy_one_character();
        if ends_an_unclosed_string {
            Context::Plain
        } else {
            Context::LiteralString { is_multi_line }
        }
    }

    /// Takes one step inside a double-quoted string: copies text, honours escapes, and replaces a
    /// reference by the value of its variable.
    ///
    /// # Errors
    ///
    /// Returns [`ExpansionError::UnsetVariable`] if the step is at a reference to a variable that is not
    /// set.
    fn step_through_a_basic_string(&mut self, is_multi_line: bool) -> Result<Context, ExpansionError> {
        let still_inside = Context::BasicString { is_multi_line };
        let closing_quotes = if is_multi_line { MULTI_LINE_BASIC_QUOTES } else { "\"" };
        if self.remaining.starts_with(closing_quotes) {
            self.copy(closing_quotes.len());
            return Ok(Context::Plain);
        }
        if self.remaining.starts_with('\\') {
            // The backslash and what it escapes, so that an escaped quote does not end the string.
            self.copy_one_character();
            self.copy_one_character();
            return Ok(still_inside);
        }
        if self.remaining.starts_with(ESCAPED_REFERENCE_OPENING) {
            self.skip(ESCAPED_REFERENCE_OPENING.len());
            self.expanded.push_str(REFERENCE_OPENING);
            return Ok(still_inside);
        }
        if let Some(variable_name) = referenced_variable(self.remaining) {
            self.replace_reference(variable_name)?;
            return Ok(still_inside);
        }
        let ends_an_unclosed_string = !is_multi_line && self.remaining.starts_with('\n');
        self.copy_one_character();
        Ok(if ends_an_unclosed_string {
            Context::Plain
        } else {
            still_inside
        })
    }

    /// Replaces the reference to `variable_name` that the text starts with by the variable's value,
    /// escaped for a double-quoted string.
    ///
    /// # Errors
    ///
    /// Returns [`ExpansionError::UnsetVariable`] if the variable is not set.
    fn replace_reference(&mut self, variable_name: &str) -> Result<(), ExpansionError> {
        let Some(value) = self.variables.get(variable_name) else {
            return Err(ExpansionError::UnsetVariable {
                variable_name: variable_name.to_owned(),
                line_number: self.line_number,
            });
        };
        push_escaped_for_a_basic_string(&mut self.expanded, value);
        self.skip(REFERENCE_OPENING.len() + variable_name.len() + REFERENCE_CLOSING.len_utf8());
        Ok(())
    }
}

/// Returns the name of the variable `text` starts with a reference to, if it does: `${NAME}` with a name
/// of ASCII letters, digits and `_` that does not start with a digit.
fn referenced_variable(text: &str) -> Option<&str> {
    let after_the_opening = text.strip_prefix(REFERENCE_OPENING)?;
    let name_length = after_the_opening.find(REFERENCE_CLOSING)?;
    let variable_name = &after_the_opening[..name_length];
    let starts_like_a_name = variable_name
        .chars()
        .next()
        .is_some_and(|first| first.is_ascii_alphabetic() || first == '_');
    let is_a_name = starts_like_a_name
        && variable_name
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || character == '_');
    is_a_name.then_some(variable_name)
}

/// Appends `value` to `output` the way it has to be written between double quotes in TOML for the
/// string to hold exactly `value`.
fn push_escaped_for_a_basic_string(output: &mut String, value: &str) {
    for character in value.chars() {
        match character {
            '"' => output.push_str("\\\""),
            '\\' => output.push_str("\\\\"),
            '\n' => output.push_str("\\n"),
            '\r' => output.push_str("\\r"),
            '\t' => output.push_str("\\t"),
            control if control.is_control() => {
                // Writing to a `String` cannot fail.
                let _ = write!(output, "\\u{:04X}", u32::from(control));
            }
            ordinary => output.push(ordinary),
        }
    }
}

#[cfg(test)]
#[path = "config_environment_test.rs"]
mod tests;
