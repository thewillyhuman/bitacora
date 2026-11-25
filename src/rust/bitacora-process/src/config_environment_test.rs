//! Tests of environment references in configuration files.
//!
//! # What is covered
//!
//! - Where a reference is replaced (inside double-quoted strings, single-line and multi-line) and where
//!   it is left alone (comments, single-quoted strings, outside any string).
//! - What counts as a reference, and how to write a literal `${`.
//! - That a value is inserted as data: whatever it contains, the configuration parses to exactly it.
//! - That an unset variable is refused by name and line.
//!
//! # How the fixtures work
//!
//! `expand` runs the expansion over a text with the variables `POD_NAME=replica-0` and `ZONE=zone-a`
//! set. Tests that care about what a daemon would finally read parse the result as TOML and look at the
//! value, which is the property that matters.

use super::*;

/// The variables every test sees set.
fn variables() -> BTreeMap<String, String> {
    let mut variables = BTreeMap::new();
    variables.insert("POD_NAME".to_owned(), "replica-0".to_owned());
    variables.insert("ZONE".to_owned(), "zone-a".to_owned());
    variables
}

/// Expands `file_text` with the variables of [`variables`].
fn expand(file_text: &str) -> Result<String, ExpansionError> {
    expand_environment(file_text, &variables())
}

/// Expands `file_text` where `SPECIAL` is set to `value`, parses the result as TOML and returns what the
/// key `setting` then holds.
fn setting_after_expansion(file_text: &str, value: &str) -> String {
    let mut variables = variables();
    variables.insert("SPECIAL".to_owned(), value.to_owned());
    let expanded = expand_environment(file_text, &variables).expect("every variable is set");
    let parsed: toml::Table = toml::from_str(&expanded).expect("the expanded text is TOML");
    parsed["setting"].as_str().expect("the setting is a string").to_owned()
}

#[test]
fn text_without_references_is_unchanged() {
    let file_text = "listen_address = \"0.0.0.0:7000\"\n# a comment\n[log]\nlevel = 'info'\n";

    assert_eq!(expand(file_text).as_deref(), Ok(file_text));
}

#[test]
fn reference_in_a_double_quoted_string_is_replaced() {
    let expanded = expand("replica_id = \"${POD_NAME}\"\n");

    assert_eq!(expanded.as_deref(), Ok("replica_id = \"replica-0\"\n"));
}

#[test]
fn reference_can_be_part_of_a_longer_string() {
    let expanded = expand("advertised_address = \"${POD_NAME}.replicas.svc:7000\"\n");

    assert_eq!(
        expanded.as_deref(),
        Ok("advertised_address = \"replica-0.replicas.svc:7000\"\n")
    );
}

#[test]
fn several_references_in_one_string_are_all_replaced() {
    let expanded = expand("name = \"${ZONE}/${POD_NAME}\"\n");

    assert_eq!(expanded.as_deref(), Ok("name = \"zone-a/replica-0\"\n"));
}

#[test]
fn references_in_the_strings_of_an_array_are_replaced() {
    let expanded = expand("failure_domains = [\"${ZONE}\", \"${POD_NAME}\"]\n");

    assert_eq!(
        expanded.as_deref(),
        Ok("failure_domains = [\"zone-a\", \"replica-0\"]\n")
    );
}

#[test]
fn reference_in_a_multi_line_double_quoted_string_is_replaced() {
    let expanded = expand("text = \"\"\"\nfirst ${ZONE}\nsecond\"\"\"\n");

    assert_eq!(expanded.as_deref(), Ok("text = \"\"\"\nfirst zone-a\nsecond\"\"\"\n"));
}

#[test]
fn reference_in_a_single_quoted_string_is_left_alone() {
    let file_text = "pattern = '${POD_NAME}'\n";

    assert_eq!(expand(file_text).as_deref(), Ok(file_text));
}

#[test]
fn reference_in_a_multi_line_single_quoted_string_is_left_alone() {
    let file_text = "pattern = '''\n${POD_NAME}\n'''\n";

    assert_eq!(expand(file_text).as_deref(), Ok(file_text));
}

#[test]
fn reference_in_a_comment_is_left_alone() {
    let file_text = "# set it to \"${NOT_SET_ANYWHERE}\" to take it from the environment\nport = 7000\n";

    assert_eq!(expand(file_text).as_deref(), Ok(file_text));
}

#[test]
fn reference_in_a_comment_after_a_value_is_left_alone() {
    let file_text = "port = 7000 # or \"${NOT_SET_ANYWHERE}\"\n";

    assert_eq!(expand(file_text).as_deref(), Ok(file_text));
}

#[test]
fn reference_outside_any_string_is_left_alone() {
    let file_text = "port = ${POD_NAME}\n";

    assert_eq!(expand(file_text).as_deref(), Ok(file_text));
}

#[test]
fn hash_inside_a_string_does_not_start_a_comment() {
    let expanded = expand("name = \"a # ${ZONE}\"\n");

    assert_eq!(expanded.as_deref(), Ok("name = \"a # zone-a\"\n"));
}

#[test]
fn escaped_quote_does_not_end_the_string() {
    let expanded = expand("name = \"say \\\"hi\\\" to ${ZONE}\"\n");

    assert_eq!(expanded.as_deref(), Ok("name = \"say \\\"hi\\\" to zone-a\"\n"));
}

#[test]
fn string_after_a_single_quoted_string_is_still_expanded() {
    let expanded = expand("a = 'say \"hi\"'\nb = \"${ZONE}\"\n");

    assert_eq!(expanded.as_deref(), Ok("a = 'say \"hi\"'\nb = \"zone-a\"\n"));
}

#[test]
fn doubled_dollar_writes_a_literal_reference() {
    let expanded = expand("template = \"$${POD_NAME}\"\n");

    assert_eq!(expanded.as_deref(), Ok("template = \"${POD_NAME}\"\n"));
}

#[test]
fn dollar_without_braces_is_left_alone() {
    let file_text = "price = \"$POD_NAME costs $5\"\n";

    assert_eq!(expand(file_text).as_deref(), Ok(file_text));
}

#[test]
fn braces_around_something_that_is_not_a_name_are_left_alone() {
    let file_text = "text = \"${not a name} ${} ${1ST}\"\n";

    assert_eq!(expand(file_text).as_deref(), Ok(file_text));
}

#[test]
fn reference_that_is_never_closed_is_left_alone() {
    let file_text = "text = \"${POD_NAME\"\n";

    assert_eq!(expand(file_text).as_deref(), Ok(file_text));
}

#[test]
fn unset_variable_is_refused_by_name() {
    let refused = expand("replica_id = \"${NO_SUCH_VARIABLE}\"\n");

    assert_eq!(
        refused,
        Err(ExpansionError::UnsetVariable {
            variable_name: "NO_SUCH_VARIABLE".to_owned(),
            line_number: 1,
        })
    );
}

#[test]
fn unset_variable_is_refused_with_the_line_it_is_on() {
    let refused = expand("# first line\nport = 7000\nreplica_id = \"${NO_SUCH_VARIABLE}\"\n");

    assert!(
        matches!(refused, Err(ExpansionError::UnsetVariable { line_number: 3, .. })),
        "{refused:?}"
    );
}

#[test]
fn unset_variable_error_says_how_to_fix_it() {
    let message = ExpansionError::UnsetVariable {
        variable_name: "ZONE".to_owned(),
        line_number: 7,
    }
    .to_string();

    assert_eq!(message, "line 7: environment variable `ZONE` is not set");
}

#[test]
fn value_with_a_quote_is_inserted_as_data() {
    let setting = setting_after_expansion("setting = \"${SPECIAL}\"\n", "say \"hi\"");

    assert_eq!(setting, "say \"hi\"");
}

#[test]
fn value_with_a_backslash_is_inserted_as_data() {
    let setting = setting_after_expansion("setting = \"${SPECIAL}\"\n", "C:\\data\\new");

    assert_eq!(setting, "C:\\data\\new");
}

#[test]
fn value_with_line_breaks_and_tabs_is_inserted_as_data() {
    let setting = setting_after_expansion("setting = \"${SPECIAL}\"\n", "one\ntwo\r\n\tthree");

    assert_eq!(setting, "one\ntwo\r\n\tthree");
}

#[test]
fn value_with_a_control_character_is_inserted_as_data() {
    let setting = setting_after_expansion("setting = \"${SPECIAL}\"\n", "bell\u{7}delete\u{7f}");

    assert_eq!(setting, "bell\u{7}delete\u{7f}");
}

#[test]
fn value_that_looks_like_toml_cannot_add_settings() {
    let hostile_value = "x\"\nextra_setting = \"injected";

    let setting = setting_after_expansion("setting = \"${SPECIAL}\"\n", hostile_value);

    assert_eq!(setting, hostile_value);
}

#[test]
fn value_that_looks_like_a_reference_is_not_expanded_again() {
    let setting = setting_after_expansion("setting = \"${SPECIAL}\"\n", "${POD_NAME}");

    assert_eq!(setting, "${POD_NAME}");
}

#[test]
fn value_with_line_breaks_keeps_later_lines_on_their_numbers() {
    let mut variables = variables();
    variables.insert("SPECIAL".to_owned(), "one\ntwo".to_owned());
    let file_text = "a = \"${SPECIAL}\"\nb = \"${NO_SUCH_VARIABLE}\"\n";

    let refused = expand_environment(file_text, &variables);

    assert!(
        matches!(refused, Err(ExpansionError::UnsetVariable { line_number: 2, .. })),
        "{refused:?}"
    );
}

#[test]
fn text_that_is_not_ascii_is_copied_whole() {
    let file_text = "name = \"réplica-ñ ${ZONE} 日本\"\n# commentaire: é\n";

    assert_eq!(
        expand(file_text).as_deref(),
        Ok("name = \"réplica-ñ zone-a 日本\"\n# commentaire: é\n")
    );
}
