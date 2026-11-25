//! Tests of the text building blocks.
//!
//! # What is covered
//!
//! - Tables: alignment, empty tables, rows that are too short or too long, characters wider than a byte.
//! - Named values: alignment.
//! - Timestamps: the epoch, a leap day, the last second of a year, a date after 2100.
//! - Ages, sizes and payloads: each unit and each edge between two units.

use super::*;

#[test]
fn table_pads_columns_to_their_widest_cell() {
    let mut table = Table::new(&["NAME", "EPOCH"]);
    table.push_row(vec!["orders".to_owned(), "7".to_owned()]);
    table.push_row(vec!["a".to_owned(), "12".to_owned()]);

    assert_eq!(table.render(), "NAME    EPOCH\norders  7\na       12\n");
}

#[test]
fn table_without_rows_shows_its_headers() {
    let table = Table::new(&["NAME", "EPOCH"]);

    assert_eq!(table.render(), "NAME  EPOCH\n");
}

#[test]
fn table_lines_carry_no_trailing_spaces() {
    let mut table = Table::new(&["NAME", "WRITER"]);
    table.push_row(vec!["orders".to_owned(), String::new()]);

    assert_eq!(table.render(), "NAME    WRITER\norders\n");
}

#[test]
fn short_row_is_padded_with_empty_cells() {
    let mut table = Table::new(&["A", "B", "C"]);
    table.push_row(vec!["1".to_owned()]);

    assert_eq!(table.render(), "A  B  C\n1\n");
}

#[test]
fn cells_beyond_the_last_column_are_dropped() {
    let mut table = Table::new(&["A"]);
    table.push_row(vec!["1".to_owned(), "extra".to_owned()]);

    assert_eq!(table.render(), "A\n1\n");
}

#[test]
fn table_measures_characters_not_bytes() {
    let mut table = Table::new(&["NAME", "N"]);
    table.push_row(vec!["bitácora".to_owned(), "1".to_owned()]);
    table.push_row(vec!["plain".to_owned(), "2".to_owned()]);

    assert_eq!(table.render(), "NAME      N\nbitácora  1\nplain     2\n");
}

#[test]
fn named_values_are_lined_up() {
    let rendered = key_value_lines(&[("name", "orders".to_owned()), ("epoch", "7".to_owned())]);

    assert_eq!(rendered, "name:  orders\nepoch: 7\n");
}

#[test]
fn unix_epoch_is_the_first_of_january_1970() {
    assert_eq!(format_timestamp(Timestamp::UNIX_EPOCH), "1970-01-01T00:00:00Z");
}

#[test]
fn leap_day_is_rendered() {
    // 2024-02-29T12:00:00Z.
    let leap_day_noon = Timestamp::from_unix_millis(1_709_208_000_000);

    assert_eq!(format_timestamp(leap_day_noon), "2024-02-29T12:00:00Z");
}

#[test]
fn last_second_of_a_year_is_rendered() {
    // 2026-12-31T23:59:59Z.
    let last_second_of_2026 = Timestamp::from_unix_millis(1_798_761_599_000);

    assert_eq!(format_timestamp(last_second_of_2026), "2026-12-31T23:59:59Z");
}

#[test]
fn first_of_march_of_a_year_that_is_no_leap_year_is_rendered() {
    // 2100 is divisible by 4 but is not a leap year: 2100-03-01T00:00:00Z.
    let first_of_march_2100 = Timestamp::from_unix_millis(4_107_542_400_000);

    assert_eq!(format_timestamp(first_of_march_2100), "2100-03-01T00:00:00Z");
}

#[test]
fn milliseconds_of_a_timestamp_are_dropped() {
    assert_eq!(
        format_timestamp(Timestamp::from_unix_millis(1_999)),
        "1970-01-01T00:00:01Z"
    );
}

#[test]
fn age_below_a_second_is_shown_in_milliseconds() {
    assert_eq!(format_age(Duration::from_millis(250)), "250ms");
}

#[test]
fn age_below_a_minute_is_shown_in_seconds() {
    assert_eq!(format_age(Duration::from_secs(59)), "59s");
}

#[test]
fn age_below_an_hour_is_shown_in_minutes() {
    assert_eq!(format_age(Duration::from_secs(3_599)), "59m");
}

#[test]
fn age_below_a_day_is_shown_in_hours() {
    assert_eq!(format_age(Duration::from_secs(86_399)), "23h");
}

#[test]
fn age_of_days_is_shown_in_days() {
    assert_eq!(format_age(Duration::from_hours(9 * 24)), "9d");
}

#[test]
fn size_below_a_kibibyte_is_shown_in_bytes() {
    assert_eq!(format_bytes(1_023), "1023 B");
}

#[test]
fn size_is_shown_in_its_largest_unit_with_one_decimal() {
    assert_eq!(format_bytes(1_536 * 1_024 * 1_024), "1.5 GiB");
}

#[test]
fn exact_kibibyte_has_a_zero_decimal() {
    assert_eq!(format_bytes(1_024), "1.0 KiB");
}

#[test]
fn size_beyond_the_largest_unit_stays_in_it() {
    assert_eq!(format_bytes(4_096 * 1_024 * 1_024 * 1_024 * 1_024), "4096.0 TiB");
}

#[test]
fn text_payload_is_shown_as_it_is() {
    assert_eq!(format_payload(b"record-7"), "record-7");
}

#[test]
fn newline_in_a_payload_is_escaped() {
    assert_eq!(format_payload(b"two\nlines"), "two\\nlines");
}

#[test]
fn bytes_that_are_not_text_are_replaced() {
    assert_eq!(format_payload(&[b'a', 0xFF, b'b']), "a\u{FFFD}b");
}

#[test]
fn hexadecimal_keeps_every_byte() {
    assert_eq!(hex_of(&[0x00, 0x0F, 0xFF]), "000fff");
}
