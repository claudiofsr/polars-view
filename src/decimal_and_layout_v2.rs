use crate::DataFormat;
use egui::{Align, Direction, Layout};
use polars::prelude::Column;
use std::sync::Arc;

// Identify the exact name(s) of the column(s) causing problems.
// Replace "your_large_digit_column_name" with the actual name(s).
pub const DEFAULT_OVERRIDE_REGEX: Option<&str> = Some("^Chave.*$");

const COL_SPECIAL: &[&str] = &["Alíq", "Aliq"]; // Column names that will receive special formatting.
const COL_DECIMAL: usize = 4; // Default number of decimal places to use for floating-point numbers.

/// Determines the formatting (decimal places and layout) for a cell
/// based on its column's data type and application settings.
///
/// A special format is applied to columns whose names are in `COL_SPECIAL` (e.g., "Alíquota").
///
/// Columns in `COL_SPECIAL` are centered, and their floating-point values have `COL_DECIMAL` decimal places.
/// Other columns' floating-point values use the `decimal` argument for determining the number of decimal places.
pub fn get_decimal_and_layout(
    column: &Column,
    format: &Arc<DataFormat>,
) -> (Option<usize>, Layout) {
    let column_name = column.name();
    let dtype = column.dtype(); // Get the data type of the column.
    let decimal = format.decimal;
    let is_special = COL_SPECIAL
        .iter()
        .any(|&special_name| column_name.contains(special_name)); // Check if the current column is one of the special columns.

    let align = format.alignments.get(dtype).unwrap_or(&Align::LEFT);

    // Determine decimal places and layout based on data type and column name.
    if dtype.is_float() {
        // If it is a float, set decimal depending on whether it is a special column.
        let selected_decimal = if is_special { COL_DECIMAL } else { decimal };

        // Set layout: centered for `COL_SPECIAL`, right-aligned for other floats.
        let layout = if is_special {
            Layout::centered_and_justified(Direction::LeftToRight)
        } else {
            match *align {
                // Layout
                Align::LEFT => Layout::left_to_right(Align::Center),
                Align::Center => Layout::centered_and_justified(Direction::LeftToRight),
                Align::RIGHT => Layout::right_to_left(Align::Center),
            }
        };

        (Some(selected_decimal), layout)
    } else if dtype.is_date() || dtype.is_bool() || dtype.is_integer() {
        let layout = match *align {
            // Layout
            Align::LEFT => Layout::left_to_right(Align::Center),
            Align::Center => Layout::centered_and_justified(Direction::LeftToRight),
            Align::RIGHT => Layout::right_to_left(Align::Center),
        };
        // For dates, booleans and integers, use centered layout and no decimal.
        (None, layout)
    } else {
        let layout = match *align {
            // Layout
            Align::LEFT => Layout::left_to_right(Align::Center),
            Align::Center => Layout::centered_and_justified(Direction::LeftToRight),
            Align::RIGHT => Layout::right_to_left(Align::Center),
        };
        // Default to left-aligned layout for other data types (e.g., String).
        (None, layout)
    }
}
