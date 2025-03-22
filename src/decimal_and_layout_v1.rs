use egui::{Direction, Layout};
use polars::prelude::Column;

/// Determines the number of decimal places to display and the layout for a given column based on its data type.
pub fn get_decimal_and_layout(column: &Column, decimal: usize) -> (Option<usize>, Layout) {
    let dtype = column.dtype(); // Get the data type of the column.

    match dtype {
        _ if dtype.is_float() => {
            // For floating-point numbers, specify the number of decimal places and right-align the content.
            (Some(decimal), Layout::right_to_left(egui::Align::Center))
        }
        _ if dtype.is_date() || dtype.is_bool() || dtype.is_integer() => (
            // For date, boolean and integer types, no decimal places are required and the content is centered.
            None,
            Layout::centered_and_justified(Direction::LeftToRight),
        ),
        _ => (
            // For other data types (e.g., strings), no decimal places are needed, and left-align the content.
            None,
            Layout::left_to_right(egui::Align::Center),
        ),
    }
}
