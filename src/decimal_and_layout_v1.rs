use egui::{Direction, Layout};
use polars::prelude::Column as PColumn;
use polars::prelude::*;

/// Determines the number of decimal places to display and the layout for a given column based on its data type.
pub fn get_decimal_and_layout(column: &PColumn, decimal: usize) -> (Option<usize>, Layout) {
    match column.dtype() {
        DataType::Float32 | DataType::Float64 => {
            // For floating-point numbers, specify the number of decimal places and right-align the content.
            (Some(decimal), Layout::right_to_left(egui::Align::Center))
        }
        DataType::Date
        | DataType::Boolean
        | DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64 => (
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
