use crate::DataFormat;
use egui::{Align, Direction, Layout};
use polars::prelude::Column;
use std::sync::Arc;

/// Determines the layout for a given column based on its data type and,
/// crucially, the alignment settings from DataFilter.
pub fn get_decimal_and_layout(
    column: &Column,
    format: &Arc<DataFormat>,
) -> (Option<usize>, Layout) {
    let dtype = column.dtype();
    let decimal = format.decimal;

    let align = format.alignments.get(dtype).unwrap_or(&Align::LEFT);

    let layout = match *align {
        Align::LEFT => Layout::left_to_right(Align::Center),
        Align::Center => Layout::centered_and_justified(Direction::LeftToRight),
        Align::RIGHT => Layout::right_to_left(Align::Center),
    };

    match dtype {
        _ if dtype.is_float() => (Some(decimal), layout),
        _ if dtype.is_date() || dtype.is_bool() || dtype.is_integer() => (None, layout),
        _ => (None, layout),
    }
}
