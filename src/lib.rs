// Modules that make up the ParqBench library.
mod args;
mod components;
mod data;
mod layout;
mod sqls;
mod traits;

// Publicly expose the contents of these modules.
pub use self::{args::Arguments, components::*, data::*, layout::*, sqls::*, traits::*};

use polars::{
    error::PolarsResult,
    prelude::{Column, DataType, RoundSeries},
};

/// Filters columns of type float64.
///
/// Subsequently, rounds the column values.
///
/// This function is currently unused, but kept for potential future use.
pub fn round_float64_columns(col: Column, decimals: u32) -> PolarsResult<Option<Column>> {
    let series = match col.as_series() {
        Some(s) => s,
        None => return Ok(Some(col)),
    };

    match series.dtype() {
        DataType::Float64 => Ok(Some(series.round(decimals)?.into())),
        _ => Ok(Some(col)),
    }
}
