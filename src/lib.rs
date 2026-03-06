#![warn(clippy::all)]
#![doc = include_str!("../README.md")]

// Modules that make up the PolarsView library.
mod args;
mod data_container;
mod data_filter;
mod data_format;
mod error;
mod file_dialog;
mod file_extension;
mod file_info;
mod layout;
mod polars;
mod sort;
mod sqls;
mod traits;

// Publicly expose the contents of these modules.
pub use self::{
    // add to lib
    args::Arguments,
    data_container::*,
    data_filter::*,
    data_format::*,
    error::*,
    file_dialog::*,
    file_extension::*,
    file_info::*,
    layout::*,
    polars::add::*,
    polars::drop::*,
    polars::normalize::*,
    polars::remove::*,
    polars::replace::*,
    sort::*,
    sqls::*,
    traits::*,
};

// https://crates.io/crates/cfg-if
cfg_if::cfg_if! {
    // Use simple or special format.
    // A special format is applied to the "Alíquota" (Tax Rate) column.
    if #[cfg(feature = "format-special")] {
        mod decimal_and_layout_v2;
        pub use decimal_and_layout_v2::*;
    } else {
        // default: "simple"
        mod decimal_and_layout_v1;
        pub use decimal_and_layout_v1::*;
    }
}
