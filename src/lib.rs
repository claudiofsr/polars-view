#![warn(clippy::all)]
#![doc = include_str!("../README.md")]

// Modules that make up the PolarsView library.
mod args;
mod container;
mod error;
mod extension;
mod file_dialog;
mod filters;
mod layout;
mod metadata;
mod polars;
mod sort;
mod sqls;
mod traits;

// Publicly expose the contents of these modules.
pub use self::{
    // add to lib
    args::Arguments,
    container::*,
    error::*,
    extension::*,
    file_dialog::*,
    filters::*,
    layout::*,
    metadata::*,
    polars::*,
    sort::*,
    sqls::*,
    traits::*,
};

// https://crates.io/crates/cfg-if
cfg_if::cfg_if! {
    // Use simple or special format.
    // A special format is applied to the "Alíquota" (Tax Rate) column.
    if #[cfg(feature = "special")] {
        mod decimal_and_layout_v2;
        pub use decimal_and_layout_v2::*;
    } else {
        // default: "simple"
        mod decimal_and_layout_v1;
        pub use decimal_and_layout_v1::*;
    }
}
