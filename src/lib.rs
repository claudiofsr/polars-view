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
