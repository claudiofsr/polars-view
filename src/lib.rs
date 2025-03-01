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
use std::{
    io,
    path::{Path, PathBuf},
};

/// Extracts the file extension from a filename, converting it to lowercase.
///
/// If no extension is found, returns `None`.
///
/// # Arguments
///
/// * `filename` - A string slice representing the filename.
///
/// # Returns
///
/// An `Option<String>` containing the lowercase file extension if found, otherwise `None`.
pub fn get_extension(path: &Path) -> Option<String> {
    path.extension() // Get the extension as an Option<&OsStr>
        .and_then(|ext| ext.to_str()) // Convert the extension to &str, returning None if the conversion fails
        .map(|ext| ext.to_lowercase()) // Convert the extension to lowercase for case-insensitive comparison
}

/// Canonicalization means converting a path to its absolute, resolved form.
/// This involves resolving symbolic links, removing relative components like "." and "..",
/// and ensuring the path points to the actual location on the filesystem.
///
/// In essence, it's about getting the "true" and unambiguous path to a file or directory.
///
/// This function takes an optional PathBuf, and if it exists, attempts to canonicalize it.
///
/// # Arguments
///
/// * `filename`: An optional PathBuf representing the path to canonicalize.
///
/// # Returns
///
/// * `Result<Option<PathBuf>, io::Error>`:  Returns `Ok(Some(canonicalized_path))` if canonicalization succeeds.
///   Returns `Ok(None)` if the input `filename` is `None`.
///   Returns `Err(io::Error)` if an error occurs during canonicalization (e.g., file not found, permissions issues).
pub fn get_canonicalized_path(filename: &Option<PathBuf>) -> Result<Option<PathBuf>, io::Error> {
    // as_ref() converts Option<PathBuf> to Option<&PathBuf>
    // map() applies the canonicalize() method to the &PathBuf if it exists
    // transpose() swaps Option<Result<T, E>> to Result<Option<T>, E>
    filename.as_ref().map(|f| f.canonicalize()).transpose()
}

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parquet_extension() {
        assert_eq!(
            get_extension(Path::new("data.parquet")),
            Some("parquet".to_string())
        );
        assert_eq!(
            get_extension(Path::new("DATA.PARQUET")),
            Some("parquet".to_string())
        ); // Case-insensitive test
    }

    #[test]
    fn test_csv_extension() {
        assert_eq!(
            get_extension(Path::new("data.csv")),
            Some("csv".to_string())
        );
        assert_eq!(
            get_extension(Path::new("data.CSV")),
            Some("csv".to_string())
        ); // Case-insensitive test
    }

    #[test]
    fn test_no_extension() {
        assert_eq!(get_extension(Path::new("data")), None); // No extension
    }

    #[test]
    fn test_empty_filename() {
        assert_eq!(get_extension(Path::new("")), None); // Empty filename
    }

    #[test]
    fn test_path_with_dots() {
        assert_eq!(
            get_extension(Path::new("path.to.file.txt")),
            Some("txt".to_string())
        );
    }
}
