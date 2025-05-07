use polars::prelude::PolarsError;
use std::{io, path::PathBuf};
use thiserror::Error;
use tokio::task::JoinError;

/**
Result type to simplify function signatures.

This is a custom result type that uses our custom `PolarsViewError` for the error type.

Functions can return `PolarsViewResult<T>` and then use `?` to automatically propagate errors.
*/
pub type PolarsViewResult<T> = Result<T, PolarsViewError>;

/**
Custom error type for Polars View.

This enum defines all the possible errors that can occur in the application.

We use the `thiserror` crate to derive the `Error` trait and automatically
implement `Display` using the `#[error(...)]` attribute.
*/
#[derive(Error, Debug)]
pub enum PolarsViewError {
    // Wrapper for standard IO errors.
    // The #[from] attribute automatically converts io::Error to PolarsViewError::Io.
    #[error("IO error: {0}")]
    Io(#[from] io::Error),

    // Wrapper for Polars errors (from the Polars library).
    // #[from] handles conversion. Handles errors from Polars operations,
    // including invalid lazy plans or errors during execution (like bad casts or regex syntax).
    #[error("Polars error: {0}")]
    Polars(#[from] PolarsError),

    // Errors encountered while parsing CSV data (e.g., inconsistent columns, invalid data).
    #[error("CSV parsing error: {0}")]
    CsvParsing(String),

    // Errors related to the file type (e.g., unsupported file extension, incorrect file format).
    #[error("File type error: {0}")]
    FileType(String),

    // Wrapper for Tokio JoinErrors, occurring when asynchronous tasks fail.
    #[error("Tokio JoinError: {0}")]
    TokioJoin(#[from] JoinError),

    // Errors occurring when receiving data from asynchronous channels.
    #[error("Channel receive error: {0}")]
    ChannelReceive(String),

    // Indicates that a specified file could not be found, storing the attempted path.
    #[error("File not found: {0:#?}")]
    FileNotFound(PathBuf),

    // Indicates an invalid CSV delimiter was provided (empty or too long).
    #[error("Invalid CSV delimiter: '{0}'")] // Added quotes for clarity
    InvalidDelimiter(String),

    // Indicates that a provided file extension or file type are not supported.
    #[error("Unsupported file type: {0}")]
    UnsupportedFileType(String),

    // --- Regex Errors ---
    /// Indicates that a regex pattern provided for column selection does not meet the required format (`*` or `^...$`).
    #[error(
        "Invalid regex pattern for column selection: '{0}'.\n\
        Pattern must be '*' or (start with '^' and end with '$')."
    )]
    InvalidRegexPattern(String),

    /// Indicates that the provided regex pattern has invalid syntax.
    #[error("Invalid regex syntax in pattern '{pattern}'\n{error}")]
    InvalidRegexSyntax { pattern: String, error: String },

    /// Indicates that the regex pattern matched columns that are not `DataType::String`, which is required for normalization.
    #[error(
        "Regex pattern '{pattern}' matched non-string columns which cannot be normalized:\n\
         {columns:#?}\n\
         Please adjust the regex pattern to exclusively target String columns."
    )]
    InvalidDataTypeForRegex {
        pattern: String,
        /// List of problematic columns (name and type)
        columns: Vec<String>,
    },

    // --- End Regex Errors ---
    #[error("Invalid value for command-line argument '{arg_name}': {reason}")]
    InvalidArgument {
        arg_name: String, // Context about *which* argument failed
        reason: String,   // The specific error reason
    },

    // A catch-all for other, less specific errors not covered by specific variants.
    // Uses a String to describe the error. Consider using this sparingly.
    #[error("Other error: {0}")]
    Other(String),
}

// Implementation of the From trait to convert a String into a PolarsViewError.
// This allows us to easily convert generic error strings into our custom error type.
impl From<String> for PolarsViewError {
    fn from(err: String) -> PolarsViewError {
        // Prefer using specific error variants when possible, fallback to Other.
        PolarsViewError::Other(err)
    }
}
