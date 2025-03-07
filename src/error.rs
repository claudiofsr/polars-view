use parquet::errors::ParquetError; // Import ParquetError
use polars::prelude::PolarsError;
use std::{io, path::PathBuf};
use thiserror::Error;
use tokio::task::JoinError;

// Result type to simplify function signatures
pub type PolarsViewResult<T> = Result<T, PolarsViewError>;

/// Custom error type for Polars View.
#[derive(Error, Debug)]
pub enum PolarsViewError {
    #[error("IO error: {0}")]
    Io(#[from] io::Error),

    #[error("Polars error: {0}")]
    Polars(#[from] PolarsError),

    #[error("CSV parsing error: {0}")]
    CsvParsing(String),

    #[error("File type error: {0}")]
    FileType(String),

    #[error("Tokio JoinError: {0}")]
    TokioJoin(#[from] JoinError),

    #[error("Channel receive error: {0}")]
    ChannelReceive(String),

    #[error("Other error: {0}")]
    Other(String),

    #[error("File not found: {0}")]
    FileNotFound(PathBuf),

    #[error("Invalid CSV delimiter: {0}")]
    InvalidDelimiter(String),

    #[error("Unsupported file type: {0}")]
    UnsupportedFileType(String),

    #[error("SQL query error: {0}")]
    SqlQueryError(String),

    #[error("Parquet error: {0}")] // Add a variant for Parquet errors
    Parquet(#[from] ParquetError), // Use #[from] for automatic conversion
}

impl From<String> for PolarsViewError {
    fn from(err: String) -> PolarsViewError {
        PolarsViewError::Other(err)
    }
}
