use crate::PathExtension;
use std::path::Path;

/// Represents the extension of a file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FileExtension {
    /// CSV file extension.
    Csv,
    /// Json file extension.
    Json,
    /// Newline-Delimited Json file extension.
    NDJson,
    /// Parquet file extension.
    Parquet,
    /// Unknown file extension, storing the extension as a string.
    Unknown(String),
    /// Missing file extension, when no extension is present in the path.
    Missing,
}

impl FileExtension {
    /// Determines the file extension from a given path.
    pub fn from_path(path: &Path) -> Self {
        match path
            .extension_as_lowercase()
            .as_deref() // Converts `Option<String>` to `Option<&str>` for matching.
        {
            Some("csv") => FileExtension::Csv,
            Some("json") => FileExtension::Json,
            Some("ndjson") => FileExtension::NDJson,
            Some("parquet") => FileExtension::Parquet,
            Some(ext) => FileExtension::Unknown(ext.to_owned()),
            None => FileExtension::Missing,
        }
    }
}
