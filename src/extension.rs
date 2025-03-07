use std::ffi::OsStr;
use std::path::Path;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FileExtension {
    Csv,
    Parquet,
    Unknown(String),
    Missing,
}

impl FileExtension {
    pub fn from_path(path: &Path) -> Self {
        match path
            .extension()
            .and_then(OsStr::to_str)
            .map(str::to_lowercase)
            .as_deref()
        {
            Some("csv") => FileExtension::Csv,
            Some("parquet") => FileExtension::Parquet,
            Some(ext) => FileExtension::Unknown(ext.to_owned()),
            None => FileExtension::Missing,
        }
    }
}
