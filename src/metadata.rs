use crate::{PolarsViewError, PolarsViewResult};

use egui::{Color32, Frame, Grid, Stroke, Ui};
use parquet::{
    basic::ColumnOrder,
    file::{
        metadata::ParquetMetaData,
        reader::{FileReader, SerializedFileReader},
    },
};
use polars::prelude::*;
use std::{fs::File, path::PathBuf, sync::Arc};

/// Enum to represent file metadata, supporting Parquet and CSV.
///
/// This enum holds either Parquet metadata or CSV metadata.  It provides a
/// unified way to access and display metadata for different file types.
pub enum FileMetadata {
    Parquet(ParquetMetadataWrapper),
    Csv(CsvMetadataWrapper),
}

/// Wrapper struct for Parquet metadata.
///
/// This struct encapsulates the `ParquetMetaData` object, providing
/// methods to extract and render specific Parquet file information.
pub struct ParquetMetadataWrapper {
    metadata: ParquetMetaData, // Parquet metadata.
}

/// Wrapper struct for CSV metadata.
///
/// This struct holds the schema and row count for a CSV file. Polars infers
/// schema and row count efficiently; we store this derived information for display.
pub struct CsvMetadataWrapper {
    schema: Arc<Schema>,
    row_count: usize,
}

impl FileMetadata {
    /// Creates `FileMetadata` from a path and file type.
    ///
    /// This function determines the file type (Parquet or CSV) based on the
    /// provided `file_type` string and constructs the appropriate metadata
    /// wrapper.
    ///
    /// ### Arguments
    ///
    /// * `path`: The PathBuf to the file.
    /// * `file_type`: A string representing the file type ("parquet" or "csv").
    /// * `schema`:  Optionally, the schema of CSV.
    /// * `row_count`: Optionally, the numbers of rows of the CSV.
    ///
    /// ### Returns
    ///
    /// A `PolarsViewResult` containing the `FileMetadata` or a `PolarsViewError` if
    /// an error occurs (e.g., file not found, unsupported file type).
    pub fn from_path(
        path: PathBuf,
        file_type: &str,
        schema: Option<Arc<Schema>>,
        row_count: Option<usize>,
    ) -> PolarsViewResult<Self> {
        match file_type {
            "parquet" => {
                // Attempt to open the file (Parquet files require reading for metadata).
                let file = File::open(&path)?;

                // Create a SerializedFileReader to read Parquet metadata.  This reader
                // efficiently accesses only the metadata without loading the entire file.
                let reader = SerializedFileReader::new(file)?;

                // Extract and store the Parquet metadata.  `to_owned()` creates
                // a copy of the metadata, allowing the `FileMetadata` object to
                // own the data.
                Ok(FileMetadata::Parquet(ParquetMetadataWrapper {
                    metadata: reader.metadata().to_owned(),
                }))
            }
            "csv" => {
                // For CSV, we expect the schema to have already been determined (e.g.,
                // during file loading in `DataFilters`). We receive the schema as
                // a parameter to avoid redundant inference.
                match (schema, row_count) {
                    (Some(schema), Some(row_count)) => {
                        Ok(FileMetadata::Csv(CsvMetadataWrapper { schema, row_count }))
                    }
                    _ => Err(PolarsViewError::CsvParsing(
                        "Schema and Row count required for CSV metadata.".to_string(),
                    )),
                }
            }
            _ => Err(PolarsViewError::UnsupportedFileType(format!(
                "Unsupported file type: {file_type}"
            ))),
        }
    }

    /// Renders the file metadata to the UI.
    ///
    /// This method dispatches to the appropriate `render_metadata` implementation
    /// based on whether the underlying data is Parquet or CSV metadata.
    pub fn render_metadata(&self, ui: &mut Ui) {
        match self {
            FileMetadata::Parquet(parquet_metadata) => {
                parquet_metadata.render_metadata(ui);
            }
            FileMetadata::Csv(csv_metadata) => {
                csv_metadata.render_metadata(ui);
            }
        }
    }

    /// Renders the file schema information to the UI.
    ///
    /// This function displays detailed schema information.  For Parquet, it
    /// shows information from `parquet-rs`; for CSV, it displays the Polars
    /// schema.
    pub fn render_schema(&self, ui: &mut Ui) {
        match self {
            FileMetadata::Parquet(parquet_metadata) => {
                parquet_metadata.render_schema(ui);
            }
            FileMetadata::Csv(csv_metadata) => {
                csv_metadata.render_schema(ui);
            }
        }
    }
}

impl ParquetMetadataWrapper {
    /// Renders the Parquet file metadata (column count, row count, version, etc.).
    ///
    /// This presents summary information extracted from the `ParquetMetaData` object.
    pub fn render_metadata(&self, ui: &mut Ui) {
        let file_metadata = self.metadata.file_metadata();

        // Frame for visual grouping and styling.
        Frame::default()
            .stroke(Stroke::new(1.0, Color32::GRAY)) // Thin gray border for separation.
            .outer_margin(2.0) // Margin *outside* the frame.
            .inner_margin(10.0) // Margin *inside* the frame, between border and content.
            .show(ui, |ui| {
                // Grid layout for structured presentation.
                Grid::new("version_grid") // Unique ID for the grid.
                    .num_columns(2) // Two columns: one for labels, one for values.
                    .spacing([10.0, 20.0]) // Spacing between columns and rows.
                    .striped(true) // Alternate row background colors for readability.
                    .show(ui, |ui| {
                        // Number of columns.
                        let column_count = file_metadata.schema_descr().num_columns();
                        ui.label("Columns:");
                        ui.label(column_count.to_string());
                        ui.end_row();

                        // Number of rows.
                        let row_count = file_metadata.num_rows();
                        ui.label("Rows:");
                        ui.label(row_count.to_string());
                        ui.end_row();
                    });
            });
    }

    /// Renders the Parquet file schema (column names, types, sort order).
    ///
    /// This function iterates through the columns in the Parquet schema and
    /// displays detailed information about each column.
    pub fn render_schema(&self, ui: &mut Ui) {
        let file_metadata = self.metadata.file_metadata();
        // Iterate over the columns in the schema.
        for (idx, field) in file_metadata.schema_descr().columns().iter().enumerate() {
            // Use a CollapsingHeader to group information for each column.
            ui.collapsing(field.name(), |ui| {
                // Get and display the column type.  `self_type` provides more
                // detailed type information than `converted_type`.
                let field_type = field.self_type();
                let field_type_str = if field_type.is_primitive() {
                    // If primitive (e.g., INT32, FLOAT), display the physical type.
                    format!("{}", field_type.get_physical_type())
                } else {
                    // Otherwise, show the converted type (logical type).
                    format!("{}", field.converted_type())
                };

                ui.label(format!("type: {}", field_type_str));

                // Display the column sort order.
                ui.label(format!(
                    "sort_order: {}",
                    match file_metadata.column_order(idx) {
                        // Get sort order for the column.
                        ColumnOrder::TYPE_DEFINED_ORDER(sort_order) => format!("{}", sort_order),
                        _ => "undefined".to_string(), // If not defined, display "undefined".
                    }
                ));
            });
        }
    }
}

impl CsvMetadataWrapper {
    /// Renders the CSV file metadata (column count, row count).
    pub fn render_metadata(&self, ui: &mut Ui) {
        // Frame provides visual grouping (similar to Parquet rendering).
        Frame::default()
            .stroke(Stroke::new(1.0, Color32::GRAY))
            .outer_margin(2.0)
            .inner_margin(10.0)
            .show(ui, |ui| {
                // Grid for layout.
                Grid::new("version_grid")
                    .num_columns(2)
                    .spacing([10.0, 20.0])
                    .striped(true)
                    .show(ui, |ui| {
                        // Number of columns (from schema).
                        let column_count = self.schema.len();
                        ui.label("Columns:");
                        ui.label(column_count.to_string());
                        ui.end_row();

                        // Number of rows.
                        let row_count = self.row_count;
                        ui.label("Rows:");
                        ui.label(row_count.to_string());
                        ui.end_row();
                    });
            });
    }

    /// Renders the CSV file schema (column names and data types).
    pub fn render_schema(&self, ui: &mut Ui) {
        // Iterate over fields in the Polars schema.
        for (name, dtype) in self.schema.iter() {
            // CollapsingHeader to group each column's information.
            ui.collapsing(name.to_string(), |ui| {
                ui.label(format!("type: {}", dtype)); // Display the data type.
            });
        }
    }
}
