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
pub enum FileMetadata {
    Parquet(ParquetMetadataWrapper),
    Csv(CsvMetadataWrapper),
}

/// Wrapper struct for Parquet metadata.
pub struct ParquetMetadataWrapper {
    metadata: ParquetMetaData, // Parquet metadata.
}

/// Wrapper struct for CSV metadata. Holds the schema and row count.
pub struct CsvMetadataWrapper {
    schema: Arc<Schema>,
    row_count: usize,
}

impl FileMetadata {
    /// Creates `FileMetadata` from a path and file type.
    pub fn from_path(
        path: PathBuf,
        file_type: &str,
        schema: Option<Arc<Schema>>,
        row_count: Option<usize>,
    ) -> PolarsViewResult<Self> {
        match file_type {
            "parquet" => {
                // Attempt to open the file.
                let file = File::open(&path)?;

                // Create a SerializedFileReader to read Parquet metadata.
                let reader = SerializedFileReader::new(file)?;

                // Extract and store the Parquet metadata.
                Ok(FileMetadata::Parquet(ParquetMetadataWrapper {
                    metadata: reader.metadata().to_owned(),
                }))
            }
            "csv" => {
                // For CSV, we need the schema to display column information.
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
    /// Renders the file metadata.
    pub fn render_metadata(&self, ui: &mut Ui) {
        let file_metadata = self.metadata.file_metadata();

        // Frame for visual grouping.
        Frame::default()
            .stroke(Stroke::new(1.0, Color32::GRAY))
            .outer_margin(2.0)
            .inner_margin(10.0)
            .show(ui, |ui| {
                // Grid layout.
                Grid::new("version_grid")
                    .num_columns(2)
                    .spacing([10.0, 20.0])
                    .striped(true)
                    .show(ui, |ui| {
                        let column_count = file_metadata.schema_descr().num_columns();
                        ui.label("Columns:");
                        ui.label(column_count.to_string());
                        ui.end_row();

                        let row_count = file_metadata.num_rows();
                        ui.label("Rows:");
                        ui.label(row_count.to_string());
                        ui.end_row();
                    });
            });
    }

    /// Renders the Parquet file schema.
    pub fn render_schema(&self, ui: &mut Ui) {
        let file_metadata = self.metadata.file_metadata();
        // Iterate over columns.
        for (idx, field) in file_metadata.schema_descr().columns().iter().enumerate() {
            // Collapsing header for each column.
            ui.collapsing(field.name(), |ui| {
                let field_type = field.self_type();
                let field_type_str = if field_type.is_primitive() {
                    format!("{}", field_type.get_physical_type())
                } else {
                    format!("{}", field.converted_type())
                };

                ui.label(format!("type: {}", field_type_str));

                ui.label(format!(
                    "sort_order: {}",
                    match file_metadata.column_order(idx) {
                        ColumnOrder::TYPE_DEFINED_ORDER(sort_order) => format!("{}", sort_order),
                        _ => "undefined".to_string(),
                    }
                ));
            });
        }
    }
}
impl CsvMetadataWrapper {
    /// Renders the CSV file metadata (column count, row count).
    pub fn render_metadata(&self, ui: &mut Ui) {
        // Frame for visual grouping.
        Frame::default()
            .stroke(Stroke::new(1.0, Color32::GRAY))
            .outer_margin(2.0)
            .inner_margin(10.0)
            .show(ui, |ui| {
                // Grid layout
                Grid::new("version_grid")
                    .num_columns(2)
                    .spacing([10.0, 20.0])
                    .striped(true)
                    .show(ui, |ui| {
                        let column_count = self.schema.len();
                        ui.label("Columns:");
                        ui.label(column_count.to_string());
                        ui.end_row();

                        let row_count = self.row_count;
                        ui.label("Rows:");
                        ui.label(row_count.to_string());
                        ui.end_row();
                    });
            });
    }

    /// Renders the CSV file schema.
    pub fn render_schema(&self, ui: &mut Ui) {
        // Iterate over schema fields.
        for (name, dtype) in self.schema.iter() {
            // Collapsing header for each column.
            ui.collapsing(name.to_string(), |ui| {
                ui.label(format!("type: {}", dtype));
            });
        }
    }
}
