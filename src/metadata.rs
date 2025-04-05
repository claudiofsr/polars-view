use crate::DataFrameContainer;

use egui::{Color32, Frame, Grid, Stroke, Ui};
use polars::prelude::*;

/// Represents file metadata.
pub struct FileMetadata {
    /// Number of rows in the dataset.
    row_count: usize,
    /// Number of columns in the dataset.
    col_count: usize,
    /// Schema of the dataset. Used for both Parquet and CSV.
    schema: SchemaRef,
}

impl FileMetadata {
    /// Creates `FileMetadata` from a `DataFrameContainer`.
    pub fn from_container(container: &DataFrameContainer) -> Option<Self> {
        let row_count = container.df.height();
        let col_count = container.df.width();
        let schema = container.df.schema().clone();

        Some(FileMetadata {
            row_count,
            col_count,
            schema,
        })
    }

    /// Renders the file metadata (row count, column count) to the UI.
    pub fn render_metadata(&self, ui: &mut Ui) {
        Frame::default()
            .stroke(Stroke::new(1.0, Color32::GRAY))
            .outer_margin(2.0)
            .inner_margin(10.0)
            .show(ui, |ui| {
                Grid::new("metadata_grid")
                    .num_columns(2)
                    .spacing([10.0, 20.0])
                    .striped(true)
                    .show(ui, |ui| {
                        ui.label("Columns:");
                        ui.label(self.col_count.to_string());
                        ui.end_row();

                        ui.label("Rows:");
                        ui.label(self.row_count.to_string());
                        ui.end_row();
                    });
            });
    }

    /// Renders the file schema information to the UI.
    /// Each column's name is displayed as a collapsing header,
    /// and the column's index and data type are shown within the collapsed section.
    /// Adds copy-to-clipboard functionality on right-click of the column name.
    pub fn render_schema(&self, ui: &mut Ui) {
        // Add a hint to inform the user about copy functionality.
        ui.label("Tip: Right-click a column name to copy it to the clipboard.");

        for (index, (name, dtype)) in self.schema.iter().enumerate() {
            // Create a collapsing header for each column.  The header displays the column name.
            let header_response = ui.collapsing(name.to_string(), |ui| {
                // Inside the collapsing section, display the column index and data type.
                ui.label(format!("index: {}", index));
                ui.label(format!("type: {}", dtype));
            });

            // Check if the header was clicked (specifically with the right mouse button).
            if header_response
                .header_response
                .clicked_by(egui::PointerButton::Secondary)
            {
                // If the right mouse button was clicked, copy the column name to the clipboard.
                ui.ctx().copy_text(name.to_string());
            }
        }
    }
}
