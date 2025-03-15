use crate::{DataFrameContainer, FileExtension, PolarsViewError, PolarsViewResult};

use egui::Context;
use polars::prelude::*;
use rfd::AsyncFileDialog;
use std::{fs::File, path::PathBuf};
use tokio::sync::oneshot;
use tracing::error;

/// Opens a file dialog asynchronously, allowing the user to choose a file.
///
/// ### Returns
///
/// - `Ok(PathBuf)`: The path to the selected file if the user successfully chooses one.
/// - `Err(PolarsViewError::FileNotFound)`:  If the user cancels the dialog (no file is selected).
pub async fn file_dialog() -> PolarsViewResult<PathBuf> {
    // Open the file dialog.
    let opt_file = AsyncFileDialog::new().pick_file().await;

    opt_file
        .map(|file| file.path().to_path_buf())
        .ok_or_else(|| PolarsViewError::FileNotFound(PathBuf::new()))
}

/// Saves the DataFrame to a file asynchronously, handling CSV and Parquet formats.
/// The user is presented with a file dialog to choose the save location and format.
///
/// ### Arguments
///
/// * `container`: The `DataFrameContainer` holding the DataFrame to save.
/// * `ctx`: The `egui::Context` for UI interaction, needed for repainting.
///
/// ### Returns
///
/// A `PolarsViewResult<()>` indicating success or failure of the save operation.
pub async fn save_file_dialog(
    container: Arc<DataFrameContainer>,
    ctx: Context,
) -> PolarsViewResult<()> {
    // 1. Determine the default file name.
    let default_file_name = container
        .filters
        .absolute_path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("dataframe.csv");

    // 2. Open the save file dialog, pre-setting the filename.
    let file = AsyncFileDialog::new()
        .add_filter("CSV", &["csv"])
        .add_filter("Json", &["json"])
        .add_filter("NDJson", &["ndjson"])
        .add_filter("Parquet", &["parquet"])
        .set_file_name(default_file_name)
        .save_file()
        .await;

    // 3. Handle the user's file selection (if any).
    if let Some(file) = file {
        let mut df = container.df.as_ref().clone();

        // 4. Create a channel for the write result *before* spawning.
        let (tx, rx) = oneshot::channel::<PolarsViewResult<()>>();

        // 5. The file-writing logic (inside spawn_blocking).
        let _handle = tokio::task::spawn_blocking(move || {
            let result: PolarsViewResult<()> = match FileExtension::from_path(file.path()) {
                FileExtension::Csv => {
                    // Use CSV separator
                    let delimiter = match container.filters.csv_delimiter.as_bytes().first() {
                        Some(byte) => *byte,
                        None => {
                            return Err(PolarsViewError::InvalidDelimiter(
                                container.filters.csv_delimiter.clone(),
                            ));
                        }
                    };
                    let mut file = File::create(file.path())?;
                    CsvWriter::new(&mut file)
                        .with_separator(delimiter)
                        .finish(&mut df)
                        .map_err(PolarsViewError::from)
                }
                FileExtension::Json => {
                    // Added json
                    let mut file = File::create(file.path())?;
                    JsonWriter::new(&mut file)
                        .with_json_format(JsonFormat::Json)
                        .finish(&mut df)
                        .map_err(PolarsViewError::from)
                }
                FileExtension::NDJson => {
                    // Added ndjson
                    // https://docs.pola.rs/user-guide/io/json/#write
                    let mut file = File::create(file.path())?;
                    JsonWriter::new(&mut file)
                        .with_json_format(JsonFormat::JsonLines)
                        .finish(&mut df)
                        .map_err(PolarsViewError::from)
                }
                FileExtension::Parquet => {
                    let mut file = File::create(file.path())?;
                    ParquetWriter::new(&mut file)
                        .finish(&mut df)
                        .map_err(PolarsViewError::from)?;
                    Ok(())
                }
                // Handle Unknown or Missing extension (this is now exhaustive).
                FileExtension::Unknown(_) | FileExtension::Missing => {
                    Err(PolarsViewError::UnsupportedFileType(
                        "Unsupported file extension for saving".to_string(),
                    ))
                }
            };

            // 6. Send result and request repaint *within* spawn_blocking.
            if tx.send(result).is_err() {
                error!("The receiver has been dropped."); // Log failure.
            }

            ctx.request_repaint(); // Still need repaint, even on error.

            Ok::<(), PolarsViewError>(()) //  explicit Ok type for clarity
        });

        // 7. Await result and return.
        let _ = rx
            .await
            .map_err(|e| PolarsViewError::ChannelReceive(e.to_string()))?; // Handle and wrap error.
    }

    Ok(()) // Successful return, even if user cancelled.
}
