use crate::{DataFrameContainer, FileExtension, PolarsViewError, PolarsViewResult};

use egui::Context;
use polars::prelude::*;
use rfd::AsyncFileDialog;
use std::{fs::File, io::BufWriter, path::PathBuf, sync::Arc};
use tokio::sync::oneshot;
use tracing::error;

/// Opens a file dialog asynchronously, allowing the user to choose a file.
///
/// This function uses the `rfd::AsyncFileDialog` to present a native file dialog
/// to the user.  If the user selects a file, the function returns the full path
/// to that file.  If the user cancels the dialog, the function returns a
/// `PolarsViewError::FileNotFound` error.
///
/// # Returns
///
/// - `Ok(PathBuf)`: The path to the selected file if the user successfully chooses one.
/// - `Err(PolarsViewError::FileNotFound)`: If the user cancels the dialog (no file is selected).
pub async fn open_file() -> PolarsViewResult<PathBuf> {
    // Open the file dialog. `pick_file` returns an `Option<FileHandle>`.
    let opt_file = AsyncFileDialog::new().pick_file().await;

    // Convert the `Option<FileHandle>` to a `PolarsViewResult<PathBuf>`.
    // - `map` is used to transform the `PathBuf` inside the `Option`.
    // - `ok_or_else` converts the `Option` to a `Result`:
    //   - If `opt_file` is `Some(file)`, it returns `Ok(PathBuf)`.
    //   - If `opt_file` is `None` (user cancelled), it returns the `FileNotFound` error.
    opt_file
        .map(|file| file.path().to_path_buf()) // Extract PathBuf from FileHandle.
        .ok_or_else(|| PolarsViewError::FileNotFound(PathBuf::new())) // Convert None to error.
}

/// Saves the DataFrame contained in `DataFrameContainer` to a file.
///
/// The file format is determined by the provided `FileExtension`. Supported formats are CSV, Json,
/// NDJson (Newline-Delimited Json), and Parquet.  The function handles potential mismatches
/// between the user-specified file extension and the original file's extension.
///
/// # Arguments
///
/// * `container`: A `DataFrameContainer` holding the DataFrame to be saved, along with
///   metadata including the user's intended file extension and the original file's extension.
/// * `ctx`: The `egui::Context` for UI interaction, needed for repainting.
///
/// # Returns
///
/// A `PolarsViewResult` indicating success or the type of error encountered.
///
/// # Errors
///
/// This function can return errors due to:
/// * File I/O issues.
/// * If the chosen file format is not supported.
/// * If the file type and file extension do not match (e.g., saving a CSV file with a .parquet extension).
///   This is handled by `PolarsViewError::UnsupportedFileType`.
/// * If the filename cannot be determined.
pub async fn save(container: Arc<DataFrameContainer>, ctx: Context) -> PolarsViewResult<()> {
    // Get the file path from the container's filters.
    let path = container.filters.absolute_path.clone();
    // Determine the file extension from the path.
    let file_extension = FileExtension::from_path(&path);

    // 4. Create a oneshot channel for communication between the file-writing task and the main thread.
    let (tx, rx) = oneshot::channel::<PolarsViewResult<()>>();

    // 5. Spawn a blocking task for file I/O. This is crucial to prevent blocking the main UI thread.
    let _handle = tokio::task::spawn_blocking(move || {
        // Create the output file.  `File::create` will create the file if it doesn't
        // exist, or truncate it if it does. The `?` operator handles potential I/O errors.
        let file = File::create(&path)?;
        let mut writer = BufWriter::new(file);

        // Validate that the file extension matches the intended file format.
        //
        // We compare the *intended* extension (from the user, based on the file name
        // they entered) with the *original* file extension (stored in the container,
        // from when the file was first loaded). This detects the case where a user
        // tries to, for example, save a CSV file *as* a .parquet file by renaming it.
        //
        // We handle the `Missing` extension case: if the *original* file didn't have
        // an extension (unlikely, but possible), we assume the user's save selection
        // is the intended format.

        let result: PolarsViewResult<()> = match (&file_extension, container.extension.as_ref()) {
            // CSV cases.
            (FileExtension::Csv, FileExtension::Csv)    // CSV -> CSV:  OK.
            | (FileExtension::Csv, FileExtension::Missing) => // Missing original -> CSV:  OK, user probably wants CSV now.
                CsvWriter::new(&mut writer)
                .finish(&mut container.df.as_ref().clone())  // Write as CSV
                .map_err(PolarsViewError::from),   // Convert PolarsError to our error type
           // Json
            (FileExtension::Json, FileExtension::Json)    // Json -> Json:  OK.
            | (FileExtension::Json, FileExtension::Missing) => // Missing original -> Json:  OK, user probably wants Json now.
                JsonWriter::new(&mut writer)
                .finish(&mut container.df.as_ref().clone()) // Write as Json
                .map_err(PolarsViewError::from),   // Convert PolarsError to our error type.
           // NDJson cases
            (FileExtension::NDJson, FileExtension::NDJson)    // NDJson -> NDJson:  OK.
            | (FileExtension::NDJson, FileExtension::Missing) => // Missing original -> NDJson:  OK, user probably wants NDJson now.
                // Corrected: NDJson uses JsonLineWriter, not JsonWriter
                JsonWriter::new(&mut writer)
                .finish(&mut container.df.as_ref().clone())  // Write as NDJson
                .map_err(PolarsViewError::from),   // Convert PolarsError to our error type
           // Parquet
            (FileExtension::Parquet, FileExtension::Parquet)    // Parquet -> Parquet: OK
            | (FileExtension::Parquet, FileExtension::Missing) => {
                // Missing original -> Parquet: OK, assume user knows what they're doing.
                ParquetWriter::new(&mut writer).finish(&mut container.df.as_ref().clone())?; // Propagate errors
                Ok(())
            }
            _ => {
                // Mismatch. Construct a detailed error message, including the problematic filename.
                // The `?` inside `ok_or_else` and `to_str` propagates errors if those operations fail
                // Get filename with extension
                let file_name = path
                    .file_name()
                    .ok_or_else(|| PolarsViewError::Other("Could not get file name".into()))? // Convert OsStr to String
                    .to_str() // Convert to &str
                    .ok_or_else(|| PolarsViewError::Other("Invalid UTF-8 in file name.".into()))?; // Propagate conversion errors

                Err(PolarsViewError::UnsupportedFileType(format!(
                    "`{file_name}`: file type and extension do not match",
                )))?
            }
        };

        // 6. Send the result and request repaint *within* spawn_blocking.
        if tx.send(result).is_err() {
            error!("The receiver has been dropped."); // Log failure.
        }

        ctx.request_repaint(); // Still need repaint, even on error.

        Ok::<(), PolarsViewError>(()) //  Explicitly return Ok
    });

    // 7. Await the result of the spawned task.  This handles errors both from the
    //    file-writing operation *and* from the channel communication (if the
    //    sender was dropped before sending, which indicates a problem).  The `?`
    //    at the end propagates these errors to the caller of `save`.
    let _ = rx
        .await
        .map_err(|e| PolarsViewError::ChannelReceive(e.to_string()))?; // Handle and wrap error.

    Ok(()) // If we get here, everything succeeded.
}

/// Saves the DataFrame to a file asynchronously, handling CSV, Json, NDJson and Parquet formats.
/// The user is presented with a file dialog to choose the save location and format.
///
/// ### Arguments
///
/// * `container`: The `DataFrameContainer` holding the DataFrame to save, wrapped in an `Arc` for shared ownership.
/// * `ctx`: The `egui::Context` for UI interaction, needed for repainting the UI after the save operation.
///
/// ### Returns
///
/// A `PolarsViewResult<()>` indicating success or failure of the save operation.  The empty tuple `()`
/// signifies successful completion.
pub async fn save_as(container: Arc<DataFrameContainer>, ctx: Context) -> PolarsViewResult<()> {
    // 1. Determine the default file name from the original file's name (if available).
    //    If the original file is "data.csv", the default save name will be "data.csv".
    //    If there's no original file, it defaults to "dataframe.csv".
    let default_file_name = container
        .filters
        .absolute_path
        .file_name() // Get the file name (e.g., "data.csv").
        .and_then(|name| name.to_str()) // Convert the `OsStr` to a `&str` (if possible).
        .unwrap_or("dataframe.csv"); // Use "dataframe.csv" if the original name isn't available.

    // 2. Open the save file dialog, pre-setting the filename and providing format filters.
    let file = AsyncFileDialog::new()
        .add_filter("CSV", &["csv"]) // Add a filter for CSV files.
        .add_filter("Json", &["json"]) // Add a filter for Json files.
        .add_filter("NDJson", &["ndjson"]) // Add a filter for NDJson files.
        .add_filter("Parquet", &["parquet"]) // Add a filter for Parquet files.
        .set_file_name(default_file_name) // Set the default file name.
        .save_file() // Show the dialog and get the chosen file (if any).
        .await;

    // 3. Handle the user's file selection (if any).  `file` is an `Option<FileHandle>`.
    if let Some(file) = file {
        // Clone the DataFrame. Needed for thread safety in the blocking task.
        let mut df = container.df.as_ref().clone();

        // 4. Create a channel for communicating the write result *before* spawning the blocking task.
        //    This ensures the receiving end is ready before sending.  The channel carries a `PolarsViewResult<()>`
        //    to signal success or propagate errors from the file writing operation.
        let (tx, rx) = oneshot::channel::<PolarsViewResult<()>>();

        // 5. Spawn a blocking task for the file-writing operation to avoid blocking the UI thread.
        // `spawn_blocking` is necessary because file I/O operations are typically blocking.
        let _handle = tokio::task::spawn_blocking(move || {
            // Determine the file extension and write the DataFrame to the file.
            let result: PolarsViewResult<()> = match FileExtension::from_path(file.path()) {
                FileExtension::Csv => {
                    // Use CSV separator from DataFilters
                    let delimiter = match container.filters.csv_delimiter.as_bytes().first() {
                        Some(byte) => *byte,
                        None => {
                            return Err(PolarsViewError::InvalidDelimiter(
                                container.filters.csv_delimiter.clone(),
                            ));
                        }
                    };
                    // Create the file (overwrites if it exists, creates if it doesn't).
                    let mut file = File::create(file.path())?;
                    // Create a CSV writer and write the DataFrame.
                    CsvWriter::new(&mut file)
                        .with_separator(delimiter) // Set the CSV delimiter/separator
                        .finish(&mut df) // Write the data and handle errors.
                        .map_err(PolarsViewError::from) // Convert PolarsError to PolarsViewError.
                }
                FileExtension::Json => {
                    // Added json
                    // Create the file
                    let mut file = File::create(file.path())?;
                    // Create a Json writer and write the DataFrame.
                    JsonWriter::new(&mut file)
                        .with_json_format(JsonFormat::Json)
                        .finish(&mut df)
                        .map_err(PolarsViewError::from) // Convert PolarsError to PolarsViewError
                }
                FileExtension::NDJson => {
                    // Added ndjson
                    // https://docs.pola.rs/user-guide/io/json/#write
                    // Create the file
                    let mut file = File::create(file.path())?;
                    // Create a Json writer and write the DataFrame.
                    JsonWriter::new(&mut file)
                        .with_json_format(JsonFormat::JsonLines) // Use JsonLines for NDJson
                        .finish(&mut df)
                        .map_err(PolarsViewError::from) // Convert PolarsError to PolarsViewError
                }
                FileExtension::Parquet => {
                    // Create the file.
                    let mut file = File::create(file.path())?;
                    // Create a Parquet writer and write the DataFrame.
                    ParquetWriter::new(&mut file)
                        .finish(&mut df)
                        .map_err(PolarsViewError::from)?; // Convert and propagate errors.
                    Ok(()) // Explicit Ok for clarity.
                }
                // Handle Unknown or Missing extension (this is now exhaustive).  If the user
                // doesn't select a filter, rfd defaults to the first filter (CSV in this case),
                // so this error should rarely, if ever, occur with the current setup.  It's
                // more relevant for the `save` function, where the user might not have an
                // extension in the original file path.
                FileExtension::Unknown(_) | FileExtension::Missing => {
                    Err(PolarsViewError::UnsupportedFileType(
                        "Unsupported file extension for saving".to_string(),
                    ))
                }
            };

            // 6. Send the result of the file-writing operation and request a UI repaint *within*
            //    the `spawn_blocking` closure. This ensures the message is sent even if the
            //    receiver has been dropped.
            if tx.send(result).is_err() {
                error!("The receiver has been dropped."); // Log a warning if sending fails.
            }

            ctx.request_repaint(); // Request a repaint of the UI. Essential for updates.

            Ok::<(), PolarsViewError>(()) //  Explicitly return Ok, even if void, for clarity
        });

        // 7. Await the result from the channel and handle potential errors. The `?` operator
        //    propagates any errors that occurred during the send or during the async task.
        let _ = rx
            .await
            .map_err(|e| PolarsViewError::ChannelReceive(e.to_string()))?; // Handle and wrap error.
    }

    Ok(()) // Return Ok even if the user cancelled the dialog (no file selected).
}
