use crate::{
    Arguments, DEFAULT_OVERRIDE_REGEX, DEFAULT_QUERY, FileExtension, PathExtension,
    PolarsViewError, PolarsViewResult, UniqueElements, sql_commands,
};
use egui::{
    Align, CollapsingHeader, Color32, DragValue, Frame, Grid, Layout, Stroke, TextEdit, Ui, Vec2,
};
use polars::{io::RowIndex, prelude::*};
use regex::Regex;
use tokio::task::spawn_blocking;

use std::{
    fmt::Debug,
    fs::File,
    num::NonZero,
    path::{Path, PathBuf},
    sync::Arc,
};

// --- Constants ---

/// Static string listing common values treated as null/missing during CSV parsing.
/// The `r#""#` syntax denotes a raw string literal, avoiding the need to escape quotes.
pub static NULL_VALUES: &str = r#""", <N/D>"#;

/// Default delimiter used for CSV parsing if not specified or detected.
/// Using `&'static str` for common, immutable delimiters saves memory allocation.
pub static DEFAULT_CSV_DELIMITER: &str = ";";

/// Default name for the row number column if added.
pub const DEFAULT_INDEX_COLUMN_NAME: &str = "Row Number";

/// Default regex
const DEFAULT_NORM_REGEX: &str = "^Val.*$";

/// Default drop regex
const DEFAULT_DROP_REGEX: &str = "^Temp.*$";

/// Default starting offset for the row index column (e.g., 1 for 1-based).
const DEFAULT_INDEX_COLUMN_OFFSET: u32 = 1;

const DEFAULT_INFER_SCHEMA_ROWS: usize = 200;

// Prevent potential infinite loops (e.g., schema keeps changing).
pub const MAX_ATTEMPTS: u32 = 1000;

// --- DataFilter Struct ---

/// Holds configuration parameters related to **loading and querying** data.
///
/// This struct focuses on settings that define how data is initially read from a file
/// and transformed via SQL queries or basic processing like null column removal.
///
/// Instances are created from `Arguments`, updated by the UI in `render_query`, and passed
/// to `DataFrameContainer::load_data`. Changes here typically trigger a data reload/requery.
#[derive(Debug, Clone, PartialEq)] // PartialEq allows simple change detection
pub struct DataFilter {
    /// The canonical, absolute path to the data file.
    pub absolute_path: PathBuf,
    /// The name assigned to the loaded DataFrame for use in SQL queries.
    pub table_name: String,
    /// The character used to separate columns in a CSV file.
    pub csv_delimiter: String,
    /// Read data from file
    pub read_data_from_file: bool,
    /// The schema (column names and data types) of the most recently loaded DataFrame.
    /// Used by `sql_commands` for generating relevant examples.
    pub schema: Arc<Schema>,
    /// Maximum rows to scan for schema inference (CSV, JSON, NDJson).
    pub infer_schema_rows: usize,
    /// Flag to control removal of all-null columns after loading/querying.
    pub exclude_null_cols: bool,
    /// Comma-separated string of values to interpret as nulls during CSV parsing.
    pub null_values: String,

    /// Regex patterns matching columns to force read as String type.
    ///
    /// List of column names to force reading as String, overriding inference.
    /// Useful for columns with large IDs/keys that look numeric.
    pub force_string_patterns: Option<String>,

    /// Flag indicating if the `query` should be executed during the next `load_data`.
    /// Set by `render_query` if relevant UI fields change or the Apply button is clicked.
    pub apply_sql: bool,
    /// The SQL query string entered by the user.
    pub query: String,

    // --- NEW FIELDS for Index Column ---
    /// Flag indicating if a row index column should be added.
    pub add_row_index: bool,
    /// The desired name for the row index column (will be checked for uniqueness later).
    pub index_column_name: String,
    /// The starting value for the row index column (e.g., 0 or 1).
    pub index_column_offset: u32,
    // --- END NEW FIELDS ---

    // --- Normalize Columns ---
    /// Flag indicating whether string columns will be normalized.
    pub normalize: bool,
    /// Regex pattern to select string columns.
    pub normalize_regex: String,

    // --- Drop Columns ---
    pub drop: bool,
    pub drop_regex: String,
}

impl Default for DataFilter {
    /// Creates default `DataFilter` with sensible initial values.
    fn default() -> Self {
        DataFilter {
            absolute_path: PathBuf::new(),
            table_name: "AllData".to_string(),
            csv_delimiter: DEFAULT_CSV_DELIMITER.to_string(),
            read_data_from_file: true,
            schema: Schema::default().into(),
            infer_schema_rows: DEFAULT_INFER_SCHEMA_ROWS,
            exclude_null_cols: false,
            null_values: NULL_VALUES.to_string(),

            force_string_patterns: DEFAULT_OVERRIDE_REGEX.map(ToString::to_string),

            apply_sql: false,
            query: DEFAULT_QUERY.to_string(),

            // --- NEW DEFAULTS ---
            add_row_index: false, // Default to false
            index_column_name: DEFAULT_INDEX_COLUMN_NAME.to_string(),
            index_column_offset: DEFAULT_INDEX_COLUMN_OFFSET,
            // --- END NEW DEFAULTS ---

            // --- NEW FIELDS for Normalize Columns ---
            normalize: false,
            normalize_regex: DEFAULT_NORM_REGEX.to_string(),
            // --- END NEW FIELDS ---
            drop: false,
            drop_regex: DEFAULT_DROP_REGEX.to_string(),
        }
    }
}

// --- Methods ---

impl DataFilter {
    /// Creates a new `DataFilter` instance configured from command-line `Arguments`.
    /// This is typically called once at application startup in `main.rs`.
    ///
    /// ### Arguments
    /// * `args`: Parsed command-line arguments (`crate::Arguments`).
    ///
    /// ### Returns
    /// A `PolarsViewResult` containing the configured `DataFilter` or an error
    /// (e.g., if the path cannot be canonicalized).
    pub fn new(args: &Arguments) -> PolarsViewResult<Self> {
        // Ensure the path exists and get its absolute, canonical form.
        let absolute_path = args.path.canonicalize()?;

        // Determine apply_sql state from the CLI argument
        let apply_sql = args.query.is_some();
        let query = args
            .query
            .clone()
            .unwrap_or_else(|| DEFAULT_QUERY.to_string()); // Use CLI arg or default        

        // Determine normalization state from the CLI argument
        let normalize = args.regex.is_some();
        let normalize_regex = args
            .regex
            .clone()
            .unwrap_or_else(|| DEFAULT_NORM_REGEX.to_string()); // Use CLI arg or default

        // Use or_else: takes a closure executed only if the first option is None.
        // This avoids the .to_string() for the default unless actually needed.
        let force_string_patterns = args
            .force_string_patterns // This is Option<String>
            .clone() // Clone the Option<String> from args if needed later, otherwise maybe take ownership
            .or(DEFAULT_OVERRIDE_REGEX.map(ToString::to_string)); // Use CLI arg or default

        Ok(DataFilter {
            absolute_path,
            table_name: args.table_name.clone(),
            csv_delimiter: args.delimiter.clone(),

            apply_sql, // Directly set based on CLI argument presence
            query,     // Directly set based on CLI argument value (or default)

            exclude_null_cols: args.exclude_null_cols,
            null_values: args.null_values.clone(), // Use user-provided nulls.

            force_string_patterns,

            normalize,            // Directly set based on CLI argument presence
            normalize_regex,      // Directly set based on CLI argument value (or default)
            ..Default::default()  // Use defaults for `schema`, `infer_schema_rows`.
        })
    }

    /// Sets the data source path, canonicalizing it.
    pub fn set_path(&mut self, path: &Path) -> PolarsViewResult<()> {
        self.absolute_path = path.canonicalize()?;
        tracing::debug!("absolute_path set to: {:#?}", self.absolute_path);
        Ok(())
    }

    /// Gets the file extension from `absolute_path` in lowercase.
    pub fn get_extension(&self) -> Option<String> {
        self.absolute_path.extension_as_lowercase()
    }

    /// Determines the configuration for an optional row index column by resolving a unique name
    /// against the provided schema.
    ///
    /// If `self.add_row_index` is true, this method finds a unique name based on
    /// `self.index_column_name` and the provided `schema`, returning a `Some(RowIndex)`.
    /// If the name resolution fails, it returns the specific PolarsError.
    /// If `self.add_row_index` is false, it returns `Ok(None)`.
    ///
    /// ### Arguments
    /// * `schema`: The schema against which the index column name should be checked for uniqueness.
    ///   This should be the schema of the DataFrame *before* adding the index column.
    ///
    /// ### Returns
    /// `PolarsResult<Option<RowIndex>>`: Ok(Some) if config is resolved, Ok(None) if disabled, Err if resolution fails.
    pub fn get_row_index(&self, schema: &Schema) -> PolarsResult<Option<RowIndex>> {
        // Check the main feature flag
        if !self.add_row_index {
            tracing::trace!("Row index addition disabled in filter.");
            return Ok(None); // Feature disabled, return None config
        }

        // Feature is enabled. Resolve a unique name using the helper.
        let unique_name = resolve_unique_column_name(
            // Use the helper function
            &self.index_column_name, // Base name
            schema,                  // Schema to check uniqueness against
        )?; // Propagate potential error from unique name resolution

        // If we successfully resolved a unique name, return the full RowIndex config
        let index_offset = self.index_column_offset;
        Ok(Some(RowIndex {
            name: unique_name,
            offset: index_offset,
        }))
    }

    /// Determines the `FileExtension` and orchestrates loading the DataFrame using the appropriate Polars reader.
    /// This method centralizes the file-type-specific loading logic. Called by `DataFrameContainer::load_data`.
    ///
    /// **Important:** It mutates `self` by potentially updating `csv_delimiter` if automatic
    /// detection during `read_csv_data` finds a different working delimiter than initially configured.
    ///
    /// ### Returns
    /// A `PolarsViewResult` containing a tuple: `(DataFrame, FileExtension)` on success,
    /// or a `PolarsViewError` (e.g., `FileType`, `CsvParsing`) on failure.
    pub async fn get_df_and_extension(&mut self) -> PolarsViewResult<(DataFrame, FileExtension)> {
        // Determine the file extension type using the helper from `extension.rs`.
        let extension = FileExtension::from_path(&self.absolute_path);

        // Match on the determined extension to call the correct reader function.
        let (df, detected_delimiter) = match &extension {
            FileExtension::Csv => self.read_csv_data().await?,
            FileExtension::Json => self.read_json_data().await?,
            FileExtension::NDJson => self.read_ndjson_data().await?,
            FileExtension::Parquet => self.read_parquet_data().await?,
            // Handle unsupported or missing extensions with specific errors.
            FileExtension::Unknown(ext) => {
                return Err(PolarsViewError::FileType(format!(
                    "Unsupported extension: `{}` for file: `{}`",
                    ext,
                    self.absolute_path.display()
                )));
            }
            FileExtension::Missing => {
                return Err(PolarsViewError::FileType(format!(
                    "Missing extension for file: `{}`",
                    self.absolute_path.display()
                )));
            }
        };

        // If reading a CSV successfully detected a working delimiter, update the filters state.
        // This ensures the UI reflects the delimiter actually used.
        if let Some(byte) = detected_delimiter {
            self.csv_delimiter = (byte as char).to_string();
        }

        tracing::debug!(
            "fn get_df_and_extension(): Successfully loaded DataFrame with extension: {:?}",
            extension
        );

        Ok((df, extension)) // Return the loaded DataFrame and the detected extension.
    }

    // --- Data Reading Helper Methods ---

    /// Reads a standard JSON file into a Polars DataFrame.
    /// Configures the reader using settings from `self` (e.g., `infer_schema_rows`).
    ///
    /// ### Returns
    /// A `PolarsViewResult` containing `(DataFrame, None)` (delimiter is not applicable to JSON).
    async fn read_json_data(&self) -> PolarsViewResult<(DataFrame, Option<u8>)> {
        tracing::debug!("Reading JSON data from: {}", self.absolute_path.display());
        let file = File::open(&self.absolute_path)?;
        let infer_schema_rows_for_task = self.infer_schema_rows;

        // Execute the blocking read operation on a separate thread
        let df = execute_polars_blocking(move || {
            JsonReader::new(file)
                .infer_schema_len(NonZero::new(infer_schema_rows_for_task))
                .finish()
        })
        .await?;

        tracing::debug!("JSON read complete. Shape: {:?}", df.shape());

        Ok((df, None))
    }

    /// Reads a Newline-Delimited JSON (NDJson / JSON Lines) file into a Polars DataFrame.
    /// Uses `LazyJsonLineReader` for potentially better performance/memory usage on large files.
    ///
    /// ### Returns
    /// A `PolarsViewResult` containing `(DataFrame, None)`.
    async fn read_ndjson_data(&self) -> PolarsViewResult<(DataFrame, Option<u8>)> {
        tracing::debug!("Reading NDJSON data from: {}", self.absolute_path.display());

        // Clone data from self needed for the task closure.
        let path_buf_for_task = PlPath::Local(self.absolute_path.clone().into());
        let infer_schema_rows_for_task = self.infer_schema_rows;

        // *** Use the helper function ***
        let df = execute_polars_blocking(move || {
            // 'move' captures path_buf_for_task, infer_schema_rows_for_task
            // This code runs on the blocking thread.
            let lazyframe = LazyJsonLineReader::new(path_buf_for_task) // Use cloned path
                .low_memory(false) // Option to optimize for memory.
                .with_infer_schema_length(NonZero::new(infer_schema_rows_for_task))
                .with_ignore_errors(true)
                .finish()?; // Returns PolarsResult<LazyFrame> (this finish() isn't the main blocking part)

            // Collect the lazy frame - THIS IS THE BLOCKING PART
            lazyframe.with_new_streaming(true).collect() // Returns PolarsResult<DataFrame>
        })
        .await?; // await the helper function

        tracing::debug!("NDJSON read complete. Shape: {:?}", df.shape());
        Ok((df, None))
    }

    /// Reads an Apache Parquet file into a Polars DataFrame.
    ///
    /// ### Returns
    /// A `PolarsViewResult` containing `(DataFrame, None)`.
    async fn read_parquet_data(&self) -> PolarsViewResult<(DataFrame, Option<u8>)> {
        tracing::debug!(
            "Reading Parquet data from: {}",
            self.absolute_path.display()
        );

        // Clone data from self needed for the task closure.
        let path_buf_for_task = PlPath::Local(self.absolute_path.clone().into());
        let args = ScanArgsParquet {
            // ScanArgsParquet should be Send
            low_memory: false, // Configure scan arguments as needed.
            ..Default::default()
        };

        let df = execute_polars_blocking(move || {
            // Use `LazyFrame::scan_parquet` for efficient scanning.
            let lazyframe = LazyFrame::scan_parquet(path_buf_for_task, args)?; // Returns PolarsResult<LazyFrame>

            // Collect into an eager DataFrame - THIS IS THE BLOCKING/COMPUTE PART.
            lazyframe.with_new_streaming(true).collect() // Returns PolarsResult<DataFrame>
        })
        .await?; // await the helper function            

        tracing::debug!("Parquet read complete. Shape: {:?}", df.shape());

        Ok((df, None))
    }

    /// Reads a CSV file, attempting automatic delimiter detection if the initial one fails.
    /// Iterates through common delimiters and tries reading a small chunk first for efficiency.
    ///
    /// ### Returns
    /// A `PolarsViewResult` containing `(DataFrame, Option<u8>)` where `Option<u8>` is the
    /// *successfully used* delimiter byte. Returns `Err(PolarsViewError::CsvParsing)` if
    /// no common delimiter works.
    async fn read_csv_data(&self) -> PolarsViewResult<(DataFrame, Option<u8>)> {
        // Get the currently configured separator byte. Error if invalid (e.g., empty string).
        let initial_separator = self.get_csv_separator()?;

        // List of common delimiters to try, starting with the configured one.
        let mut delimiters_to_try = vec![initial_separator, b',', b';', b'|', b'\t', b':'];
        // Remove duplicates if the initial separator is already in the common list.
        delimiters_to_try.unique();
        tracing::debug!(
            "Attempting CSV read. Delimiters to try: {:?}",
            delimiters_to_try
                .iter()
                .map(|&b| b as char)
                .collect::<Vec<_>>()
        );

        // Look at the next element of the iterator without consuming it.
        let mut iterator = delimiters_to_try.iter().peekable();

        // Iterate through the potential delimiters.
        while let Some(&delimiter) = iterator.next() {
            // If peek() returns None, it means the current item was the last one
            let is_last_element = iterator.peek().is_none();

            // 1. Quick Check: Try reading only a small number of rows (NROWS_CHECK).
            // This fails fast if the delimiter is fundamentally wrong (e.g., results in 1 column).
            if let Ok(schema) = self
                .attempt_csv_parse_structure(delimiter, is_last_element)
                .await
            {
                // 2. Full Read: If the quick check passed, attempt to read the entire file.
                tracing::debug!(
                    "Trying to read full CSV file with delimiter: '{}'",
                    delimiter as char
                );
                match self.attempt_read_csv(delimiter, &schema).await {
                    Ok(lazyframe) => {
                        // Success! Return the DataFrame and the delimiter that worked.
                        tracing::info!(
                            "Successfully read CSV with delimiter: '{}'",
                            delimiter as char
                        );

                        // Execute the lazy plan and collect into an eager DataFrame on a blocking thread
                        let df = execute_polars_blocking(move || {
                            lazyframe.with_new_streaming(true).collect()
                        })
                        .await?;

                        tracing::debug!("Data collection complete. Shape: {:?}", df.shape());
                        return Ok((df, Some(delimiter)));
                    }
                    Err(e) => {
                        // Full read failed even after quick check passed. Log and try next delimiter.
                        tracing::warn!(
                            "Full CSV read failed with delimiter '{}' after quick check passed: {}",
                            delimiter as char,
                            e
                        );
                        continue; // Try the next delimiter.
                    }
                }
            }
            // If quick check fails, implicitly try the next delimiter.
        }

        // If all delimiters failed, return a parsing error.
        let msg = format!(
            "Failed to read CSV '{}' with common delimiters. Check format or specify delimiter.",
            self.absolute_path.display()
        );
        let error = PolarsViewError::CsvParsing(msg);
        tracing::error!("{}", error);
        Err(error)
    }

    /// Retrieves the CSV separator byte from the `csv_delimiter` String configuration.
    ///
    /// ### Returns
    /// `Ok(u8)` containing the first byte, or `Err(PolarsViewError::InvalidDelimiter)`
    /// if the string is empty or contains multi-byte characters (only first byte is used).
    fn get_csv_separator(&self) -> PolarsViewResult<u8> {
        self.csv_delimiter
            .as_bytes() // Convert String to byte slice.
            .first() // Get the first byte.
            .copied() // Copy the byte out of the Option<&u8>.
            // Map `None` (empty string) to an InvalidDelimiter error.
            .ok_or_else(|| PolarsViewError::InvalidDelimiter(self.csv_delimiter.clone()))
    }

    /// Attempts to parse the CSV structure from the initial chunk of the file
    /// using a specific delimiter and validates the result.
    async fn attempt_csv_parse_structure(
        &self,
        delimiter: u8,
        is_last_element: bool,
    ) -> PolarsViewResult<Arc<Schema>> {
        // Constant: Number of data rows to ask Polars to read *after* the header
        // during this quick probe. 100 is a common heuristic to get enough
        // context without reading the whole file.
        const ROW_LIMIT: usize = 100;

        tracing::debug!(
            "Trying to parse CSV with delimiter: '{}'",
            delimiter as char,
        );

        let file_path = &self.absolute_path;

        // Perform a partial read from the file using the given delimiter.
        let data_frame = read_csv_partial_from_path(delimiter, ROW_LIMIT, file_path).await?;

        // **Basic Validation**: Check resulting width (important for delimiter detection loops)
        // it's highly likely the delimiter was incorrect. Return an error early.
        // This check is crucial for the delimiter detection loop in `read_csv_data`.
        let min_expected_cols_on_success = if self.add_row_index { 2 } else { 1 }; // Assumes index is added *later*

        if data_frame.width() <= min_expected_cols_on_success && !is_last_element {
            tracing::warn!(
                "CSV read with delimiter '{}' resulted in {} columns (expected > {}). Assuming incorrect delimiter.",
                delimiter as char,
                data_frame.width(),
                min_expected_cols_on_success
            );
            // Return a specific error type or message indicating a likely delimiter issue.
            return Err(PolarsViewError::CsvParsing(format!(
                "Delimiter '{}' likely incorrect (resulted in {} columns)",
                delimiter as char,
                data_frame.width()
            )));
        }

        tracing::debug!(
            "CSV read successful with delimiter '{}'. Final shape (rows, columns): {:?}",
            delimiter as char,
            data_frame.shape()
        );

        Ok(data_frame.schema().clone())
    }

    async fn attempt_read_csv(
        &self,
        delimiter: u8,
        previous_scheme: &Arc<Schema>,
    ) -> PolarsViewResult<LazyFrame> {
        tracing::debug!(
            "Attempting CSV read with delimiter: '{}'",
            delimiter as char,
        );

        /*
        When LazyCsvReader tries to determine the data types (with_infer_schema_length()), it looks at the first N rows.
        If your problematic column contains only digits in those initial rows,
        Polars will likely infer a numeric type (like Int64, UInt64, or Float64).

        However, a standard 64-bit integer or float cannot represent an arbitrarily long sequence of digits (like a 44-digit key).
        When the reader later encounters these huge numbers and tries to parse them into the inferred numeric type, the parsing fails.
        with_ignore_errors(true), instead of stopping, Polars replaces the unparseable value with null.
        If all values in that column exceed the capacity of the inferred numeric type, the entire column becomes null, appearing "empty".

        Solution:
        .with_dtype_overwrite(dtypes_opt)
        */

        let mut dtypes_opt: Option<Arc<Schema>> = None;

        if let Some(force_string_patterns) = &self.force_string_patterns {
            // Build dtype overrides using the dedicated function
            //    Pass the actual headers and the configured regex patterns.
            let override_schema = build_dtype_override_schema(
                previous_scheme,
                force_string_patterns, // Get patterns from self
            )?; // Propagate potential regex compilation errors

            // Convert the resulting Schema into Option<Arc<Schema>>.
            if !override_schema.is_empty() {
                dtypes_opt = Some(Arc::new(override_schema));
            };
        }

        let plpath = PlPath::Local(self.absolute_path.clone().into());

        // Configure the LazyCsvReader using settings from `self`.
        let lazyframe = LazyCsvReader::new(plpath)
            .with_low_memory(false) // Can be set to true for lower memory usage at cost of speed.
            .with_encoding(CsvEncoding::LossyUtf8) // Gracefully handle potential encoding errors.
            .with_has_header(true) // Assume a header row.
            .with_try_parse_dates(true) // Attempt automatic date parsing.
            .with_separator(delimiter) // Use the specified delimiter.
            .with_infer_schema_length(Some(self.infer_schema_rows)) // Use filter setting for inference.
            .with_dtype_overwrite(dtypes_opt)
            .with_ignore_errors(true) // Rows with parsing errors become nulls instead of stopping the read.
            .with_missing_is_null(true) // Treat missing fields as null.
            .with_null_values(None) // Apply fn replace_values_with_null()
            .with_n_rows(None) // Apply row limit if specified.
            .with_decimal_comma(false) // If files use ',' as decimal separator.
            .with_row_index(None) // Apply fn add_row_index_column()
            .with_rechunk(true) // Rechunk the memory to contiguous chunks when parsing is done.
            .finish()?; // Finalize configuration and create the LazyFrame.

        Ok(lazyframe)
    }

    /// Parses the comma-separated `null_values` string into a `Vec<&str>`,
    /// removing surrounding double quotes if present.
    ///
    /// Logic:
    /// 1. Splits the input string (`self.null_values`) by commas.
    /// 2. Iterates through each resulting substring (`s`).
    /// 3. For each substring:
    ///    a. Trims leading and trailing whitespace.
    ///    b. Checks if the `trimmed` string has at least 2 characters AND starts with `"` AND ends with `"`.
    ///    c. If true, returns a slice (`&str`) representing the content *between* the quotes.
    ///    Example: `"\"\""` becomes `""`, `" N/A "` becomes `"N/A"`, `" " "` becomes `" "`.
    ///    d. If false (no surrounding quotes), returns a slice (`&str`) of the `trimmed` string itself.
    ///    Example: `<N/D>` remains `<N/D>`, ` NA ` becomes `NA`.
    /// 4. Collects all the resulting string slices into a `Vec<&str>`.
    ///
    /// Example Input: `"\"\", \" \", <N/D>, NA "`
    /// Example Output: `vec!["", " ", "<N/D>", "NA"]`
    pub fn parse_null_values(&self) -> Vec<&str> {
        self.null_values
            .split(',') // 1. Split the string by commas.
            .map(|s| {
                // For each part resulting from the split:
                // 3a. Trim leading/trailing whitespace.
                let trimmed = s.trim();
                // 3b. Check if it's quoted (length >= 2, starts/ends with ").
                if trimmed.len() >= 2 && trimmed.starts_with('"') && trimmed.ends_with('"') {
                    // 3c. If quoted, return the slice between the quotes.
                    trimmed[1..trimmed.len() - 1].trim()
                } else {
                    // 3d. If not quoted, return the trimmed slice directly.
                    trimmed
                }
            })
            .collect() // 4. Collect the processed slices into a vector.
    }

    // --- UI Rendering Methods ---

    /// Renders the UI widgets for configuring data filters within the "Query" collapsing header.
    /// This function is called by `layout.rs::render_side_panel`.
    ///
    /// **Crucially, it takes `&mut self`. Widgets modify `self` directly.**
    /// It compares the state of `self` *before* and *after* rendering the widgets.
    /// If any change occurred (user typed in a field, clicked a checkbox), it returns
    /// `Some(self.clone())` containing the *modified* state. Otherwise, it returns `None`.
    ///
    /// The `layout.rs` code uses this return value:
    /// - If `Some(new_filters)`, it triggers an asynchronous `DataFrameContainer::load_data` call.
    /// - If `None`, no user change was detected in this frame, so no action is taken.
    ///
    /// It also sets `self.apply_sql = true` if any changes are detected, ensuring the SQL
    /// query is re-applied upon reload.
    ///
    /// ### Arguments
    /// * `ui`: The `egui::Ui` context for drawing the widgets.
    ///
    /// ### Returns
    /// * `Some(DataFilter)`: If any filter setting was changed by the user in this frame.
    /// * `None`: If no changes were detected.
    pub fn render_query(&mut self, ui: &mut Ui) -> Option<DataFilter> {
        // Clone the state *before* rendering UI widgets to detect changes later.
        let filters_before_render = self.clone();
        let mut result = None;

        let width_min = 450.0; // Minimum width for the grid area.

        // Use a grid layout for label-input pairs.
        let grid = Grid::new("data_query_grid")
            .num_columns(2)
            .spacing([10.0, 20.0]) // Horizontal and vertical spacing.
            .striped(true); // Alternating row backgrounds.

        // Allocate UI space for the grid.
        ui.allocate_ui_with_layout(
            Vec2::new(ui.available_width(), ui.available_height()), // Occupy available width.
            Layout::top_down(Align::LEFT),
            |ui| {
                grid.show(ui, |ui| {
                    ui.set_min_width(width_min);

                    // --- Render Individual Filter Widgets ---
                    // Each `render_*` method takes `&mut self` and `ui`.

                    self.render_add_row_number(ui);

                    self.render_exclude_null_cols(ui);

                    self.render_exclude_columns(ui);

                    self.render_normalize_numbers(ui);

                    self.render_null_values(ui);

                    // Input for schema inference length (only for relevant file types).
                    if matches!(
                        self.get_extension().as_deref(), // Get extension as &str
                        Some("csv" | "json" | "ndjson")  // Check if it's one of these
                    ) {
                        self.render_schema_length_input(ui);
                    }

                    // CSV-specific settings: delimiter.
                    if self.get_extension().as_deref() == Some("csv") {
                        self.render_csv_delimiter(ui);
                    }

                    // Input for table name used in SQL.
                    self.render_table_name_input(ui);

                    // Multiline input for the SQL query.
                    self.render_sql_query_input(ui);

                    // --- Change Detection & Apply Button ---

                    // Compare current state `self` with the state before rendering.
                    if *self != filters_before_render {
                        // Mark that SQL needs to be (re-)applied.
                        self.apply_sql = true;
                        tracing::debug!("Change detected in DataFilter UI.");
                    }

                    if (self.csv_delimiter != filters_before_render.csv_delimiter)
                        || (self.infer_schema_rows != filters_before_render.infer_schema_rows)
                    {
                        self.read_data_from_file = true;
                    }

                    // Add the "Apply SQL commands" button.
                    ui.label(""); // For alignment.
                    ui.with_layout(Layout::top_down(Align::Center), |ui| {
                        if ui.button("Apply SQL commands").clicked() {
                            if self.apply_sql {
                                // Result contains DataFilter after editing some fields
                                result = Some(self.clone());
                            }

                            tracing::debug!("Apply SQL commands: {}", self.apply_sql);
                        }
                    });
                    ui.end_row();
                }); // End grid.show
            }, // End allocate_ui_with_layout
        ); // End allocation

        // Display the SQL examples section (collapsible).
        self.render_sql_examples(ui);

        result // Return the potentially updated filters.
    }

    // --- Helper Rendering Methods ---

    fn render_add_row_number(&mut self, ui: &mut Ui) {
        // --- Row 1: Feature Checkbox ---
        ui.label("Add Row Number:");
        // The checkbox directly modifies `self.add_row_index`
        ui.checkbox(&mut self.add_row_index, "")
            .on_hover_text("Add a new column that counts the rows (first column).");
        ui.end_row();

        // --- Conditional Configuration Inputs ---
        // These rows are only added to the grid if the checkbox is checked.
        if self.add_row_index {
            // --- Index Name Input ---
            // Use simple indentation in the label for visual structure
            ui.label("\tName:");
            let name_edit =
                TextEdit::singleline(&mut self.index_column_name).desired_width(f32::INFINITY); // Use available width in the grid cell
            ui.add(name_edit)
                .on_hover_text("Name for the new index column (uniqueness checked later).");
            ui.end_row();

            // --- Index Offset Input ---
            // Use simple indentation
            ui.label("\tOffset:");
            let offset_drag = DragValue::new(&mut self.index_column_offset)
                .speed(1) // Increment by 1
                .range(0..=u32::MAX); // Allow 0-based or 1-based commonly
            ui.add(offset_drag)
                .on_hover_text("Starting value for the index (e.g., 0 or 1).");
            ui.end_row();
        }
        // No 'else' needed. If add_row_index is false, these rows are simply skipped.
    }

    /// Renders the checkbox for the "Remove Null Cols" option.
    /// Modifies `self.exclude_null_cols` directly.
    fn render_exclude_null_cols(&mut self, ui: &mut Ui) {
        ui.label("Exclude Null Cols:");
        ui.checkbox(&mut self.exclude_null_cols, "")
            .on_hover_text("Remove columns containing only null values.");
        ui.end_row();
    }

    fn render_exclude_columns(&mut self, ui: &mut Ui) {
        // --- Row 1: Feature Checkbox ---
        ui.label("Remove Columns:");
        ui.checkbox(&mut self.drop, "")
            .on_hover_text("Remove columns whose names match the specified regex pattern.");
        ui.end_row();

        // --- Conditional Configuration Inputs ---
        // These rows are only added to the grid if the checkbox is checked.
        if self.drop {
            // --- Regex Input ---
            // Use simple indentation in the label for visual structure
            ui.label("\tRegex:");
            let name_edit = TextEdit::singleline(&mut self.drop_regex).desired_width(f32::INFINITY); // Use available width in the grid cell
            ui.add(name_edit).on_hover_text(
                "Enter the regex pattern to identify columns to drop by name.\n\n\
                Format Requirements:\n\
                - Use `*` to drop ALL columns.\n\
                - Use `^YourPattern$` to match the entire column name.\n  \
                   (Must start with `^` and end with `$`).\n\n\
                Regex Examples:\n\
                - `^Temp.*$`   (Matches columns starting with 'Temp')\n\
                - `^Value B$`    (Matches the exact column named 'Value B')\n\
                - `^(ID|Key|Index)$` (Matches 'ID', 'Key', or 'Index' exactly)\n\
                - `^.*_OLD$`    (Matches columns ending with '_OLD')\n\n\
                (Invalid regex syntax or format will cause errors.)",
            );
            ui.end_row();
        }
    }

    fn render_normalize_numbers(&mut self, ui: &mut Ui) {
        // --- Row 1: Feature Checkbox ---
        ui.label("Normalize Columns:");
        ui.checkbox(&mut self.normalize, "").on_hover_text(
            "Normalize Euro-style number strings in selected column names (via regex) to Float64.\n\
            Example: '1.234,56' (String) to '1234.56' (Float64).",
        );
        ui.end_row();

        // --- Conditional Configuration Inputs ---
        // These rows are only added to the grid if the checkbox is checked.
        if self.normalize {
            // --- Regex Input ---
            // Use simple indentation in the label for visual structure
            ui.label("\tRegex:");
            let name_edit =
                TextEdit::singleline(&mut self.normalize_regex).desired_width(f32::INFINITY); // Use available width in the grid cell
            ui.add(name_edit).on_hover_text(
                r#"
Enter a regex pattern to select String columns by name.

Rules:
- Use '*' for ALL String columns (caution!).
- Use '^PATTERN$' for specific names (matches entire name).

Example Columns:
Row Number, Value1, Value2, ValueA, Valor, Total, SubTotal, Last Info

Example Patterns:
1. To select 'Value1', 'Value2':
   ^Value\d$

2. To select 'Value1', 'Value2', 'ValueA':
   ^Value.*$

3. To select 'Value1', 'Value2', 'ValueA', 'Valor':
   ^Val.*$

4. To select 'Value1', 'Value2', 'ValueA', 'Valor', 'Total', 'SubTotal':
   ^(Val|.*Total).*$

5. To select only 'Last Info' (note the space):
   ^Last Info$

(Applies only to columns that Polars identifies as String type.)"#,
            );
            ui.end_row();
        }
    }

    /// Renders the `TextEdit` widget for specifying custom null values.
    /// Modifies `self.null_values` directly based on user input.
    fn render_null_values(&mut self, ui: &mut Ui) {
        // Null Values Input Label
        ui.label("Null Values:");

        // Single-line text edit widget bound to the `self.null_values` string.
        let null_values_edit =
            TextEdit::singleline(&mut self.null_values).desired_width(f32::INFINITY); // Take available horizontal space.

        // Add the widget to the UI and set its hover text.
        ui.add(null_values_edit).on_hover_text(
            "Comma-separated values to interpret as null during loading.\n\
            Leading/trailing whitespace for each value is automatically trimmed.",
        );

        // End the row in the parent Grid layout.
        ui.end_row();
    }

    /// Renders the `DragValue` widget for setting `infer_schema_rows`.
    /// Modifies `self.infer_schema_rows` directly.
    fn render_schema_length_input(&mut self, ui: &mut Ui) {
        ui.label("Infer Rows:");
        ui.add(
            DragValue::new(&mut self.infer_schema_rows)
                .speed(1) // Increment/decrement speed.
                .range(0..=usize::MAX), // 0: No inference
        )
        .on_hover_text(
            "Number of rows to scan for inferring data types (CSV/JSON)\n0: No inference",
        );
        ui.end_row();
    }

    /// Renders the `TextEdit` widgets for CSV-specific settings: delimiter.
    /// Modifies `self.csv_delimiter` directly.
    fn render_csv_delimiter(&mut self, ui: &mut Ui) {
        // CSV Delimiter Input
        ui.label("CSV Delimiter:");
        let csv_delimiter_edit = TextEdit::singleline(&mut self.csv_delimiter)
            .char_limit(1) // Restrict to a single character.
            .desired_width(f32::INFINITY);
        ui.add(csv_delimiter_edit)
            .on_hover_text("Enter the single character CSV delimiter");
        ui.end_row();
    }

    /// Renders the `TextEdit` widget for the SQL table name.
    /// Modifies `self.table_name` directly.
    fn render_table_name_input(&mut self, ui: &mut Ui) {
        ui.label("SQL Table Name:");
        let table_name_edit =
            TextEdit::singleline(&mut self.table_name).desired_width(f32::INFINITY);
        ui.add(table_name_edit)
            .on_hover_text("Name of the table to use in SQL queries (e.g., FROM TableName)");
        ui.end_row();
    }

    /*
    /// Renders the multiline `TextEdit` widget for the SQL query.
    /// Modifies `self.query` directly.
    fn render_sql_query_input(&mut self, ui: &mut Ui) {
        ui.label("SQL Query:");
        let query_edit = TextEdit::multiline(&mut self.query)
            .desired_width(f32::INFINITY)
            // Set a reasonable initial height for the multiline input.
            .desired_rows(4);
        ui.add(query_edit)
            .on_hover_text("Enter SQL query to filter/transform data (uses Polars SQL syntax)");
        ui.end_row();
    }
    */

    /// Renders tabbed SQL examples and the editable query input `self.query`.
    /// Handles selecting examples and editing the query. Tabs will wrap if needed.
    /// ### Logic
    /// 1. Generate SQL examples via `sql_commands` using `self.schema`.
    /// 2. Manage selected tab index using `egui::Memory`.
    /// 3. Render **wrapping horizontal tabs** for examples using `ui.horizontal_wrapped`.
    /// 4. On tab click: update index, copy example to `self.query`.
    /// 5. Render multiline `TextEdit` bound to `&mut self.query`.
    ///
    /// Note: Actual *triggering* of reload happens in `render_query` based on overall state change detection or Apply click.
    fn render_sql_query_input(&mut self, ui: &mut Ui) {
        ui.label("SQL Query:"); // Label for the whole section
        ui.vertical(|ui| {
            // Group the examples and editor vertically
            // Configure minimum width for the vertical group if needed
            ui.set_min_width(300.0);

            // 1. Generate examples based on the current schema
            let examples = sql_commands(&self.schema);
            if examples.is_empty() {
                // If no schema or examples, just show the editor
                ui.add(
                    TextEdit::multiline(&mut self.query)
                        .desired_width(f32::INFINITY)
                        .desired_rows(8) // Slightly more rows if no examples
                        .font(egui::TextStyle::Monospace),
                );
                return; // Skip rendering examples if none exist
            }

            // 2. Get/Set selected tab index from Memory for persistence
            let tab_id = ui.id().with("sql_query_tab_index");
            let mut selected_tab_index =
                ui.memory_mut(|mem| *mem.data.get_persisted_mut_or_default::<usize>(tab_id));

            // Clamp selected index to be valid (in case number of examples changes)
            selected_tab_index = selected_tab_index.min(examples.len().saturating_sub(1));

            // 3. Render Tabs using horizontal_wrapped
            ui.separator();
            ui.label("Examples:"); // Label for the tabs

            // Use horizontal_wrapped to lay out tabs, wrapping them onto new lines
            ui.horizontal_wrapped(|ui| {
                // Iterate through examples to create selectable labels (tabs)
                for i in 0..examples.len() {
                    let is_selected = selected_tab_index == i;
                    let tab_name = format!("{}", i + 1); // Simple number for the tab

                    // Create the selectable label (acting as a tab)
                    let resp = ui
                        .selectable_label(is_selected, tab_name)
                        // Show the first line of the SQL query as hover text
                        .on_hover_text(
                            examples
                                .get(i) // Safely get the example string
                                .and_then(|s| s.lines().next()) // Get the first line
                                .unwrap_or(""), // Default to empty string if error/empty
                        );

                    // 4. Handle tab click logic
                    if resp.clicked() && !is_selected {
                        selected_tab_index = i; // Update the selected index

                        // Update the main query editor text with the clicked example
                        if let Some(example_query) = examples.get(i) {
                            self.query = example_query.clone(); // Set editor text
                            // Change is detected by render_query comparing before/after state
                            tracing::debug!(
                                "Switched SQL Query tab to Example {}, query text updated.",
                                i + 1
                            );
                        }

                        // Store the newly selected index back into egui's memory
                        ui.memory_mut(|mem| mem.data.insert_persisted(tab_id, selected_tab_index));
                    }
                }
            }); // End horizontal_wrapped

            ui.separator(); // Separator between tabs and editor

            // 5. Render the ACTIVE query editor below the tabs
            ui.add(
                TextEdit::multiline(&mut self.query)
                    .desired_width(f32::INFINITY) // Take full available width
                    .desired_rows(6) // Set preferred number of visible lines
                    .font(egui::TextStyle::Monospace), // Use a monospace font for SQL
            )
            .on_hover_text(
                "Enter SQL query (Polars SQL).\n\
                Click Example tabs above.\n\
                Changes trigger reload on Apply/focus change.",
            );
        }); // End vertical group
        ui.end_row(); // End the row in the parent Grid layout
    }

    /// Renders the collapsible section displaying SQL command examples.
    /// Uses `sql_commands` to generate examples relevant to the current `self.schema`.
    fn render_sql_examples(&self, ui: &mut Ui) {
        CollapsingHeader::new("SQL Command Examples")
            .default_open(false)
            .show(ui, |ui| {
                // Tip about quoting identifiers.
                let quoting_tip = "Tip: Use double quotes (\") or backticks (`) around column names with spaces or special characters (e.g., \"Column Name\" or `Column Name`).";
                ui.label(quoting_tip);

                // Frame around the examples.
                Frame::default()
                    .stroke(Stroke::new(1.0, Color32::GRAY))
                    .outer_margin(2.0)
                    .inner_margin(10.0)
                    .show(ui, |ui| {
                        // Link to Polars SQL documentation.
                        ui.vertical_centered(|ui| {
                             let polars_sql_url = "https://docs.pola.rs/api/python/stable/reference/sql/index.html";
                            ui.hyperlink_to("Polars SQL Reference", polars_sql_url).on_hover_text(polars_sql_url);
                        });
                        ui.separator();

                        // Generate and display SQL examples based on the current schema.
                        // The `sql_commands` function (in `sqls.rs`) dynamically creates these.
                        let examples = sql_commands(&self.schema);
                        let mut ex_num = Vec::new();
                        for (index, example) in examples.iter().enumerate() {
                            ex_num.push(format!("Example {count}:\n{example}", count = index + 1));
                        }

                        // Make the examples selectable for easy copying.
                        ui.add(egui::Label::new(ex_num.join("\n\n")).selectable(true));
                    });
            });
    }
}

/// Reads a CSV file from the specified path using Polars, applying given options
/// and limiting the number of data rows read.
///
/// This function configures a Polars CsvReader with specific parsing and reading
/// options and executes the read operation directly from the file path.
/// It's suitable for getting the schema (when `infer_schema_length(Some(0))`) or
/// reading a limited number of initial data rows (`with_n_rows`).
///
/// ### Configuration Behavior:
/// - `has_header(true)`: Assumes the file has a header row for column names.
/// - `infer_schema_length(Some(0))`: Instructs Polars to infer column names from the
///   header row *only*, using default types (typically String), without using data
///   rows to guess types. If combined with `n_rows > 0`, it reads data
///   rows but ignores their content for type inference.
/// - `with_n_rows(n_rows)`: Limits the number of *data* rows parsed after the header.
/// - `ignore_errors(true)`: Skips rows/fields with parsing errors rather than stopping.
/// - `missing_is_null(true)`: Treats empty fields (`""`) as null values.
pub async fn read_csv_partial_from_path(
    delimiter: u8,
    n_rows: usize,
    path: &Path,
) -> PolarsViewResult<DataFrame> {
    tracing::debug!("Read a CSV file using Polars limited to {} rows.", n_rows,);

    // 1. Define the CSV parsing options.
    let csv_parse_options = CsvParseOptions::default()
        .with_encoding(CsvEncoding::LossyUtf8) // Handle potentially non-strict UTF8
        .with_missing_is_null(true) // Treat empty fields as nulls
        .with_separator(delimiter); // Set the chosen delimiter

    // 2. Define the main CSV reading options.
    let csv_read_options = CsvReadOptions::default()
        .with_parse_options(csv_parse_options) // Apply the parsing sub-options
        .with_has_header(true) // File has a header row
        .with_infer_schema_length(Some(0)) // Number of rows to use for schema inference (0 means header only)
        .with_ignore_errors(true) // Allow skipping rows/fields that fail to parse
        .with_n_rows(Some(n_rows)) // Limits the number of rows to read.
        .try_into_reader_with_file_path(Some(path.to_path_buf()))?;

    // 3. Execute the blocking read operation on a separate thread
    let df = execute_polars_blocking(move || csv_read_options.finish()).await?;

    tracing::debug!("Partial CSV read complete. Shape: {:?}", df.shape());
    Ok(df)
}

/// Builds a Polars Schema specifying DataType::String overrides for columns
/// whose names match a given regex pattern or wildcard.
///
/// This function creates a schema intended for the `with_dtypes` option
/// of Polars readers (like `LazyCsvReader`), ensuring specific columns are treated
/// as text regardless of their inferred type.
fn build_dtype_override_schema(
    input_schema: &Arc<Schema>,
    regex_pattern: &str,
) -> PolarsViewResult<Schema> {
    let mut overrides_schema = Schema::default(); // Initialize the resulting schema

    // --- Handle Wildcard Case ("*") ---
    // If the pattern is "*", override ALL columns to String.
    if regex_pattern.trim() == "*" {
        tracing::debug!(
            "Wildcard pattern '{regex_pattern}' provided. Overriding all columns to String."
        );
        return Ok(input_schema.as_ref().clone()); // Return the fully populated override schema
    }

    // --- Handle Specific Regex Pattern Case ---
    // If it's not a wildcard, compile the regex pattern.

    // Validate the required ^...$ format *before* compiling
    if !(regex_pattern.starts_with('^') && regex_pattern.ends_with('$')) {
        return Err(PolarsViewError::InvalidRegexPattern(
            regex_pattern.to_string(),
        ));
    }

    // Attempt to compile the regex
    let compiled_regex = match Regex::new(regex_pattern) {
        Ok(re) => re,
        Err(e) => {
            // Return specific error for invalid syntax
            return Err(PolarsViewError::InvalidRegexSyntax {
                pattern: regex_pattern.to_string(),
                error: e.to_string(),
            });
        }
    };

    // Check this compiled regex against each actual header name.
    for col_name in input_schema.iter_names() {
        if compiled_regex.is_match(col_name) {
            // Insert the override into the schema.
            overrides_schema.insert(col_name.clone(), DataType::String);
        }
    }

    // Log the final outcome for debugging purposes.
    if !overrides_schema.is_empty() {
        tracing::debug!(
            override_cols = ?overrides_schema.iter_names().collect::<Vec<_>>(),
            "Pattern '{}' matched {} columns: ",
            regex_pattern,
            overrides_schema.len()
        );
    } else {
        tracing::debug!("Provided regex patterns did not match any header columns.");
    }

    Ok(overrides_schema) // Return the successfully built schema (might be empty)
}

/// Helper function to find a unique column name based on a base name and schema.
/// Appends suffixes "_1", "_2", etc., if the base name conflicts with existing column names.
fn resolve_unique_column_name(base_name: &str, schema: &Schema) -> PolarsResult<PlSmallStr> {
    // Check if the base name is available first (most common case)
    if schema.get(base_name).is_none() {
        tracing::debug!("Base name '{}' is available.", base_name);
        return Ok(base_name.into());
    }

    // Base name conflicts, generate alternative names with suffixes.
    tracing::debug!(
        "Base name '{}' conflicts. Searching unique name.",
        base_name
    );
    let mut suffix_counter = 1u32;
    loop {
        let candidate_name = format!("{base_name}_{suffix_counter}");

        if schema.get(&candidate_name).is_none() {
            // Found a unique name
            tracing::debug!("Found unique name: '{}'.", candidate_name);
            return Ok(candidate_name.into()); // Return the unique name
        }

        // Safety check for potential overflow and limit attempts
        suffix_counter = suffix_counter.checked_add(1).unwrap_or(MAX_ATTEMPTS); // If overflow, go to max attempts

        // Prevent infinite loops. Return error if a unique name cannot be found after max attempts.
        if suffix_counter >= MAX_ATTEMPTS {
            let msg = format!(
                "Failed to find a unique column name starting with '{base_name}' after {MAX_ATTEMPTS} attempts."
            );
            tracing::error!("{}", msg);
            return Err(PolarsError::ComputeError(msg.into()));
        }
    }
}

/// Executes a potentially blocking Polars operation on a separate Tokio blocking thread.
///
/// Wraps the closure `op` which is expected to return a `PolarsResult<T>`,
/// runs it with `spawn_blocking`, awaits the result, and maps both the
/// `JoinError` and the inner `PolarsError` to `PolarsViewError`.
///
/// ### Arguments
/// * `op`: A closure that performs the blocking work and returns `PolarsResult<T>`.
///   It must be `Send` and have a `'static` lifetime, meaning it must
///   take ownership of or only use data that can be moved across threads
///   and lives for the duration of the program (or the task).
///
/// ### Returns
/// A `PolarsViewResult<T>` containing the result of the operation `T` on success,
/// or a mapped `PolarsViewError` if the spawned task fails (`TokioJoin`) or
/// the Polars operation itself fails (`Polars`).
async fn execute_polars_blocking<T, F>(op: F) -> PolarsViewResult<T>
where
    // F is the type of the closure
    F: FnOnce() -> Result<T, PolarsError> + Send + 'static, // The closure trait bounds
    // T is the success type returned by the closure (e.g., DataFrame)
    T: Debug + Send + 'static, // The success type must be Send and have static lifetime
                               // PolarsError: Debug,
{
    // Spawn the blocking task
    let result_from_task = spawn_blocking(op).await; // Result<Result<T, PolarsError>, JoinError>

    // Map JoinError to PolarsViewError::TokioJoin
    let polars_result = result_from_task.map_err(PolarsViewError::from)?; // Requires PolarsViewError::from(JoinError)

    // Map PolarsError to PolarsViewError::Polars
    let final_result = polars_result.map_err(PolarsViewError::from)?; // Requires PolarsViewError::from(PolarsError)

    Ok(final_result) // Return the successfully extracted value or the mapped PolarsError
}

//----------------------------------------------------------------------------//
//                                   Tests                                    //
//----------------------------------------------------------------------------//

/// Run tests with:
/// cargo test -- --show-output tests_override_columns
#[cfg(test)]
mod tests_override_columns {
    use super::*;
    use std::{fs::File, io::Write};
    use tempfile::NamedTempFile;

    // --- Test Setup Helper (Unchanged conceptually, ensure path and error mapping ok) ---
    fn setup_test_csv(
        content: &str, // CSV content as string
        delimiter: char,
        force_string_patterns: Option<String>, // Regex Columns to configure for override
    ) -> PolarsViewResult<(NamedTempFile, DataFilter)> {
        let temp_file = NamedTempFile::new()?;
        let file_path = temp_file.path().to_path_buf();

        // Write content to the temp file
        let mut file = File::create(&file_path)?;
        file.write_all(content.as_bytes())?;
        file.flush()?; // Ensure data is written

        // Create DataFilter using struct update syntax (Clippy Fix)
        let filter = DataFilter {
            absolute_path: file_path,             // Set specific value
            force_string_patterns,                // Set specific value
            csv_delimiter: delimiter.to_string(), // Set specific value
            ..Default::default()                  // Fill the rest with defaults
        };

        Ok((temp_file, filter))
    }

    // --- Test Case 1: Override applied correctly ---
    #[tokio::test] // Requires tokio features in dev-dependencies
    async fn test_csv_read_with_override_success() -> PolarsViewResult<()> {
        println!("\n--- Test: Override Applied Successfully ---");
        // 1. Define CSV Content with large numbers AS TEXT
        let csv_content = "\
long_id;value;text
12345678901234567890123456789012345678901234;10.5;abc
98765432109876543210987654321098765432109876;20.0;def
12345;30.7;ghi";
        // No need for df_input - the csv_content is the direct input representation
        println!("Input CSV Content:\n{csv_content}\n");

        // 2. Define Expected Output DataFrame (long_id is String)
        let df_expected = df!(
            "long_id" => &[
                "12345678901234567890123456789012345678901234",
                "98765432109876543210987654321098765432109876",
                "12345"
            ],
            "value" => &[10.5, 20.0, 30.7],
            "text" => &["abc", "def", "ghi"]
        )
        .expect("Failed to create expected DataFrame");
        println!("Expected DF (After Read):\n{df_expected}");

        // 3. Setup: Use helper to create CSV and Filter WITH the override
        let delimiter = ';';
        let col_regex = "^long_id$".to_string();
        let (_temp_file, filter) = // Keep _temp_file handle!
                setup_test_csv(csv_content, delimiter, Some(col_regex))?;

        let schema = filter
            .attempt_csv_parse_structure(delimiter as u8, false)
            .await?;
        println!("schema: {schema:#?}");

        // 4. Execute the function under test
        let lazyframe = filter.attempt_read_csv(delimiter as u8, &schema).await?;
        println!("get lazyframe");

        // Execute the lazy plan and collect into an eager DataFrame
        // *** WRAP COLLECT IN SPAWN_BLOCKING ***
        let df_output =
            execute_polars_blocking(move || lazyframe.with_new_streaming(true).collect()).await?;

        println!("Output DF (Actual Read):\n{df_output}");

        // 5. Assertions
        assert_eq!(
            df_output.schema().get("long_id"),
            Some(&DataType::String),
            "Schema Check Failed: 'long_id' should be DataType::String"
        );
        assert_eq!(
            df_output.schema().get("value"),
            Some(&DataType::Float64),
            "Schema Check Failed: 'value' should be DataType::Float64"
        );
        assert_eq!(
            df_output.schema().get("text"),
            Some(&DataType::String),
            "Schema Check Failed: 'text' should be DataType::String"
        );
        assert_eq!(
            df_output, df_expected,
            "Content Check Failed: Output DF does not match expected DF"
        );

        Ok(())
    }

    // --- Test Case 2: Override *not* applied (expect nulls) ---
    #[tokio::test] // Requires tokio features in dev-dependencies
    async fn test_csv_read_without_override_yields_nulls() -> PolarsViewResult<()> {
        println!("\n--- Test: No Override Applied (Expect Nulls) ---");
        // 1. Define CSV Content (same large numbers AS TEXT)
        let csv_content = "\
long_id;value;text
12345678901234567890123456789012345678901234;10.5;abc
98765432109876543210987654321098765432109876;20.0;def";
        println!("Input CSV Content:\n{csv_content}\n");

        // 2. Define Expected Output Pattern (long_id should be all nulls)
        let df_expected_pattern = df!(
            "long_id" => Series::new_null("long_id".into(), 2).cast(&DataType::Int64)?, // Series of 2 nulls
            "value" => &[10.5, 20.0],
            "text" => &["abc", "def"]
        )
        .expect("Failed to create expected pattern DataFrame");
        println!("Expected DF Pattern (After Read, note long_id nulls):\n{df_expected_pattern}");

        // 3. Setup: Use helper with an EMPTY override list
        let delimiter = ';';
        let col_regex = "^Col Name$".to_string();
        let (_temp_file, filter) = setup_test_csv(csv_content, delimiter, Some(col_regex))?;

        let schema = filter
            .attempt_csv_parse_structure(delimiter as u8, false)
            .await?;
        println!("schema: {schema:#?}");

        // 4. Execute the function under test
        let lazyframe = filter.attempt_read_csv(delimiter as u8, &schema).await?;
        println!("get lazyframe");

        // Execute the lazy plan and collect into an eager DataFrame
        // *** WRAP COLLECT IN SPAWN_BLOCKING ***
        let df_output = spawn_blocking(move || lazyframe.with_new_streaming(true).collect())
            .await
            .map_err(PolarsViewError::from)? // Convert Tokio JoinError to PolarsViewError
            .map_err(PolarsViewError::from)?; // Convert PolarsError to PolarsViewError

        println!("Output DF (Actual Read):\n{df_output}");

        // 5. Assertions
        let long_id_col = df_output.column("long_id")?;
        assert!(
            long_id_col.is_null().all(), // Verify ALL values are null
            "Content Check Failed: 'long_id' column should be all nulls without override. Type: {:?}, Null count: {}",
            long_id_col.dtype(),
            long_id_col.null_count()
        );
        // Verify other columns match the pattern
        assert_eq!(
            df_output.column("value")?,
            df_expected_pattern.column("value")?
        );
        assert_eq!(
            df_output.column("text")?,
            df_expected_pattern.column("text")?
        );

        Ok(())
    }
}
