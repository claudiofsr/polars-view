use crate::{
    Arguments, DEFAULT_QUERY, FileExtension, PathExtension, PolarsViewError, PolarsViewResult,
    UniqueElements, sql_commands,
};
use egui::{
    Align, CollapsingHeader, Color32, DragValue, Frame, Grid, Layout, Stroke, TextEdit, Ui, Vec2,
};
use polars::prelude::*;

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
pub static NULL_VALUES: &str = r#""", " ", <N/D>"#;

/// Default delimiter used for CSV parsing if not specified or detected.
/// Using `&'static str` for common, immutable delimiters saves memory allocation.
pub static DEFAULT_CSV_DELIMITER: &str = ";";

// --- DataFilters Struct ---

/// Holds configuration parameters related to **loading and querying** data.
///
/// This struct focuses on settings that define how data is initially read from a file
/// and transformed via SQL queries or basic processing like null column removal.
///
/// Instances are created from `Arguments`, updated by the UI in `render_query`, and passed
/// to `DataFrameContainer::load_data`. Changes here typically trigger a data reload/requery.
#[derive(Debug, Clone, PartialEq)] // PartialEq allows simple change detection
pub struct DataFilters {
    /// The canonical, absolute path to the data file.
    pub absolute_path: PathBuf,
    /// The name assigned to the loaded DataFrame for use in SQL queries.
    pub table_name: String,
    /// The character used to separate columns in a CSV file.
    pub csv_delimiter: String,
    /// The schema (column names and data types) of the most recently loaded DataFrame.
    /// Used by `sql_commands` for generating relevant examples.
    pub schema: Arc<Schema>,
    /// Maximum rows to scan for schema inference (CSV, JSON, NDJson).
    pub infer_schema_rows: usize,
    /// The SQL query string entered by the user.
    pub query: String,
    /// Flag indicating if the `query` should be executed during the next `load_data`.
    /// Set by `render_query` if relevant UI fields change or the Apply button is clicked.
    pub apply_sql: bool,
    /// Flag to control removal of all-null columns after loading/querying.
    pub remove_null_cols: bool,
    /// Comma-separated string of values to interpret as nulls during CSV parsing.
    pub null_values: String,
}

impl Default for DataFilters {
    /// Creates default `DataFilters` with sensible initial values.
    fn default() -> Self {
        DataFilters {
            absolute_path: PathBuf::new(),
            table_name: "AllData".to_string(),
            csv_delimiter: DEFAULT_CSV_DELIMITER.to_string(),
            schema: Schema::default().into(),
            infer_schema_rows: 200,
            query: DEFAULT_QUERY.to_string(),
            apply_sql: false,
            remove_null_cols: false,
            null_values: NULL_VALUES.to_string(),
        }
    }
}

// --- Methods ---

impl DataFilters {
    /// Creates a new `DataFilters` instance configured from command-line `Arguments`.
    /// This is typically called once at application startup in `main.rs`.
    ///
    /// ### Arguments
    /// * `args`: Parsed command-line arguments (`crate::Arguments`).
    ///
    /// ### Returns
    /// A `PolarsViewResult` containing the configured `DataFilters` or an error
    /// (e.g., if the path cannot be canonicalized).
    pub fn new(args: &Arguments) -> PolarsViewResult<Self> {
        // Ensure the path exists and get its absolute, canonical form.
        let absolute_path = args.path.canonicalize()?;
        // Determine if the user provided a custom SQL query (different from the default).
        let apply_sql = DEFAULT_QUERY.trim() != args.query.trim();

        Ok(DataFilters {
            absolute_path,
            table_name: args.table_name.clone(),
            csv_delimiter: args.delimiter.clone(),
            query: args.query.clone(),
            apply_sql, // Set based on whether the query is custom.
            remove_null_cols: args.remove_null_cols,
            null_values: args.null_values.clone(), // Use user-provided nulls.
            ..Default::default()                   // Use defaults for `schema`, `infer_schema_rows`.
        })
    }

    /// Sets the data source path, canonicalizing it.
    pub fn set_path(&mut self, path: &Path) -> PolarsViewResult<()> {
        self.absolute_path = path.canonicalize().map_err(PolarsViewError::Io)?;
        tracing::debug!("absolute_path set to: {:#?}", self.absolute_path);
        Ok(())
    }

    /// Gets the file extension from `absolute_path` in lowercase.
    pub fn get_extension(&self) -> Option<String> {
        self.absolute_path.extension_as_lowercase()
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
        let file = File::open(&self.absolute_path)?; // Open the file.
        let df = JsonReader::new(file)
            // Use `infer_schema_rows` from self to control schema inference depth.
            .infer_schema_len(NonZero::new(self.infer_schema_rows))
            // Potentially add other configurations like `.low_memory(true)` if needed.
            .finish()?;
        Ok((df, None))
    }

    /// Reads a Newline-Delimited JSON (NDJson / JSON Lines) file into a Polars DataFrame.
    /// Uses `LazyJsonLineReader` for potentially better performance/memory usage on large files.
    ///
    /// ### Returns
    /// A `PolarsViewResult` containing `(DataFrame, None)`.
    async fn read_ndjson_data(&self) -> PolarsViewResult<(DataFrame, Option<u8>)> {
        tracing::debug!("Reading NDJSON data from: {}", self.absolute_path.display());
        let lazyframe = LazyJsonLineReader::new(&self.absolute_path) // Path is directly used.
            .low_memory(false) // Option to optimize for memory.
            .with_infer_schema_length(NonZero::new(self.infer_schema_rows)) // Use filter setting.
            .with_ignore_errors(true) // Skip lines that cause parsing errors.
            .finish()?;

        // Collect the lazy frame into an eager DataFrame.
        let df = lazyframe.collect()?;

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
        let args = ScanArgsParquet {
            low_memory: false, // Configure scan arguments as needed.
            ..Default::default()
        };

        // Use `LazyFrame::scan_parquet` for efficient scanning.
        let lazyframe = LazyFrame::scan_parquet(&self.absolute_path, args)?;

        // Collect into an eager DataFrame.
        let df = lazyframe.collect()?;

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
        let mut delimiters_to_try = vec![initial_separator, b',', b';', b'|', b'\t'];
        // Remove duplicates if the initial separator is already in the common list.
        delimiters_to_try.unique();
        tracing::debug!(
            "Attempting CSV read. Delimiters to try: {:?}",
            delimiters_to_try
                .iter()
                .map(|&b| b as char)
                .collect::<Vec<_>>()
        );

        // Number of rows to read for the quick initial check.
        const NROWS_CHECK: usize = 100;

        // Iterate through the potential delimiters.
        for delimiter in delimiters_to_try {
            // 1. Quick Check: Try reading only a small number of rows (NROWS_CHECK).
            // This fails fast if the delimiter is fundamentally wrong (e.g., results in 1 column).
            if self
                .attempt_read_csv(delimiter, Some(NROWS_CHECK))
                .await
                .is_ok()
            {
                // 2. Full Read: If the quick check passed, attempt to read the entire file.
                match self.attempt_read_csv(delimiter, None).await {
                    Ok(df) => {
                        // Success! Return the DataFrame and the delimiter that worked.
                        tracing::info!(
                            "Successfully read CSV with delimiter: '{}'",
                            delimiter as char
                        );
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

    /// Attempts to read a CSV file using a *specific* delimiter and optional row limit.
    /// Configures `LazyCsvReader` extensively using settings from `self`.
    ///
    /// ### Arguments
    /// * `delimiter`: The specific `u8` byte to use as the CSV separator.
    /// * `rows_max`: `Option<usize>` limiting the number of rows read. `None` reads all rows.
    ///
    /// ### Returns
    /// A `PolarsViewResult<DataFrame>` containing the loaded DataFrame on success,
    /// or `Err(PolarsViewError)` (often `CsvParsing` or Polars internal errors) on failure.
    /// Includes a basic validation check (`df.width() > 1`) to quickly reject incorrect delimiters.
    async fn attempt_read_csv(
        &self,
        delimiter: u8,
        rows_max: Option<usize>,
    ) -> PolarsViewResult<DataFrame> {
        tracing::debug!(
            "Attempting CSV read with delimiter: '{}', max rows: {:?}",
            delimiter as char,
            rows_max
        );

        // Parse the comma-separated `null_values` string into the format Polars expects.
        let null_value_list: Vec<PlSmallStr> = self.parse_null_values();
        tracing::debug!("Parsed null values: {:?}", null_value_list);

        // Configure the LazyCsvReader using settings from `self`.
        let lazyframe = LazyCsvReader::new(&self.absolute_path)
            .with_low_memory(false) // Can be set to true for lower memory usage at cost of speed.
            .with_encoding(CsvEncoding::LossyUtf8) // Gracefully handle potential encoding errors.
            .with_has_header(true) // Assume a header row.
            .with_try_parse_dates(true) // Attempt automatic date parsing.
            .with_separator(delimiter) // Use the specified delimiter.
            .with_infer_schema_length(Some(self.infer_schema_rows)) // Use filter setting for inference.
            .with_ignore_errors(true) // Rows with parsing errors become nulls instead of stopping the read.
            .with_missing_is_null(true) // Treat empty fields ("") as null.
            .with_null_values(Some(NullValues::AllColumns(null_value_list))) // Apply custom null values.
            .with_n_rows(rows_max) // Apply row limit if specified.
            // .with_decimal_comma(true) // Uncomment if files use ',' as decimal separator.
            .finish()?; // Finalize configuration and create the LazyFrame.

        // Execute the lazy plan and collect into an eager DataFrame.
        let df = lazyframe.collect()?;

        // **Basic Validation:** If the delimiter resulted in only one column (or zero),
        // it's highly likely the delimiter was incorrect. Return an error early.
        // This check is crucial for the delimiter detection loop in `read_csv_data`.
        if df.width() <= 1 {
            tracing::warn!(
                "CSV read with delimiter '{}' resulted in {} columns. Assuming incorrect delimiter.",
                delimiter as char,
                df.width()
            );
            // Return a specific error type or message indicating a likely delimiter issue.
            return Err(PolarsViewError::CsvParsing(format!(
                "Delimiter '{}' likely incorrect (resulted in {} columns)",
                delimiter as char,
                df.width()
            )));
        }

        Ok(df)
    }

    /// Parses the `null_values` string (e.g., `" , NA, N/D "`) into a `Vec<PlSmallStr>`.
    /// Splits the string by commas and trims whitespace from each resulting value.
    /// `PlSmallStr` is a Polars type often used for efficiency with short strings.
    fn parse_null_values(&self) -> Vec<PlSmallStr> {
        self.null_values
            .split(',') // Split the string by commas.
            .map(|s| s.trim().into()) // Trim whitespace and convert to PlSmallStr.
            .collect() // Collect into a Vec.
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
    /// * `Some(DataFilters)`: If any filter setting was changed by the user in this frame.
    /// * `None`: If no changes were detected.
    pub fn render_query(&mut self, ui: &mut Ui) -> Option<DataFilters> {
        // Clone the state *before* rendering UI widgets to detect changes later.
        let filters_before_render = self.clone();
        let mut result = None;

        let width_max = ui.available_width(); // Available width for inputs.
        let width_min = 420.0; // Minimum width for the grid area.

        // Use a grid layout for label-input pairs.
        let grid = Grid::new("data_query_grid")
            .num_columns(2)
            .spacing([10.0, 20.0]) // Horizontal and vertical spacing.
            .striped(true); // Alternating row backgrounds.

        // Allocate UI space for the grid.
        ui.allocate_ui_with_layout(
            Vec2::new(width_max, ui.available_height()), // Occupy available width.
            Layout::top_down(Align::LEFT),
            |ui| {
                grid.show(ui, |ui| {
                    ui.set_min_width(width_min);

                    // --- Render Individual Filter Widgets ---
                    // Each `render_*` method takes `&mut self` and `ui`.

                    // Checkbox for removing null columns.
                    self.render_remove_null_cols_checkbox(ui);

                    // Input for schema inference length (only for relevant file types).
                    if matches!(
                        self.get_extension().as_deref(), // Get extension as &str
                        Some("csv" | "json" | "ndjson")  // Check if it's one of these
                    ) {
                        self.render_schema_length_input(ui);
                    }

                    // CSV-specific settings (delimiter, null values).
                    if self.get_extension().as_deref() == Some("csv") {
                        self.render_csv_settings(ui, width_max);
                    }

                    // Input for table name used in SQL.
                    self.render_table_name_input(ui, width_max);

                    // Multiline input for the SQL query.
                    self.render_sql_query_input(ui, width_max);

                    // --- Change Detection & Apply Button ---

                    // Compare current state `self` with the state before rendering.
                    if *self != filters_before_render {
                        // Mark that SQL needs to be (re-)applied.
                        self.apply_sql = true;
                        tracing::debug!("Change detected in DataFilters UI.");
                    }

                    // Add the "Apply SQL commands" button.
                    ui.label(""); // For alignment.
                    ui.with_layout(Layout::top_down(Align::Center), |ui| {
                        if ui.button("Apply SQL commands").clicked() {
                            if self.apply_sql {
                                // Result contains DataFilters after editing some fields
                                result = Some(self.clone());
                            }

                            tracing::debug!("Apply SQL commands: {}", self.apply_sql);
                            tracing::debug!("result_sql: {result:#?}");
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

    /// Renders the checkbox for the "Remove Null Cols" option.
    /// Modifies `self.remove_null_cols` directly.
    fn render_remove_null_cols_checkbox(&mut self, ui: &mut Ui) {
        ui.label("Exclude Null Cols:");
        ui.checkbox(&mut self.remove_null_cols, "")
            .on_hover_text("Remove columns containing only null values after loading");
        ui.end_row();
    }

    /// Renders the `DragValue` widget for setting `infer_schema_rows`.
    /// Modifies `self.infer_schema_rows` directly.
    fn render_schema_length_input(&mut self, ui: &mut Ui) {
        ui.label("Schema Inference Rows:");
        ui.add(
            DragValue::new(&mut self.infer_schema_rows)
                .speed(1) // Increment/decrement speed.
                .range(1..=usize::MAX), // Ensure at least 1 row.
        )
        .on_hover_text("Number of rows to scan for inferring data types (CSV/JSON)");
        ui.end_row();
    }

    /// Renders the `TextEdit` widgets for CSV-specific settings: delimiter and null values.
    /// Modifies `self.csv_delimiter` and `self.null_values` directly.
    fn render_csv_settings(&mut self, ui: &mut Ui, width_max: f32) {
        // CSV Delimiter Input
        ui.label("CSV Delimiter:");
        let csv_delimiter_edit = TextEdit::singleline(&mut self.csv_delimiter)
            .char_limit(1) // Restrict to a single character.
            .desired_width(width_max);
        ui.add(csv_delimiter_edit)
            .on_hover_text("Enter the single character CSV delimiter");
        ui.end_row();

        // Null Values Input
        ui.label("CSV Null Values:");
        let null_values_edit = TextEdit::singleline(&mut self.null_values).desired_width(width_max);
        ui.add(null_values_edit)
            .on_hover_text("Comma-separated values to treat as null (e.g., \"\", NA, N/A, <N/D>)");
        ui.end_row();
    }

    /// Renders the `TextEdit` widget for the SQL table name.
    /// Modifies `self.table_name` directly.
    fn render_table_name_input(&mut self, ui: &mut Ui, width_max: f32) {
        ui.label("SQL Table Name:");
        let table_name_edit = TextEdit::singleline(&mut self.table_name).desired_width(width_max);
        ui.add(table_name_edit)
            .on_hover_text("Name of the table to use in SQL queries (e.g., FROM TableName)");
        ui.end_row();
    }

    /// Renders the multiline `TextEdit` widget for the SQL query.
    /// Modifies `self.query` directly.
    fn render_sql_query_input(&mut self, ui: &mut Ui, width_max: f32) {
        ui.label("SQL Query:");
        let query_edit = TextEdit::multiline(&mut self.query)
            .desired_width(width_max)
            // Set a reasonable initial height for the multiline input.
            .desired_rows(4);
        ui.add(query_edit)
            .on_hover_text("Enter SQL query to filter/transform data (uses Polars SQL syntax)");
        ui.end_row();
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
                        // Make the examples selectable for easy copying.
                        ui.add(egui::Label::new(examples.join("\n\n")).selectable(true));
                    });
            });
    }
}

// --- Tests ---
// Unit tests remain unchanged, verifying specific Polars CSV parsing behavior.
#[cfg(test)]
mod tests {
    use polars::{error::PolarsResult, prelude::*};

    #[test]
    fn test_quoted_bool_ints() -> PolarsResult<()> {
        let csv = r#"
foo,bar,baz
1,"4","false"
3,"5","false"
5,"6","true"
"#;
        let file = std::io::Cursor::new(csv); // Create a cursor for the in-memory CSV data.
        let df = CsvReader::new(file).finish()?; // Read the CSV data into a DataFrame.

        // Define the expected DataFrame.
        let expected = df![
            "foo" => [1, 3, 5],
            "bar" => [4, 5, 6],
            "baz" => [false, false, true],
        ]?;

        // Assert that the loaded DataFrame equals the expected DataFrame.
        assert!(df.equals_missing(&expected));
        Ok(())
    }
}
