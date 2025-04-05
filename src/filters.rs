use crate::{
    Arguments, DEFAULT_QUERY, FileExtension, PathExtension, PolarsViewError, PolarsViewResult,
    UniqueElements, sql_commands,
};
use egui::{
    Align, CollapsingHeader, Color32, DragValue, Frame, Grid, Layout, ScrollArea, Stroke, TextEdit,
    Ui, Vec2,
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
pub static NULL_VALUES: &str = r#""", <N/D>"#;

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
            .with_null_values(None) // Apply fn replace_strings_with_null()
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
    /// * `Some(DataFilters)`: If any filter setting was changed by the user in this frame.
    /// * `None`: If no changes were detected.
    pub fn render_query(&mut self, ui: &mut Ui) -> Option<DataFilters> {
        // Clone the state *before* rendering UI widgets to detect changes later.
        let filters_before_render = self.clone();
        let mut result = None;

        let width_min = 420.0; // Minimum width for the grid area.

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

                    // Checkbox for removing null columns.
                    self.render_remove_null_cols_checkbox(ui);

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
        ui.label("Schema Inference Rows:");
        ui.add(
            DragValue::new(&mut self.infer_schema_rows)
                .speed(1) // Increment/decrement speed.
                .range(1..=usize::MAX), // Ensure at least 1 row.
        )
        .on_hover_text("Number of rows to scan for inferring data types (CSV/JSON)");
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
    /// Handles selecting examples and editing the query.
    /// ### Logic
    /// 1. Generate SQL examples via `sql_commands` using `self.schema`.
    /// 2. Manage selected tab index using `egui::Memory`.
    /// 3. Render horizontal tabs for examples within a `ScrollArea`.
    /// 4. On tab click: update index, copy example to `self.query`.
    /// 5. Render multiline `TextEdit` bound to `&mut self.query`.
    ///
    /// Note: Actual *triggering* of reload happens in `render_query` based on overall state change detection or Apply click.
    fn render_sql_query_input(&mut self, ui: &mut Ui) {
        ui.label("SQL Query:");
        ui.vertical(|ui| {
            // Group tabs and editor vertically
            ui.set_min_width(300.0);

            // 1. Generate examples
            let examples = sql_commands(&self.schema);
            if examples.is_empty() {
                return;
            }

            // 2. Get/Set selected tab index from Memory
            let tab_id = ui.id().with("sql_query_tab_index");
            let mut selected_tab_index =
                ui.memory_mut(|mem| *mem.data.get_persisted_mut_or_default::<usize>(tab_id));
            selected_tab_index = selected_tab_index.min(examples.len().saturating_sub(1));

            // 3. Render tabs
            ui.separator();
            ui.label("Examples:");
            ScrollArea::horizontal()
                .id_salt("sql_tabs_scroll")
                .auto_shrink([false, true])
                .show(ui, |ui| {
                    ui.horizontal(|ui| {
                        for i in 0..examples.len() {
                            let is_selected = selected_tab_index == i;
                            let tab_name = format!("{}", i + 1);
                            let resp = ui
                                .selectable_label(is_selected, tab_name)
                                // Show first line of the example query on hover
                                .on_hover_text(
                                    examples
                                        .get(i)
                                        .map_or("", |s| s.lines().next().unwrap_or("")),
                                );

                            // 4. Handle tab click
                            if resp.clicked() && !is_selected {
                                selected_tab_index = i;
                                // Update the active query text
                                if let Some(example_query) = examples.get(i) {
                                    self.query = example_query.clone(); // Update active query
                                    // *Don't* set apply_sql here directly, let render_query's change detection handle it.
                                    tracing::debug!(
                                        "Switched SQL Query tab to Example {}, query text updated.",
                                        i + 1
                                    );
                                }
                                // Store changed index
                                ui.memory_mut(|mem| {
                                    mem.data.insert_persisted(tab_id, selected_tab_index)
                                });
                            }
                        }
                    });
                });
            ui.separator();

            // 5. Render ACTIVE query editor
            ui.add(
                TextEdit::multiline(&mut self.query)
                    .desired_width(f32::INFINITY)
                    .desired_rows(6)
                    .font(egui::TextStyle::Monospace),
            ) // Use monospace
            .on_hover_text(
                "Enter SQL query (Polars SQL). Changes trigger reload on Apply/focus change.",
            );
        }); // End vertical group
        ui.end_row(); // End row in parent grid
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
