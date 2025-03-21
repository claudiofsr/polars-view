use crate::{
    // format_dataframe_columns,
    Arguments,
    DEFAULT_QUERY,
    FileExtension,
    PathExtension,
    PolarsViewError,
    PolarsViewResult,
    SortState,
    sql_commands,
};
use egui::{
    Align, CollapsingHeader, Color32, DragValue, Frame, Grid, Hyperlink, Layout, Stroke, TextEdit,
    Ui, Vec2,
};
use polars::prelude::*;

use std::{
    collections::HashSet,
    fmt::Debug,
    fs::File,
    num::NonZero,
    path::{Path, PathBuf},
    sync::Arc,
};

/// Static string values that are treated as null/missing values.
///
/// The `r` character at the start of a string literal denotes a raw string literal.
pub static NULL_VALUES: &str = r#""", " ", <N/D>"#;

/// Deduplicates elements in a vector while preserving the original order.
///
/// This function iterates through the input vector and keeps only the first occurrence
/// of each element, effectively removing duplicates and maintaining the order in which
/// elements first appear.
///
/// ### Type Parameters
///
/// * `T`: The type of elements in the vector. Must implement `Eq`, `Hash`, and `Clone`.
///     - `Eq` and `Hash` are required for elements to be used as keys in a `HashSet`
///       for efficient duplicate detection.
///     - `Clone` is necessary because elements are cloned when inserted into the `HashSet`.
///
/// ### Arguments
///
/// * `v`: A mutable reference to the vector to be deduplicated. The vector is modified in place.
///
/// ### Example
///
/// ```
/// use polars_view::unique_ordered;
///
/// let mut vec = vec![1, 2, 2, 3, 1, 4, 3, 2, 5];
/// unique_ordered(&mut vec);
/// assert_eq!(vec, vec![1, 2, 3, 4, 5]);
/// ```
pub fn unique_ordered<T>(v: &mut Vec<T>)
where
    T: Eq + std::hash::Hash + Clone,
{
    // `HashSet` to keep track of elements we've already encountered.
    let mut seen = HashSet::new();

    // `retain` iterates through the vector and keeps elements based on the closure's return value.
    v.retain(|x| {
        // `seen.insert(x.clone())` attempts to insert a clone of the current element `x` into the `HashSet`.
        // - If `x` is already in the `HashSet`, `insert` returns `false`.
        // - If `x` is NOT in the `HashSet`, `insert` inserts it and returns `true`.
        // We want to keep the element only if it's the first time we're seeing it (i.e., `insert` returns `true`).
        seen.insert(x.clone()) // Keep element if it's the first time we see it
    });
}

/// Holds filters and configurations for data loading and processing.
#[derive(Debug, Clone, PartialEq)]
pub struct DataFilters {
    /// Absolute path of the data source.
    pub absolute_path: PathBuf,
    /// Table name (for SQL queries).
    pub table_name: String,
    /// CSV delimiter.
    pub csv_delimiter: String,
    /// The DataFrame schema.
    pub schema: Arc<Schema>,
    /// Number of rows to use for schema inference.
    pub infer_schema_rows: usize,
    /// Number of decimal places for float formatting.
    pub decimal: usize,
    /// SQL query to apply.
    pub query: String,
    /// Apply SQL Commands.
    pub apply_sql: bool,
    /// Column sorting state.
    pub sort: Option<Arc<SortState>>,
    /// Flag to indicate whether to remove columns containing only null values.
    pub remove_null_cols: bool,
    /// Custom null values.
    pub null_values: String,
}

impl Default for DataFilters {
    fn default() -> Self {
        DataFilters {
            absolute_path: PathBuf::new(),
            table_name: "AllData".to_string(), // Default table name.
            csv_delimiter: ";".to_string(),    // Default CSV delimiter.
            schema: Schema::default().into(),  // Default DataFrame schema.
            infer_schema_rows: 200,            // Default schema length (rows to infer schema).
            decimal: 2,                        // Default: 2 decimal places.
            query: DEFAULT_QUERY.to_string(),  // Default query (selects all).
            apply_sql: false,
            sort: None,                           // Default: no sorting.
            remove_null_cols: false,              // Default: don't remove null columns
            null_values: NULL_VALUES.to_string(), // Default null values.
        }
    }
}

impl DataFilters {
    /// Creates a new `DataFilters` instance from command-line arguments.
    pub fn new(args: &Arguments) -> PolarsViewResult<Self> {
        // Get the canonical, absolute path.
        let absolute_path = args.path.canonicalize()?;
        let apply_sql = DEFAULT_QUERY.trim() != args.query.trim();

        Ok(DataFilters {
            absolute_path,
            table_name: args.table_name.clone(),
            csv_delimiter: args.delimiter.clone(),
            query: args.query.clone(),
            apply_sql,
            remove_null_cols: args.remove_null_cols,
            null_values: args.null_values.to_string(),
            ..Default::default() // Use defaults for other fields.
        })
    }

    /// Sets the data source path, converting to absolute path.
    pub fn set_path(&mut self, path: &Path) -> PolarsViewResult<()> {
        self.absolute_path = path.canonicalize().map_err(PolarsViewError::Io)?;
        tracing::debug!("absolute_path: {:#?}", self.absolute_path);
        Ok(())
    }

    /// Gets the (lowercased) file extension.
    pub fn get_extension(&self) -> Option<String> {
        self.absolute_path.extension_as_lowercase()
    }

    /// Determines FileExtension and loads the DataFrame.
    pub async fn get_df_and_extension(&mut self) -> PolarsViewResult<(DataFrame, FileExtension)> {
        let extension = FileExtension::from_path(&self.absolute_path);

        let (df, delimiter) = match &extension {
            FileExtension::Csv => self.read_csv_data().await?,
            FileExtension::Json => self.read_json_data().await?,
            FileExtension::NDJson => self.read_ndjson_data().await?,
            FileExtension::Parquet => self.read_parquet_data().await?,
            FileExtension::Unknown(ext) => {
                return Err(PolarsViewError::FileType(format!(
                    "Unknown extension: `{}` for file: `{}`",
                    ext,
                    self.absolute_path.display()
                )));
            }
            FileExtension::Missing => {
                return Err(PolarsViewError::FileType(format!(
                    "Extension not found for file: `{}`",
                    self.absolute_path.display()
                )));
            }
        };

        // Format the DataFrame to 2 decimal places.
        // let formatted_df = format_dataframe_columns(df, 2)?;

        // Update delimiter if `read_csv` found one.
        if let Some(byte) = delimiter {
            self.csv_delimiter = (byte as char).to_string();
        }

        // Update DataFrame schema.
        self.schema = df.schema().clone();

        // dbg!(&self);
        tracing::debug!("fn get_df_and_extension()\nDataFilters: {self:#?}");

        Ok((df, extension)) // Return the DataFrame and FileExtension.
    }

    /// Reads a Json file into a Polars DataFrame.
    async fn read_json_data(&self) -> PolarsViewResult<(DataFrame, Option<u8>)> {
        let file = File::open(&self.absolute_path)?;
        let df = JsonReader::new(file)
            .infer_schema_len(NonZero::new(self.infer_schema_rows))
            .finish()?;
        Ok((df, None))
    }

    /// Reads a NDJson file into a Polars DataFrame.
    async fn read_ndjson_data(&self) -> PolarsViewResult<(DataFrame, Option<u8>)> {
        let lazyframe = LazyJsonLineReader::new(&self.absolute_path)
            .low_memory(false)
            .with_infer_schema_length(NonZero::new(self.infer_schema_rows))
            .with_ignore_errors(true)
            .finish()?;

        let df = lazyframe.collect()?;

        Ok((df, None))
    }

    /// Reads a Parquet file into a Polars DataFrame.
    async fn read_parquet_data(&self) -> PolarsViewResult<(DataFrame, Option<u8>)> {
        let args = ScanArgsParquet {
            low_memory: false,
            ..Default::default()
        };

        let lazyframe = LazyFrame::scan_parquet(&self.absolute_path, args)?;

        let df = lazyframe.collect()?;

        Ok((df, None))
    }

    /// Reads a CSV file, tries delimiters, returns DataFrame and used delimiter.
    async fn read_csv_data(&self) -> PolarsViewResult<(DataFrame, Option<u8>)> {
        let separator = self.get_csv_separator()?;

        // Common delimiters to try.
        let mut delimiters = vec![separator, b',', b';', b'|', b'\t', b' '];
        unique_ordered(&mut delimiters);

        // Number of rows to read for initial delimiter detection.
        const NROWS: usize = 100;

        // Try each delimiter.
        for delimiter in delimiters {
            // First, try reading a small chunk of the file to quickly check the delimiter.
            if self.attempt_read_csv(delimiter, Some(NROWS)).await.is_ok() {
                // If reading the chunk was successful, read the entire file with that delimiter.
                if let Ok(df) = self.attempt_read_csv(delimiter, None).await {
                    return Ok((df, Some(delimiter))); // Return the DataFrame and the delimiter.
                }
            }
        }

        // If all delimiters fail, return an error.
        let msg = "Failed CSV read with common delimiters or inconsistent data.";
        let error = PolarsViewError::CsvParsing(msg.to_string());
        tracing::error!("{}", error);
        Err(error)
    }

    /// Retrieves the CSV separator byte from the configured delimiter string.
    fn get_csv_separator(&self) -> PolarsViewResult<u8> {
        self.csv_delimiter
            .as_bytes()
            .first()
            .copied()
            .ok_or_else(|| PolarsViewError::InvalidDelimiter(self.csv_delimiter.clone()))
    }

    /// Tries reading CSV with a specific delimiter.
    async fn attempt_read_csv(
        &self,
        delimiter: u8,
        rows_max: Option<usize>,
    ) -> PolarsViewResult<DataFrame> {
        // dbg!(delimiter as char); // Debug output: delimiter.
        tracing::debug!("delimiter: '{}'", delimiter as char);

        // Use the custom null values from the DataFilters.
        let null_values: Vec<PlSmallStr> = self.parse_null_values();

        tracing::debug!("null_values: {null_values:?}");

        // Configure the LazyCsvReader.
        let lazyframe = LazyCsvReader::new(&self.absolute_path)
            .with_low_memory(false) // Reduce memory usage at the expense of performance
            .with_encoding(CsvEncoding::LossyUtf8) // Handle encoding issues gracefully.
            .with_has_header(true) // Assume the first row is a header.
            .with_try_parse_dates(true) // Try to parse dates automatically.
            .with_separator(delimiter) // Set the delimiter.
            .with_infer_schema_length(Some(self.infer_schema_rows)) // Infer schema from the first rows.
            .with_ignore_errors(true) // Ignore parsing errors (rows with errors will have nulls).
            .with_missing_is_null(true) // Treat missing values as null.
            .with_null_values(Some(NullValues::AllColumns(null_values))) // Specify null values.
            .with_n_rows(rows_max) // Optionally limit the number of rows read.
            //.with_decimal_comma(true)
            .finish()?;

        // Collect the lazy frame into a DataFrame.
        let df = lazyframe.collect()?;

        // Basic validation: check if we have a reasonable number of columns.
        if df.width() <= 1 {
            return Err(PolarsViewError::CsvParsing(format!(
                "Error in delimiter: {}",
                delimiter as char
            )));
        }

        Ok(df)
    }

    /// Parses the `null_values` string into a vector of `PlSmallStr`.
    fn parse_null_values(&self) -> Vec<PlSmallStr> {
        self.null_values
            .split(",")
            .map(|s| s.trim().into())
            .collect()
    }

    /// Renders the UI for configuring data filters.
    ///
    /// This function creates the UI elements for adjusting various data loading and processing
    /// settings, such as the table name, CSV delimiter, schema inference length, decimal
    /// places, SQL query, and the option to remove columns with all null values.
    ///
    /// It uses the `egui` library for creating the UI components.
    ///
    /// If the user modifies and applies changes, the function returns a new `DataFilters`
    /// instance reflecting those changes.
    pub fn render_filter(&mut self, ui: &mut Ui) -> Option<DataFilters> {
        let filters_former = self.clone();
        let mut result = None;
        let width_max = ui.available_width();
        let width_min = 500.0;

        // Use a grid layout.
        let grid = Grid::new("data_filters_grid")
            .num_columns(2) // Two columns: one for labels, one for input fields.
            .spacing([10.0, 20.0]) // Horizontal and vertical spacing.
            .striped(true); // Alternate row background colors for better readability.

        // Layout the entire section with specified width and top-down alignment.
        ui.allocate_ui_with_layout(
            Vec2::new(width_max, ui.available_height()), // Occupy available width.
            Layout::top_down(Align::LEFT),               // Layout elements from top to bottom.
            |ui| {
                grid.show(ui, |ui| {
                    ui.set_min_width(width_min);

                    // Table name input.
                    self.render_table_name_input(ui, width_max);

                    // Conditional CSV settings (only if file extension is "csv").
                    if self.get_extension().as_deref() == Some("csv") {
                        self.render_csv_settings(ui, width_max);
                    }

                    // Conditional CSV, Json or NDJson settings
                    if matches!(
                        self.get_extension().as_deref(),
                        Some("csv" | "json" | "ndjson")
                    ) {
                        self.render_schema_length_input(ui);
                    }

                    // Decimal places input (using DragValue).
                    self.render_decimal_places_input(ui);

                    // Option to remove columns with all null values.
                    self.render_remove_null_cols_checkbox(ui);

                    // SQL query input (multiline).
                    self.render_sql_query_input(ui, width_max);

                    // If the current Query Panel is different from the previous Query Panel.
                    if *self != filters_former {
                        self.apply_sql = true;
                    }

                    // "Apply" button and filter application logic.
                    ui.label(""); // For alignment.
                    ui.with_layout(Layout::top_down(Align::Center), |ui| {
                        if ui.button("Apply SQL Commands").clicked() {
                            if self.apply_sql {
                                // Result contains DataFilters after editing some fields
                                result = Some(self.clone());
                            }

                            tracing::debug!("Apply SQL: {}", self.apply_sql);
                            tracing::debug!("Apply SQL Commands\nresult: {result:#?}");
                        }
                    });
                    ui.end_row();
                });
            },
        );

        // SQL Command Examples (Collapsing Header).
        self.render_sql_examples(ui);

        result // Return the potentially updated filters.
    }

    /// Renders the UI for inputting the table name.
    fn render_table_name_input(&mut self, ui: &mut Ui, width_max: f32) {
        ui.label("Table Name:");
        let table_name_edit = TextEdit::singleline(&mut self.table_name).desired_width(width_max);
        ui.add(table_name_edit)
            .on_hover_text("Enter table name for SQL queries...");
        ui.end_row();
    }

    /// Renders the UI for CSV-specific settings like delimiter and null values.
    fn render_csv_settings(&mut self, ui: &mut Ui, width_max: f32) {
        // CSV delimiter input.
        ui.label("CSV Delimiter:");
        let csv_delimiter_edit = TextEdit::singleline(&mut self.csv_delimiter)
            .char_limit(1) // CSV Delimiter must be a single character.
            .desired_width(width_max);
        ui.add(csv_delimiter_edit)
            .on_hover_text("Enter the CSV delimiter character...");
        ui.end_row();

        // Null Values Input
        ui.label("Null Values:");
        let null_values_edit = TextEdit::singleline(&mut self.null_values).desired_width(width_max);
        ui.add(null_values_edit).on_hover_text(
            "Enter comma-separated values to be treated as null (e.g., \"\", \" \", <N/D>)",
        );
        ui.end_row();
    }

    /// Renders the UI for the schema length input.
    fn render_schema_length_input(&mut self, ui: &mut Ui) {
        // Schema length input (using DragValue).
        ui.label("Schema length:");
        ui.add(
            DragValue::new(&mut self.infer_schema_rows)
                .speed(1)
                .range(1..=u64::MAX),
        )
        .on_hover_text("Number of rows to use for inferring schema...");
        ui.end_row();
    }

    /// Renders the UI for the decimal places input.
    fn render_decimal_places_input(&mut self, ui: &mut Ui) {
        // Decimal places input (using DragValue).
        ui.label("Decimal places:");
        ui.add(DragValue::new(&mut self.decimal).speed(1).range(0..=10))
            .on_hover_text("Decimal places for float formatting...");
        ui.end_row();
    }

    /// Renders the UI for the option to remove columns with all null values.
    fn render_remove_null_cols_checkbox(&mut self, ui: &mut Ui) {
        // Option to remove columns with all null values.
        ui.label("Exclude Null Cols:");
        ui.checkbox(&mut self.remove_null_cols, "")
            .on_hover_text("Remove columns containing only null values");
        ui.end_row();
    }

    /// Renders the UI for the SQL query input.
    fn render_sql_query_input(&mut self, ui: &mut Ui, width_max: f32) {
        // SQL query input (multiline).
        ui.label("SQL Query:");
        // Creates the TextEdit widget for SQL Query input.
        let query_edit = TextEdit::multiline(&mut self.query).desired_width(width_max);
        ui.add(query_edit)
            .on_hover_text("Enter SQL query to filter data...");
        ui.end_row();
    }

    /// Renders the UI for SQL command examples in a collapsing header.
    fn render_sql_examples(&self, ui: &mut Ui) {
        CollapsingHeader::new("SQL Command Examples:")
            .default_open(false)
            .show(ui, |ui| {
                // help message about quoting.
                let msg = "Tip: Use double quotes (\") or backticks (`) to refer to column names, especially \
                if they contain spaces or special characters.  For example: \"Column Name\" or `Column Name`.";
                ui.label(msg);

                Frame::default()
                    .stroke(Stroke::new(1.0, Color32::GRAY))
                    .outer_margin(2.0)
                    .inner_margin(10.0)
                    .show(ui, |ui| {
                        ui.with_layout(egui::Layout::top_down(egui::Align::Center), |ui| {
                            let url =
                                "https://docs.pola.rs/api/python/stable/reference/sql/index.html";
                            // Hyperlink to Polars SQL documentation.
                            let heading = Hyperlink::from_label_and_url("SQL Interface", url);
                            ui.add(heading).on_hover_text(url);
                        });

                        // Display SQL examples (selectable for copying).
                        let sql_examples = sql_commands(&self.schema);
                        ui.add(egui::Label::new(sql_examples.join("\n\n")).selectable(true));
                    });
            });
    }
}

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
