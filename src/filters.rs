use crate::{
    // format_dataframe_columns,
    Arguments,
    DataFrameContainer,
    FileExtension,
    PolarsViewError,
    PolarsViewResult,
    SQL_COMMANDS,
    SortState,
};
use egui::{
    Align, CollapsingHeader, Color32, DragValue, Frame, Grid, Hyperlink, Layout, Stroke, TextEdit,
    Ui, Vec2,
};
use polars::prelude::*;

use std::{
    collections::HashSet,
    ffi::OsStr,
    fmt::Debug,
    fs::File,
    future::Future,
    path::{Path, PathBuf},
};

/// Type alias for a Result with a `DataFrameContainer`.
pub type DataResult = PolarsViewResult<DataFrameContainer>;
/// Type alias for a boxed, dynamically dispatched Future that returns a `DataResult`.
pub type DataFuture = Box<dyn Future<Output = DataResult> + Unpin + Send + 'static>;

/// Static array of string values that are treated as null/missing values.
static NULL_VALUES: &[&str] = &["", " ", "<N/D>", "*DIVERSOS*"];

/// Deduplicate vector while preserving order
fn dedup<T>(v: &mut Vec<T>)
where
    T: Eq + std::hash::Hash + Clone,
{
    let mut set = HashSet::new();

    v.retain(|x| set.insert(x.clone()));
}

/// Holds filters and configurations for data loading and processing.
#[derive(Debug, Clone)]
pub struct DataFilters {
    /// Absolute path of the data source.
    pub absolute_path: PathBuf,
    /// Table name (for SQL queries).
    pub table_name: String,
    /// CSV delimiter.
    pub csv_delimiter: String,
    /// Number of rows to use for schema inference.
    pub schema_length: usize,
    /// Number of decimal places for float formatting.
    pub decimal: usize,
    /// SQL query to apply.
    pub query: String,
    /// Column sorting state.
    pub sort: Option<SortState>,
}

impl Default for DataFilters {
    fn default() -> Self {
        DataFilters {
            absolute_path: PathBuf::from("."), // Default to the current directory.
            table_name: "AllData".to_string(), // Default table name.
            csv_delimiter: ";".to_string(),    // Default CSV delimiter.
            schema_length: 200,                // Default schema length (rows to infer schema).
            decimal: 2,                        // Default: 2 decimal places.
            query: SQL_COMMANDS[0].to_string(), // Default query (typically selects all).
            sort: None,                        // Default: no sorting.
        }
    }
}

impl DataFilters {
    /// Creates a new `DataFilters` instance from command-line arguments.
    pub fn new(args: &Arguments) -> PolarsViewResult<Self> {
        // Get the canonical, absolute path.
        let absolute_path = args.path.canonicalize()?;

        Ok(DataFilters {
            absolute_path,
            table_name: args.table_name.clone(),
            csv_delimiter: args.delimiter.clone(),
            query: args.query.clone(),
            ..Default::default() // Use defaults for other fields.
        })
    }

    /// Sets the data source path, converting to absolute path.
    pub fn set_path(&mut self, path: &Path) {
        self.absolute_path = path
            .canonicalize()
            .unwrap_or_else(|_| panic!("Failed to get absolute path from: {:#?}", path));
    }

    /// Gets the (lowercased) file extension.
    pub fn get_extension(&self) -> Option<String> {
        self.absolute_path
            .extension()
            .and_then(OsStr::to_str)
            .map(str::to_lowercase)
    }

    /// Checks if filter fields are empty (except numeric ones with defaults).
    pub fn is_empty(&self) -> bool {
        self.table_name.is_empty() || self.csv_delimiter.is_empty() || self.query.is_empty()
    }

    /// Determines FileExtension and loads the DataFrame.
    pub async fn get_df_and_extension(&mut self) -> PolarsViewResult<(DataFrame, FileExtension)> {
        dbg!(&self);

        let extension = FileExtension::from_path(&self.absolute_path);

        let (df, delimiter) = match &extension {
            // Match on reference
            FileExtension::Parquet => Self::read_parquet(self).await?,
            FileExtension::Csv => Self::read_csv(self).await?,
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

        // Update delimiter if `read_csv` found one.
        if let Some(byte) = delimiter {
            self.csv_delimiter = (byte as char).to_string();
        }

        dbg!(&self);
        Ok((df, extension)) // Return the DataFrame and FileExtension.
    }

    /// Reads a Parquet file into a Polars DataFrame.
    async fn read_parquet(&self) -> PolarsViewResult<(DataFrame, Option<u8>)> {
        let file = File::open(&self.absolute_path)?; // Open the file (synchronous I/O, but wrapped in async fn).
        let df = ParquetReader::new(file).finish()?; // Read the Parquet data.
        Ok((df, None))
    }

    /// Reads a CSV file, tries delimiters, returns DataFrame and used delimiter.
    async fn read_csv(&self) -> PolarsViewResult<(DataFrame, Option<u8>)> {
        let separator = match self.csv_delimiter.as_bytes().first() {
            Some(byte) => *byte,
            None => {
                return Err(PolarsViewError::InvalidDelimiter(
                    self.csv_delimiter.clone(),
                ));
            }
        };

        // Common delimiters to try.
        let mut delimiters = vec![separator, b',', b';', b'|', b'\t', b' '];
        dedup(&mut delimiters);

        // Number of rows to read for initial delimiter detection.
        const NROWS: usize = 100;

        // Try each delimiter.
        for delimiter in delimiters {
            // First, try reading a small chunk of the file to quickly check the delimiter.
            if Self::attempt_read_csv(self, delimiter, Some(NROWS))
                .await
                .is_ok()
            {
                // If reading the chunk was successful, read the entire file with that delimiter.
                if let Ok(df) = Self::attempt_read_csv(self, delimiter, None).await {
                    return Ok((df, Some(delimiter))); // Return the DataFrame and the delimiter.
                }
            }
        }

        // If all delimiters fail, return an error.
        let msg = "Failed CSV read with common delimiters or inconsistent data.";
        tracing::error!("{msg}"); // Log the error.
        Err(PolarsViewError::CsvParsing(msg.to_string()))
    }

    /// Tries reading CSV with a specific delimiter.
    async fn attempt_read_csv(
        &self,
        delimiter: u8,
        n_rows: Option<usize>,
    ) -> PolarsViewResult<DataFrame> {
        dbg!(delimiter as char); // Debug output: delimiter.

        // Convert the static NULL_VALUES slice to a Vec<PlSmallStr>.
        // Set values that will be interpreted as missing/null.
        let null_values: Vec<PlSmallStr> = NULL_VALUES.iter().map(|&s| s.into()).collect();

        // Configure the LazyCsvReader.
        let lazyframe = LazyCsvReader::new(&self.absolute_path)
            .with_encoding(CsvEncoding::LossyUtf8) // Handle encoding issues gracefully.
            .with_has_header(true) // Assume the first row is a header.
            .with_try_parse_dates(true) // Try to parse dates automatically.
            .with_separator(delimiter) // Set the delimiter.
            .with_infer_schema_length(Some(self.schema_length)) // Infer schema from the first rows.
            .with_ignore_errors(true) // Ignore parsing errors (rows with errors will have nulls).
            .with_missing_is_null(true) // Treat missing values as null.
            .with_null_values(Some(NullValues::AllColumns(null_values))) // Specify null values.
            .with_n_rows(n_rows) // Optionally limit the number of rows read.
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

    /// Renders the UI for configuring data filters.
    pub fn render_filter(&mut self, ui: &mut Ui) -> Option<DataFilters> {
        // Create mutable copies for editing within the UI.
        let mut path_str = self.absolute_path.to_string_lossy().to_string();
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
                    // Path input.
                    ui.label("Path:");
                    // `TextEdit::singleline` creates a single-line text input field.
                    let path_edit = TextEdit::singleline(&mut path_str)
                        .desired_width(width_max)
                        .min_size([width_min, ui.available_height()].into());
                    ui.add(path_edit).on_hover_text("Enter path and press the Apply button...");
                    ui.end_row();

                    // Table name input.
                    ui.label("Table Name:");
                    let table_name_edit =
                        TextEdit::singleline(&mut self.table_name).desired_width(width_max);
                    ui.add(table_name_edit).on_hover_text("Enter table name for SQL queries...");
                    ui.end_row();

                    // Conditional CSV settings (only if file extension is "csv").
                    if let Some("csv") = self.get_extension().as_deref() {
                        // CSV delimiter input.
                        ui.label("CSV Delimiter:");
                        let csv_delimiter_edit =
                            TextEdit::singleline(&mut self.csv_delimiter)
                                .char_limit(1) // CSV Delimiter must be a single character.
                                .desired_width(width_max);
                        ui.add(csv_delimiter_edit).on_hover_text("Enter the CSV delimiter character...");
                        ui.end_row();

                        // Schema length input (using DragValue).
                        ui.label("Schema length:");
                        ui.add(
                            DragValue::new(&mut self.schema_length)
                                .speed(1)
                                .range(1..=usize::MAX),
                        )
                        .on_hover_text("Number of rows to use for inferring schema...");
                        ui.end_row();
                    }

                    // Decimal places input (using DragValue).
                    ui.label("Decimal places:");
                    ui.add(
                        DragValue::new(&mut self.decimal)
                            .speed(1)
                            .range(0..=32),
                    )
                    .on_hover_text("Decimal places for float formatting...");
                    ui.end_row();

                    // SQL query input (multiline).
                    ui.label("SQL Query:");
                    // Creates the TextEdit widget for SQL Query input.
                    let query_edit = TextEdit::multiline(&mut self.query).desired_width(width_max);
                    ui.add(query_edit).on_hover_text("Enter SQL query to filter data...");
                    ui.end_row();

                    // "Apply" button and filter application logic.
                    ui.label(""); // For alignment.
                    ui.with_layout(Layout::top_down(Align::Center), |ui| {
                        if ui.button("Apply SQL Commands").clicked() {
                            self.query = self.query.trim().to_string(); //trim spaces.
                            let path_new = PathBuf::from(path_str.trim());

                            // Input validation:
                            if path_new.exists() && !self.is_empty() {
                                result = Some(DataFilters {
                                    absolute_path: path_new,
                                    table_name: self.table_name.clone(),
                                    csv_delimiter: self.csv_delimiter.clone(),
                                    schema_length: self.schema_length,
                                    decimal: self.decimal,
                                    query: self.query.clone(),
                                    sort: self.sort.clone(), // Preserve sort.
                                });
                            } else {
                                // Error handling for empty fields or invalid path.
                                let msg = "Error: Path, Table Name, and Query cannot be empty, and the Path must exist.";
                                tracing::error!(msg); // Log error
                            }
                        }
                    });
                    ui.end_row();
                });
            },
        );

        // Update the DataFilters instance with the (potentially) modified values from the UI.
        self.absolute_path = PathBuf::from(path_str.trim());

        // SQL Command Examples (Collapsing Header).
        CollapsingHeader::new("SQL Command Examples:")
            .default_open(false)
            .show(ui, |ui| {
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
                        ui.add(egui::Label::new(SQL_COMMANDS.join("\n\n")).selectable(true));
                    });
            });

        result // Return the potentially updated filters.
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
