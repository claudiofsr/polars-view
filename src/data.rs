use crate::{Arguments, SQL_COMMANDS};
use egui::{
    Align, CollapsingHeader, Color32, Frame, Grid, Hyperlink, Layout, Stroke, TextEdit, Ui, Vec2,
};
use polars::{prelude::*, sql::SQLContext};
use std::{
    fmt::Debug,
    fs::File,
    future::Future,
    path::{Path, PathBuf},
    sync::Arc,
};

pub type DataResult = Result<DataFrameContainer, String>;
pub type DataFuture = Box<dyn Future<Output = DataResult> + Unpin + Send + 'static>;

// Set values that will be interpreted as missing/null.
static NULL_VALUES: &[&str] = &["", " ", "<N/D>", "*DIVERSOS*"];

/// Represents the sorting state for a column.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum SortState {
    /// The column is not sorted.
    NotSorted(String),
    /// The column is sorted in ascending order.
    Ascending(String),
    /// The column is sorted in descending order.
    Descending(String),
}

/// Holds filters to be applied to the data.
#[derive(Debug, Clone)]
pub struct DataFilters {
    /// Absolute path of the data source.
    pub absolute_path: PathBuf,
    /// Table name for registering with Polars SQL Context.
    pub table_name: String,
    /// CSV delimiter.
    pub csv_delimiter: String,
    /// Optional SQL query to apply to the data.
    pub query: Option<String>,
    /// Optional column sorting state.
    pub sort: Option<SortState>,
}

impl Default for DataFilters {
    fn default() -> Self {
        let args = Arguments::default();
        DataFilters::new(&args)
    }
}

impl DataFilters {
    /// Creates a new `DataFilters` instance from command line arguments.
    pub fn new(args: &Arguments) -> Self {
        let msg = format!("Failed to get the absolute path from: {:#?}", args.path);

        // Returns the canonical, absolute form of the path.
        let absolute_path = args.path.canonicalize().expect(&msg);

        DataFilters {
            absolute_path,
            table_name: args.table_name.clone(),
            csv_delimiter: args.delimiter.clone(),
            query: args.query.clone(),
            sort: None,
        }
    }

    pub fn set_path(&mut self, path: &Path) {
        let msg = format!("Failed to get the absolute path from: {:#?}", path);

        // Returns the canonical, absolute form of the path.
        let absolute_path = path.canonicalize().expect(&msg);

        self.absolute_path = absolute_path;
    }

    /// Extracts the file extension from the absolute path, converting it to lowercase for
    /// case-insensitive comparison.
    pub fn get_extension(&self) -> Option<String> {
        self.absolute_path
            .extension() // Get the extension as an Option<&OsStr>
            .and_then(|ext| ext.to_str()) // Convert the extension to &str, returning None if the conversion fails
            .map(|ext| ext.to_lowercase()) // Convert the extension to lowercase for case-insensitive comparison
    }

    /// Renders the query pane UI for configuring data filters.
    pub fn render_filter(&mut self, ui: &mut Ui) -> Option<DataFilters> {
        // Create mutable copies of the filter values to allow editing.
        let mut path_str = self.absolute_path.to_string_lossy().to_string();
        let mut table_name = self.table_name.clone();
        let mut csv_delimiter = self.csv_delimiter.clone();
        let mut query = self.query.clone()?;

        let width_max = ui.available_width();

        // Create a grid layout for the filter configuration.
        let mut result = None; // Move the declaration outside the Grid

        let grid = Grid::new("data_filters_grid")
            .num_columns(2)
            .spacing([10.0, 20.0])
            .striped(true);

        ui.allocate_ui_with_layout(
            Vec2::new(width_max, ui.available_height()),
            Layout::top_down(Align::LEFT),
            |ui| {
                grid.show(ui, |ui| {
                    ui.label("path:");
                    let path_edit = TextEdit::singleline(&mut path_str).desired_width(width_max);
                    ui.add(path_edit)
                        .on_hover_text("Enter path and press the Apply button...");
                    ui.end_row();

                    ui.label("Table Name:");
                    let table_name_edit =
                        TextEdit::singleline(&mut table_name).desired_width(width_max);
                    ui.add(table_name_edit)
                        .on_hover_text("Enter table name for SQL queries...");
                    ui.end_row();

                    ui.label("CSV Delimiter:");
                    let csv_delimiter_edit =
                        TextEdit::singleline(&mut csv_delimiter).desired_width(width_max);
                    ui.add(csv_delimiter_edit)
                        .on_hover_text("Enter the CSV delimiter character...");
                    ui.end_row();

                    ui.label("SQL Query:");
                    let query_edit = TextEdit::multiline(&mut query).desired_width(width_max);
                    ui.add(query_edit)
                        .on_hover_text("Enter SQL query to filter and transform the data...");
                    ui.end_row();

                    // Add the button to the grid.
                    ui.label(""); // Empty label to align with the label column.
                    ui.with_layout(Layout::top_down(Align::Center), |ui| {
                        if ui.button("Apply SQL Commands").clicked() {
                            // Only create and return DataFilters if the required fields are not empty.
                            if !path_str.trim().is_empty()
                                && !table_name.trim().is_empty()
                                && !csv_delimiter.is_empty()
                                && !query.trim().is_empty()
                            {
                                result = Some(DataFilters {
                                    absolute_path: PathBuf::from(path_str.clone()),
                                    table_name: table_name.clone(),
                                    csv_delimiter: csv_delimiter.clone(),
                                    query: Some(query.clone()),
                                    sort: self.sort.clone(), // Preserve existing sort state
                                });
                            } else {
                                // Handle the case where required fields are empty.
                                eprintln!(
                                    "Error: path, Table Name, CSV Delimiter, and Query cannot be empty."
                                );
                                result = None;
                            }
                        }
                    });
                    ui.end_row();
                });
            });

        // Update the filter values with the edited values.
        self.absolute_path = PathBuf::from(path_str);
        self.table_name = table_name;
        self.csv_delimiter = csv_delimiter;
        self.query = Some(query);

        // Collapsing header for SQL command examples.
        CollapsingHeader::new("SQL Command Examples:")
            .default_open(false) // Initially collapsed.
            .show(ui, |ui| {
                // Highlighted frame for displaying SQL command examples.
                Frame::default()
                    .stroke(Stroke::new(1.0, Color32::GRAY))
                    .outer_margin(2.0)
                    .inner_margin(10.0)
                    .show(ui, |ui| {
                        ui.with_layout(egui::Layout::top_down(egui::Align::Center), |ui| {
                            let url =
                                "https://docs.pola.rs/api/python/stable/reference/sql/index.html";
                            let heading = Hyperlink::from_label_and_url("SQL Interface", url);
                            ui.add(heading).on_hover_text(url);
                        });
                        ui.add(egui::Label::new(SQL_COMMANDS.join("\n\n")).selectable(true));
                    });
            });

        result // Return the result
    }
}

/// Contains a DataFrame along with associated metadata and filters.
#[derive(Debug, Clone)]
pub struct DataFrameContainer {
    /// The Polars DataFrame, wrapped in an Arc for shared ownership and thread-safe access.
    pub df: Arc<DataFrame>,
    /// String with "parquet" or "csv"
    pub extension: String,
    /// Filters applied to the DataFrame.
    pub filters: Arc<DataFilters>,
}

impl DataFrameContainer {
    /// Loads data from a file (Parquet or CSV).
    pub async fn load_data(mut filters: DataFilters) -> Result<Self, String> {
        dbg!(&filters);
        let (df, extension) = Self::get_df_and_extension(&mut filters).await?;

        Ok(Self {
            df: Arc::new(df),
            extension,
            filters: Arc::new(filters),
        })
    }

    /// Loads data from a file (Parquet or CSV) And applies SQL query using Polars.
    pub async fn load_data_with_sql(mut filters: DataFilters) -> Result<Self, String> {
        dbg!(&filters);
        let (df, extension) = Self::get_df_and_extension(&mut filters).await?;

        let df_new = match &filters.query {
            Some(query) => {
                let table_name = filters.table_name.clone();

                // Create a SQL context and register the DataFrame
                let mut ctx = SQLContext::new();
                ctx.register(&table_name, df.lazy());

                // Execute the query and collect the results
                let df_sql: DataFrame = ctx
                    .execute(query)
                    .map_err(|e| format!("Polars SQL error: {}", e))?
                    .collect()
                    .map_err(|e| format!("DataFrame error: {}", e))?;

                df_sql
            }
            None => df,
        };

        Ok(Self {
            df: Arc::new(df_new),
            extension,
            filters: Arc::new(filters),
        })
    }

    /// Gets the DataFrame and file extension based on the provided `DataFilters`.
    ///
    /// This function determines the file type based on the extension provided by
    /// `filters.get_extension()`. It then reads the file into a `DataFrame`
    /// using the appropriate reading function (either `read_parquet` or `read_csv`).
    ///
    /// ### Arguments
    ///
    /// * `filters`: A mutable reference to the `DataFilters` struct, containing
    ///              the file path and CSV delimiter.
    ///
    /// ### Returns
    ///
    /// A `Result` containing:
    ///   - `Ok((DataFrame, String))`: A tuple containing the loaded `DataFrame` and the file extension as a `String`.
    ///   - `Err(String)`: An error message indicating the reason for failure (e.g., unknown extension, invalid delimiter, file reading error).
    pub async fn get_df_and_extension(
        filters: &mut DataFilters,
    ) -> Result<(DataFrame, String), String> {
        dbg!(&filters);

        let absolute_path = &filters.absolute_path;
        let csv_delimiter = &filters.csv_delimiter;

        // Determine extension based on extension and load accordingly.
        let (df, delimiter, extension) = match filters.get_extension().as_deref() {
            Some("parquet") => {
                // Read the parquet file.
                let df = Self::read_parquet(absolute_path).await?;

                (df, None, "parquet") // No delimiter for parquet
            }
            Some("csv") => {
                // Validate csv_delimiter
                if csv_delimiter.len() != 1 {
                    let msg1 = "The CSV delimiter must be a single character.".to_string();
                    let msg2 = format!("CSV Delimiter: {:#?}", csv_delimiter);
                    return Err([msg1, msg2].join("\n\n"));
                }

                // Read the csv file.
                let (df, delimiter) = Self::read_csv(&absolute_path).await?;

                (df, delimiter, "csv")
            }
            Some(ext) => {
                let msg1 = format!("File: {:#?}", absolute_path);
                let msg2 = format!("Unknown extension: {:#?}", ext);
                return Err([msg1, msg2].join("\n\n"));
            }
            None => {
                let msg1 = format!("File: {:#?}", absolute_path);
                let msg2 = "Extension not found!".to_string();
                return Err([msg1, msg2].join("\n\n"));
            }
        };

        // Update the filters.csv_delimiter ONLY if we received a delimiter FROM the reader (read_csv)
        if let Some(byte) = delimiter {
            // Attempt to convert the byte to a Unicode character.
            if let Some(c) = char::from_u32(byte as u32) {
                // If the conversion is successful, set the delimiter.
                filters.csv_delimiter = c.to_string();
            }
        }

        dbg!(&filters);

        Ok((df, extension.to_string()))
    }

    /// Reads a Parquet file into a Polars DataFrame.
    async fn read_parquet(path: impl AsRef<Path>) -> Result<DataFrame, String> {
        let file = File::open(path).map_err(|e| format!("Error opening file: {}", e))?;
        let df = ParquetReader::new(file)
            .finish()
            .map_err(|e| format!("Error reading parquet: {}", e))?;

        Ok(df)
    }

    /// Attempts to read a CSV file with different delimiters until successful.
    async fn read_csv(path: impl AsRef<Path> + Debug) -> Result<(DataFrame, Option<u8>), String> {
        // Common delimiters to attempt when reading CSV files.
        let delimiters = [b',', b';', b'|', b'\t'];

        // Number of rows to read for delimiter detection.
        // Using NROWS helps quickly determine if a delimiter is viable without processing the entire file.
        const NROWS: usize = 100;

        for delimiter in delimiters {
            // Attempt to read the CSV with the current delimiter, limiting the number of rows for faster initial parsing.
            let result_df = Self::attempt_read_csv(&path, delimiter, Some(NROWS)).await;

            if result_df.is_ok() {
                // Delimiter seems promising. Now read the entire file.
                let result_df = Self::attempt_read_csv(&path, delimiter, None).await;

                if let Ok(df_entire) = result_df {
                    // Successfully read the entire CSV file. Return the DataFrame and the delimiter used.
                    return Ok((df_entire, Some(delimiter)));
                }
            }
        }

        // All delimiters failed.
        let msg = "Failed to read CSV with common delimiters or inconsistent data.";
        eprintln!("{msg}");
        Err(msg.to_string())
    }

    /// Attempts to read a CSV file using a specific delimiter.
    async fn attempt_read_csv(
        path: impl AsRef<Path> + Debug,
        delimiter: u8,
        n_rows: Option<usize>,
    ) -> Result<DataFrame, String> {
        dbg!(&path, delimiter as char);

        // Set values that will be interpreted as missing/null.
        let null_values: Vec<PlSmallStr> = NULL_VALUES.iter().map(|&s| s.into()).collect();

        // Configure the CSV reader with flexible options.
        let lazyframe = LazyCsvReader::new(path)
            .with_encoding(CsvEncoding::LossyUtf8) // Handle various encodings
            .with_has_header(true) // Assume the first row is a header
            .with_try_parse_dates(true) // use regex
            .with_separator(delimiter) // Set the delimiter
            .with_infer_schema_length(Some(200)) // Limit schema inference to the first 200 rows.
            .with_ignore_errors(true) // Ignore parsing errors
            .with_missing_is_null(true) // Treat missing values as null
            .with_null_values(Some(NullValues::AllColumns(null_values)))
            .with_n_rows(n_rows)
            .finish()
            .map_err(|e| {
                format!(
                    "Error reading CSV with delimiter '{}': {}",
                    delimiter as char, e
                )
            })?;

        // Collect the lazy DataFrame into a DataFrame
        let df = lazyframe
            //.with_columns(cols()).apply(|col| round, GetOutput::from_type(DataType::String))
            .collect()
            .map_err(|e| e.to_string())?;

        /*
        let lz = lazyframe // Formatar colunas
            .with_columns([
                all().map(|series| {
                    series.fill_null(FillNullStrategy::Zero)
                }, GetOutput::from_type(DataType::String))
                /*
                .map(|series| round_float64_columns(series, 2),
                    GetOutput::same_type()
                    //GetOutput::from_type(DataType::String)
                )
                */
            ]);
        */

        // Check if the number of columns is reasonable
        if df.width() <= 1 {
            let msg = format!("Erro em delimiter: {}", delimiter as char);
            return Err(msg.to_string());
        }

        Ok(df)
    }

    /// Sorts the data based on the provided filters.
    pub async fn sort(mut self, opt_filters: Option<DataFilters>) -> Result<Self, String> {
        // If no filters are provided, return the DataFrame as is.
        let Some(filters) = opt_filters else {
            return Ok(self);
        };

        // If no sort is specified, return the DataFrame as is.
        let Some(sort) = &filters.sort else {
            return Ok(self);
        };

        // Extract sort column and order from filters
        let (col_name, ascending) = match sort {
            SortState::Ascending(col_name) => (col_name, true),
            SortState::Descending(col_name) => (col_name, false),
            SortState::NotSorted(_col_name) => return Ok(self),
        };

        dbg!(sort);
        dbg!(col_name);
        dbg!(ascending);

        // Define sort options
        let sort_options = SortMultipleOptions::default()
            .with_maintain_order(true)
            .with_multithreaded(true)
            .with_order_descending(!ascending) // Sort order: ascending or descending
            .with_nulls_last(false);

        // Sort the DataFrame using Polars
        self.df = self
            .df
            .sort([col_name], sort_options)
            .map_err(|e| format!("Polars sort error: {}", e))?
            .into();
        self.filters = Arc::new(filters); //Update filters

        Ok(self)
    }
}

// font: polars-0.46.0/tests/it/io/csv.rs
#[test]
fn test_quoted_bool_ints() -> PolarsResult<()> {
    let csv = r#"foo,bar,baz
1,"4","false"
3,"5","false"
5,"6","true"
"#;
    let file = std::io::Cursor::new(csv);
    let df = CsvReader::new(file).finish()?;
    let expected = df![
        "foo" => [1, 3, 5],
        "bar" => [4, 5, 6],
        "baz" => [false, false, true],
    ]?;
    assert!(df.equals_missing(&expected));

    Ok(())
}
