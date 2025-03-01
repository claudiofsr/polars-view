use crate::{Arguments, SQL_COMMANDS, get_canonicalized_path, get_extension};
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
#[derive(Debug, Default, Clone)]
pub struct DataFilters {
    /// Optional path of the data source.
    pub path: Option<PathBuf>,
    /// Table name for registering with Polars SQL Context.
    pub table_name: String,
    /// CSV delimiter.
    pub csv_delimiter: String,
    /// Optional SQL query to apply to the data.
    pub query: Option<String>,
    /// Optional column sorting state.
    pub sort: Option<SortState>,
}

impl DataFilters {
    /// Creates a new `DataFilters` instance from command line arguments.
    pub fn new(args: &Arguments) -> Self {
        let full_path =
            get_canonicalized_path(&args.path).expect("Failed to get canonicalized path!");

        DataFilters {
            path: full_path,
            table_name: args.table_name.clone(),
            csv_delimiter: args.delimiter.clone(),
            query: args.query.clone(),
            sort: None,
        }
    }

    /// Renders the query pane UI for configuring data filters.
    pub fn render_filter(&mut self, ui: &mut Ui) -> Option<DataFilters> {
        // Create mutable copies of the filter values to allow editing.
        let mut path = self.path.clone()?.to_string_lossy().to_string();
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
                    let path_edit = TextEdit::singleline(&mut path).desired_width(width_max);
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
                            if !path.trim().is_empty()
                                && !table_name.trim().is_empty()
                                && !csv_delimiter.trim().is_empty()
                                && !query.trim().is_empty()
                            {
                                result = Some(DataFilters {
                                    path: Some(PathBuf::from(path.clone())),
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
        self.path = Some(PathBuf::from(path));
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
    /// The path associated with the DataFrame.
    pub path: PathBuf,
    /// The Polars DataFrame, wrapped in an Arc for shared ownership and thread-safe access.
    pub df: Arc<DataFrame>,
    /// Filters applied to the DataFrame.
    pub filters: DataFilters,
    /// String with "parquet" or "csv"
    pub table_type: String,
}

impl DataFrameContainer {
    /// Loads data from a file (Parquet or CSV) using Polars.
    pub async fn load_data(filters: DataFilters) -> Result<Self, String> {
        dbg!(&filters);

        let full_path: PathBuf = get_canonicalized_path(&filters.path)
            .map_err(|e| format!("Failed to get the absolute path: {}", e))?
            .ok_or_else(|| "No path provided to load.".to_string())?;

        dbg!(&full_path);

        // Determine file type based on extension and load accordingly.
        let (df, table_type) = match get_extension(&full_path).as_deref() {
            Some("parquet") => (Self::read_parquet(&full_path).await?, "parquet".to_string()),
            Some("csv") => (Self::read_csv(&full_path).await?, "csv".to_string()),
            _ => {
                let msg = format!("Unknown file type: {:#?}", full_path);
                return Err(msg);
            }
        };

        Ok(Self {
            path: full_path,
            df: Arc::new(df),
            filters,
            table_type,
        })
    }

    /// Loads data from a file (Parquet or CSV) using Polars and DataFilters.
    pub async fn load_data_with_query(filters: DataFilters) -> Result<Self, String> {
        if filters.query.is_some() {
            Self::load_data_with_sql(filters).await
        } else {
            Self::load_data(filters).await
        }
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
    async fn read_csv(path: impl AsRef<Path> + Debug) -> Result<DataFrame, String> {
        // Delimiters to attempt when reading CSV files.
        let delimiters = [b',', b';', b'|', b'\t'];

        for delimiter in delimiters {
            let result_df = Self::attempt_read_csv(&path, delimiter).await;

            if let Ok(df) = result_df {
                return Ok(df); // Return the DataFrame on success
            }
        }

        let msg = "Failed to read CSV with common delimiters or inconsistent data.";
        eprintln!("{msg}");
        Err(msg.to_string())
    }

    /// Attempts to read a CSV file using a specific delimiter.
    async fn attempt_read_csv(
        path: impl AsRef<Path> + Debug,
        delimiter: u8,
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

    /// Loads data and applies a SQL query using Polars.
    pub async fn load_data_with_sql(filters: DataFilters) -> Result<Self, String> {
        dbg!(&filters);

        // Extract required parameters from filters
        let Some(path) = filters.path.clone() else {
            return Err("No path".to_string());
        };

        let table_name = filters.table_name.clone();

        let csv_delimiter = filters.csv_delimiter.clone();

        let Some(query) = &filters.query else {
            return Err("No query provided".to_string());
        };

        // Load the DataFrame from the file
        let (df, table_type): (DataFrame, String) = match get_extension(&path).as_deref() {
            Some("parquet") => (Self::read_parquet(&path).await?, "parquet".to_string()),
            Some("csv") => {
                // Convert csv_delimiter string to u8 delimiter
                match csv_delimiter.len() {
                    1 => csv_delimiter.as_bytes()[0],
                    _ => {
                        let msg = "Error: The CSV delimiter must be a single character.";
                        return Err(msg.to_string());
                    }
                };

                (Self::read_csv(&path).await?, "csv".to_string())
            }
            _ => {
                let msg = format!("Unknown file type: {:?}", path);
                return Err(msg);
            }
        };

        // Create a SQL context and register the DataFrame
        let mut ctx = SQLContext::new();
        ctx.register(&table_name, df.lazy());

        // Execute the query and collect the results
        let sql_df: DataFrame = ctx
            .execute(query)
            .map_err(|e| format!("Polars SQL error: {}", e))?
            .collect()
            .map_err(|e| format!("DataFrame error: {}", e))?;

        Ok(Self {
            path,
            df: Arc::new(sql_df),
            filters,
            table_type,
        })
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
        self.filters = filters; //Update filters

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
