use egui::{Direction, Layout, TextStyle, Ui};
use egui_extras::{Column, TableBuilder, TableRow};
use polars::{prelude::*, sql::SQLContext};
use std::sync::Arc;

use crate::{
    // format_dataframe_columns,
    DataFilters,
    ExtraInteractions,
    FileExtension,
    PolarsViewResult,
    SortState,
};

/// Contains a DataFrame, file extension, and filters.
///
/// Provides methods for loading, processing, and displaying data in an egui table.
#[derive(Debug, Clone)]
pub struct DataFrameContainer {
    /// The Polars DataFrame, wrapped in an Arc for shared ownership.
    pub df: Arc<DataFrame>,
    /// File extension ("parquet", "csv", "json", etc.).
    pub extension: Arc<FileExtension>,
    /// Applied data filters (including path, query, delimiter, etc.).
    pub filters: Arc<DataFilters>,
}

impl Default for DataFrameContainer {
    fn default() -> Self {
        DataFrameContainer {
            df: Arc::new(DataFrame::default()),          // Empty DataFrame.
            extension: Arc::new(FileExtension::Missing), // No extension.
            filters: Arc::new(DataFilters::default()),   // Default filters.
        }
    }
}

impl DataFrameContainer {
    /// Loads data from a file (Parquet, CSV, Json or NDJson) and optionally applies an SQL query.
    ///
    /// ### Arguments
    ///
    /// * `filters`: A `DataFilters` struct containing file path, query, and other settings.
    /// * `execute_query`: A boolean flag indicating whether to execute the SQL query (if present).
    ///
    /// ### Returns
    ///
    /// A `PolarsViewResult` containing the `DataFrameContainer` or a Polars error.
    pub async fn load_data(mut filters: DataFilters) -> PolarsViewResult<Self> {
        // dbg!(&filters);
        tracing::debug!("fn load_data()\nfilters: {filters:#?}");

        // Load DataFrame based on extension and get the file extension.
        let (mut df, extension) = filters.get_df_and_extension().await?;

        // Format the DataFrame to n decimal places.
        // let mut formatted_df = format_dataframe_columns(df, filters.decimal)?;

        // Apply the SQL query if the current SQL query is different from the previous SQL query.
        if filters.execute_sql_query() {
            // Create a new SQL context.
            let mut ctx = SQLContext::new();

            // Register the DataFrame as a table with the specified name for use in the query.
            ctx.register(&filters.table_name, df.lazy());

            // Execute the SQL query and collect the results into a new DataFrame.
            // The first `?` propagates any errors that occur during query execution.
            df = ctx.execute(&filters.query)?.collect()?;

            // Update DataFrame schema and query_previous.
            filters.schema = df.schema().clone();
            filters.query_previous = filters.query.clone();

            // dbg!(&filters);
            tracing::debug!("fn load_data()\nfilters: {filters:#?}");
        }

        // Create and return a new DataFrameContainer, wrapping the DataFrame and filters in Arcs.
        Ok(Self {
            df: Arc::new(df),               // Wrap DataFrame in Arc for shared ownership
            extension: Arc::new(extension), // Wrap extension in Arc for shared ownership
            filters: Arc::new(filters),     // Wrap the filters in Arc for shared ownership
        })
    }

    /// Sorts the DataFrame based on the provided `DataFilters`.
    ///
    /// If no sorting is specified in the filters, the original DataFrame is returned.
    pub async fn sort(self, filters: DataFilters) -> PolarsViewResult<Self> {
        // Return immediately if no sort are provided.
        let Some(sort) = &filters.sort else {
            return Ok(self);
        };

        // Determine column name and sort order from the SortState.
        let (col_name, ascending) = match sort.as_ref() {
            SortState::Ascending(col_name) => (col_name, true),
            SortState::Descending(col_name) => (col_name, false),
            SortState::NotSorted(_) => return Ok(self), // No sorting needed
        };

        // dbg!(sort, col_name, ascending);
        tracing::debug!("fn sort()\nsort: {sort:#?}\ncol_name: {col_name}\nascending: {ascending}");

        // Create sort options. Maintain original order for equal elements, use multiple threads.
        let sort_options = SortMultipleOptions::default()
            .with_maintain_order(true) // Preserve original order of equal elements.
            .with_multithreaded(true) // Use multiple threads for sorting.
            .with_order_descending(!ascending) // Set descending based on `ascending`.
            .with_nulls_last(false); // Place nulls at the beginning.

        let df_sorted = self.df.sort([col_name], sort_options)?;

        // Return the sorted DataFrameContainer
        Ok(DataFrameContainer {
            // Apply sorting to the DataFrame. Using `sort_multiple` even with one column.
            df: Arc::new(df_sorted),
            extension: self.extension,
            // Update the filters to reflect the current sorting state.
            filters: Arc::new(filters), // Update filters.
        })
    }

    /// Renders the DataFrame as an `egui` table.
    ///
    /// ### Arguments
    ///
    /// * `ui`: A mutable reference to the `egui::Ui` where the table will be rendered.
    ///
    /// ### Returns
    ///
    /// An `Option<DataFilters>` containing updated filters if sorting is applied, or `None` otherwise.
    pub fn render_table(&self, ui: &mut Ui) -> Option<DataFilters> {
        let mut filters: Option<DataFilters> = None;
        let mut sorted_column = self.filters.sort.clone();

        // Header rendering closure: creates sort buttons for each column.
        let analyze_header = |mut table_row: TableRow<'_, '_>| {
            for column_name in self.df.get_column_names() {
                table_row.col(|ui| {
                    // Determine current sort state of the column (Ascending, Descending, or NotSorted).
                    let sort_state = match &self.filters.sort {
                        // If sorted, get the SortState.
                        Some(sort) if sort.is_sorted_column(column_name) => sort.clone(),
                        // Otherwise, it's not sorted.
                        _ => SortState::NotSorted(column_name.to_string()).into(),
                    };

                    // Create a horizontally centered layout for the sort button.
                    ui.horizontal_centered(|ui| {
                        // Create the sort button.
                        // The `sort_button` method is provided by the `ExtraInteractions` trait.
                        if ui.sort_button(&mut sorted_column, sort_state).clicked() {
                            // If the sort button is clicked, create new `DataFilters` with the updated `sort` state.
                            // Other filter settings are inherited from the current `self.filters`.
                            filters = Some(DataFilters {
                                sort: sorted_column.clone(), // Updates the filters with the new sort state.
                                ..self.filters.as_ref().clone()  // Inherit other filter settings.
                            });
                        }
                    });
                });
            }
        };

        // Rows rendering closure: displays the data for each row in the DataFrame.
        let analyze_rows = |mut table_row: TableRow<'_, '_>| {
            let row_index = table_row.index();

            // Iterate over all columns in the DataFrame.
            for column in self.df.get_columns() {
                let column_name = column.name();
                // Check if the column name contains "Alíquota" (Portuguese for tax rate).
                let is_aliquota = column_name.contains("Alíquota"); // Check for "Alíquota" (tax rate).
                let dtype = column.dtype();

                // Determine decimal places and layout based on data type and column name.
                let (decimals, layout) = if dtype.is_float() {
                    // If it's a float, set decimals based on whether it's an "Alíquota" column.
                    let decimals = if is_aliquota { 4 } else { self.filters.decimal };
                    // Set layout: centered for "Alíquota", right-aligned for other floats.
                    let layout = if is_aliquota {
                        Layout::centered_and_justified(Direction::LeftToRight)
                    } else {
                        Layout::right_to_left(egui::Align::Center)
                    };
                    (Some(decimals), layout)
                } else if dtype.is_integer() || dtype.is_date() || dtype.is_bool() {
                    // For integers and dates, use centered layout and no decimal formatting.
                    (None, Layout::centered_and_justified(Direction::LeftToRight))
                } else {
                    // Default to left-aligned layout for other data types (e.g., String).
                    (None, Layout::left_to_right(egui::Align::Center))
                };

                // Get the cell value and format it as a string.
                let value = match (column.get(row_index), decimals) {
                    // If 'decimals' is Some(n), we know it's a float.
                    // Format it to 'n' decimal places.
                    (Ok(any_value), Some(decimals)) => match any_value {
                        AnyValue::Float32(f) => format!("{:.*}", decimals, f),
                        AnyValue::Float64(f) => format!("{:.*}", decimals, f),
                        AnyValue::Null => "".to_string(), // Handle null values.
                        _ => "Unexpected Value".to_string(), // For unexpected types.
                    },
                    // If 'decimals' is None, it's not a float.
                    // Convert the AnyValue to a String.
                    (Ok(any_value), None) => match any_value {
                        AnyValue::String(s) => s.to_string(), // Directly use the string.
                        AnyValue::Null => "".to_string(),     // Display empty string for nulls.
                        av => av.to_string(),                 // Use to_string() for other types.
                    },
                    (Err(_), _) => "Error: Value not found".to_string(),
                };

                // Add the cell to the table row.
                table_row.col(|ui| {
                    // Set the layout for the cell (determined earlier) and disable text wrapping.
                    ui.with_layout(layout.with_main_wrap(false), |ui| {
                        ui.label(value); // Display the formatted value.
                    });
                });
            }
        };

        let style = ui.style();
        let text_height = TextStyle::Body.resolve(style).size;
        let col_number = self.df.width().max(1) as f32;
        let available_space = ui.available_width()
            - col_number * style.spacing.item_spacing.x
            - style.spacing.scroll.bar_width;

        // Initial and minimal column widths, calculated based on available space and number of columns.
        let initial_col_width = available_space / col_number;
        let header_height = style.spacing.interact_size.y + 2.0 * style.spacing.item_spacing.y;
        let min_col_width = style.spacing.interact_size.x.max(initial_col_width / 4.0);

        // Configure table columns with initial width, minimum width, resizability, and clipping.
        let column = Column::initial(initial_col_width)
            .at_least(min_col_width)
            .resizable(true)
            .clip(true);

        // Build and display the table using `egui_extras::TableBuilder`.
        TableBuilder::new(ui)
            .striped(true) // Alternate row background colors for better readability.
            .columns(column, self.df.width()) // Set up the columns.
            .column(Column::remainder()) // Add the remainder
            .auto_shrink([false, false]) // Disable auto-shrinking to fit content.
            .header(header_height, analyze_header) // Render the table header.
            .body(|body| {
                let num_rows = self.df.height();
                body.rows(text_height, num_rows, analyze_rows); // Render the table rows.
            });

        filters // Return potentially updated filters.
    }
}
