use egui::{TextStyle, Ui};
use egui_extras::{Column, TableBuilder, TableRow};
use polars::prelude::Column as PColumn;
use polars::{prelude::*, sql::SQLContext};
use std::sync::Arc;

use crate::{
    // format_dataframe_columns, // Commented out as it's not currently used.
    DataFilters,
    ExtraInteractions,
    FileExtension,
    PolarsViewError,
    PolarsViewResult,
    SortState,
    get_decimal_and_layout,
    remove_null_columns,
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
    /// Loads data from a file (Parquet, CSV, Json, or NDJson) and optionally applies an SQL query.
    ///
    /// This function orchestrates the loading, preprocessing, and filtering of data based on the provided `DataFilters`.
    ///
    /// ### Arguments
    ///
    /// * `filters`: A `DataFilters` struct containing file path, query, and other settings.
    ///
    /// ### Returns
    ///
    /// A `PolarsViewResult` containing the `DataFrameContainer` or a Polars error.
    pub async fn load_data(mut filters: DataFilters) -> PolarsViewResult<Self> {
        // dbg!(&filters); // Debugging statement (consider removing in production code).
        tracing::debug!("fn load_data()\nfilters: {filters:#?}");

        // Validate the path *before* attempting to load.
        // Gives a more specific error message.
        if !filters.absolute_path.exists() {
            return Err(PolarsViewError::FileNotFound(filters.absolute_path.clone()));
        }

        // Load DataFrame based on file extension and retrieve the extension.
        let (mut df, extension) = filters.get_df_and_extension().await?;

        // Format the DataFrame to n decimal places.
        // let mut formatted_df = format_dataframe_columns(df, filters.decimal)?;

        // Apply the SQL query if the apply_sql flag is true.
        if filters.apply_sql {
            // Create a new SQL context.
            let mut ctx = SQLContext::new();

            // Register the DataFrame as a table with the specified name.
            ctx.register(&filters.table_name, df.lazy());

            // Execute the SQL query and collect the results into a new DataFrame.
            df = ctx.execute(&filters.query)?.collect()?;

            // Reset the apply_sql flag to prevent re-application of the same query.
            filters.apply_sql = false;
        }

        // Remove columns containing only null values if the flag is set.
        if filters.remove_null_cols {
            df = remove_null_columns(&df)?;
        }

        // Update DataFrame schema in filters.
        filters.schema = df.schema().clone();

        // dbg!(&filters); // Debugging statement (consider removing in production code).
        tracing::debug!("fn load_data()\nfilters: {filters:#?}");

        // Create and return a new DataFrameContainer.
        Ok(Self {
            df: Arc::new(df),               // Wrap DataFrame in Arc for shared ownership.
            extension: Arc::new(extension), // Wrap extension in Arc.
            filters: Arc::new(filters),     // Wrap the updated filters in Arc.
        })
    }

    /// Sorts the DataFrame based on the provided `DataFilters`.
    ///
    /// If no sorting is specified, the original DataFrame is returned.
    pub async fn sort(self, filters: DataFilters) -> PolarsViewResult<Self> {
        // If no sort order is provided, return the original DataFrameContainer.
        let Some(sort) = &filters.sort else {
            return Ok(self);
        };

        // Determine column name and sort order (ascending/descending) from the SortState.
        let (col_name, ascending) = match sort.as_ref() {
            SortState::Ascending(col_name) => (col_name, true),
            SortState::Descending(col_name) => (col_name, false),
            SortState::NotSorted(_) => return Ok(self), // No sorting needed.
        };

        // dbg!(sort, col_name, ascending);  // Debugging statement (consider removing).
        tracing::debug!("fn sort()\nsort: {sort:#?}\ncol_name: {col_name}\nascending: {ascending}");

        // Create sort options.  Maintain original order for equal elements, use multithreading.
        let sort_options = SortMultipleOptions::default()
            .with_maintain_order(true) // Preserve original order of equal elements.
            .with_multithreaded(true) // Use multiple threads for sorting.
            .with_order_descending(!ascending) // Set descending based on `ascending`.
            .with_nulls_last(false); // Place nulls at the beginning.

        // Apply the sorting to the DataFrame.
        let df_sorted = self.df.sort([col_name], sort_options)?;

        // Return a new DataFrameContainer with the sorted DataFrame.
        Ok(DataFrameContainer {
            df: Arc::new(df_sorted),    // The sorted DataFrame.
            extension: self.extension,  // Keep the original extension.
            filters: Arc::new(filters), // Use the updated filters.
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
        let mut filters: Option<DataFilters> = None; // To store any updated filters.
        let mut sorted_column = self.filters.sort.clone();
        let decimal = self.filters.decimal; // Get decimal from filters.

        // Header rendering closure: creates sort buttons for each column.
        let analyze_header = |mut table_row: TableRow<'_, '_>| {
            self.render_table_header(&mut table_row, &mut sorted_column, &mut filters);
        };

        // Rows rendering closure: displays the data for each row.
        let analyze_rows = |mut table_row: TableRow<'_, '_>| {
            self.render_table_row(&mut table_row, decimal);
        };

        // Build the table (moved *before* defining analyze_header and analyze_rows).
        self.build_table(ui, analyze_header, analyze_rows);

        filters // Return potentially updated filters (only if sorting changed them).
    }

    /// Renders the header row of the table, with sort buttons for each column.
    fn render_table_header(
        &self,
        table_row: &mut TableRow<'_, '_>,
        sorted_column: &mut Option<Arc<SortState>>,
        filters: &mut Option<DataFilters>,
    ) {
        for column_name in self.df.get_column_names() {
            table_row.col(|ui| {
                // Determine if this column is currently sorted and its direction.
                let sort_state = match &self.filters.sort {
                    Some(sort) if sort.is_sorted_column(column_name) => sort.clone(), // Current sort.
                    _ => SortState::NotSorted(column_name.to_string()).into(), // Default: Not sorted.
                };

                ui.horizontal_centered(|ui| {
                    // Create the sort button (using the `ExtraInteractions` trait).
                    if ui.sort_button(sorted_column, sort_state).clicked() {
                        // Update filters *only if* the sort button is clicked.
                        *filters = Some(DataFilters {
                            sort: sorted_column.clone(),     // New sort state.
                            ..self.filters.as_ref().clone()  // Keep other filter settings.
                        });
                    }
                });
            });
        }
    }

    /// Renders a single row of the table, formatting each cell according to its data type.
    fn render_table_row(&self, table_row: &mut TableRow<'_, '_>, decimal: usize) {
        let row_index = table_row.index();

        for column in self.df.get_columns() {
            // Determine decimal places and layout *once* per column.
            let (opt_decimal, layout) = get_decimal_and_layout(column, decimal); // Get formatting

            // Format the cell value and add it to the table row.
            let value_str = Self::format_cell_value(column, row_index, opt_decimal); // Get value

            table_row.col(|ui| {
                ui.with_layout(layout.with_main_wrap(false), |ui| {
                    ui.label(value_str); // Display value with determined layout.
                });
            });
        }
    }

    /// Retrieves and formats the value of a cell as a string, ready for display.
    fn format_cell_value(column: &PColumn, row_index: usize, opt_decimal: Option<usize>) -> String {
        match (column.get(row_index), opt_decimal) {
            (Ok(any_value), Some(decimal)) => match any_value {
                AnyValue::Float32(f) => format!("{:.*}", decimal, f), // Format float with decimal.
                AnyValue::Float64(f) => format!("{:.*}", decimal, f), // Format float with decimal.
                AnyValue::Null => "".to_string(),                     // Handle null.
                _ => "Unexpected Value".to_string(),
            },
            (Ok(any_value), None) => match any_value {
                AnyValue::String(s) => s.to_string(), // String - no formatting needed.
                AnyValue::Null => "".to_string(),     // Null - empty string.
                av => av.to_string(),                 // Everything else: use to_string.
            },
            (Err(_), _) => "Error: Value not found".to_string(),
        }
    }

    /// Constructs and displays the `egui` table using the
    /// provided closures for header and row rendering.
    fn build_table(
        &self,
        ui: &mut Ui,
        analyze_header: impl FnMut(TableRow<'_, '_>),
        analyze_rows: impl FnMut(TableRow<'_, '_>),
    ) {
        let style = ui.style();
        let text_height = TextStyle::Body.resolve(style).size; // Calculate text height
        let col_number = self.df.width().max(1) as f32; // Number of columns. Ensure at least 1 column.

        // Calculate available space, accounting for spacing and scrollbar.
        let available_space = ui.available_width()
            - (col_number + 1.0) * style.spacing.item_spacing.x // Space taken by gaps between columns.
            - style.spacing.scroll.bar_width; // Space taken by the scrollbar.

        // Calculate initial and minimum column widths.
        let initial_col_width = available_space.max(col_number) / col_number; // Use max(1) to prevent division by zero.
        let header_height = style.spacing.interact_size.y + 2.0 * style.spacing.item_spacing.y;
        let min_col_width = style.spacing.interact_size.x.max(initial_col_width / 4.0);

        // Configure table columns.
        let column = Column::initial(initial_col_width) // The starting width of each column.
            .at_least(min_col_width) // The minimum width each column can shrink to.
            .resizable(true) // Whether the user can resize the columns manually.
            .clip(true); // Clip the contents of the cell if they overflow

        // Build the table using egui_extras::TableBuilder.
        TableBuilder::new(ui)
            .striped(true) // Alternate row background colors.
            .columns(column, self.df.width()) // Set up the columns
            .column(Column::remainder()) // For the remaining columns
            .auto_shrink([false, false]) // Disable auto-shrinking.
            .header(header_height, analyze_header) // Add the table header
            .body(|body| {
                // Add the table body.
                let num_rows = self.df.height();
                body.rows(text_height, num_rows, analyze_rows);
            });
    }
}
