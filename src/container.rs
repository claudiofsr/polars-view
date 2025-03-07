use egui::{Direction, Layout, TextStyle, Ui};
use egui_extras::{Column, TableBuilder, TableRow};
use polars::{prelude::*, sql::SQLContext};
use std::sync::Arc;

use crate::{DataFilters, ExtraInteractions, FileExtension, PolarsViewResult, SortState};

/// Contains a DataFrame, file extension, and filters.
#[derive(Debug, Clone)]
pub struct DataFrameContainer {
    /// The Polars DataFrame, wrapped in an Arc for shared ownership.
    pub df: Arc<DataFrame>,
    /// File extension ("parquet" or "csv").
    pub extension: FileExtension,
    /// Applied data filters.
    pub filters: Arc<DataFilters>,
}

impl DataFrameContainer {
    /// Loads data from a file (Parquet or CSV) and optionally applies an SQL query.
    ///
    /// ### Arguments
    ///
    /// * `filters`: A `DataFilters` struct containing file path, query, and other settings.
    /// * `execute_query`: A boolean flag indicating whether to execute the SQL query (if present).
    ///
    /// ### Returns
    ///
    /// A `PolarsViewResult` containing the `DataFrameContainer` or a Polars error.
    pub async fn load_data(
        mut filters: DataFilters,
        execute_query: bool,
    ) -> PolarsViewResult<Self> {
        dbg!(&filters, &execute_query);

        // Load DataFrame based on extension.
        let (mut df, extension) = filters.get_df_and_extension().await?;

        // Format the DataFrame to n decimal places.
        // let mut formatted_df = format_dataframe_columns(df, filters.decimal)?;

        // Apply SQL query if requested.
        if execute_query && !filters.query.is_empty() {
            // Added empty check
            // Create a SQL context.
            let mut ctx = SQLContext::new();

            // Register the DataFrame as a table with the specified name.
            ctx.register(&filters.table_name, df.lazy());

            // Execute the query and collect the results.
            df = ctx.execute(&filters.query)?.collect()?;
        }

        Ok(Self {
            df: Arc::new(df),
            extension,
            filters: Arc::new(filters),
        })
    }

    /// Sorts the DataFrame based on `DataFilters`.
    pub async fn sort(mut self, opt_filters: Option<DataFilters>) -> PolarsViewResult<Self> {
        // If no filters are provided, return the DataFrame as is.
        let Some(filters) = opt_filters else {
            return Ok(self);
        };

        // If no sort is specified, return the DataFrame as is.
        let Some(sort) = &filters.sort else {
            return Ok(self);
        };

        // Extract the column name and sort order from the SortState.
        let (col_name, ascending) = match sort {
            SortState::Ascending(col_name) => (col_name, true),
            SortState::Descending(col_name) => (col_name, false),
            SortState::NotSorted(_) => return Ok(self),
        };

        dbg!(sort, col_name, ascending);

        // Configure sort options.
        let sort_options = SortMultipleOptions::default()
            .with_maintain_order(true) // Preserve original order of equal elements.
            .with_multithreaded(true) // Use multiple threads for sorting.
            .with_order_descending(!ascending) // Set descending based on `ascending`.
            .with_nulls_last(false); // Place nulls at the beginning.

        // Sort the DataFrame.
        self.df = Arc::new(self.df.sort([col_name], sort_options)?);
        self.filters = Arc::new(filters); // Update the filters.

        Ok(self) // Return the sorted DataFrameContainer.
    }

    /// Renders the DataFrame as an `egui` table.
    pub fn render_table(&self, ui: &mut Ui) -> Option<DataFilters> {
        let style = ui.style();
        let mut filters: Option<DataFilters> = None;
        let mut sorted_column = self.filters.sort.clone();

        let text_height = TextStyle::Body.resolve(style).size;
        //Initial and minimal column width.
        let initial_col_width =
            (ui.available_width() - style.spacing.scroll.bar_width) / (self.df.width() + 1) as f32;
        let header_height = style.spacing.interact_size.y + 2.0 * style.spacing.item_spacing.y;
        let min_col_width = style.spacing.interact_size.x.max(initial_col_width / 4.0);

        // Configure table columns.
        let column = Column::initial(initial_col_width)
            .at_least(min_col_width)
            .resizable(true)
            .clip(true);

        // Header rendering closure (creates sort buttons).
        let analyze_header = |mut table_row: TableRow<'_, '_>| {
            for column_name in self.df.get_column_names() {
                table_row.col(|ui| {
                    // Determine the current sort state of the column.
                    let sort_state = match &sorted_column {
                        // If is sorted returns value
                        Some(sort) if sort.is_sorted_column(column_name) => sort.clone(),
                        // All except that cases are mapped it as not sorted.
                        _ => SortState::NotSorted(column_name.to_string()),
                    };

                    // Create a centered layout for the sort button.
                    ui.horizontal_centered(|ui| {
                        // Creates the sort button using the ExtraInteractions trait.
                        let response = ui.sort_button(&mut sorted_column, sort_state);
                        if response.clicked() {
                            // If the sort button is clicked, create a DataFilters to trigger a resort.
                            filters = Some(DataFilters {
                                sort: sorted_column.clone(), // Updates the filters with the new sort state.
                                ..self.filters.as_ref().clone()  // Inherit other filter settings.
                            });
                        }
                    });
                });
            }
        };

        // Define a closure to render the table rows.  This closure takes a TableRow
        // and populates it with the data from the corresponding row in the DataFrame.
        let analyze_rows = |mut table_row: TableRow<'_, '_>| {
            let row_index = table_row.index();

            // Iterate over the columns in the DataFrame.
            for column in self.df.get_columns() {
                let column_name = column.name();
                // Check if the column name contains "Alíquota" (Portuguese for tax rate).
                let is_aliquota = column_name.contains("Alíquota");
                let dtype = column.dtype();

                // Determine decimal places and layout based on data type.
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
                } else if dtype.is_integer() || dtype.is_date() {
                    // For integers and dates, use centered layout and no decimal formatting.
                    (None, Layout::centered_and_justified(Direction::LeftToRight))
                } else {
                    // Default to left-aligned layout for other data types (e.g., String, Boolean).
                    (None, Layout::left_to_right(egui::Align::Center))
                };

                // Get the cell value and format it appropriately.
                let value = match (column.get(row_index), decimals) {
                    // If 'decimals' is Some(n), we know it's a float.
                    // Format it to 'n' decimal places.
                    (Ok(any_value), Some(decimals)) => match any_value {
                        AnyValue::Float32(f) => format!("{:.*}", decimals, f),
                        AnyValue::Float64(f) => format!("{:.*}", decimals, f),
                        AnyValue::Null => "".to_string(),
                        _ => "Unexpected Value".to_string(),
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
                        ui.label(value); // Display the formatted value in the cell.
                    });
                });
            }
        };

        // Build the table using egui_extras::TableBuilder.
        TableBuilder::new(ui)
            .striped(false) // Disable striped rows.
            .columns(column, self.df.width()) // Set up the columns.
            .column(Column::remainder())
            .auto_shrink([false, false]) // Disable auto-shrinking to fit content.
            .header(header_height, analyze_header) // Render the table header.
            .body(|body| {
                let num_rows = self.df.height();
                body.rows(text_height, num_rows, analyze_rows); // Render the table rows.
            });

        filters // Return potential updated filters (from sorting).
    }
}
