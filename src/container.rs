use egui::{Color32, Id, TextStyle, Ui};
use egui_extras::{Column, TableBuilder, TableRow};
use polars::prelude::Column as PColumn;
use polars::{prelude::*, sql::SQLContext};
use std::sync::Arc;

use crate::{
    DataFilters, DataFormat, ExtraInteractions, FileExtension, PolarsViewError, PolarsViewResult,
    SelectionDepth, SortState, SortableHeaderWidget, get_decimal_and_layout, remove_null_columns,
};

/// Contains a Polars DataFrame along with associated metadata, filters, and format settings.
/// Provides methods for loading data from files, applying transformations (SQL, sorting),
/// updating formatting, and rendering the data within an `egui` table.
///
/// State like the DataFrame, filters, and format are wrapped in `Arc` to allow for efficient
/// sharing and cloning, particularly important when passing state between the UI thread
/// and asynchronous tasks or when updating parts of the state without deep copies.
#[derive(Debug, Clone)]
pub struct DataFrameContainer {
    /// The Polars DataFrame, wrapped in an `Arc` for cheap cloning and shared ownership.
    /// This is crucial for passing the data to async tasks without expensive copies.
    pub df: Arc<DataFrame>,
    /// The detected file extension (e.g., Csv, Parquet) of the loaded data. Wrapped in `Arc`.
    pub extension: Arc<FileExtension>,
    /// Applied data filters and loading configurations (path, query, delimiter, etc.).
    /// Wrapped in `Arc` to avoid deep copies when only the format or data changes.
    pub filters: Arc<DataFilters>,
    /// Applied data formatting settings (decimal places, alignment, column expansion).
    /// Wrapped in `Arc` to avoid deep copies when only the filters or data changes.
    pub format: Arc<DataFormat>,
}

impl Default for DataFrameContainer {
    /// Creates an empty `DataFrameContainer` with default settings.
    fn default() -> Self {
        DataFrameContainer {
            df: Arc::new(DataFrame::default()), // Start with an empty DataFrame.
            extension: Arc::new(FileExtension::Missing), // Default to no specific extension.
            filters: Arc::new(DataFilters::default()), // Use default filter settings.
            format: Arc::new(DataFormat::default()), // Use default format settings.
        }
    }
}

impl DataFrameContainer {
    /// Loads data asynchronously from a file (Parquet, CSV, Json, or NDJson).
    /// Optionally applies an SQL query and removes null columns based on `DataFilters`.
    ///
    /// This function orchestrates the data loading pipeline:
    /// 1. Validates the file path.
    /// 2. Reads the data using the appropriate Polars reader based on the file extension.
    /// 3. Optionally executes an SQL query against the loaded data.
    /// 4. Optionally removes columns containing only null values.
    /// 5. Updates the schema within the filters.
    /// 6. Returns a *new* `DataFrameContainer` instance containing the processed data and updated filters.
    ///
    /// Takes `filters` by value and moves it in. This avoids cloning `DataFilters` just for the loading operation.
    /// The returned container will hold these (potentially updated) filters within an `Arc`.
    ///
    /// ### Arguments
    ///
    /// * `filters`: A `DataFilters` struct containing file path, query, and other loading settings. Moved into the function.
    /// * `format`: The initial `DataFormat` settings to be associated with the loaded data.
    ///
    /// ### Returns
    ///
    /// A `PolarsViewResult` containing the *new* `DataFrameContainer` or a `PolarsViewError`.
    pub async fn load_data(mut filters: DataFilters, format: DataFormat) -> PolarsViewResult<Self> {
        // 1. Validate the path *before* attempting to load to provide a specific error.
        if !filters.absolute_path.exists() {
            return Err(PolarsViewError::FileNotFound(filters.absolute_path.clone()));
        }

        // 2. Load DataFrame based on file extension (delegated to `filters.get_df_and_extension`).
        let (mut df, extension) = filters.get_df_and_extension().await?;

        // (Note: Formatting to n decimal places during load was considered but removed for now).
        // let mut formatted_df = format_dataframe_columns(df, filters.decimal)?;

        // 3. Apply the SQL query if the `apply_sql` flag is true.
        if filters.apply_sql {
            // Create a new SQL context for this operation.
            let mut ctx = SQLContext::new();
            // Register the loaded DataFrame as a table named according to `filters.table_name`.
            ctx.register(&filters.table_name, df.lazy());
            // Execute the SQL query and collect the results into a new DataFrame, overwriting `df`.
            df = ctx.execute(&filters.query)?.collect()?;
            // Reset the apply_sql flag to prevent re-application on subsequent operations (like sorting).
            filters.apply_sql = false;
        }

        // 4. Remove columns containing only null values if the flag is set.
        if filters.remove_null_cols {
            df = remove_null_columns(&df)?;
        }

        // 5. Update the schema stored within the filters to match the final DataFrame.
        filters.schema = df.schema().clone();

        tracing::debug!("fn load_data() successful. Filters applied: {filters:#?}");

        // 6. Create and return a new DataFrameContainer instance.
        //    The DataFrame, extension, filters, and format are wrapped in Arcs.
        Ok(Self {
            df: Arc::new(df),               // Wrap final DataFrame in Arc.
            extension: Arc::new(extension), // Wrap detected extension in Arc.
            filters: Arc::new(filters),     // Wrap the (potentially updated) filters in Arc.
            format: Arc::new(format),       // Wrap the provided format settings in Arc.
        })
    }

    /// Creates a *new* `DataFrameContainer` with updated format settings, keeping other state identical.
    ///
    /// This function is called asynchronously when format settings (like `decimal` or `auto_col_width`)
    /// are changed in the UI (`layout.rs`). It takes the *current* container (`data_container`)
    /// and the *new* format settings (`format`) provided by the UI.
    ///
    /// It returns a *new* container instance containing the *original* data (`df`),
    /// `extension`, and `filters`, but with the newly provided `format`.
    ///
    /// This operation is inherently lightweight as it primarily involves cloning `Arc` pointers,
    /// not the underlying data. The main application loop (`layout.rs`) replaces the old container
    /// with this new one once the asynchronous operation completes via `check_data_pending`.
    ///
    /// Although the core logic is synchronous (just cloning Arcs and struct creation),
    /// it's kept `async` to maintain consistency with the `load_data` and `sort` operations
    /// within the `layout.rs` update pattern, which treats all container updates as futures.
    ///
    /// # Arguments
    /// * `data_container`: An `Arc` pointing to the current `DataFrameContainer`.
    /// * `format`: An `Arc` containing the `DataFormat` struct with the new settings from the UI.
    ///
    /// # Returns
    /// A `PolarsViewResult` containing the *new* `DataFrameContainer` instance.
    pub async fn update_format(
        data_container: Arc<DataFrameContainer>,
        format: Arc<DataFormat>, // Takes ownership of the Arc containing new format settings.
    ) -> PolarsViewResult<Self> {
        tracing::debug!(
            "fn update_format() creating new container with updated format: {format:#?}"
        );

        // Create and return a *new* container.
        // Clone the Arcs for df, extension, and filters (cheap pointer copies).
        // Use the newly received 'format' Arc directly.
        Ok(Self {
            df: data_container.df.clone(), // Clone Arc pointer to original df.
            extension: data_container.extension.clone(), // Clone Arc pointer to original extension.
            filters: data_container.filters.clone(), // Clone Arc pointer to original filters.
            format,                        // Use the new format Arc provided as input.
        })
    }

    /// Creates a *new* `DataFrameContainer` with the DataFrame sorted according to the provided `DataFilters`.
    ///
    /// This function is called asynchronously when the user clicks a column header to sort (`layout.rs`).
    /// It takes the *current* container (`data_container`) and the *new* filter settings (`filters`)
    /// which now include the desired `SortState`.
    ///
    /// If no sorting is specified in the `filters`, it simply returns a clone of the original container.
    /// Otherwise, it performs the sort operation using Polars and returns a *new* container instance
    /// containing the *sorted* DataFrame, the original `extension` and `format`, and the *updated* `filters`.
    ///
    /// # Arguments
    /// * `data_container`: An `Arc` pointing to the current `DataFrameContainer`.
    /// * `filters`: The `DataFilters` struct containing the desired `SortState` and other filter settings.
    ///
    /// # Returns
    /// A `PolarsViewResult` containing the *new*, potentially sorted, `DataFrameContainer`.
    pub async fn sort(
        data_container: Arc<DataFrameContainer>,
        filters: DataFilters, // Takes ownership of the updated filters.
    ) -> PolarsViewResult<Self> {
        // Extract the sort state from the provided filters.
        let Some(sort) = &filters.sort else {
            // If no sort order is requested, return a clone of the existing container.
            // Cloning the container involves only cloning Arcs, which is cheap.
            return Ok(data_container.as_ref().clone());
        };

        // Determine column name and sort direction (ascending/descending) from the SortState.
        let (col_name, ascending) = match sort.as_ref() {
            SortState::Ascending(col_name) => (col_name, true),
            SortState::Descending(col_name) => (col_name, false),
            // If somehow the state is NotSorted but sort wasn't None, treat it as no-op.
            SortState::NotSorted(_) => return Ok(data_container.as_ref().clone()),
        };

        tracing::debug!(
            "fn sort() applying sort. SortState: {sort:#?}, Column: {col_name}, Ascending: {ascending}"
        );

        // Configure Polars sort options.
        let sort_options = SortMultipleOptions::default()
            .with_maintain_order(true) // Keep order of equal elements (stable sort).
            .with_multithreaded(true) // Use multiple threads for sorting if possible.
            .with_order_descending(!ascending) // Polars uses `descending` flag.
            .with_nulls_last(false); // Place nulls at the beginning.

        // Perform the sort operation on the DataFrame held by the current container.
        let df_sorted = data_container.df.sort([col_name], sort_options)?;

        // Return a *new* DataFrameContainer instance.
        Ok(DataFrameContainer {
            df: Arc::new(df_sorted), // Wrap the newly sorted DataFrame in an Arc.
            extension: data_container.extension.clone(), // Clone Arc pointer to original extension.
            filters: filters.into(), // Wrap the input filters (containing the sort state) into an Arc.
            format: data_container.format.clone(), // Clone Arc pointer to original format.
        })
    }

    /// Renders the DataFrame as an interactive `egui` table using `egui_extras::TableBuilder`.
    ///
    /// This function orchestrates the table rendering process:
    /// 1. Sets up closures for rendering the header row (`analyze_header`) and data rows (`analyze_rows`).
    /// 2. The `analyze_header` closure creates sortable buttons for each column header. Clicking these buttons
    ///    updates the sort state and signals the need for a data sort by returning updated `DataFilters`.
    /// 3. The `analyze_rows` closure renders the data for each cell in a table row, applying formatting.
    /// 4. Calls `build_table` to construct the actual `egui_extras::Table` using the defined closures.
    ///
    /// ### Arguments
    ///
    /// * `ui`: A mutable reference to the `egui::Ui` context where the table will be drawn.
    ///
    /// ### Returns
    ///
    /// * `Option<DataFilters>`: Returns `Some(updated_filters)` if a sort button was clicked, indicating
    ///   that the data needs to be re-sorted based on the new `SortState` in the filters.
    ///   Returns `None` otherwise.
    pub fn render_table(&self, ui: &mut Ui) -> Option<DataFilters> {
        // Variable to potentially hold updated filters if sorting is requested by clicking a header.
        let mut updated_filters_for_sort: Option<DataFilters> = None;
        // Store the current sort state to be potentially modified by header clicks.
        // Cloning the Option<Arc<SortState>> is cheap.
        let mut current_sort_state = self.filters.sort.clone();

        // Closure responsible for rendering the table header row.
        // It takes a `TableRow` context and modifies `current_sort_state` and `updated_filters_for_sort`
        // if a sort button is clicked.
        let analyze_header = |mut table_row: TableRow<'_, '_>| {
            self.render_table_header(
                &mut table_row,
                &mut current_sort_state,
                &mut updated_filters_for_sort,
            );
        };

        // Closure responsible for rendering a single data row in the table.
        // It takes a `TableRow` context and renders each cell for that row index.
        let analyze_rows = |mut table_row: TableRow<'_, '_>| {
            self.render_table_row(&mut table_row);
        };

        // Build and render the table using egui_extras::TableBuilder.
        // Pass the header and row rendering closures.
        self.build_table(ui, analyze_header, analyze_rows);

        // Return the potentially updated filters. If a sort button was clicked, this will be Some,
        // triggering the sort operation in layout.rs. Otherwise, it's None.
        updated_filters_for_sort
    }

    /// Renders the header row for the data table.
    /// The appearance and interaction logic depend on Cargo features:
    /// - `header-simple`: Uses `ExtraInteractions::sort_button` for a simple, single-line button.
    /// - `header-wrapping` (default): Uses `SortableHeaderWidget` for wrapping, colored text, icon click.    
    fn render_table_header(
        &self,
        table_row: &mut TableRow<'_, '_>,
        sorted_column: &mut Option<Arc<SortState>>, // Input/Output: Global sort state
        filters: &mut Option<DataFilters>,          // Output: Set if filters need update
    ) {
        // 1. Iterate through each column name.
        for column_name in self.df.get_column_names() {
            // 2. Add a cell to the table row.
            table_row.col(|ui| {
                // --- Common Setup for Both Header Types ---
                // 3. Determine the current sort state for *this* column.
                let column_sort_state_arc = match &self.filters.sort {
                    Some(sort) if sort.is_sorted_column(column_name) => sort.clone(),
                    _ => Arc::new(SortState::NotSorted(column_name.to_string())),
                };
                // --- End Common Setup ---

                // --- Feature-Based Conditional Rendering ---
                if self.format.header_wrapping {
                    // --- START: Wrapping Header Logic ---

                    // 4a. Determine text color based on theme.
                    let column_name_color = if ui.visuals().dark_mode {
                        Color32::from_rgb(160, 200, 255)
                    } else {
                        Color32::from_rgb(0, 80, 160)
                    };

                    // 5a. Call the custom widget trait method.
                    //     It draws the icon and wrapping text, returns icon's response.
                    let icon_response = ui.sortable_wrapping_header(
                        column_name,
                        &column_sort_state_arc, // Pass borrowed SortState
                        column_name_color,
                    );

                    // 6a. Check if the *icon* was clicked.
                    if icon_response.clicked() {
                        // 7a. Calculate next sort state.
                        let next_sort_state_arc = Arc::new(column_sort_state_arc.inc());
                        // 8a. Update global state.
                        *sorted_column = Some(next_sort_state_arc);
                        // 9a. Signal filter change.
                        *filters = Some(DataFilters {
                            sort: sorted_column.clone(),
                            ..self.filters.as_ref().clone()
                        });
                    }
                    // --- END: Wrapping Header Logic ---
                } else {
                    // Fallback: default or if 'header-simple' is explicitly enabled
                    // --- START: Simple Header Logic ---

                    // 4b. Use the standard `sort_button` from the `ExtraInteractions` trait.
                    //     This widget handles drawing the (non-wrapping) label + icon
                    //     and the click logic internally to update `*sorted_column`.
                    //     `sort_button` takes the Arc directly.
                    let button_response = ui
                        .sort_button(
                            sorted_column,                 // Mutably updates the Option<Arc<SortState>>
                            column_sort_state_arc.clone(), // Pass the Arc for this column's state
                        )
                        // Add standard hover text.
                        .on_hover_text(column_name.to_string());

                    // 5b. Check if the button click resulted in a state change *this frame*.
                    //     The `sort_button` widget itself modifies `sorted_column`.
                    //     We just need to signal the change if it occurred.
                    //     The `.clicked()` here isn't quite right - `sort_button` modifies sorted_column
                    //     directly. We should check if `changed()` is true, as the button
                    //     itself indicates a state change. (Or simply compare old/new sorted_column if possible)
                    //     Let's assume `sort_button` returns a response where `.changed()`
                    //     accurately reflects if the *output state* `sorted_column` was altered.
                    //     *Correction:* The original simple code checked `.clicked()` and *then* assigned
                    //     the filters. Let's stick to that pattern, assuming clicking = desire to sort.
                    if button_response.clicked() {
                        // 6b. Signal filter change. The `sorted_column` has *already* been updated
                        //     internally by the `sort_button` call.
                        *filters = Some(DataFilters {
                            // Clone the potentially updated state.
                            sort: sorted_column.clone(),
                            ..self.filters.as_ref().clone()
                        });
                    }
                    // --- END: Simple Header Logic ---
                } // End of else branch (simple header)
            }); // End of table_row.col closure
        } // End of loop through column names
    }

    /// Renders a single data row of the table.
    ///
    /// Iterates through each column of the DataFrame. For the current row index (`table_row.index()`),
    /// it retrieves the cell value, determines the appropriate formatting (decimal places and alignment)
    /// using `get_decimal_and_layout`, formats the value into a string using `format_cell_value`,
    /// and adds the formatted string as a label to the corresponding cell in the `TableRow`.
    ///
    /// ### Arguments
    /// * `table_row`: The `egui_extras::TableRow` context for the current data row.
    fn render_table_row(&self, table_row: &mut TableRow<'_, '_>) {
        // Get the index of the current row being rendered.
        let row_index = table_row.index();

        // Iterate through each column (Polars Series) in the DataFrame.
        for column in self.df.get_columns() {
            // Determine the appropriate decimal places (if any) and text layout (alignment)
            // for this specific column based on its data type and the global format settings.
            let (opt_decimal, layout) = get_decimal_and_layout(column, &self.format);

            // Retrieve the value from the current column at the current row index,
            // and format it into a String according to the determined decimal places.
            let value_str = Self::format_cell_value(column, row_index, opt_decimal);

            // Add a column cell to the current data row.
            table_row.col(|ui| {
                // Apply the determined layout (alignment) to the cell content.
                // `with_main_wrap(false)` prevents text wrapping within the cell.
                ui.with_layout(layout.with_main_wrap(false), |ui| {
                    // Add the formatted value as a simple label.
                    ui.label(value_str);
                });
            });
        }
    }

    /// Retrieves and formats the value of a specific cell (column and row index) as a String.
    ///
    /// Handles different data types, applying decimal formatting for floats if specified.
    /// Returns an empty string for Null values and an error message if the value cannot be retrieved.
    ///
    /// ### Arguments
    /// * `column`: A reference to the Polars `Column` (Series).
    /// * `row_index`: The index of the row to retrieve the value from.
    /// * `opt_decimal`: An `Option<usize>` specifying the number of decimal places for float formatting. `None` for non-floats.
    ///
    /// ### Returns
    /// A `String` representation of the cell value, formatted for display.
    fn format_cell_value(column: &PColumn, row_index: usize, opt_decimal: Option<usize>) -> String {
        // Attempt to get the AnyValue at the specified row index.
        match column.get(row_index) {
            // Value retrieval successful.
            Ok(any_value) => {
                match (any_value, opt_decimal) {
                    // Float value and decimal places specified.
                    (AnyValue::Float32(f), Some(decimal)) => format!("{:.*}", decimal, f),
                    (AnyValue::Float64(f), Some(decimal)) => format!("{:.*}", decimal, f),
                    // Null value (represented as empty string).
                    (AnyValue::Null, _) => "".to_string(),
                    // String value (no special formatting needed, just convert).
                    (AnyValue::String(s), None) => s.to_string(),
                    // String value when decimal is Some (shouldn't happen often, but handle defensively).
                    (AnyValue::String(s), Some(_)) => s.to_string(),
                    // Any other value type (integers, booleans, dates, etc.) - use default `to_string`.
                    (av, _) => av.to_string(),
                }
            }
            // Value retrieval failed (e.g., index out of bounds, though unlikely with TableBuilder).
            Err(_) => "Error: Value not found".to_string(),
        }
    }

    /// Constructs and displays the `egui` table using `egui_extras::TableBuilder`.
    ///
    /// This function configures the `TableBuilder` based on the DataFrame's dimensions
    /// and the current format settings (`self.format`). It calculates initial column widths
    /// but allows columns to be resizable.
    ///
    /// **Crucially**, it assigns a unique `egui::Id` to the `TableBuilder` using
    /// `Id::new("data_table_view").with(self.format.auto_col_width)`. This ID depends on the
    /// `auto_col_width` setting. When `auto_col_width` changes (toggled in the UI), the ID changes.
    /// This forces `egui` to discard any cached layout state (like manually resized column widths)
    /// associated with the *previous* ID, ensuring that the table correctly redraws with
    /// either initial calculated widths (`Column::initial`) or automatic content-based widths
    /// (`Column::auto`) when the setting is toggled.
    ///
    /// ### Arguments
    /// * `ui`: The `egui::Ui` context for drawing.
    /// * `analyze_header`: A closure that renders the header row (defined in `render_table`).
    /// * `analyze_rows`: A closure that renders each data row (defined in `render_table`).
    fn build_table(
        &self,
        ui: &mut Ui,
        analyze_header: impl FnMut(TableRow<'_, '_>),
        analyze_rows: impl FnMut(TableRow<'_, '_>),
    ) {
        // Get UI style for calculating dimensions.
        let style = ui.style();
        let text_height = TextStyle::Body.resolve(style).size; // Height for text rows.
        let col_number = self.df.width().max(1) as f32; // Number of columns.

        // Calculate available width for columns, considering spacing and scrollbar.
        let available_space = ui.available_width()
            - (col_number + 1.0) * style.spacing.item_spacing.x // Space between columns.
            - style.spacing.scroll.bar_width; // Width of the vertical scrollbar.

        // Calculate an initial width for columns if not using auto-sizing.
        let initial_col_width = (available_space / col_number).max(1.0); // Ensure at least 1.0 width.
        let header_height = style.spacing.interact_size.y + 3.0 * style.spacing.item_spacing.y; // Height for header row.
        let min_col_width = style.spacing.interact_size.x.max(initial_col_width / 4.0); // Minimum width a column can be resized to.

        // --- Key Change: Define the column sizing strategy based on `auto_col_width` ---
        let column_sizing_strategy = if self.format.auto_col_width {
            // Use automatic column sizing based on content. Can be slower for large tables.
            // tracing::debug!("build_table: Using Column::auto() because format.auto_col_width is true");
            Column::auto()
        } else {
            // Use the calculated initial width. Usually faster rendering.
            // tracing::debug!("build_table: Using Column::initial({}) because format.auto_col_width is false", initial_col_width);
            Column::initial(initial_col_width)
        }
        .at_least(min_col_width) // Apply minimum width constraint.
        .resizable(true) // Allow user to resize columns.
        .clip(true); // Clip content that overflows the cell width.

        // --- Key Change: Create a unique ID for the TableBuilder based on the sizing strategy ---
        // This ID changes when `self.format.auto_col_width` changes. This forces egui to
        // discard cached layout state (like manually resized widths or previous auto-sizing calculations)
        // when the column definition type (`Column::initial` vs `Column::auto`) changes,
        // ensuring the visual change takes effect immediately.
        let table_id = Id::new("data_table_view").with(self.format.auto_col_width);
        // tracing::debug!("build_table: Using Table ID: {:#?}", table_id);

        // Configure and build the table using egui_extras::TableBuilder.
        TableBuilder::new(ui)
            .id_salt(table_id) // <--- Assign the unique ID calculated above.
            .striped(true) // Alternate row background colors.
            // Apply the chosen column sizing strategy to all data columns.
            .columns(column_sizing_strategy, self.df.width())
            // Add a remainder column to fill any remaining space (prevents last column stretching excessively).
            .column(Column::remainder())
            .auto_shrink([false, false]) // Prevent table from shrinking horizontally or vertically.
            .header(header_height, analyze_header) // Set header height and provide the rendering closure.
            .body(|body| {
                // Render the table body.
                let num_rows = self.df.height();
                // Efficiently add all rows using the `analyze_rows` closure for rendering each one.
                body.rows(text_height, num_rows, analyze_rows);
            });
    }
}
