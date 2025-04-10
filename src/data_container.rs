use egui::{Id, TextStyle, Ui};
use egui_extras::{Column, TableBuilder, TableRow};
use polars::prelude::Column as PColumn;
use polars::{prelude::*, sql::SQLContext};
use std::sync::Arc;

use crate::{
    DataFilter, DataFormat, FileExtension, HeaderSortState, PolarsViewError, PolarsViewResult,
    SortBy, SortableHeaderRenderer, add_row_index_column, get_decimal_and_layout,
    normalize_float_strings_by_regex, remove_null_columns, replace_values_with_null,
};

/// Internal struct holding calculated configuration for `TableBuilder`.
/// Generated by `prepare_table_build_config`.
struct TableBuildConfig {
    text_height: f32,
    num_columns: usize,
    header_height: f32,
    column_sizing_strategy: Column, // Use 'static as Column doesn't take a lifetime here
    table_id: Id,
}

/// Container for the Polars DataFrame and its associated display and filter state.
///
/// ## State Management:
/// - Holds the core data (`df`, `unsorted_df`) and related settings.
/// - **`df`**: The currently displayed DataFrame, potentially sorted based on `self.sort`.
/// - **`unsorted_df`**: The DataFrame state immediately after loading/querying, before UI sorts.
/// - **`filter`**: Configuration used for *loading* the data (path, delimiter, SQL query, etc.). **Does NOT contain sorting information.**
/// - **`format`**: Configuration for *displaying* the data (alignment, decimals, etc.).
/// - **`sort`**: `Vec<SortBy>` defining the active sort order applied to `df`. An empty Vec means `df` reflects `unsorted_df`.
/// - All key components are wrapped in `Arc` for efficient cloning and sharing between UI and async tasks.
/// - Updates (load, format, sort) typically create *new* `DataContainer` instances via async methods.
///
/// ## Interaction with `layout.rs`:
/// - `PolarsViewApp` holds the current state as `Option<Arc<DataContainer>>`.
/// - UI actions trigger async methods here (`load_data`, `update_format`, `apply_sort`).
/// - Async methods return `PolarsViewResult<DataContainer>` via a channel.
/// - `layout.rs` updates the app state with the received new container.
#[derive(Debug, Clone)]
pub struct DataContainer {
    /// The currently displayed Polars DataFrame. May be sorted according to `self.sort`.
    pub df: Arc<DataFrame>,

    /// A reference to the DataFrame state *before* any UI-driven sort was applied.
    /// Allows resetting the view efficiently.
    pub unsorted_df: Arc<DataFrame>,

    /// Detected file extension of the originally loaded data.
    pub extension: Arc<FileExtension>,

    /// Filters and loading configurations (path, query, delimiter) that resulted
    /// in the initial `unsorted_df`. **This does NOT store the current sort state.**
    pub filter: Arc<DataFilter>,

    /// Applied data formatting settings (decimal places, alignment, column sizing, header style).
    pub format: Arc<DataFormat>,

    /// **The active sort criteria (column name and direction) applied to `df`.**
    /// An empty vector signifies that `df` should be the same as `unsorted_df`.
    /// Order in the vector determines sort precedence.
    pub sort: Vec<SortBy>,
}

// Default implementation initializes with an empty sort vector.
impl Default for DataContainer {
    /// Creates an empty `DataContainer` with default settings.
    fn default() -> Self {
        let default_df = Arc::new(DataFrame::default());
        DataContainer {
            df: default_df.clone(),
            unsorted_df: default_df,
            extension: Arc::new(FileExtension::Missing),
            filter: Arc::new(DataFilter::default()), // Filters has no sort field
            format: Arc::new(DataFormat::default()),
            sort: Vec::new(), // Initialize sort as empty Vec
        }
    }
}

impl DataContainer {
    /// Asynchronously loads data based on `DataFilter`, applies transformations,
    /// and returns a **new**, unsorted `DataContainer`.
    ///
    /// Triggered by file open, drag-drop, or filter/SQL changes in `layout.rs`.
    ///
    /// ## Execution Steps:
    /// 1. Validate Path existence.
    /// 2. Read data file (CSV, JSON, etc.) based on `filter`. Delimiter might be updated.
    /// 3. Apply string normalization using `filter.normalize` if provided.
    /// 4. Replace specified string values (`filter.null_values`) with `Null` in String columns.
    /// 5. Execute SQL query (`filter.query`) if `filter.apply_sql` is true. Reset `apply_sql`.
    /// 6. Remove all-null columns if `filter.exclude_null_cols` is true.
    /// 7. Add a row index column if `filter.add_row_index` is true. Store schema without index otherwise.
    /// 8. Update `filter.schema` with the final `DataFrame` schema.
    /// 9. Create and return `Ok(new DataContainer)`:
    ///    - `df` and `unsorted_df` point to the *same* new `DataFrame`.
    ///    - `filter` Arc stores the applied loading filters.
    ///    - `format` Arc stores the provided format settings.
    ///    - `sort` is initialized to an empty `Vec`.
    pub async fn load_data(mut filter: DataFilter, format: DataFormat) -> PolarsViewResult<Self> {
        // --- 1. Path Validation ---
        if !filter.absolute_path.exists() {
            tracing::error!("load_data: File not found: {:?}", filter.absolute_path);
            return Err(PolarsViewError::FileNotFound(filter.absolute_path.clone()));
        }

        // --- 2. Data Reading ---
        // Gets DataFrame and extension, might auto-detect/update filter.csv_delimiter
        let (mut df, extension) = filter.get_df_and_extension().await?;
        tracing::debug!(
            "load_data: Initial read done. Dims: {}x{}, Ext: {:?}, Delimiter: '{}'",
            df.height(),
            df.width(),
            extension,
            filter.csv_delimiter
        );

        // --- 3. String Column Normalization (Regex) ---
        // Checks the 'normalize' flag which reflects the initial CLI setting or UI changes.
        if filter.normalize {
            tracing::debug!(
                "Normalizing Euro-style numbers in string columns matching regex: '{}'",
                filter.regex
            );
            df = normalize_float_strings_by_regex(df, &filter.regex)?; // Assuming this helper exists
        }

        // --- 4. Replace Values with Null ---
        // Parses the `filter.null_values` string and applies replacement to String columns
        let null_value_list: Vec<&str> = filter.parse_null_values();
        tracing::debug!("Parsed null values list: {:?}", null_value_list);
        df = replace_values_with_null(df, &null_value_list, false)?; // Target only String columns
        tracing::debug!("Applied replace_values_with_null to String columns.");

        // --- 5. SQL Execution ---
        // Checks the 'apply_sql' flag which reflects the initial CLI setting or UI changes.
        if filter.apply_sql {
            tracing::debug!("load_data: Applying SQL: '{}'", filter.query);
            let mut ctx = SQLContext::new();
            ctx.register(&filter.table_name, df.lazy()); // Register current df
            df = ctx.execute(&filter.query)?.collect()?; // Execute and collect result
            filter.apply_sql = false; // Reset flag after execution
            tracing::debug!(
                "load_data: SQL applied. New Dims: {}x{}",
                df.height(),
                df.width()
            );
        }

        // --- 6. Null Column Removal ---
        // Removes columns containing only null values if flag is set
        if filter.exclude_null_cols {
            let initial_width = df.width();
            df = remove_null_columns(&df)?; // Call helper from polars.rs
            tracing::debug!(
                "load_data: Null columns removed. Width {} -> {}",
                initial_width,
                df.width()
            );
        }

        // --- 7. Add Row Index Column ---
        // Adds a row number column if flag is set, using filter's name/offset settings
        if filter.add_row_index {
            df = add_row_index_column(df, &filter)?; // Assuming this helper exists
        } else {
            // Store the schema *before* index column is conceptually added
            filter.schema_without_index = df.schema().clone();
        }

        // --- 8. Update Schema in Filter ---
        // Stores the final schema (potentially with index col) in the filter struct
        filter.schema = df.schema().clone();

        // --- 9. Create Arcs & Container ---
        // Wraps results in Arcs for efficient sharing
        let final_df_arc = Arc::new(df);

        tracing::debug!("Load data successfully!\n{filter:#?}");

        Ok(Self {
            df: final_df_arc.clone(),       // Displayed DataFrame
            unsorted_df: final_df_arc,      // Reference to state before UI sort
            extension: Arc::new(extension), // File extension
            filter: Arc::new(filter),       // Filter state used for this load
            format: Arc::new(format),       // Provided display format
            sort: Vec::new(),               // Start with no sorting applied
        })
    }

    /// Asynchronously creates a *new* `DataContainer` with updated format settings.
    /// Preserves the existing data (`df`, `unsorted_df`) and sort criteria (`sort`).
    ///
    /// Triggered by `layout.rs` when format UI elements change. This is a very fast operation.
    ///
    /// ### State Update:
    /// - Creates a new container.
    /// - Reuses existing Arcs for `df`, `unsorted_df`, `extension`, `filter`, `sort` via cloning.
    /// - Incorporates the *new* `format` Arc.
    pub async fn update_format(
        data_container: Arc<DataContainer>, // Arc to current state
        format: Arc<DataFormat>,            // Arc containing the NEW format settings
    ) -> PolarsViewResult<Self> {
        tracing::debug!(
            "update_format: Updating format to {:#?}, preserving sort state: {:#?}",
            format,
            data_container.sort
        );
        Ok(Self {
            df: data_container.df.clone(),
            unsorted_df: data_container.unsorted_df.clone(),
            extension: data_container.extension.clone(),
            filter: data_container.filter.clone(),
            format,                            // Use the new format Arc
            sort: data_container.sort.clone(), // Keep the current sort criteria
        })
    }

    /// Asynchronously creates a *new* `DataContainer` with the `df` sorted according
    /// to the provided `new_sort_criteria`.
    ///
    /// Triggered by `layout.rs` after a user clicks a sortable header, resulting in new criteria.
    /// Handles multi-column sorting based on the order, direction, and nulls_last settings
    /// in `new_sort_criteria`.
    /// If `new_sort_criteria` is empty, it resets the view by setting `df` to `unsorted_df`.
    ///
    /// ## Logic & State Update:
    /// 1. Check if `new_sort_criteria` is empty.
    /// 2. **Handle Empty (Reset):** If empty, create new container:
    ///    *   `df`: Cloned `Arc` of the input `unsorted_df`.
    ///    *   `unsorted_df`: Cloned `Arc` of the input `unsorted_df`.
    ///    *   `sort`: The empty `new_sort_criteria` vector.
    ///    *   Other fields cloned from input container.
    /// 3. **Handle Non-Empty (Apply Sort):** If not empty:
    ///    a. Extract column names, descending flags, and **nulls_last flags** from `new_sort_criteria`.
    ///    b. Configure Polars `SortMultipleOptions`.
    ///    c. Call `data_container.df.sort()` using the *currently displayed* df as input.
    ///    d. Create new container:
    ///    *   `df`: *New* `Arc` wrapping the **sorted** DataFrame.
    ///    *   `unsorted_df`: *Cloned* `Arc` of the **input** `unsorted_df`.
    ///    *   `sort`: The `new_sort_criteria` that *caused* this sort.
    ///    *   Other fields cloned from input container.
    /// 4. Return `Ok(new_container)`.
    pub async fn apply_sort(
        data_container: Arc<Self>,      // Current container state
        new_sort_criteria: Vec<SortBy>, // The *desired* new sort state
    ) -> PolarsViewResult<Self> {
        if new_sort_criteria.is_empty() {
            // --- 2. Handle Empty (Reset) ---
            tracing::debug!(
                "apply_sort: Sort criteria list is empty. Resetting df to unsorted_df."
            );
            return Ok(DataContainer {
                df: data_container.unsorted_df.clone(), // Reset displayed df
                unsorted_df: data_container.unsorted_df.clone(), // Keep original unsorted ref
                extension: data_container.extension.clone(),
                format: data_container.format.clone(),
                filter: data_container.filter.clone(),
                sort: new_sort_criteria, // Store the empty Vec as the current state
            });
        }

        // --- 3. Handle Non-Empty (Apply Sort) ---
        tracing::debug!(
            "apply_sort: Applying cumulative sort. Criteria: {:#?}",
            new_sort_criteria
        );

        // 3a. Extract sort parameters
        let column_names: Vec<PlSmallStr> = new_sort_criteria
            .iter()
            .map(|sort| sort.column_name.clone().into()) // PlSmallStr is efficient here
            .collect();

        let descending_flags: Vec<bool> = new_sort_criteria
            .iter()
            .map(|sort| !sort.ascending)
            .collect();

        let nulls_last_flags: Vec<bool> = new_sort_criteria
            .iter()
            .map(|sort| sort.nulls_last)
            .collect();

        // 3b. Configure Polars Sort Options
        // Set descending flags and **nulls_last flags** for multi-column sort.
        let sort_options = SortMultipleOptions::default()
            .with_order_descending_multi(descending_flags)
            .with_nulls_last_multi(nulls_last_flags) // Use the extracted flags
            .with_maintain_order(true) // Maintain relative order of equal elements
            .with_multithreaded(true);

        // 3c. Perform Sorting on the *current* df
        // NOTE: Sorting based on the *new cumulative* criteria.
        let df_sorted = data_container.df.sort(column_names, sort_options)?;
        tracing::debug!("apply_sort: Polars multi-column sort successful.");

        // 3d. Create New Container with sorted data and new criteria
        Ok(DataContainer {
            df: Arc::new(df_sorted), // Use the newly sorted DataFrame
            unsorted_df: data_container.unsorted_df.clone(), // Keep original unsorted ref
            extension: data_container.extension.clone(),
            format: data_container.format.clone(),
            filter: data_container.filter.clone(),
            sort: new_sort_criteria, // Store the criteria that produced this state
        })
    }

    // --- UI Rendering Methods ---

    /// Renders the main data table using `egui_extras::TableBuilder`.
    /// Handles sort interactions via `render_table_header`.
    ///
    /// Returns `Some(new_sort_criteria)` if a header click requires a sort state update.
    pub fn render_table(&self, ui: &mut Ui) -> Option<Vec<SortBy>> {
        // Variable to capture the new sort criteria if a header is clicked.
        let mut updated_sort_criteria: Option<Vec<SortBy>> = None;

        // Closure to render the header row. Captures `self` and the output Option.
        let analyze_header = |mut table_row: TableRow<'_, '_>| {
            self.render_table_header(
                &mut table_row,
                &mut updated_sort_criteria, // Pass mutable ref to capture signal
            );
        };

        // Closure to render data rows.
        let analyze_rows = |mut table_row: TableRow<'_, '_>| {
            self.render_table_row(&mut table_row);
        };

        // Configure and build the table.
        self.build_configured_table(ui, analyze_header, analyze_rows);

        // Return the signal from header interactions.
        updated_sort_criteria
    }

    /// Renders the header row, creating clickable cells for sorting.
    /// Reads the current sort state (`self.sort`), including nulls_last. On click,
    /// calculates the *next* sort state (cycling through 4 sorted states + NotSorted),
    /// modifies a *cloned* sort criteria `Vec`, and signals this *new `Vec`* back
    /// via the `sort_signal` output parameter.
    ///
    /// ### Arguments
    /// * `table_row`: Egui context for the header row.
    /// * `sort_signal`: Output parameter (`&mut Option<Vec<SortBy>>`). Set to `Some(new_criteria)`
    ///   if a click occurred that requires updating the sort state.
    fn render_table_header(
        &self,
        table_row: &mut TableRow<'_, '_>,
        sort_signal: &mut Option<Vec<SortBy>>,
    ) {
        for column_name in self.df.get_column_names() {
            table_row.col(|ui| {
                // 1. Determine current interaction state based on `ascending` and `nulls_last`.
                let (current_interaction_state, sort_index) = self
                    .sort
                    .iter()
                    .position(|criterion| criterion.column_name == *column_name)
                    .map_or((HeaderSortState::NotSorted, None), |index| {
                        let criterion = &self.sort[index];
                        // ** Map to the correct 4-state enum based on both bools **
                        let state = match (criterion.ascending, criterion.nulls_last) {
                            (false, false) => HeaderSortState::DescendingNullsFirst,
                            (true, false) => HeaderSortState::AscendingNullsFirst,
                            (false, true) => HeaderSortState::DescendingNullsLast,
                            (true, true) => HeaderSortState::AscendingNullsLast,
                        };
                        (state, Some(index))
                    });

                // 2. Render the sortable header widget (uses the new state and get_icon).
                let response = ui.render_sortable_header(
                    column_name,
                    &current_interaction_state,
                    sort_index, // Pass index for display (e.g., "1▼")
                    self.format.use_enhanced_header,
                );

                // 3. Handle Click Response.
                if response.clicked() {
                    tracing::debug!(
                        "Header clicked: '{}'. Current state: {:?}, Index: {:?}",
                        column_name,
                        current_interaction_state,
                        sort_index
                    );
                    // Calculate the next state in the 5-state cycle
                    let next_interaction_state = current_interaction_state.cycle_next();
                    tracing::debug!("Next interaction state: {:#?}", next_interaction_state);

                    // 4. Prepare the *new* list of sort criteria based on the click outcome.
                    let mut new_sort_criteria = self.sort.clone(); // Start with current criteria
                    let column_name_string = column_name.to_string();
                    let current_pos = new_sort_criteria
                        .iter()
                        .position(|c| c.column_name == *column_name);

                    // 5. Modify the cloned vector based on the next interaction state.
                    match next_interaction_state {
                        HeaderSortState::NotSorted => {
                            // Remove the sort criterion for this column if it exists.
                            if let Some(pos) = current_pos {
                                new_sort_criteria.remove(pos);
                            }
                        }
                        // Handle the 4 sorted states: update existing or add new.
                        _ => {
                            // ** Determine new ascending and nulls_last from the next state **
                            let (new_ascending, new_nulls_last) = match next_interaction_state {
                                HeaderSortState::DescendingNullsFirst => (false, false),
                                HeaderSortState::AscendingNullsFirst => (true, false),
                                HeaderSortState::DescendingNullsLast => (false, true),
                                HeaderSortState::AscendingNullsLast => (true, true),
                                // NotSorted case is handled above, this is exhaustive for sorted states
                                HeaderSortState::NotSorted => {
                                    unreachable!("NotSorted case already handled")
                                }
                            };

                            if let Some(pos) = current_pos {
                                // Update existing criterion in place.
                                new_sort_criteria[pos].ascending = new_ascending;
                                new_sort_criteria[pos].nulls_last = new_nulls_last;
                            } else {
                                // Add new criterion to the end of the vector.
                                new_sort_criteria.push(SortBy {
                                    column_name: column_name_string,
                                    ascending: new_ascending,
                                    nulls_last: new_nulls_last,
                                });
                            }
                        }
                    } // end match next_interaction_state

                    tracing::debug!(
                        "Signaling new sort criteria for async update: {:#?}",
                        new_sort_criteria
                    );

                    // 6. Set the output parameter to signal the required action and the new sort state.
                    *sort_signal = Some(new_sort_criteria);
                } // end if response.clicked()
            }); // End cell definition
        } // End loop over columns
    }

    /// Renders a single data row in the table body.
    ///
    /// Called by the `analyze_rows` closure (defined in `render_table`) for each row index
    /// provided by the `egui_extras::TableBuilder`.
    ///
    /// For each cell in the row:
    /// 1. Calls `get_decimal_and_layout` (using `self.format`) to determine the `egui::Layout` (for alignment)
    ///    and `Option<usize>` (for decimal places, if applicable) based on the column's `DataType`.
    /// 2. Calls `Self::format_cell_value` to retrieve the `AnyValue` from the DataFrame and format it
    ///    into a `String`, applying decimal rounding if needed.
    /// 3. Adds a cell to the `egui` row (`table_row.col`) and renders the formatted string as a `Label`
    ///    within the determined `Layout`.
    ///
    /// ### Arguments
    /// * `table_row`: The `egui_extras::TableRow` context providing the `row_index` and cell adding methods.
    fn render_table_row(&self, table_row: &mut TableRow<'_, '_>) {
        let row_index = table_row.index(); // Get the 0-based data row index.

        // Iterate through each column (Polars Series) in the DataFrame.
        for column_series in self.df.get_columns() {
            // Determine alignment and decimal places using the feature-flagged helper.
            // Passes the Series and the current format settings Arc.
            let (opt_decimal, layout) = get_decimal_and_layout(column_series, &self.format);

            // Get the raw AnyValue and format it into a display String.
            let value_str = self.format_cell_value(column_series, row_index, opt_decimal);

            // Add a cell to the egui row.
            table_row.col(|ui| {
                // Apply the determined layout (alignment) to the cell content. Prevent wrapping.
                ui.with_layout(layout.with_main_wrap(false), |ui| {
                    ui.label(value_str); // Display the formatted value.
                });
            });
        }
    }

    /// Retrieves and formats a single cell's `AnyValue` into a displayable `String`.
    /// Called repeatedly by `render_table_row`.
    ///
    /// Logic:
    /// 1. Get `AnyValue` from `column` at `row_index` using `column.get()`.
    /// 2. Handle `Result`: Return error string on `Err`.
    /// 3. On `Ok(any_value)`:
    ///    - Match on `(any_value, opt_decimal)`:
    ///      - Floats with `Some(decimal)`: Format using `format!("{:.*}", decimal, f)`.
    ///      - `AnyValue::Null`: Return `""`.
    ///      - `AnyValue::String(s)`: Return `s.to_string()`.
    ///      - Other types (Ints, Bool, Date, etc.) or Floats with `None` decimal: Use `any_value.to_string()`.
    ///
    /// ### Arguments
    /// * `column`: Reference to the Polars `Series` (`PColumn`).
    /// * `row_index`: Row index within the series.
    /// * `opt_decimal`: `Option<usize>` specifying decimal places for floats (from `get_decimal_and_layout`).
    ///
    /// ### Returns
    /// `String`: The formatted cell value.
    fn format_cell_value(
        &self,
        column: &PColumn,
        row_index: usize,
        opt_decimal: Option<usize>, // Info comes from get_decimal_and_layout which uses self.format
    ) -> String {
        match column.get(row_index) {
            Ok(any_value) => {
                // Format based on the AnyValue variant and decimal setting.
                match (any_value, opt_decimal) {
                    // Float with specific decimal request: Apply precision formatting.
                    (AnyValue::Float32(f), Some(decimal)) => format!("{:.*}", decimal, f),
                    (AnyValue::Float64(f), Some(decimal)) => format!("{:.*}", decimal, f),

                    // Null value: Display as empty string.
                    (AnyValue::Null, _) => String::new(),

                    // String value: Convert inner &str to String.
                    (AnyValue::String(s), _) => s.to_string(), // Handle StringOwned too if necessary.

                    // Other AnyValue types OR Float without specific decimal: Use default Polars to_string().
                    (other_av, _) => other_av.to_string(),
                }
            }
            Err(e) => {
                // Handle error retrieving value (e.g., index out of bounds, though unlikely with TableBuilder).
                tracing::warn!(
                    "format_cell_value: Failed get value col '{}' row {}: {}",
                    column.name(),
                    row_index,
                    e
                );
                "⚠ Err".to_string() // Return placeholder error string for display.
            }
        }
    }

    /// Prepares configuration values needed for `TableBuilder`.
    /// Encapsulates calculations for sizes, strategies, and IDs based on current format and UI state.
    ///
    /// Called by `build_configured_table`.
    fn prepare_table_build_config(&self, ui: &Ui) -> TableBuildConfig {
        // --- Calculate Style and Dimensions ---
        let style = ui.style();
        let text_height = TextStyle::Body.resolve(style).size; // Standard row height
        let num_columns = self.df.width().max(1); // Ensure at least 1 column logically
        let suggested_width = 150.0; // A sensible starting point for auto/initial width

        // --- Calculate Column Widths ---
        // Base available width excluding spacings and potential scrollbar
        let available_width = ui.available_width()
            - ((num_columns + 1) as f32 * style.spacing.item_spacing.x) // Account for inter-column spacing
            - style.spacing.scroll.bar_width; // Assume scrollbar might be present

        // Initial width used in non-auto mode, ensure it's not too small
        let initial_col_width = (available_width / num_columns as f32).max(suggested_width);

        // Minimum width any column can be resized to
        let min_col_width = style.spacing.interact_size.x.max(20.0);

        // --- Calculate Header Height ---
        // Determine padding based on header style setting
        let padding = if self.format.use_enhanced_header {
            self.format.header_padding
        } else {
            self.format.get_default_padding()
        };

        // Calculate height: base interact size + internal spacing + custom padding
        let header_height = style.spacing.interact_size.y // Base height for clickable elements
                           + 2.0 * style.spacing.item_spacing.y // Top/bottom internal spacing
                           + padding; // Add configured extra padding

        // --- Determine Column Sizing Strategy ---
        let column_sizing_strategy = if self.format.auto_col_width {
            // Automatic: sizes based on content, potentially slower
            tracing::trace!(
                "prepare_table_build_config: Using Column::auto_with_initial_suggestion({})",
                suggested_width
            );
            Column::auto_with_initial_suggestion(suggested_width)
        } else {
            // Fixed initial: faster, uses calculated width
            tracing::trace!(
                "prepare_table_build_config: Using Column::initial({})",
                initial_col_width
            );
            Column::initial(initial_col_width)
        }
        // Common constraints applied to either strategy
        .at_least(min_col_width) // Min resize width
        .resizable(true) // Allow user resizing
        .clip(true); // Clip content within cell bounds

        // --- Generate Table ID ---
        // **Key**: ID incorporates `auto_col_width`. Changing this flag results in a *different* ID,
        // forcing egui to discard cached layout state (like manually resized widths)
        // and recompute the layout using the new column sizing strategy.
        let table_id = Id::new("data_table_view").with(self.format.auto_col_width);
        tracing::trace!(
            "prepare_table_build_config: Using table_id: {:?} based on auto_col_width={}",
            table_id,
            self.format.auto_col_width
        );

        // --- Log Calculated Values ---
        tracing::trace!(
            "prepare_table_build_config: text_height={}, num_cols={}, header_height={}, auto_width={}, table_id={:?}",
            text_height,
            num_columns,
            header_height,
            self.format.auto_col_width,
            table_id
        );

        // --- Return the configuration struct ---
        TableBuildConfig {
            text_height,
            num_columns,
            header_height,
            column_sizing_strategy,
            table_id,
        }
    }

    /// Configures and builds the `egui_extras::Table` using `TableBuilder` and pre-calculated configuration.
    ///
    /// ## Configuration Source
    /// Relies on `prepare_table_build_config` to provide layout parameters, sizing strategies,
    /// and the crucial `table_id` for layout persistence control.
    ///
    /// ### Arguments
    /// * `ui`: The `egui::Ui` context for drawing.
    /// * `analyze_header`: Closure for rendering the header row content.
    /// * `analyze_rows`: Closure for rendering data row content.
    fn build_configured_table(
        &self,
        ui: &mut Ui,
        analyze_header: impl FnMut(TableRow<'_, '_>), // Closure to draw the header.
        analyze_rows: impl FnMut(TableRow<'_, '_>),   // Closure to draw data rows.
    ) {
        // 1. Get the calculated configuration values.
        let config = self.prepare_table_build_config(ui);

        // 2. Configure and Build the Table using values from `config`.
        TableBuilder::new(ui)
            // Set the ID controlling layout persistence (crucial for `auto_col_width` toggle).
            .id_salt(config.table_id)
            .striped(true) // Alternate row backgrounds.
            // Define sizing strategy for data columns using config.
            .columns(config.column_sizing_strategy, config.num_columns)
            // Add a final 'remainder' column to fill unused space.
            .column(Column::remainder())
            .resizable(true) // Allow resizing via separators.
            .auto_shrink([false, false]) // Don't shrink horizontally or vertically.
            // Define the header section using calculated height and the provided closure.
            .header(config.header_height, analyze_header)
            // Define the body section.
            .body(|body| {
                let num_rows = self.df.height(); // Get total rows from the DataFrame.
                // Use `body.rows` for efficient virtual scrolling.
                // Provide row height, total rows, and the row drawing closure.
                body.rows(config.text_height, num_rows, analyze_rows);
            }); // End table configuration. Egui draws the table.
    }
}
