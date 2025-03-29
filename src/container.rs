use egui::{Color32, Id, TextStyle, Ui};
use egui_extras::{Column, TableBuilder, TableRow};
use polars::prelude::Column as PColumn; // Rename Polars Column to avoid conflict with egui_extras::Column
use polars::{prelude::*, sql::SQLContext};
use std::sync::Arc; // Used extensively for efficient sharing of state.

use crate::{
    // Internal crate types:
    DataFilters, // Configuration for data loading and querying.
    DataFormat,  // Configuration for data display formatting.
    // External crate types used directly:
    ExtraInteractions,      // Trait extension for egui::Ui (adds sort_button).
    FileExtension,          // Enum representing the file type.
    PolarsViewError,        // Custom error type for the application.
    PolarsViewResult,       // Custom Result alias using PolarsViewError.
    SelectionDepth,         // Trait for cycling through states (used for SortState).
    SortState,              // Enum representing column sort status (Asc/Desc/None).
    SortableHeaderWidget,   // Trait for the custom wrapping header widget.
    get_decimal_and_layout, // Helper function to determine cell formatting.
    remove_null_columns,    // Helper function to remove all-null columns from DataFrame.
};

/// Container for the Polars DataFrame and its associated state.
///
/// This struct holds the primary data (`DataFrame`) loaded into the application,
/// along with metadata (`extension`), configuration (`filters`, `format`), and UI-related
/// state derived from that configuration.
///
/// ## Use of `Arc`
/// All major fields (`df`, `extension`, `filters`, `format`) are wrapped in `Arc` (Atomic Reference Counting).
/// This is crucial for the application's architecture:
/// 1.  **Efficient Sharing:** Allows multiple parts of the application (UI thread, background tasks)
///     to hold references to the same data/configuration without expensive deep copies. Cloning an `Arc`
///     is very cheap (just increments a reference counter).
/// 2.  **Asynchronous Operations:** Makes it easy and safe to pass the container's state (or parts of it)
///     to `tokio` tasks running on separate threads (e.g., for loading, sorting, saving).
/// 3.  **State Updates:** The typical pattern for updating state (e.g., applying sorting or changing format)
///     involves creating a *new* `DataFrameContainer` instance. This new instance often reuses existing
///     `Arc`s for the parts of the state that *didn't* change (e.g., updating format clones the `df` `Arc`
///     but uses a new `format` `Arc`), while creating new `Arc`s for the parts that *did* change (e.g.,
///     sorting creates a new `Arc<DataFrame>`). This immutability-focused update pattern simplifies state management.
///
/// ## Responsibilities
/// - Stores the `DataFrame` and its related configurations (`DataFilters`, `DataFormat`).
/// - Provides methods for:
///     - Asynchronously loading data from files (`load_data`).
///     - Asynchronously applying sorting (`sort`).
///     - Asynchronously applying formatting changes (`update_format`).
/// - Renders the `DataFrame` content into an interactive `egui` table (`render_table`).
///
/// ## Integration with `layout.rs` (`PolarsViewApp`)
/// - An `Option<Arc<DataFrameContainer>>` is held in `PolarsViewApp`.
/// - The `layout.rs` module orchestrates calls to `load_data`, `sort`, `update_format`
///   by spawning `tokio` tasks that run these async methods.
/// - Results from these tasks (a `PolarsViewResult<DataFrameContainer>`) are sent back
///   to `layout.rs` via channels, which then updates the `PolarsViewApp` state with the
///   new `Arc<DataFrameContainer>`.
/// - `layout.rs` calls `render_table` during the UI update phase to display the data.
#[derive(Debug, Clone)] // Clone is cheap due to Arc. Debug is useful for logging.
pub struct DataFrameContainer {
    /// The Polars DataFrame, containing the actual tabular data.
    /// Wrapped in an `Arc` for cheap cloning and shared ownership, crucial for performance
    /// when passing data between UI and async tasks or updating state without full copies.
    pub df: Arc<DataFrame>,

    /// The detected file extension (e.g., Csv, Parquet) of the originally loaded data.
    /// Stored in `Arc` for consistency, although its size is negligible. It's part of the container's identity.
    pub extension: Arc<FileExtension>,

    /// Applied data filters and loading configurations (path, query, delimiter, sorting state, etc.).
    /// Defined in `filters.rs`. Wrapped in `Arc` because filter changes trigger a full data reload/requery,
    /// creating a new container. Reusing the Arc is efficient if only formatting changes.
    pub filters: Arc<DataFilters>,

    /// Applied data formatting settings (decimal places, alignment, column expansion behavior).
    /// Defined in `format.rs`. Wrapped in `Arc`. Format changes are handled differently
    /// (via `update_format`) and typically only require replacing this Arc while cloning others.
    pub format: Arc<DataFormat>,
}

impl Default for DataFrameContainer {
    /// Creates an empty `DataFrameContainer` with default settings.
    /// Useful for the initial state of the application before any data is loaded.
    fn default() -> Self {
        DataFrameContainer {
            df: Arc::new(DataFrame::default()),          // An empty DataFrame.
            extension: Arc::new(FileExtension::Missing), // Default to no specific extension.
            filters: Arc::new(DataFilters::default()),   // Default filter settings (filters.rs).
            format: Arc::new(DataFormat::default()),     // Default format settings (format.rs).
        }
    }
}

impl DataFrameContainer {
    /// Asynchronously loads data from a file based on provided `DataFilters`.
    /// Optionally applies an SQL query and removes columns containing only nulls.
    ///
    /// This is a core asynchronous operation triggered by `layout.rs` (e.g., on file open,
    /// drag-drop, or filter changes in the UI). It runs within a `tokio` task.
    ///
    /// ## Steps:
    /// 1.  **Path Validation:** Checks if the file specified in `filters.absolute_path` exists.
    /// 2.  **Data Reading:** Delegates to `filters.get_df_and_extension()` which determines the
    ///     file type and uses the appropriate Polars reader (CSV, Parquet, etc.), potentially
    ///     updating the `csv_delimiter` within `filters` if auto-detection was successful.
    /// 3.  **SQL Execution:** If `filters.apply_sql` is true, creates a `SQLContext`, registers
    ///     the loaded DataFrame, executes `filters.query`, and replaces `df` with the result.
    ///     Resets `filters.apply_sql` to `false` afterward.
    /// 4.  **Null Column Removal:** If `filters.remove_null_cols` is true, calls `remove_null_columns`.
    /// 5.  **Schema Update:** Updates `filters.schema` to reflect the final schema of the processed `df`.
    /// 6.  **Container Creation:** Constructs and returns a *new* `DataFrameContainer` instance.
    ///     All resulting components (`df`, `extension`, updated `filters`, `format`) are wrapped
    ///     in `Arc`s before being placed into the new container struct.
    ///
    /// ## Ownership and Return Value
    /// - Takes `filters` by value (`mut filters: DataFilters`). This allows the function to modify
    ///   filters internally (e.g., `csv_delimiter`, `apply_sql`, `schema`) without needing a `&mut` borrow,
    ///   which simplifies passing filters modified in the UI to this async function.
    /// - Takes `format` by value (`format: DataFormat`).
    /// - Returns `PolarsViewResult<Self>`, containing the *newly created* `DataFrameContainer`
    ///   (with its state wrapped in `Arc`s) on success, or a `PolarsViewError` on failure.
    ///
    /// # Arguments
    ///
    /// * `filters`: A `DataFilters` struct containing file path, query, delimiter, and other loading settings.
    ///              **Moved into** the function. The resulting container will hold an `Arc` pointing to this potentially modified `filters` struct.
    /// * `format`: The initial `DataFormat` settings to associate with the loaded data. **Moved into** the function.
    ///             The resulting container will hold an `Arc` pointing to this format.
    ///
    /// # Returns
    ///
    /// A `PolarsViewResult` wrapping:
    /// * `Ok(DataFrameContainer)`: A *new* container instance holding the loaded and processed data.
    /// * `Err(PolarsViewError)`: An error encountered during loading or processing.
    pub async fn load_data(mut filters: DataFilters, format: DataFormat) -> PolarsViewResult<Self> {
        // --- 1. Path Validation ---
        if !filters.absolute_path.exists() {
            tracing::error!(
                "fn load_data(): File not found at path: {:?}",
                filters.absolute_path
            );
            return Err(PolarsViewError::FileNotFound(filters.absolute_path.clone()));
        }
        tracing::debug!(
            "fn load_data(): Loading data from: {:?}. Initial filters: {:#?}, format: {:#?}",
            filters.absolute_path,
            filters,
            format
        );

        // --- 2. Data Reading ---
        // Calls the filter method responsible for determining file type and reading data.
        // This method might mutate `filters.csv_delimiter`.
        let (mut df, extension) = filters.get_df_and_extension().await?;
        tracing::debug!(
            "fn load_data(): Data loaded successfully. Dims: {}x{}, Detected extension: {:?}. Delimiter might be updated: '{}'",
            df.height(),
            df.width(),
            extension,
            filters.csv_delimiter
        );

        // --- 3. SQL Execution ---
        if filters.apply_sql {
            tracing::debug!("fn load_data(): Applying SQL query: '{}'", filters.query);
            // Create a new SQL context for this operation.
            let mut ctx = SQLContext::new();
            // Register the loaded DataFrame as a table named according to `filters.table_name`.
            ctx.register(&filters.table_name, df.lazy());
            // Execute the SQL query, collect results into a new DataFrame, overwriting `df`.
            df = ctx.execute(&filters.query)?.collect()?;
            // Reset the apply_sql flag to prevent re-application on subsequent sorts or format changes.
            filters.apply_sql = false;
            tracing::debug!(
                "fn load_data(): SQL applied successfully. New Dims: {}x{}",
                df.height(),
                df.width()
            );
        }

        // --- 4. Null Column Removal ---
        if filters.remove_null_cols {
            let initial_width = df.width();
            tracing::debug!(
                "fn load_data(): Removing null columns. Initial width: {}",
                initial_width
            );
            df = remove_null_columns(&df)?; // Replace df with the version without all-null columns.
            tracing::debug!(
                "fn load_data(): Null columns removed. Final width: {}",
                df.width()
            );
        }

        // --- 5. Schema Update ---
        // Ensure the schema stored in the filters matches the final DataFrame state.
        filters.schema = df.schema().clone(); // Schema clone is relatively cheap. Use into() for Arc.
        tracing::debug!("fn load_data(): Schema updated in filters.");

        // --- 6. Container Creation ---
        // Create the final DataFrameContainer, wrapping all components in Arcs.
        let new_container = Self {
            df: Arc::new(df),               // Wrap final DataFrame in Arc.
            extension: Arc::new(extension), // Wrap detected extension in Arc.
            filters: Arc::new(filters),     // Wrap the (potentially updated) filters in Arc.
            format: Arc::new(format),       // Wrap the provided format settings in Arc.
        };
        tracing::debug!("fn load_data() successful. Returning new container.");

        Ok(new_container)
    }

    /// Asynchronously creates a *new* `DataFrameContainer` with updated format settings.
    ///
    /// This function is called by `layout.rs` when format settings (like `decimal` or `auto_col_width`)
    /// are changed in the side panel UI. It runs inside a `tokio` task spawned by `layout.rs::run_data_future`.
    ///
    /// It takes the *current* container's state (via `Arc<DataFrameContainer>`) and the *new*
    /// format settings (`Arc<DataFormat>`) provided by the UI update logic in `layout.rs`.
    ///
    /// It returns a *new* container instance which reuses the existing `Arc`s for `df`,
    /// `extension`, and `filters`, but incorporates the newly provided `format` `Arc`.
    ///
    /// ## Performance
    /// This operation is inherently lightweight because it primarily involves cloning `Arc` pointers,
    /// *not* the underlying data or configurations. No data processing occurs here.
    ///
    /// ## Asynchronicity
    /// Although the core logic (cloning Arcs and struct creation) is synchronous, this function
    /// is marked `async` to maintain consistency with the `load_data` and `sort` operations
    /// within the `layout.rs` update pattern, which treats all container updates as asynchronous
    /// futures handled by `run_data_future` and `check_data_pending`.
    ///
    /// ## State Update Flow
    /// 1. UI detects format change (`format.rs::render_format`).
    /// 2. `layout.rs` receives the new `DataFormat`, clones the current `container` Arc.
    /// 3. `layout.rs` calls `DataFrameContainer::update_format`, passing the container Arc and the new format Arc.
    /// 4. `layout.rs` spawns this `update_format` future using `run_data_future`.
    /// 5. This function executes, quickly creating the new container struct with updated Arcs.
    /// 6. The result (`Ok(new_container)`) is sent back via channel.
    /// 7. `layout.rs::check_data_pending` receives the new container and updates `PolarsViewApp.data_container`.
    ///
    /// # Arguments
    /// * `data_container`: An `Arc` pointing to the *current* `DataFrameContainer`. Used to access existing Arcs.
    /// * `format`: An `Arc` containing the `DataFormat` struct with the *new* settings from the UI. This Arc is moved into the new container.
    ///
    /// # Returns
    /// A `PolarsViewResult` wrapping:
    /// * `Ok(DataFrameContainer)`: A *new* container instance reflecting the format change.
    /// * `Err(PolarsViewError)`: Should not typically occur in this method, but maintains API consistency.
    pub async fn update_format(
        data_container: Arc<DataFrameContainer>, // Reference to the *current* container's state.
        format: Arc<DataFormat>,                 // The *new* format settings to apply.
    ) -> PolarsViewResult<Self> {
        tracing::debug!(
            "fn update_format(): Creating new container with updated format: {format:#?}"
        );

        // Create and return a *new* container instance.
        Ok(Self {
            // Clone existing Arcs for unchanged components (cheap pointer copies).
            df: data_container.df.clone(),
            extension: data_container.extension.clone(),
            filters: data_container.filters.clone(),
            // Use the newly received `format` Arc directly.
            format, // Moves the passed `format` Arc into the new struct.
        })
    }

    /// Asynchronously creates a *new* `DataFrameContainer` with the DataFrame sorted according to the provided `DataFilters`.
    ///
    /// This function is called by `layout.rs` when the user clicks a column header (`render_table_header` signals the sort),
    /// running within a `tokio` task. It receives the *current* container state (`Arc<DataFrameContainer>`)
    /// and the *new* `DataFilters` which now include the desired `SortState`.
    ///
    /// ## Logic
    /// 1.  Checks if `filters.sort` contains a `SortState` other than `NotSorted`.
    /// 2.  If no sort is needed, it efficiently returns a clone of the original container (`Ok(data_container.as_ref().clone())`).
    /// 3.  If sorting is required, it extracts the column name and direction from the `SortState`.
    /// 4.  Configures Polars `SortMultipleOptions` (stable sort, multithreaded, nulls position).
    /// 5.  Calls `data_container.df.sort()` to perform the potentially expensive sorting operation.
    /// 6.  Creates and returns a *new* `DataFrameContainer` instance containing:
    ///     *   A *new* `Arc` wrapping the *sorted* DataFrame.
    ///     *   Cloned `Arc`s for the original `extension` and `format`.
    ///     *   A *new* `Arc` wrapping the input `filters` (which contain the active `SortState`).
    ///
    /// ## Asynchronicity & Performance
    /// Sorting can be computationally intensive, hence this operation is `async` and executed via `tokio`
    /// to avoid blocking the UI thread. The use of Polars' optimized sorting algorithms makes it fast,
    /// but offloading it remains important for responsiveness.
    ///
    /// ## State Update Flow (Simplified)
    /// 1. User clicks header in UI (`container.rs::render_table_header`).
    /// 2. `render_table_header` calculates next `SortState`, updates local mutable state, creates `new_filters`.
    /// 3. `render_table` returns `Some(new_filters)` to `layout.rs::render_central_panel`.
    /// 4. `layout.rs` calls `DataFrameContainer::sort`, passing current `container` Arc and `new_filters`.
    /// 5. `layout.rs` spawns this `sort` future using `run_data_future`.
    /// 6. This function executes (potentially taking time for the Polars `sort`).
    /// 7. The result (`Ok(new_sorted_container)`) is sent back via channel.
    /// 8. `layout.rs::check_data_pending` receives the new container and updates `PolarsViewApp.data_container`.
    ///
    /// # Arguments
    /// * `data_container`: An `Arc` pointing to the *current* `DataFrameContainer`.
    /// * `filters`: The `DataFilters` struct containing the desired `SortState`. **Takes ownership**.
    ///
    /// # Returns
    /// A `PolarsViewResult` wrapping:
    /// * `Ok(DataFrameContainer)`: A *new*, potentially sorted, `DataFrameContainer`. If no sort was needed, it's effectively a clone of the input.
    /// * `Err(PolarsViewError)`: An error encountered during sorting (e.g., a PolarsError).
    pub async fn sort(
        data_container: Arc<DataFrameContainer>,
        filters: DataFilters, // Takes ownership of filters potentially modified by UI to include sort state.
    ) -> PolarsViewResult<Self> {
        // --- 1. Check if Sorting is Needed ---
        let Some(sort) = &filters.sort else {
            // No sort state defined in the filters.
            tracing::debug!(
                "fn sort(): No sort requested in filters. Returning original container clone."
            );
            // Return a clone of the existing container. Cloning involves only copying Arcs, which is cheap.
            return Ok(data_container.as_ref().clone());
        };

        // --- 2. Extract Sort Parameters ---
        let (col_name, ascending) = match sort.as_ref() {
            // Get column name and direction (ascending true/false).
            SortState::Ascending(col_name) => (col_name.as_str(), true),
            SortState::Descending(col_name) => (col_name.as_str(), false),
            // If SortState is NotSorted but the Option was Some, treat as no-op.
            SortState::NotSorted(_) => {
                tracing::debug!(
                    "fn sort(): SortState is NotSorted. Returning original container clone."
                );
                return Ok(data_container.as_ref().clone());
            }
        };

        tracing::debug!(
            "fn sort(): Applying sort. Column: '{}', Ascending: {}",
            col_name,
            ascending
        );

        // --- 3. Configure Polars Sort ---
        let sort_options = SortMultipleOptions::default()
            .with_maintain_order(true) // Keep order of equal elements (stable sort).
            .with_multithreaded(true) // Use multiple threads for sorting if possible.
            .with_order_descending(!ascending) // Polars uses `descending` flag (opposite of our `ascending`).
            .with_nulls_last(false); // Place nulls at the beginning (consistent behavior).

        // --- 4. Perform Sorting ---
        // This is the potentially long-running operation.
        let df_sorted = data_container.df.sort([col_name], sort_options)?;
        tracing::debug!("fn sort(): Polars sort operation completed successfully.");

        // --- 5. Create and Return New Container ---
        Ok(DataFrameContainer {
            df: Arc::new(df_sorted), // Wrap the *newly sorted* DataFrame in an Arc.
            extension: data_container.extension.clone(), // Clone Arc pointer to original extension.
            filters: filters.into(), // Convert the input `filters` (which contains the active sort state) into an Arc.
            format: data_container.format.clone(), // Clone Arc pointer to original format.
        })
    }

    /// Renders the DataFrame as an interactive `egui` table using `egui_extras::TableBuilder`.
    /// This method is called by `layout.rs` within the main UI update loop (`render_central_panel`).
    ///
    /// ## Orchestration:
    /// 1.  **State Setup:** Initializes `updated_filters_for_sort` to `None` and clones the current `sort` state.
    /// 2.  **Closures Definition:** Defines two closures:
    ///     *   `analyze_header`: Responsible for rendering the header row. Calls `render_table_header`. Crucially, `render_table_header` modifies the `current_sort_state` and `updated_filters_for_sort` captured by this closure if a header is clicked.
    ///     *   `analyze_rows`: Responsible for rendering data rows. Calls `render_table_row` for each row index provided by `TableBuilder`.
    /// 3.  **Table Building:** Calls `self.build_table`, passing the `ui` context and the defined closures. `build_table` configures and renders the `egui_extras::Table`.
    /// 4.  **Return Value:** Returns the `updated_filters_for_sort` Option.
    ///
    /// ## Interaction Flow for Sorting
    /// - User clicks a header in the table rendered by this function.
    /// - The click is handled within `render_table_header` (called by `analyze_header`).
    /// - `render_table_header` updates the `current_sort_state` and sets `updated_filters_for_sort` to `Some(new_filters_with_sort_state)`.
    /// - This function (`render_table`) returns that `Some(...)` value.
    /// - `layout.rs` checks the return value. If it's `Some`, it knows a sort was requested and triggers the asynchronous `DataFrameContainer::sort` operation (see `sort` method comments).
    ///
    /// # Arguments
    ///
    /// * `ui`: A mutable reference to the `egui::Ui` context where the table will be drawn.
    ///
    /// # Returns
    ///
    /// * `Option<DataFilters>`:
    ///     - `Some(updated_filters)`: If a sort button was clicked in the header during this frame. Contains the `DataFilters` state reflecting the *newly requested* sort order. This signals to `layout.rs` to initiate an async sort.
    ///     - `None`: If no sort action was triggered in this frame.
    pub fn render_table(&self, ui: &mut Ui) -> Option<DataFilters> {
        // Variable to potentially hold updated filters IF sorting is requested by clicking a header.
        // Initialized to None each frame.
        let mut updated_filters_for_sort: Option<DataFilters> = None;

        // Store the *current* sort state (cheap Arc clone) to be passed mutably to the header closure.
        // The header rendering logic will potentially update this based on clicks.
        let mut current_sort_state = self.filters.sort.clone();

        // --- Closure for Rendering the Header ---
        // This closure is passed to `build_table`. It captures `self`, `current_sort_state`,
        // and `updated_filters_for_sort` mutably.
        let analyze_header = |mut table_row: TableRow<'_, '_>| {
            // Delegate the actual header cell rendering logic.
            // This function call might modify `current_sort_state` and `updated_filters_for_sort`.
            self.render_table_header(
                &mut table_row,                // The egui table row context.
                &mut current_sort_state,       // The captured sort state (potentially updated).
                &mut updated_filters_for_sort, // The captured Option (set if sort occurs).
            );
        };

        // --- Closure for Rendering Data Rows ---
        // This closure is passed to `build_table`'s `body.rows()` method.
        // It captures `self` immutably.
        let analyze_rows = |mut table_row: TableRow<'_, '_>| {
            // Delegate the actual data cell rendering logic for the given row index.
            self.render_table_row(&mut table_row);
        };

        // --- Build and Render the Table ---
        // Call the helper method to configure and draw the egui_extras table.
        // Pass the UI context and the two closures defined above.
        self.build_table(ui, analyze_header, analyze_rows);

        // --- Return Result ---
        // Return the value of `updated_filters_for_sort`.
        // If a header click occurred inside `analyze_header` -> `render_table_header`,
        // this will be Some(new_filters), signaling `layout.rs` to trigger a sort.
        // Otherwise, it remains None.
        updated_filters_for_sort
    }

    /// Renders the header row of the data table, creating a clickable, sortable cell for each column.
    ///
    /// This method iterates through the DataFrame's columns and, for each column:
    /// 1.  Determines the column's current sort state relative to the overall table sort state (`self.filters.sort`).
    /// 2.  Conditionally renders the header cell using either:
    ///     *   The custom `SortableHeaderWidget` (feature `header-wrapping`, default): Provides wrapping text, custom color, and precise icon click detection.
    ///     *   The simpler `ExtraInteractions::sort_button` (feature `header-simple` or fallback): Uses a standard non-wrapping button, internal click logic.
    /// 3.  If the interactive element (icon or button) is clicked:
    ///     *   Calculates the *next* `SortState` for that column (e.g., None -> Desc -> Asc -> Desc...).
    ///     *   Updates the mutable `sorted_column` reference (the overall table sort state).
    ///     *   Sets the mutable `filters` reference to `Some(DataFilters { ... })`, creating a new filters state
    ///         reflecting the *new* sort state. This is the signal propagated back to `layout.rs`.
    ///
    /// # Arguments
    ///
    /// * `table_row`: The `egui_extras::TableRow` context for the header row.
    /// * `sorted_column`: A mutable reference to the `Option<Arc<SortState>>` representing the
    ///                    *currently applied sort* for the entire table (shared from `render_table`).
    ///                    This will be *updated directly* by the widgets/logic if a sort is triggered.
    /// * `filters`: A mutable reference to an `Option<DataFilters>` (shared from `render_table`).
    ///              This will be set to `Some(new_filters_state)` if a sort click occurs, signaling
    ///              the need for an update to `layout.rs`.
    fn render_table_header(
        &self,
        table_row: &mut TableRow<'_, '_>, // egui context for the header row
        sorted_column: &mut Option<Arc<SortState>>, // IN/OUT: Overall table sort state (modified on click)
        filters: &mut Option<DataFilters>, // OUT: Set to Some(new_filters) if a click occurs
    ) {
        // --- 1. Iterate Through Column Names ---
        for column_name in self.df.get_column_names() {
            // --- 2. Add Header Cell to Row ---
            table_row.col(|ui| {
                // This closure defines the content of a single header cell.

                // --- 3. Determine Current Sort State *for this specific column* ---
                // Check the overall table sort state (`self.filters.sort`) and see if it applies to this column.
                let column_sort_state_arc = match &self.filters.sort {
                    // If the table IS sorted and it's by THIS column, clone the Arc.
                    Some(sort_arc) if sort_arc.is_sorted_column(column_name) => sort_arc.clone(),
                    // Otherwise (table not sorted, or sorted by a different column),
                    // create a new Arc representing "NotSorted" for *this* column.
                    _ => Arc::new(SortState::NotSorted(column_name.to_string())),
                };

                // --- Feature-Based Conditional Rendering ---
                // Select the rendering logic based on the `header_wrapping` format setting.
                if self.format.header_wrapping {
                    // --- START: Wrapping Header Logic --- (Feature: header-wrapping, default)

                    // 4a. Determine text color based on egui theme (dark/light mode).
                    let column_name_color = if ui.visuals().dark_mode {
                        Color32::from_rgb(160, 200, 255) // Light blue for dark mode
                    } else {
                        Color32::from_rgb(0, 80, 160) // Dark blue for light mode
                    };

                    // 5a. Call the custom widget trait method (`SortableHeaderWidget`).
                    //     This draws the icon (clickable) and the wrapping column name label.
                    //     It takes the sort state for *this column* by reference.
                    //     Returns the egui::Response associated *only with the icon*.
                    let icon_response = ui.sortable_wrapping_header(
                        column_name,
                        &column_sort_state_arc, // Pass current state for this column.
                        column_name_color,      // Pass text color.
                    );

                    // 6a. Check if the *icon* was specifically clicked.
                    if icon_response.clicked() {
                        // 7a. Calculate the *next* sort state by incrementing the current state.
                        //     Uses the `inc()` method from the `SelectionDepth` trait implemented for `SortState`.
                        let next_sort_state_arc = Arc::new(column_sort_state_arc.inc());

                        // 8a. Update the *overall table sort state* (the mutable reference captured from `render_table`).
                        *sorted_column = Some(next_sort_state_arc);

                        // 9a. Signal that filters need updating by setting the captured `filters` Option.
                        //     Create a new DataFilters struct containing the *updated* `sorted_column` state
                        //     and clone the rest of the existing filter settings (`self.filters`).
                        //     This `DataFilters` will be returned by `render_table` to `layout.rs`.
                        *filters = Some(DataFilters {
                            sort: sorted_column.clone(), // Use the just-updated overall sort state.
                            ..self.filters.as_ref().clone()  // Clone other filter fields.
                        });
                        tracing::debug!(
                            "Header '{}' clicked (wrapping). New sort state requested: {:#?}",
                            column_name,
                            filters
                        );
                    }
                    // --- END: Wrapping Header Logic ---
                } else {
                    // Fallback: default or if 'header-simple' is explicitly enabled
                    // --- START: Simple Header Logic --- (Feature: header-simple)

                    // 4b. Use the standard `sort_button` provided by the `ExtraInteractions` trait.
                    //     This widget draws a non-wrapping label and icon together as one button.
                    //     Crucially, it takes the `sorted_column` mutable reference *directly* and
                    //     updates it *internally* based on its own click logic (cycling through states).
                    //     We pass the specific state for *this column* (`column_sort_state_arc`) as well.
                    let button_response = ui
                        .sort_button(
                            sorted_column, // Pass mut ref to overall sort state (updated internally by widget).
                            column_sort_state_arc.clone(), // Pass Arc for *this* column's current state.
                        )
                        .on_hover_text(column_name.to_string()); // Add standard tooltip.

                    // 5b. Check if the button click *occurred*. The `sort_button` widget handles updating
                    //     the `sorted_column` internally. We just need to check if a click happened
                    //     on *this frame* to signal the filter update.
                    if button_response.clicked() {
                        // 6b. Signal filter change. The `sorted_column` Option<Arc<SortState>> has
                        //     *already* been potentially updated by the `sort_button` call above.
                        //     We just need to create the new `DataFilters` struct reflecting this
                        //     potential change and assign it to the output `filters` Option.
                        *filters = Some(DataFilters {
                            sort: sorted_column.clone(), // Clone the potentially updated overall sort state.
                            ..self.filters.as_ref().clone()  // Clone other filter fields.
                        });
                        tracing::debug!(
                            "Header '{}' clicked (simple). New sort state requested: {:#?}",
                            column_name,
                            filters
                        );
                    }
                    // --- END: Simple Header Logic ---
                } // End of else branch (simple header)
            }); // End of table_row.col closure for this specific column header.
        } // End of loop through column names.
    }

    /// Renders a single data row within the `egui_extras` table body.
    ///
    /// This method is called by the `analyze_rows` closure (defined in `render_table`)
    /// for each row index managed by the `TableBuilder`.
    ///
    /// For the given `table_row`:
    /// 1.  Gets the `row_index` from the `table_row`.
    /// 2.  Iterates through each `Polars::prelude::Column` (Series) in the `self.df`.
    /// 3.  For each column/cell:
    ///     a.  Calls `get_decimal_and_layout` to determine the appropriate text alignment (`egui::Layout`)
    ///         and optional decimal places based on the column's `DataType` and the current `self.format` settings.
    ///     b.  Calls `Self::format_cell_value` to retrieve the `polars::prelude::AnyValue` for the cell
    ///         at `(row_index, col_index)` and formats it into a `String`, applying decimal formatting if needed.
    ///     c.  Adds a cell to the `table_row` using `table_row.col(|ui| ...)`.
    ///     d.  Inside the cell UI closure, applies the determined `layout` and adds the formatted `value_str` as an `egui::Label`.
    ///         Disables text wrapping within the cell for typical table behavior.
    ///
    /// # Arguments
    /// * `table_row`: The `egui_extras::TableRow` context for the current data row, providing access
    ///                to the row index (`table_row.index()`) and methods to add cells (`.col()`).
    fn render_table_row(&self, table_row: &mut TableRow<'_, '_>) {
        // Get the index of the current row being rendered by the TableBuilder.
        let row_index = table_row.index();

        // Iterate through each column (Polars Series) in the DataFrame.
        // `self.df.get_columns()` returns a `Vec<Series>`.
        for column_series in self.df.get_columns() {
            // Rename to avoid confusion with egui_extras::Column
            // Determine the appropriate formatting (decimal places) and alignment (layout)
            // for this specific column based on its data type and the global format settings (`self.format`).
            // `get_decimal_and_layout` is defined based on feature flags in lib.rs.
            let (opt_decimal, layout) = get_decimal_and_layout(column_series, &self.format);

            // Retrieve the value from the current column at the current row index,
            // and format it into a String according to the determined decimal places (if any).
            let value_str = Self::format_cell_value(column_series, row_index, opt_decimal);

            // Add a column cell (`.col()`) to the current egui data row (`table_row`).
            table_row.col(|ui| {
                // Apply the determined layout (which controls alignment: left, right, center)
                // to the content *within* this specific cell.
                // `.with_main_wrap(false)` prevents text wrapping inside the cell.
                ui.with_layout(layout.with_main_wrap(false), |ui| {
                    // Add the formatted value string as a simple, non-interactive label.
                    ui.label(value_str);
                }); // End layout scope for the cell content.
            }); // End cell definition for this column in this row.
        } // End loop through columns for this row.
    }

    /// Retrieves and formats the value of a specific cell (column and row index) as a `String` for display.
    ///
    /// This is a helper function called by `render_table_row`.
    ///
    /// ## Logic
    /// 1.  Uses `column.get(row_index)` to access the `polars::prelude::AnyValue` at the specified cell.
    /// 2.  Handles the `Result` from `get`:
    ///     *   `Ok(AnyValue)`: Proceeds to format the value.
    ///     *   `Err(_)`: Returns a placeholder error string (should be rare with `TableBuilder`).
    /// 3.  Formats based on `AnyValue` variant and `opt_decimal`:
    ///     *   `Float32`/`Float64` with `Some(decimal)`: Uses `format!` macro with precision specifier (`:.*`).
    ///     *   `Null`: Returns an empty string `""`.
    ///     *   `String`: Converts the `&str` payload to `String`. Ignores `opt_decimal`.
    ///     *   Other types (Ints, Bool, Date, etc.): Uses the standard `to_string()` implementation for the `AnyValue`.
    ///
    /// # Arguments
    /// * `column`: A reference to the Polars `Column` (Series) from which to get the value.
    /// * `row_index`: The zero-based index of the row to retrieve the value from.
    /// * `opt_decimal`: An `Option<usize>` specifying the number of decimal places for float formatting.
    ///                  `None` if the column is not a float or if no specific precision is needed.
    ///
    /// # Returns
    /// A `String` representation of the cell value, ready for display in an `egui::Label`.
    fn format_cell_value(
        column: &PColumn,           // The Polars Series (renamed to avoid name clash)
        row_index: usize,           // Index of the row
        opt_decimal: Option<usize>, // Number of decimals for floats, None otherwise
    ) -> String {
        // Attempt to get the AnyValue at the specified row index from the Polars Series.
        match column.get(row_index) {
            // --- Value Retrieval Successful ---
            Ok(any_value) => {
                // Match on the retrieved AnyValue *and* the presence of decimal formatting.
                match (any_value, opt_decimal) {
                    // Float value *and* decimal places specified: Format with precision.
                    (AnyValue::Float32(f), Some(decimal)) => format!("{:.*}", decimal, f),
                    (AnyValue::Float64(f), Some(decimal)) => format!("{:.*}", decimal, f),

                    // Null value: Represent as an empty string in the UI.
                    (AnyValue::Null, _) => "".to_string(),

                    // String value: Just convert the &str payload to String. Ignore opt_decimal.
                    (AnyValue::String(s), None) => s.to_string(), // If opt_decimal is None
                    (AnyValue::String(s), Some(_)) => s.to_string(), // Also handle defensively if opt_decimal is Some

                    // Any other value type (integers, booleans, dates, lists, etc.):
                    // Use the default `to_string()` representation provided by Polars' AnyValue.
                    (other_av, _) => other_av.to_string(),
                }
            }
            // --- Value Retrieval Failed ---
            // This could happen if row_index is out of bounds, though TableBuilder usually prevents this.
            Err(e) => {
                tracing::warn!(
                    "Failed to get value for column '{}' at row {}: {}",
                    column.name(),
                    row_index,
                    e
                );
                "⚠ Err".to_string() // Display a simple error indicator in the cell.
            }
        }
    }

    /// Constructs and renders the `egui` table using `egui_extras::TableBuilder`.
    ///
    /// This method is called by `render_table` after defining the header and row rendering closures.
    /// It handles the configuration of the `TableBuilder` based on the DataFrame's structure
    /// and the current formatting settings (`self.format`).
    ///
    /// ## Key Logic: `auto_col_width` and `egui::Id`
    /// A crucial part of this function is how it handles the `self.format.auto_col_width` flag:
    /// 1.  **Column Sizing Strategy:** It determines whether to use `Column::auto()` (size based on content)
    ///     or `Column::initial(calculated_width)` based on the `auto_col_width` flag.
    /// 2.  **Unique Table ID:** It creates a unique `egui::Id` for the `TableBuilder` using
    ///     `Id::new("data_table_view").with(self.format.auto_col_width)`. This means the ID itself
    ///     *depends on the boolean state* of `auto_col_width`.
    /// 3.  **Layout State Reset:** When `auto_col_width` is toggled in the UI (`format.rs::render_auto_col` -> `layout.rs` triggers `update_format` -> new `DataFrameContainer`), the next call to this `build_table` function will generate a *different* `table_id`. When `egui` sees a widget (the `TableBuilder`) with a *new* ID, it **discards any cached layout state** associated with the *previous* ID. This includes manually resized column widths (when switching *from* initial *to* auto) and previous auto-sizing calculations (when switching *from* auto *to* initial). This forced state discard ensures the table immediately redraws using the *new* column sizing strategy (`Column::auto` or `Column::initial`) as intended.
    ///
    /// # Arguments
    /// * `ui`: The `egui::Ui` context for drawing.
    /// * `analyze_header`: A closure (passed from `render_table`) responsible for rendering the header row content.
    /// * `analyze_rows`: A closure (passed from `render_table`) responsible for rendering the content of each data row.
    fn build_table(
        &self,
        ui: &mut Ui,
        analyze_header: impl FnMut(TableRow<'_, '_>), // Closure to draw header
        analyze_rows: impl FnMut(TableRow<'_, '_>),   // Closure to draw rows
    ) {
        // Get UI style details for dimension calculations.
        let style = ui.style();
        let text_height = TextStyle::Body.resolve(style).size; // Height for typical data rows.
        let num_columns = self.df.width().max(1); // Number of data columns (ensure at least 1).

        // --- Column Width Calculations ---
        // Calculate available width, subtracting space needed for column spacing and scrollbar.
        let available_width = ui.available_width()
            - ((num_columns + 1) as f32 * style.spacing.item_spacing.x) // Space between columns + margins.
            - style.spacing.scroll.bar_width; // Approx. width of vertical scrollbar.

        // Calculate a default initial width if not using auto-sizing. Distribute available space.
        let initial_col_width = (available_width / num_columns as f32).max(1.0); // Ensure positive width.
        // Calculate header height based on interact size and spacing.
        let header_height = style.spacing.interact_size.y + 3.0 * style.spacing.item_spacing.y;
        // Determine a minimum reasonable width for columns when resizing.
        let min_col_width = style.spacing.interact_size.x.max(initial_col_width / 4.0);

        // --- Key Logic: Select Column Sizing Strategy ---
        // Determine the Column definition based on the `auto_col_width` format setting.
        let column_sizing_strategy = if self.format.auto_col_width {
            // Auto-sizing: adjusts based on content. Potentially slower for many columns/rows.
            tracing::trace!("build_table: Using Column::auto() [auto_col_width=true]");
            Column::auto()
        } else {
            // Initial fixed width: calculated above. Usually faster rendering, user can resize.
            tracing::trace!(
                "build_table: Using Column::initial({}) [auto_col_width=false]",
                initial_col_width
            );
            Column::initial(initial_col_width)
        }
        .at_least(min_col_width) // Apply the minimum width constraint.
        .resizable(true) // Allow user to drag column separators to resize.
        .clip(true); // Clip cell content that overflows horizontally.

        // --- Key Logic: Generate Unique Table ID ---
        // Create an egui ID that incorporates the `auto_col_width` state.
        // When `auto_col_width` changes, this ID changes, forcing egui to rebuild
        // the table's layout state from scratch, applying the new sizing strategy.
        let table_id = Id::new("data_table_view").with(self.format.auto_col_width);
        tracing::trace!(
            "build_table: Using Table ID: {table_id:?} derived from auto_col_width={}",
            self.format.auto_col_width
        );

        // --- Configure and Build the Table ---
        TableBuilder::new(ui)
            .id_salt(table_id) // <--- Apply the unique ID calculated above.
            .striped(true) // Alternate row background colors for readability.
            // Apply the chosen column sizing strategy to all `num_columns` data columns.
            .columns(column_sizing_strategy, num_columns)
            // Add a final "remainder" column to fill any leftover horizontal space.
            // This prevents the last data column from stretching excessively if space is available.
            .column(Column::remainder())
            .resizable(true) // Allow the whole table structure (separator positions) to be interactive.
            .auto_shrink([false, false]) // Prevent table from collapsing smaller than available space.
            // Define the header section: height and the closure to render its content.
            .header(header_height, analyze_header)
            // Define the body section.
            .body(|body| {
                // Get the total number of data rows in the DataFrame.
                let num_rows = self.df.height();
                // Efficiently render all rows using the provided closure.
                // `body.rows` calls the `analyze_rows` closure for each `row_index` from 0 to `num_rows - 1`.
                body.rows(text_height, num_rows, analyze_rows);
            }); // End of table definition. egui draws it here.
    }
}
