use egui::{Align, DragValue, Grid, Layout, Ui, Vec2};
use polars::prelude::*;

use std::{collections::HashMap, fmt::Debug, sync::LazyLock};

// --- Constants ---

/// A static, lazily initialized, thread-safe `HashMap` defining the *default* text alignments
/// for various Polars `DataType`s when displayed in the `egui` table.
///
/// This map provides sensible defaults (e.g., right-align numbers, left-align text)
/// but can be overridden by user preferences stored in `DataFormat.alignments`.
/// It is consulted by `get_decimal_and_layout` to determine the `egui::Layout`
/// to apply to a table cell.
///
/// ## Implementation Details: `std::sync::LazyLock`
/// `LazyLock` ensures that the `HashMap` is initialized:
/// 1.  **Exactly Once:** The closure provided to `LazyLock::new` runs only the very first
///     time `DEFAULT_ALIGNMENTS` is accessed across *any* thread.
/// 2.  **Lazily:** Initialization is deferred until the first access, avoiding upfront cost.
/// 3.  **Thread-Safely:** `LazyLock` handles the necessary synchronization internally,
///     making it safe to use even if multiple threads might try to access it concurrently
///     (though in `egui` context, access is typically from the main UI thread).
pub static DEFAULT_ALIGNMENTS: LazyLock<HashMap<DataType, Align>> = LazyLock::new(|| {
    // Define the default DataType -> egui::Align mapping.
    let tuples = [
        // Numerical types: Typically right-aligned for easy comparison of magnitude.
        (DataType::Float32, Align::RIGHT),
        (DataType::Float64, Align::RIGHT),
        // Integer/Categorical types: Often centered for visual balance.
        (DataType::Int8, Align::Center),
        (DataType::Int16, Align::Center),
        (DataType::Int32, Align::Center),
        (DataType::Int64, Align::Center),
        (DataType::UInt8, Align::Center),
        (DataType::UInt16, Align::Center),
        (DataType::UInt32, Align::Center),
        (DataType::UInt64, Align::Center),
        (DataType::Date, Align::Center),
        (DataType::Time, Align::Center),
        // Specific Datetime/Duration variants (can add more if needed)
        (
            DataType::Datetime(TimeUnit::Milliseconds, None),
            Align::Center,
        ),
        (
            DataType::Datetime(TimeUnit::Nanoseconds, None),
            Align::Center,
        ),
        (DataType::Duration(TimeUnit::Milliseconds), Align::Center),
        (DataType::Duration(TimeUnit::Nanoseconds), Align::Center),
        (DataType::Boolean, Align::Center),
        // Textual data: Typically left-aligned, standard reading direction.
        (DataType::String, Align::LEFT),
        (DataType::Binary, Align::LEFT), // Binary data often represented textually (hex/base64).
                                         // Add other DataType defaults as needed.
                                         // Types not specified here will likely default based on egui's behavior
                                         // or the logic in `get_decimal_and_layout` if it provides a fallback.
    ];

    // Create the HashMap from the array of tuples.
    // This closure code runs only once, when `DEFAULT_ALIGNMENTS` is first accessed.
    HashMap::from(tuples)
});

// --- Data Structures ---

/// Holds user-configurable settings related to the visual presentation of data in the table.
///
/// This struct is managed within `PolarsViewApp` (`layout.rs`) as `applied_format` and is used by:
/// - `layout.rs` (`render_side_panel`/`render_format`): To display and modify settings via the UI.
///   Crucially, `render_format` compares the state before and after UI interaction to detect changes.
/// - `container.rs` (`render_table_row`): To determine cell alignment and float formatting during table rendering, primarily via the `get_decimal_and_layout` helper function which reads `decimal` and `alignments`.
/// - `container.rs` (`build_table`): To control column auto-sizing behavior (`auto_col_width`). Toggling this flag causes a rebuild of the table with a new ID, resetting column sizes.
///
/// Instances are cloned in `layout.rs` to detect UI changes. A change triggers an asynchronous `DataFrameContainer::update_format` call, which creates a new `DataFrameContainer` with the updated `DataFormat` settings (wrapped in `Arc`).
#[derive(Debug, Clone, PartialEq)]
pub struct DataFormat {
    /// Stores the *current* alignment settings for each `DataType`.
    /// Initially populated from `DEFAULT_ALIGNMENTS`, but users can modify these
    /// via the UI rendered by `render_alignment_panel`. The `HashMap` allows overriding
    /// defaults on a per-type basis. Keys are `polars::prelude::DataType`, values are `egui::Align`.
    /// This map is read by `get_decimal_and_layout` to choose the correct `egui::Layout`.
    pub alignments: HashMap<DataType, Align>,

    /// Controls the column sizing strategy used by `egui_extras::TableBuilder` in `container.rs::build_table`:
    /// - `true`: Use `Column::auto()`, resizing columns based on content width. Can be slower for very wide tables.
    /// - `false`: Use `Column::initial(calculated_width)`, providing faster initial rendering with manually resizable columns.
    ///
    /// Modified by the checkbox in `render_auto_col`. The change detection for this flag
    /// is crucial. When this changes, `layout.rs` triggers `DataFrameContainer::update_format`.
    /// The new `DataFormat` is passed to `container.rs::build_table`, which generates a *different*
    /// `egui::Id` for the `TableBuilder` based on this flag's value. This ID change forces `egui`
    /// to discard cached layout state (like manually resized widths) and apply the new sizing strategy.
    pub auto_col_width: bool,

    /// The number of decimal places to display for floating-point number columns
    /// (`Float32`, `Float64`) in the table. Modified by the UI in `render_decimal_input`.
    /// Used by `get_decimal_and_layout` and subsequently by `container.rs::format_cell_value`
    /// to format float values into strings for display.
    pub decimal: usize,

    /// Flag to control whether table headers should wrap text or be simple single-line buttons.
    /// - `true` (default): Use the custom `SortableHeaderWidget` in `container.rs::render_table_header`, which allows wrapping text and applies colors.
    /// - `false`: Use the simpler `ExtraInteractions::sort_button`, resulting in non-wrapping, standard egui buttons.
    ///
    /// This setting affects the visual appearance and interaction of column headers.
    pub header_wrapping: bool,
}

// --- Implementations ---

impl Default for DataFormat {
    /// Creates a `DataFormat` instance with default settings.
    /// Initializes `alignments` by cloning the static `DEFAULT_ALIGNMENTS`,
    /// sets `decimal` to 2, `auto_col_width` to `false`, and `header_wrapping` to `true`.
    fn default() -> Self {
        DataFormat {
            // Clone the globally defined default alignments. `LazyLock` ensures the original HashMap
            // is created only once; cloning it afterwards is standard HashMap cloning.
            alignments: DEFAULT_ALIGNMENTS.clone(),
            // Default to fixed initial column widths for potentially better performance,
            // letting the user resize manually.
            auto_col_width: false,
            // A sensible default for displaying most floating-point numbers.
            decimal: 2,
            // Default to the wrapping, styled header.
            header_wrapping: true,
        }
    }
}

impl DataFormat {
    /// Renders the UI controls for modifying data format settings within the "Format" side panel section.
    ///
    /// **Change Detection Logic:**
    /// This function takes `&mut self`. The UI widgets it creates (like `DragValue`, `radio_value`, `checkbox`)
    /// directly modify the fields of `self` (e.g., `self.decimal`, entries in `self.alignments`, `self.auto_col_width`).
    /// By cloning `self` *before* rendering the UI (`format_former = self.clone()`) and comparing it
    /// to the state of `self` *after* rendering, we can detect if any user interaction modified the format settings.
    ///
    /// **Return Value:**
    /// - `Some(updated_format)`: If a change was detected (`*self != format_former`), a clone of the *newly modified* `DataFormat` state is returned.
    /// - `None`: If no changes occurred during this frame.
    ///
    /// **Integration with `layout.rs`:**
    /// `layout.rs::render_side_panel` calls this method. If it receives `Some(new_format)`, it knows the user changed
    /// a setting and triggers an asynchronous update via `DataFrameContainer::update_format`, passing the `new_format`.
    /// This efficiently updates the application state with the new formatting rules without needing a full data reload.
    ///
    /// # Arguments
    ///
    /// * `ui`: A mutable reference to the `egui::Ui` context where the format controls will be drawn.
    ///
    /// # Returns
    ///
    /// * `Option<DataFormat>`: Contains the updated `DataFormat` if changes were made, otherwise `None`.
    pub fn render_format(&mut self, ui: &mut Ui) -> Option<DataFormat> {
        // --- 1. Capture Pre-UI State for Change Detection ---
        // Clone the current state *before* any UI widgets are drawn and potentially modify `self`.
        // This allows comparison after the UI widgets have had a chance to mutate `self`.
        let format_former = self.clone();
        // Initialize the result to None (no change assumed initially). Will be updated if changes are detected.
        let mut result = None;

        // Get available width for layout within the side panel.
        let width_max = ui.available_width();
        let width_min = 200.0; // Define a minimum width for the format section UI elements.

        // Use a grid layout for label-widget pairs for consistent alignment.
        let grid = Grid::new("data_format_grid")
            .num_columns(2) // Column 0 for labels, Column 1 for widgets.
            .spacing([10.0, 20.0]) // Spacing between columns and rows.
            .striped(true); // Alternating row backgrounds for readability.

        // Allocate space for the whole format section using the available width.
        ui.allocate_ui_with_layout(
            Vec2::new(width_max, ui.available_height()), // Use available width, auto height.
            Layout::top_down(Align::LEFT),               // Arrange controls vertically.
            |ui| {
                // --- 2. Render UI Controls within the Grid ---
                // `grid.show` takes the UI context and a closure to define the grid's content.
                grid.show(ui, |ui| {
                    ui.set_min_width(width_min); // Enforce the minimum width defined earlier.

                    // Render the alignment settings subsection (in a collapsing header).
                    self.render_alignment_panel(ui); // Modifies self.alignments directly via radio buttons.

                    // Render the decimal places input field.
                    self.render_decimal_input(ui); // Modifies self.decimal directly via DragValue.

                    // Render the automatic column width toggle checkbox.
                    self.render_auto_col(ui); // Modifies self.auto_col_width directly via checkbox.

                    // Render the header wrapping toggle checkbox.
                    self.render_header_wrapping(ui); // Modifies self.header_wrapping directly via checkbox.

                    // --- 3. Detect Changes Post-UI Rendering ---
                    // Compare the potentially modified `self` with the state captured *before* UI rendering.
                    if *self != format_former {
                        // If the state is different, it means a UI interaction modified a format setting.
                        // Store a clone of the *new* state to be returned by this function.
                        result = Some(self.clone());
                        // Log the detected change for debugging purposes.
                        tracing::debug!(
                            "Format change detected in render_format. New state: {:#?}",
                            self
                        );
                    }
                    // Note: There's no explicit "Apply" button for format changes. Changes are detected
                    // automatically per frame based on the direct modification of `self` by the UI widgets.
                    // The detection happens *after* all widgets for this section have been rendered.
                });
            },
        );

        // --- 4. Return Result ---
        // Returns `Some(new_format)` if a change occurred during this frame, otherwise `None`.
        // This signals to `layout.rs` whether an async format update is needed.
        result
    }

    /// Renders the collapsible UI section for configuring text alignment for different `DataType`s.
    /// Groups alignment settings under an `egui::CollapsingHeader` for better organization within the side panel.
    /// Uses a nested `egui::Grid` for the alignment radio buttons themselves.
    ///
    /// This method calls `show_alignment_row` for each relevant data type, passing the mutable
    /// `self.alignments` map, which allows the radio buttons within `show_alignment_row` to
    /// directly modify the state.
    ///
    /// # Arguments
    ///
    /// * `ui`: Mutable reference to the `egui::Ui` context where the collapsing header and nested grid will be drawn.
    fn render_alignment_panel(&mut self, ui: &mut Ui) {
        ui.label("Alignment:"); // Section label within the main format grid.
        // Use a collapsing header to keep the UI tidy, only showing the options when expanded.
        ui.collapsing("Data Types", |ui| {
            // Use a nested grid specifically for the alignment options (Label, Left, Center, Right).
            Grid::new("align_grid")
                .num_columns(4) // Column for DataType Label | Left Radio | Center Radio | Right Radio
                .spacing([10.0, 10.0]) // Use tighter spacing within the collapsing section.
                .striped(true)
                .show(ui, |ui| {
                    // Render alignment rows for a selection of relevant Polars DataTypes.
                    // **Important**: `&mut self.alignments` is passed to `show_alignment_row`.
                    // `show_alignment_row` uses `egui::radio_value`, which directly modifies the
                    // `Align` value associated with the `DataType` key within this HashMap if a
                    // different radio button is clicked. The change is reflected immediately in `self.alignments`.
                    Self::show_alignment_row(ui, &DataType::Float64, &mut self.alignments);
                    Self::show_alignment_row(ui, &DataType::Int64, &mut self.alignments);
                    Self::show_alignment_row(ui, &DataType::Date, &mut self.alignments);
                    Self::show_alignment_row(ui, &DataType::Boolean, &mut self.alignments);
                    Self::show_alignment_row(ui, &DataType::String, &mut self.alignments);
                    // Add more `show_alignment_row` calls for other DataTypes as desired.
                    // Currently omitted types will use their defaults from DEFAULT_ALIGNMENTS or fall back.
                });
        });
        ui.end_row(); // End the row for "Alignment:" in the *outer* grid (`data_format_grid`).
    }

    /// Helper function to render a single row within the alignment configuration grid.
    /// This row consists of a `DataType` label followed by radio buttons for Left, Center, and Right alignment.
    /// Modifies the `alignments` `HashMap` directly based on user interaction with the radio buttons.
    ///
    /// # Arguments
    ///
    /// * `ui`: Mutable reference to the `egui::Ui` context, specifically *within* the `align_grid`.
    /// * `data_type`: The `polars::DataType` that this row configures alignment for (borrowed).
    /// * `alignments`: A mutable reference to the `HashMap<DataType, Align>` storing the application's
    ///                 current alignment settings. `egui::radio_value` will directly query and potentially
    ///                 update the corresponding `Align` value in this map.
    fn show_alignment_row(
        ui: &mut Ui,
        data_type: &DataType, // The Polars data type this row represents.
        alignments: &mut HashMap<DataType, Align>, // Map to read *and potentially update directly*.
    ) {
        // Get a mutable reference to the current alignment setting for this specific `DataType`
        // within the `alignments` map.
        // The `entry(data_type).or_insert(Align::LEFT)` pattern ensures:
        // - If `data_type` already exists as a key in `alignments`, `entry()` finds it, and `.or_insert()` does nothing but return a mutable reference to the existing `Align` value.
        // - If `data_type` doesn't exist as a key (e.g., if it wasn't in DEFAULT_ALIGNMENTS initially),
        //   `entry()` indicates this, and `.or_insert(Align::LEFT)` inserts `Align::LEFT` as the default value *for this specific data type* into the map, then returns a mutable reference to this newly inserted value.
        // This guarantees `current_align` is always a valid mutable reference bound to the HashMap entry.
        let current_align: &mut Align = alignments.entry(data_type.clone()).or_insert(Align::LEFT);

        // Display the DataType name as a label in the first column of the inner alignment grid.
        ui.label(format!("{data_type:?}")); // Using Debug format for DataType.

        // Render the radio buttons for Left, Center, Right alignment.
        // `ui.radio_value(current_state_mut, option_value, label)` is the core interaction:
        // - It takes a mutable reference `current_align` to the value it controls (the `Align` in the HashMap).
        // - It displays a radio button with the given `label` ("Left", "Center", "Right").
        // - The `option_value` (`Align::LEFT`, `Align::Center`, `Align::RIGHT`) is the specific value this button represents.
        // - *If* the user clicks this radio button *and* its `option_value` is different from the current value
        //   referenced by `current_align`, `egui` *automatically updates* the value pointed to by `current_align`
        //   (i.e., it directly modifies `*current_align = option_value` in the `HashMap`).
        // - It returns `true` if the click caused a change, `false` otherwise. We don't need the return value
        //   here because the change detection happens later in `render_format`.
        ui.radio_value(current_align, Align::LEFT, "Left")
            .on_hover_text("Align text to the left");
        ui.radio_value(current_align, Align::Center, "Center")
            .on_hover_text("Align text to the center");
        ui.radio_value(current_align, Align::RIGHT, "Right")
            .on_hover_text("Align text to the right");

        ui.end_row(); // End this row in the *inner* grid (`align_grid`).
    }

    /// Renders the `DragValue` widget for setting the number of decimal places for float display.
    /// Modifies `self.decimal` directly based on user input (typing or dragging).
    /// The change is detected later in `render_format` by comparing `self` before and after rendering.
    ///
    /// # Arguments
    ///
    /// * `ui`: Mutable reference to the `egui::Ui` context within the main format grid (`data_format_grid`).
    fn render_decimal_input(&mut self, ui: &mut Ui) {
        ui.label("Decimals:"); // Label for the input widget in the outer grid.
        // `DragValue::new(&mut self.decimal)` creates the widget and binds its state directly
        // to the `self.decimal` field. User interaction (typing in the field or dragging it)
        // modifies `self.decimal` immediately within this frame.
        ui.add(
            DragValue::new(&mut self.decimal) // Bind widget state to the `decimal` field.
                .speed(1) // Set the sensitivity of dragging.
                .range(0..=10), // Allow values between 0 and 10 decimal places inclusive.
        )
        .on_hover_text(
            "Number of decimal places for floating-point numbers (e.g., Float32, Float64)",
        );
        ui.end_row(); // End the row for "Decimals:" in the *outer* grid (`data_format_grid`).
    }

    /// Renders the checkbox for toggling the 'Auto Column Width' behavior (`self.auto_col_width`).
    /// Modifies `self.auto_col_width` directly when the checkbox is clicked.
    ///
    /// **Impact:**
    /// When the user clicks this checkbox, `self.auto_col_width` is toggled immediately.
    /// The change to `self.auto_col_width` is then detected by the comparison logic at the
    /// end of `render_format`. This detected change signals `layout.rs` to trigger
    /// `DataFrameContainer::update_format`, which creates a new `DataFrameContainer` instance
    /// containing the updated `DataFormat` (with the new `auto_col_width` value) wrapped in an `Arc`.
    /// Subsequently, `container.rs::build_table` receives this updated container. Inside `build_table`,
    /// the `egui::Id` used for the `TableBuilder` (`Id::new("data_table_view").with(self.format.auto_col_width)`)
    /// depends directly on the `auto_col_width` flag. Because the flag changed, the ID changes.
    /// This forces `egui` to discard any cached layout state (like manually resized column widths or previous
    /// auto-sizing calculations) associated with the *previous* ID. The table is then rebuilt using
    /// the column sizing strategy (`Column::auto` or `Column::initial`) corresponding to the *new*
    /// `auto_col_width` value, ensuring the visual change takes effect immediately and correctly.
    ///
    /// # Arguments
    ///
    /// * `ui`: Mutable reference to the `egui::Ui` context within the main format grid (`data_format_grid`).
    fn render_auto_col(&mut self, ui: &mut Ui) {
        ui.label("Auto Col Width:"); // Label for the checkbox in the outer grid.
        // `ui.checkbox(&mut self.auto_col_width, "")` creates the checkbox and binds its checked state
        // directly to the `self.auto_col_width` boolean field. The label text is empty as the
        // actual label ("Auto Col Width:") is placed in the preceding grid cell.
        // Clicking the checkbox toggles the value of `self.auto_col_width` directly.
        ui.checkbox(&mut self.auto_col_width, "") // Bind checkbox state to the `auto_col_width` field.
            .on_hover_text("Enable: Automatically adjust column widths based on content (can be slower).\nDisable: Use uniform initial widths for faster rendering, allows manual resize.");
        ui.end_row(); // End the row for "Auto Col Width:" in the *outer* grid (`data_format_grid`).
    }

    /// Renders the checkbox for toggling the header wrapping behavior (`self.header_wrapping`).
    /// Modifies `self.header_wrapping` directly when the checkbox is clicked.
    /// Affects which widget (`SortableHeaderWidget` or `ExtraInteractions::sort_button`) is used in `container.rs::render_table_header`.
    ///
    /// # Arguments
    ///
    /// * `ui`: Mutable reference to the `egui::Ui` context within the main format grid.
    fn render_header_wrapping(&mut self, ui: &mut Ui) {
        ui.label("Header wrapping:"); // Label for the checkbox.
        // Bind the checkbox state directly to the `header_wrapping` field.
        // Clicking the checkbox toggles the value of `self.header_wrapping`.
        ui.checkbox(&mut self.header_wrapping, "") // Label is separate.
            .on_hover_text("Enable: Wrapping, multi-line, styled header (default).\nDisable: Simple, single-line button header.");
        ui.end_row(); // End the row in the *outer* grid (`data_format_grid`).
    }
}
