use egui::{Align, DragValue, Grid, Layout, Ui, Vec2};
use polars::prelude::*;

use std::{collections::HashMap, fmt::Debug, sync::LazyLock};

// --- Constants ---

/// A static, lazily initialized, thread-safe `HashMap` defining the *default* text alignments
/// for various Polars `DataType`s when displayed in the `egui` table.
///
/// This map provides sensible defaults (e.g., right-align numbers, left-align text)
/// but can be overridden by user preferences stored in `DataFormat.alignments`.
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
/// This struct is managed within `PolarsViewApp` (`layout.rs`) and is used by:
/// - `layout.rs` (`render_side_panel`/`render_format`): To display and modify settings via the UI.
/// - `container.rs` (`render_table_row`): To determine cell alignment and formatting during table rendering, primarily via the `get_decimal_and_layout` helper function.
/// - `container.rs` (`build_table`): To control column auto-sizing behavior (`auto_col_width`).
///
/// Instances are cloned and compared in `layout.rs` to detect UI changes and trigger asynchronous updates.
/// It's also included (wrapped in `Arc`) within `DataFrameContainer`.
#[derive(Debug, Clone, PartialEq)]
pub struct DataFormat {
    /// Stores the *current* alignment settings for each `DataType`.
    /// Initially populated from `DEFAULT_ALIGNMENTS`, but users can modify these
    /// via the UI rendered by `render_alignment_panel`. The `HashMap` allows overriding
    /// defaults on a per-type basis. Keys are `polars::prelude::DataType`, values are `egui::Align`.
    pub alignments: HashMap<DataType, Align>,

    /// Controls the column sizing strategy used by `egui_extras::TableBuilder` in `container.rs::build_table`:
    /// - `true`: Use `Column::auto()`, resizing columns based on content width. Can be slower.
    /// - `false`: Use `Column::initial(calculated_width)`, providing faster initial rendering with manually resizable columns.
    ///
    /// Modified by the checkbox in `render_auto_col`. The change detection for this flag
    /// is crucial as it requires rebuilding the table with a different `egui::Id` to force a layout reset.
    pub auto_col_width: bool,

    /// The number of decimal places to display for floating-point number columns
    /// (`Float32`, `Float64`) in the table. Modified by the UI in `render_decimal_input`.
    /// Used by `get_decimal_and_layout` and `container.rs::format_cell_value`.
    pub decimal: usize,

    pub header_wrapping: bool,
}

// --- Implementations ---

impl Default for DataFormat {
    /// Creates a `DataFormat` instance with default settings.
    /// Initializes `alignments` by cloning the static `DEFAULT_ALIGNMENTS`,
    /// sets `decimal` to 2, and `auto_col_width` to `false`.
    fn default() -> Self {
        DataFormat {
            // Clone the globally defined defaults. This is cheap as LazyLock holds the HashMap.
            alignments: DEFAULT_ALIGNMENTS.clone(),
            // Default to fixed initial column widths for potentially better performance.
            auto_col_width: false,
            // Sensible default for displaying floating-point numbers.
            decimal: 2,
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
    /// - `Some(updated_format)`: If a change was detected (`*self != format_former`), a clone of the *newly modified* state is returned.
    /// - `None`: If no changes occurred during this frame.
    ///
    /// **Integration with `layout.rs`:**
    /// `layout.rs::render_side_panel` calls this method. If it receives `Some(new_format)`, it knows the user changed
    /// a setting and triggers an asynchronous update via `DataFrameContainer::update_format`, passing the `new_format`.
    /// This efficiently updates the application state with the new formatting rules.
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
        let format_former = self.clone();
        // Initialize the result to None (no change assumed initially).
        let mut result = None;

        // Get available width for layout.
        let width_max = ui.available_width();
        let width_min = 200.0; // Minimum width for the format section UI.

        // Use a grid layout for label-widget pairs for consistent alignment.
        let grid = Grid::new("data_format_grid")
            .num_columns(2) // Column 0 for labels, Column 1 for widgets.
            .spacing([10.0, 20.0]) // Spacing between columns and rows.
            .striped(true); // Alternating row backgrounds for readability.

        // Allocate space for the whole format section.
        ui.allocate_ui_with_layout(
            Vec2::new(width_max, ui.available_height()), // Use available width, auto height.
            Layout::top_down(Align::LEFT),               // Arrange controls vertically.
            |ui| {
                // --- 2. Render UI Controls within the Grid ---
                grid.show(ui, |ui| {
                    ui.set_min_width(width_min); // Enforce minimum width.

                    // Render the alignment settings subsection (in a collapsing header).
                    self.render_alignment_panel(ui); // Modifies self.alignments directly.

                    // Render the decimal places input field.
                    self.render_decimal_input(ui); // Modifies self.decimal directly.

                    // Render the column expansion toggle checkbox.
                    self.render_auto_col(ui); // Modifies self.auto_col_width directly.

                    self.render_header(ui);

                    // --- 3. Detect Changes Post-UI Rendering ---
                    // Compare the potentially modified `self` with the state captured before UI rendering.
                    if *self != format_former {
                        // If the state is different, it means a UI interaction modified a setting.
                        result = Some(self.clone()); // Store a clone of the *new* state to be returned.
                        tracing::debug!(
                            "Format change detected in render_format. New state: {:#?}",
                            self
                        );
                    }
                    // Note: There's no explicit "Apply" button. Changes are detected automatically per frame
                    // based on the direct modification of `self` by the UI widgets.
                });
            },
        );

        // --- 4. Return Result ---
        // Returns `Some(new_format)` if a change occurred, otherwise `None`.
        result
    }

    /// Renders the collapsible UI section for configuring text alignment for different `DataType`s.
    /// Groups alignment settings under a `CollapsingHeader` for better organization.
    /// Uses a nested `Grid` for the alignment radio buttons.
    ///
    /// # Arguments
    ///
    /// * `ui`: Mutable reference to the `egui::Ui` context.
    fn render_alignment_panel(&mut self, ui: &mut Ui) {
        ui.label("Alignment:"); // Section label.
        // Use a collapsing header to make the UI less cluttered.
        ui.collapsing("Data Types", |ui| {
            // Use a nested grid for the alignment options.
            Grid::new("align_grid")
                .num_columns(4) // Label | Left Radio | Center Radio | Right Radio
                .spacing([10.0, 10.0]) // Tighter spacing within the collapsing section.
                .striped(true)
                .show(ui, |ui| {
                    // Render alignment rows for relevant DataTypes.
                    // **Important**: Pass `self.alignments` mutably. `show_alignment_row` will modify it directly
                    // when radio buttons are clicked, due to `egui::radio_value`'s behavior.
                    Self::show_alignment_row(ui, &DataType::Float64, &mut self.alignments);
                    Self::show_alignment_row(ui, &DataType::Int64, &mut self.alignments);
                    Self::show_alignment_row(ui, &DataType::Date, &mut self.alignments);
                    Self::show_alignment_row(ui, &DataType::Boolean, &mut self.alignments);
                    Self::show_alignment_row(ui, &DataType::String, &mut self.alignments);
                    // Add more `show_alignment_row` calls for other DataTypes as desired.
                });
        });
        ui.end_row(); // End the row in the *outer* grid (`data_format_grid`).
    }

    /// Helper function to render a single row in the alignment grid (label + radio buttons).
    /// Modifies the `alignments` `HashMap` directly based on user interaction.
    ///
    /// # Arguments
    ///
    /// * `ui`: Mutable reference to the `egui::Ui` context within the alignment grid.
    /// * `data_type`: The `polars::DataType` this row configures.
    /// * `alignments`: A mutable reference to the `HashMap` storing the alignment settings.
    ///                 `radio_value` will directly update the corresponding entry in this map.
    fn show_alignment_row(
        ui: &mut Ui,
        data_type: &DataType, // The data type for this row (borrowed).
        alignments: &mut HashMap<DataType, Align>, // Map to read *and update directly*.
    ) {
        // Get a mutable reference to the current alignment setting for this specific `DataType`.
        // `entry().or_insert()` pattern:
        // - If `data_type` already exists as a key in `alignments`, get a mutable ref to its `Align` value.
        // - If `data_type` doesn't exist, insert `Align::LEFT` as a default *for this specific interaction*,
        //   and then get a mutable ref to the newly inserted value.
        // This ensures we always have a mutable reference to work with for `radio_value`.
        let current_align: &mut Align = alignments.entry(data_type.clone()).or_insert(Align::LEFT);

        // Display the DataType name as a label in the first column.
        ui.label(format!("{:?}", data_type));

        // Render the radio buttons for Left, Center, Right alignment.
        // `ui.radio_value(&mut current_value, option_value, label)`:
        // - Binds the radio group to `current_align` (the mutable reference to the alignment in the HashMap).
        // - `option_value` is the specific `Align` value this button represents (e.g., `Align::LEFT`).
        // - When a radio button is clicked, `egui` automatically updates the value pointed to
        //   by `current_value` (which is `*current_align` here, modifying the HashMap directly) to the clicked `option_value`.
        ui.radio_value(current_align, Align::LEFT, "Left")
            .on_hover_text("Align text to the left");
        ui.radio_value(current_align, Align::Center, "Center")
            .on_hover_text("Align text to the center");
        ui.radio_value(current_align, Align::RIGHT, "Right")
            .on_hover_text("Align text to the right");

        ui.end_row(); // End the row in the *inner* grid (`align_grid`).
    }

    /// Renders the `DragValue` widget for setting the number of decimal places for float display.
    /// Modifies `self.decimal` directly.
    ///
    /// # Arguments
    ///
    /// * `ui`: Mutable reference to the `egui::Ui` context within the main format grid.
    fn render_decimal_input(&mut self, ui: &mut Ui) {
        ui.label("Decimals:"); // Label for the input widget.
        // `DragValue::new(&mut self.decimal)` binds the widget directly to the `decimal` field.
        // User interaction (typing or dragging) modifies `self.decimal`.
        ui.add(
            DragValue::new(&mut self.decimal) // Bind to the `decimal` field.
                .speed(1) // Sensitivity of dragging.
                .range(0..=10), // Allow 0 to 10 decimal places.
        )
        .on_hover_text(
            "Number of decimal places for floating-point numbers (e.g., Float32, Float64)",
        );
        ui.end_row(); // End the row in the *outer* grid (`data_format_grid`).
    }

    /// Renders the checkbox for toggling the 'Expand Columns' behavior (`self.auto_col_width`).
    /// Modifies `self.auto_col_width` directly when clicked.
    ///
    /// **Impact:** The change to `self.auto_col_width` is detected by `render_format`. This triggers
    /// `DataFrameContainer::update_format`, leading to a new `DataFrameContainer` with the updated flag.
    /// `container.rs::build_table` uses this flag and changes the `TableBuilder`'s `Id`
    /// based on it, forcing `egui` to recalculate column widths using either `Column::auto` or `Column::initial`.
    ///
    /// # Arguments
    ///
    /// * `ui`: Mutable reference to the `egui::Ui` context within the main format grid.
    fn render_auto_col(&mut self, ui: &mut Ui) {
        ui.label("Auto Col Width:"); // Label for the checkbox.
        // `ui.checkbox(&mut self.auto_col_width, ...)` binds the checkbox state to the boolean field.
        // Clicking the checkbox toggles the value of `self.auto_col_width`.
        ui.checkbox(&mut self.auto_col_width, "") // Bind to the `auto_col_width` field. Label is separate.
            .on_hover_text("Enable: Automatically adjust column widths based on content (can be slower).\nDisable: Use uniform initial widths for faster rendering.");
        ui.end_row(); // End the row in the *outer* grid (`data_format_grid`).
    }

    fn render_header(&mut self, ui: &mut Ui) {
        ui.label("Header wrapping:"); // Label for the checkbox.
        // Clicking the checkbox toggles the value of `header_wrapping`.
        ui.checkbox(&mut self.header_wrapping, "") // Bind to the `auto_col_width` field. Label is separate.
            .on_hover_text("Enable: wrapping and colored header.\nDisable: single-line header.");
        ui.end_row(); // End the row in the *outer* grid (`data_format_grid`).
    }
}
