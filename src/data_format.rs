use egui::{Align, DragValue, Grid, Layout, Ui, Vec2};
use polars::prelude::*;

use std::{collections::HashMap, fmt::Debug, sync::LazyLock};

// --- Constants ---

/// A static, lazily initialized map defining the *default* text alignments
/// for various Polars `DataType`s used in the `egui` table.
///
/// 1. **Purpose**: Provides sensible default alignments (e.g., numbers right, text left).
/// 2. **Usage**: Read by `get_decimal_and_layout` to determine cell `egui::Layout`.
/// 3. **Override**: User preferences in `DataFormat.alignments` take precedence.
/// 4. **Implementation**: `LazyLock` ensures thread-safe, lazy, one-time initialization.
pub static DEFAULT_ALIGNMENTS: LazyLock<HashMap<DataType, Align>> = LazyLock::new(|| {
    HashMap::from([
        // Numerical types: Right-aligned.
        (DataType::Float32, Align::RIGHT),
        (DataType::Float64, Align::RIGHT),
        // Integer/Temporal/Boolean types: Centered.
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
        // Textual/Binary data: Left-aligned.
        (DataType::String, Align::LEFT),
        (DataType::Binary, Align::LEFT),
    ])
});

// --- Data Structures ---

/// Holds user-configurable settings for data presentation in the table.
///
/// ## State Management & Interaction:
/// - **UI State**: An instance is held in `PolarsViewApp` (`layout.rs`) as `applied_format`, representing the
///   current UI configuration. `render_format` modifies this instance directly.
/// - **Data State**: An `Arc<DataFormat>` is stored within each `DataFrameContainer` (`container.rs`),
///   capturing the format settings active when that data state was created (e.g., after load or sort).
/// - **Update Flow**: Changes in `render_format` are detected, triggering an async `DataFrameContainer::update_format`
///   call in `layout.rs`. This creates a new `DataFrameContainer` with the updated `Arc<DataFormat>`,
///   ensuring the table re-renders with the new settings.
#[derive(Debug, Clone, PartialEq)]
pub struct DataFormat {
    /// Stores the *current* alignment setting for each `DataType`, overriding `DEFAULT_ALIGNMENTS`.
    /// - Modified by UI widgets rendered by `render_alignment_panel`.
    /// - Read by `get_decimal_and_layout` to determine `egui::Layout` for table cells.
    pub alignments: HashMap<DataType, Align>,

    /// Controls the table column sizing strategy (`container.rs::build_table`).
    /// - `true`: `Column::auto()` (content-based, potentially slower).
    /// - `false` (Default): `Column::initial()` (uniform fixed widths, faster).
    /// - **Important**: Toggling this changes the `TableBuilder` ID salt in `container.rs::build_table`,
    ///   forcing egui to discard cached column widths and apply the new strategy.
    pub auto_col_width: bool,

    /// Number of decimal places for displaying floats (`Float32`, `Float64`).
    /// - Modified by the `DragValue` in `render_decimal_input`.
    /// - Used by `get_decimal_and_layout` / `data_container.rs::format_cell_value`.
    ///   (Note: `decimal_and_layout_v2` might override for specific columns).
    pub decimal: usize,

    /// User-configurable *additional* vertical padding for the table header row.
    /// - Applied in `container.rs::build_table` when calculating header height.
    /// - Modified by `DragValue` in `render_header_padding_input` (if `use_enhanced_header`).
    pub header_padding: f32,

    /// Toggles the table header's visual style and click behavior.
    /// - Modified by checkbox in `render_header`.
    /// - Read by `container.rs::render_table_header` to choose between:
    ///   - `true` (Default): Enhanced (styled text, wrapping, icon-only sort click).
    ///   - `false`: Simple (plain button, non-wrapping, full button sort click).
    pub use_enhanced_header: bool,
}

// --- Implementations ---

impl Default for DataFormat {
    /// Creates a `DataFormat` with default settings.
    /// Initializes `alignments` by cloning the `DEFAULT_ALIGNMENTS` map.
    fn default() -> Self {
        DataFormat {
            alignments: DEFAULT_ALIGNMENTS.clone(), // Clone defaults for this instance.
            auto_col_width: true,                   // Default automatic content-based sizing.
            decimal: 2,                             // Default float precision.
            header_padding: 5.0,                    // Default extra padding for enhanced header.
            use_enhanced_header: true,              // Default to enhanced header style.
        }
    }
}

impl DataFormat {
    /// Gets the default header padding value defined in `DataFormat::default()`.
    /// Used internally, e.g., by container.rs when `use_enhanced_header` is false.
    pub fn get_default_padding(&self) -> f32 {
        // Retrieve the padding value from a temporary default instance.
        Self::default().header_padding
    }

    /// Renders UI controls for modifying format settings in the side panel ("Format" section).
    ///
    /// ## Change Detection & Update Flow:
    /// 1. **Capture Initial State:** Clones `self` before rendering widgets (`format_former`).
    /// 2. **Render Widgets:** Widgets (`checkbox`, `radio_value`, `DragValue`) bind to `&mut self`
    ///    and modify its fields directly based on user interaction within this frame.
    /// 3. **Compare States:** After rendering all widgets, compares the potentially modified `self`
    ///    with `format_former`.
    /// 4. **Signal Change:** If `self != format_former`, returns `Some(self.clone())`. `layout.rs` uses
    ///    this signal to trigger an asynchronous `DataFrameContainer::update_format` task, applying
    ///    the changes efficiently without reloading data. If no change, returns `None`.
    ///
    /// ### Arguments
    /// * `ui`: Mutable reference to the `egui::Ui` context for drawing.
    ///
    /// ### Returns
    /// * `Option<DataFormat>`: `Some(updated_format)` if a setting was changed, otherwise `None`.
    pub fn render_format(&mut self, ui: &mut Ui) -> Option<DataFormat> {
        // 1. Capture the state *before* potential modifications.
        let format_former = self.clone();
        let mut result = None; // Assume no change initially.

        // Layout setup for the format section UI.
        let width_max = ui.available_width();
        let width_min = 200.0; // Minimum reasonable width for the controls.
        let grid = Grid::new("data_format_grid")
            .num_columns(2) // Labels in col 0, widgets in col 1.
            .spacing([10.0, 20.0])
            .striped(true);

        ui.allocate_ui_with_layout(
            Vec2::new(width_max, ui.available_height()),
            Layout::top_down(Align::LEFT),
            |ui| {
                // 2. Render UI Controls within the Grid. These potentially modify `self`.
                grid.show(ui, |ui| {
                    ui.set_min_width(width_min);

                    self.render_alignment_panel(ui); // Modifies `self.alignments`.
                    self.render_decimal_input(ui); // Modifies `self.decimal`.
                    self.render_auto_col(ui); // Modifies `self.auto_col_width`.
                    self.render_header(ui); // Modifies `self.use_enhanced_header`.

                    // Only show padding control if the enhanced header is active.
                    if self.use_enhanced_header {
                        self.render_header_padding_input(ui); // Modifies `self.header_padding`.
                    }

                    // 3. Detect Changes after all widgets rendered for this frame.
                    if *self != format_former {
                        result = Some(self.clone()); // Signal the change with the new state.
                        tracing::debug!(
                            "Format change detected in render_format. New state: {:#?}",
                            self
                        );
                    }
                });
            },
        );

        // 4. Return the result, signalling to layout.rs whether an update is needed.
        result
    }

    /// Renders the collapsible UI section for configuring text alignment per `DataType`.
    ///
    /// Uses a nested `egui::Grid` within a `CollapsingHeader`. Calls `show_alignment_row`
    /// for each data type, passing `&mut self.alignments` which is modified directly by the
    /// radio buttons in the helper function.
    fn render_alignment_panel(&mut self, ui: &mut Ui) {
        ui.label("Alignment:"); // Section label.

        // Group alignment settings.
        ui.collapsing("Data Types", |ui| {
            // Nested grid for layout (DataType | Left | Center | Right).
            Grid::new("align_grid")
                .num_columns(4)
                .spacing([10.0, 10.0])
                .striped(true)
                .show(ui, |ui| {
                    // Render rows for relevant DataTypes.
                    self.show_alignment_row(ui, &DataType::Float64);
                    self.show_alignment_row(ui, &DataType::Float32);
                    self.show_alignment_row(ui, &DataType::Int64);
                    self.show_alignment_row(ui, &DataType::Int32);
                    self.show_alignment_row(ui, &DataType::Int16);
                    self.show_alignment_row(ui, &DataType::Int8);
                    self.show_alignment_row(ui, &DataType::UInt64);
                    self.show_alignment_row(ui, &DataType::UInt32);
                    self.show_alignment_row(ui, &DataType::UInt16);
                    self.show_alignment_row(ui, &DataType::UInt8);
                    self.show_alignment_row(ui, &DataType::Date);
                    self.show_alignment_row(ui, &DataType::Time);
                    self.show_alignment_row(ui, &DataType::Boolean);
                    self.show_alignment_row(ui, &DataType::Binary);
                    self.show_alignment_row(ui, &DataType::String);
                });
        });
        ui.end_row(); // End row in the outer format grid.
    }

    /// Renders a single row in the alignment grid for a specific `DataType`.
    fn show_alignment_row(&mut self, ui: &mut Ui, data_type: &DataType) {
        // 1. Get/Insert alignment setting for this data type. Defaults to Left if not present.
        let current_align: &mut Align = self
            .alignments
            .entry(data_type.clone())
            .or_insert(Align::LEFT);

        // 2. Display the DataType name.
        ui.label(format!("{data_type:?}"));

        // 3. Render radio buttons. `radio_value` updates `current_align` (mutates map) on click.
        ui.radio_value(current_align, Align::LEFT, "Left")
            .on_hover_text("Align column content to the left.");
        ui.radio_value(current_align, Align::Center, "Center")
            .on_hover_text("Align column content to the center.");
        ui.radio_value(current_align, Align::RIGHT, "Right")
            .on_hover_text("Align column content to the right.");

        ui.end_row(); // End this row in the alignment grid.
    }

    /// Renders the `DragValue` widget for setting the number of decimal places (`self.decimal`).
    /// Modifies `self.decimal` directly based on user input.
    fn render_decimal_input(&mut self, ui: &mut Ui) {
        let decimal_max = 10;
        ui.label("Decimals:");
        // Bind DragValue to `self.decimal`.
        ui.add(
            DragValue::new(&mut self.decimal)
                .speed(1) // Integer steps.
                .range(0..=decimal_max), // Sensible range for display.
        )
        .on_hover_text(format!(
            "Number of decimal places for floating-point numbers.\n\
            Maximum decimal places: {decimal_max}"
        ));
        ui.end_row();
    }

    /// Renders the checkbox for toggling automatic column width (`self.auto_col_width`).
    /// Modifies `self.auto_col_width` directly.
    ///
    /// Toggling this is detected by `render_format`, triggering `update_format`. The change
    /// in `auto_col_width` causes `container.rs::build_table` to use a different `egui::Id`,
    /// resetting table layout state (like manual widths) and applying the new sizing mode.
    fn render_auto_col(&mut self, ui: &mut Ui) {
        ui.label("Auto Col Width:");
        // Bind checkbox to `self.auto_col_width`.
        ui.checkbox(&mut self.auto_col_width, "").on_hover_text(
            "Enable: Size columns based on content (slower).\n\
            Disable: Use uniform initial widths (faster), allows manual resize.",
        );
        ui.end_row();
    }

    /// Renders the checkbox for toggling the table header style (`self.use_enhanced_header`).
    /// Modifies `self.use_enhanced_header` directly. Affects rendering in `container.rs::render_table_header`.
    fn render_header(&mut self, ui: &mut Ui) {
        ui.label("Enhanced Header:");
        // Bind checkbox to `self.use_enhanced_header`.
        ui.checkbox(&mut self.use_enhanced_header, "")
            .on_hover_text(
                "Enable: Styled, wrapping text with icon-only sort click.\n\
                Disable: Simpler button header.",
            );
        ui.end_row();
    }

    /// Renders the `DragValue` widget for adjusting header padding (`self.header_padding`).
    /// Shown conditionally based on `self.use_enhanced_header`.
    /// Modifies `self.header_padding` directly. Affects header height calculation in `container.rs::build_table`.
    fn render_header_padding_input(&mut self, ui: &mut Ui) {
        let heigth_max = 800.0;
        ui.label("Header Padding:");
        // Bind DragValue to `self.header_padding`.
        ui.add(
            DragValue::new(&mut self.header_padding)
                .speed(0.5)
                .range(0.0..=heigth_max) // Reasonable padding range.
                .suffix(" px"), // Display units.
        )
        .on_hover_text(format!(
            "Additional vertical padding for the enhanced table header.\n\
            Maximum header padding: {:.*} px",
            1, heigth_max
        ));
        ui.end_row();
    }
}

//----------------------------------------------------------------------------//
//                                   Tests                                    //
//----------------------------------------------------------------------------//

/// Run tests with:
/// `cargo test -- --show-output tests_format`
#[cfg(test)]
mod tests_format {
    use polars::prelude::*;

    #[test]
    fn test_quoted_bool_ints() -> PolarsResult<()> {
        let csv = r#"
foo,bar,baz
1,"4","false"
3,"5","false"
5,"6","true"
"#;
        let file = std::io::Cursor::new(csv); // Create a cursor for the in-memory CSV data.
        let df = CsvReader::new(file).finish()?; // Read the CSV data into a DataFrame.
        println!("df = {df}");

        // Define the expected DataFrame.
        let expected = df![
            "foo" => [1, 3, 5],
            "bar" => [4, 5, 6],
            "baz" => [false, false, true],
        ]?;

        // Assert that the loaded DataFrame equals the expected DataFrame.
        assert!(df.equals_missing(&expected));
        Ok(())
    }
}
