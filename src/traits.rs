//! Defines custom traits, trait implementations for `egui` types, and general utility traits.
//!
//! This module centralizes extensions to existing types (`egui::Context`, `egui::Ui`, `std::path::Path`, `Vec`)
//! and defines interfaces (`Notification`, `SortableHeaderRenderer`) for common UI patterns.
//! It interacts primarily with `layout.rs` (for styling, notifications) and `container.rs` (for header rendering).

use crate::HeaderSortState; // Use the interaction enum for UI state

use egui::{
    Color32, Context,
    FontFamily::Proportional,
    FontId, Frame, Response, RichText, Sense, Spacing, Stroke, Style,
    TextStyle::{self, Body, Button, Heading, Monospace, Small},
    Ui, Vec2, Visuals, Window,
    style::ScrollStyle,
};
use polars::prelude::*;
use std::{collections::HashSet, ffi::OsStr, hash::Hash, path::Path};

/// Defines custom text styles for the egui context.
/// Overrides default `egui` font sizes for different logical text styles (Heading, Body, etc.).
/// Used by `MyStyle::set_style_init`.
pub const CUSTOM_TEXT_STYLE: [(egui::TextStyle, egui::FontId); 5] = [
    (Heading, FontId::new(20.0, Proportional)),
    (Body, FontId::new(18.0, Proportional)),
    (Button, FontId::new(18.0, Proportional)),
    (Monospace, FontId::new(18.0, Proportional)), // Adjusted size for Proportional font
    (Small, FontId::new(16.0, Proportional)),
];

/// A trait for applying custom styling to the `egui` context (`Context`).
/// Used once at startup by `layout.rs::PolarsViewApp::new`.
pub trait MyStyle {
    /// Applies a pre-defined application style to the `egui` context.
    fn set_style_init(&self, visuals: Visuals);
}

impl MyStyle for Context {
    /// Configures the application's look and feel (theme, spacing, text styles) by modifying `egui::Style`.
    ///
    /// ### Logic
    /// 1. Define custom scrollbar settings (`ScrollStyle`).
    /// 2. Define custom widget spacing (`Spacing`).
    /// 3. Create a full `Style` struct incorporating `Visuals` (theme), `Spacing`, and `CUSTOM_TEXT_STYLE`.
    /// 4. Apply the constructed `Style` to the `egui::Context`.
    fn set_style_init(&self, visuals: Visuals) {
        // 1. Define ScrollStyle.
        let scroll = ScrollStyle {
            handle_min_length: 32.0,
            ..ScrollStyle::default()
        };

        // 2. Define Spacing.
        let spacing = Spacing {
            scroll,
            item_spacing: [8.0, 6.0].into(),
            ..Spacing::default()
        };

        // 3. Create the main Style struct.
        let style = Style {
            visuals,                               // Apply provided theme (Light/Dark).
            spacing,                               // Apply custom spacing.
            text_styles: CUSTOM_TEXT_STYLE.into(), // Apply custom text styles.
            ..Style::default()
        };

        // 4. Set the style on the egui Context.
        self.set_global_style(style);
    }
}

/// Trait for modal Notification windows (like errors or settings dialogs).
/// Allows `layout.rs` to manage different notification types polymorphically via `Box<dyn Notification>`.
pub trait Notification: Send + Sync + 'static {
    /// Renders the notification window using `egui::Window`.
    /// Called repeatedly by `layout.rs::check_notification` while the notification is active.
    ///
    /// ### Returns
    /// `true` if the window should remain open, `false` if closed.
    fn show(&mut self, ctx: &Context) -> bool;
}

/// Placeholder Notification struct for future Settings dialog. Implements `Notification`.
pub struct Settings {}

impl Notification for Settings {
    /// Renders the placeholder Settings window.
    ///
    /// ### Logic
    /// 1. Define `open` state (initially `true`).
    /// 2. Create `egui::Window` bound to `open`.
    /// 3. Configure window (e.g., non-collapsible).
    /// 4. Define content (currently disabled).
    /// 5. Return the `open` state (whether the window is still visible).
    fn show(&mut self, ctx: &Context) -> bool {
        let mut open = true; // 1. Window starts open.

        // 2. Create window.
        Window::new("Settings")
            .collapsible(false) // 3. Configure.
            .open(&mut open)
            .show(ctx, |ui| {
                ctx.style_ui(ui, egui::Theme::Dark);
                ui.disable(); // 4. Placeholder content.
            });

        open // 5. Return state.
    }
}

/// Notification struct for displaying error messages. Implements `Notification`.
pub struct Error {
    /// The error message content. Set by the caller in `layout.rs`.
    pub message: String,
}

impl Notification for Error {
    /// Renders the Error notification window.
    fn show(&mut self, ctx: &Context) -> bool {
        let mut open = true; // Window starts open.
        let width_min = 500.0; // Minimum width for the error window content.

        // Create window.
        Window::new("Error")
            .collapsible(false) // Configure
            .resizable(true) // Allow resizing if needed for long messages
            .min_width(width_min)
            .open(&mut open)
            .show(ctx, |ui| {
                // Add styled frame.
                Frame::default()
                    .fill(Color32::from_rgb(255, 200, 200)) // Light red bg
                    .stroke(Stroke::new(1.0_f32, Color32::DARK_RED)) // Dark red border
                    .inner_margin(10.0) // Add padding inside the frame, around the content.
                    .show(ui, |ui| {
                        ui.set_max_width(ui.available_width()); // Allow text to wrap within frame
                        ui.colored_label(Color32::BLACK, &self.message);
                    });
            });

        open // Return state.
    }
}

/// Trait defining a widget for rendering a sortable table header cell.
/// Provides a consistent interface for `container.rs::render_table_header`.
pub trait SortableHeaderRenderer {
    /// Renders a table header cell with sort indicator (including index if sorted) and name.
    ///
    /// ### Arguments
    /// * `column_name`: The text label for the column.
    /// * `interaction_state`: The `HeaderSortState` for *this* column (NotSorted, Ascending, Descending).
    /// * `sort_index`: `Option<usize>` (0-based) indicating sort precedence if this column is currently sorted.
    /// * `use_enhanced_style`: Controls visual appearance (wrapping, color).
    ///
    /// ### Returns
    /// * `egui::Response`: Interaction response from the clickable sort icon/indicator. The caller handles clicks.
    fn render_sortable_header(
        &mut self,
        column_name: &str,
        interaction_state: &HeaderSortState, // Input: How this header should look based on clicks
        sort_index: Option<usize>,           // Input: 1-based index if part of sort criteria
        use_enhanced_style: bool,
    ) -> Response;
}

impl SortableHeaderRenderer for Ui {
    fn render_sortable_header(
        &mut self,
        column_name: &str,
        interaction_state: &HeaderSortState,
        sort_index: Option<usize>,
        use_enhanced_style: bool,
    ) -> Response {
        // 1. Informações de estilo e string do ícone atual
        let column_name_color = get_column_header_text_color(self.visuals());
        let icon_string = interaction_state.get_icon(sort_index);
        let text_style = TextStyle::Button;

        // 2. Calcula a largura do contêiner com base no maior tamanho real possível
        //    (testando todos os 5 símbolos com 2 dígitos de precedência: 99)
        let candidates = ["99↕", "99⏷", "99⏶", "99⬇", "99⬆"];
        let icon_container_size = candidates
            .iter()
            .map(|sample| calculate_icon_container_size_for_string(self, &text_style, sample))
            .fold(Vec2::ZERO, |acc, size| acc.max(size));

        // 3. Layout horizontal
        let outer_response = self.horizontal_centered(|ui| {
            ui.style_mut().override_text_style = Some(text_style.clone());
            let msg1 = format!("Click to sort by: {column_name:#?}");
            let msg2 = "↕ Not Sorted";
            let msg3 = "Sort with Nulls First:";
            let msg4 = "    ⏷ Sort in Descending order";
            let msg5 = "    ⏶ Sort in Ascending order";
            let msg6 = "Sort with Nulls Last:";
            let msg7 = "    ⬇ Sort in Descending order";
            let msg8 = "    ⬆ Sort in Ascending order";
            let msg = [&msg1, "", msg2, msg3, msg4, msg5, msg6, msg7, msg8].join("\n");

            // 4. Contêiner dimensionado com .extend() no Label para evitar reticências (...)
            let icon_response = ui
                .add_sized(icon_container_size, |ui: &mut Ui| {
                    ui.centered_and_justified(|ui| {
                        ui.add(
                            egui::Label::new(&icon_string)
                                .extend() // Impede truncamento com reticências
                                .sense(Sense::click()),
                        )
                    })
                    .inner
                })
                .on_hover_text(msg);

            // 5. Rótulo com o nome da coluna
            ui.add(if use_enhanced_style {
                egui::Label::new(RichText::new(column_name).color(column_name_color)).wrap()
            } else {
                egui::Label::new(RichText::new(column_name))
            });

            icon_response
        });

        outer_response.inner
    }
}

/// Helper: Calcula a dimensão do contêiner para uma amostra de texto com margem de segurança.
fn calculate_icon_container_size_for_string(
    ui: &Ui,
    text_style: &TextStyle,
    sample_str: &str,
) -> Vec2 {
    let text_height = ui.text_style_height(text_style);

    let max_width = {
        let font_id = text_style.resolve(ui.style());
        let galley = ui
            .fonts_mut(|f| f.layout_no_wrap(sample_str.to_string(), font_id, Color32::PLACEHOLDER));
        // Adiciona margem de segurança de 4.0 px para absorver variações de renderização/DPI
        (galley.size().x + 4.0).ceil()
    };

    Vec2::new(max_width, text_height)
}

/// Helper: Determines header text color based on theme for contrast.
/// Called by `render_sortable_header`.
fn get_column_header_text_color(visuals: &Visuals) -> Color32 {
    if visuals.dark_mode {
        Color32::from_rgb(160, 200, 255) // Lighter blue for dark mode
    } else {
        Color32::from_rgb(0, 80, 160) // Darker blue for light mode
    }
}

/// Trait to extend `Path` with a convenient method for getting the lowercase file extension.
/// Used by `extension.rs`, `file_dialog.rs`, `filters.rs`.
pub trait PathExtension {
    /// Returns the file extension as a lowercase `String`, or `None`.
    fn extension_as_lowercase(&self) -> Option<String>;
}

impl PathExtension for Path {
    /// Implementation for `Path`. Gets extension, converts to &str (lossy), then lowercases.
    ///
    /// ### Logic
    /// 1. Call `self.extension()` -> `Option<&OsStr>`.
    /// 2. Convert `OsStr` to `&str` via `to_str` -> `Option<&str>`.
    /// 3. Map `&str` to lowercase `String` -> `Option<String>`.
    fn extension_as_lowercase(&self) -> Option<String> {
        self.extension() // 1. Get OsStr extension.
            .and_then(OsStr::to_str) // 2. Try converting to &str.
            .map(str::to_lowercase) // 3. Convert to lowercase String if successful.
    }
}

/// A trait for deduplicating vectors while preserving the original order of elements.
/// Added to `Vec<T>`. Used by `filters.rs` for delimiter guessing.
pub trait UniqueElements<T> {
    /// Removes duplicate elements in place, keeping the first occurrence.
    fn unique(&mut self)
    where
        T: Eq + Hash + Clone;
}

impl<T> UniqueElements<T> for Vec<T> {
    /// Implementation using `HashSet` for efficiency.
    ///
    /// ### Logic
    /// 1. Create an empty `HashSet` to track seen elements.
    /// 2. Use `Vec::retain` to iterate and filter the vector in place.
    /// 3. Inside `retain`, try inserting a clone of the current element into the `seen` set.
    /// 4. `HashSet::insert` returns `true` if the element was *newly* inserted (i.e., first time seen).
    /// 5. Keep the element (`retain` closure returns `true`) only if `insert` returned `true`.
    fn unique(&mut self)
    where
        T: Eq + Hash + Clone, // Constraints required for HashSet.
    {
        let mut seen = HashSet::new(); // 1. Track seen elements.
        self.retain(|x| {
            // 2. Filter in place.
            seen.insert(x.clone()) // 3, 4, 5: Keep if insert succeeds (element is new).
        });
    }
}

/// Trait extension for `LazyFrame` to provide additional functionalities.
pub trait LazyFrameExtension {
    /// Rounds float columns (Float32 and Float64) in a LazyFrame to a specified
    /// number of decimal places using optimized Polars expressions.
    ///
    /// Columns of other data types remain unchanged.
    fn round_float_columns(self, decimals: u32) -> Self;
}

impl LazyFrameExtension for LazyFrame {
    fn round_float_columns(self, decimals: u32) -> Self {
        // Select columns with Float32 or Float64 data types
        let float_cols_selector = dtype_cols(&[DataType::Float32, DataType::Float64])
            .as_selector()
            .as_expr();

        self.with_columns([
            // Apply the round expression directly to the selected float columns
            float_cols_selector
                .round(decimals, RoundMode::HalfAwayFromZero)
                .name()
                .keep(), // Keep original column name
        ])
    }
}

//----------------------------------------------------------------------------//
//                                   Tests                                    //
//----------------------------------------------------------------------------//

/// Run tests with:
/// `cargo test -- --show-output tests_path_extension`
#[cfg(test)]
mod tests_path_extension {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn test_extension_as_lowercase_some() {
        let path = PathBuf::from("my_file.TXT");
        assert_eq!(path.extension_as_lowercase(), Some("txt".to_string()));
    }

    // ... other path extension tests ...
    #[test]
    fn test_extension_as_lowercase_none() {
        let path = PathBuf::from("myfile");
        assert_eq!(path.extension_as_lowercase(), None);
    }
    #[test]
    fn test_extension_as_lowercase_no_final_part() {
        let path = PathBuf::from("path/to/directory/."); // Current directory in path.
        assert_eq!(path.extension_as_lowercase(), None);
    }
    #[test]
    fn test_extension_as_lowercase_multiple_dots() {
        let path = PathBuf::from("file.name.with.multiple.dots.ext");
        assert_eq!(path.extension_as_lowercase(), Some("ext".to_string()));
    }
}

/// Run tests with:
/// `cargo test -- --show-output tests_unique`
#[cfg(test)]
mod tests_unique {
    use super::*;

    #[test]
    fn test_unique() {
        let mut vec = vec![1, 2, 2, 3, 1, 4, 3, 2, 5];
        vec.unique();
        assert_eq!(vec, vec![1, 2, 3, 4, 5]);
    }

    // ... other unique tests ...
    #[test]
    fn test_unique_empty() {
        let mut vec: Vec<i32> = vec![];
        vec.unique();
        assert_eq!(vec, Vec::<i32>::new());
    }
    #[test]
    fn test_unique_all_same() {
        let mut vec = vec![1, 1, 1, 1, 1];
        vec.unique();
        assert_eq!(vec, vec![1]);
    }
    #[test]
    fn test_unique_strings() {
        let mut vec = vec!["a", "b", "b", "c", "a", "d", "c", "b", "e"];
        vec.unique();
        assert_eq!(vec, vec!["a", "b", "c", "d", "e"]);
    }
}

/// Run tests with:
/// `cargo test -- --show-output tests_format_columns`
#[cfg(test)]
mod tests_format_columns {
    use super::*;

    /// `cargo test -- --show-output test_format_col`
    #[test]
    fn round_float_columns() -> PolarsResult<()> {
        let df_input = df!(
            "int_col" => &[Some(1), Some(2), None],
            "f32_col" => &[Some(1.2345f32), None, Some(3.9876f32)],
            "f64_col" => &[None, Some(10.11111), Some(-5.55555)],
            "str_col" => &[Some("a"), Some("b"), Some("c")],
            "float_col" => &[1.1234, 2.5650001, 3.965000],
            "opt_float" => &[Some(1.0), None, Some(3.45677)],
        )?;
        let df_expected = df!(
            "int_col" => &[Some(1), Some(2), None],
            "f32_col" => &[Some(1.23f32), None, Some(3.99f32)],
            "f64_col" => &[None, Some(10.11), Some(-5.56)],
            "str_col" => &[Some("a"), Some("b"), Some("c")],
            "float_col" => &[1.12, 2.57, 3.97],
            "opt_float" => &[Some(1.0), None, Some(3.46)],
        )?;
        let decimals = 2;

        dbg!(&df_input);
        dbg!(&decimals);
        let df_output = df_input.lazy().round_float_columns(decimals).collect()?;
        dbg!(&df_output);

        assert!(
            df_output.equals_missing(&df_expected),
            "Failed round float columns.\nOutput:\n{df_output:?}\nExpected:\n{df_expected:?}"
        );

        Ok(())
    }

    #[test]
    fn round_no_float_columns() -> PolarsResult<()> {
        let df_input = df!(
            "int_col" => &[1, 2, 3],
            "str_col" => &["x", "y", "z"]
        )?;
        let df_expected = df_input.clone();
        let decimals = 2;

        dbg!(&df_input);
        dbg!(&decimals);
        let df_output = df_input.lazy().round_float_columns(decimals).collect()?;
        dbg!(&df_output);

        assert!(df_output.equals(&df_expected)); // equals is fine here as no nulls involved
        Ok(())
    }

    #[test]
    fn round_with_zero_decimals() -> PolarsResult<()> {
        let df_input = df!(
            "f64_col" => &[1.2, 1.8, -0.4, -0.9]
        )?;
        let df_expected = df!(
            "f64_col" => &[1.0, 2.0, 0.0, -1.0] // Rounding 0.5 up, -0.5 towards zero (check Polars convention)
                                                // Note: Standard rounding (>= .5 rounds away from zero) means 1.8 -> 2.0, -0.9 -> -1.0
                                                // -0.4 -> 0.0. Need to confirm Polars specific behavior if critical.
                                                // It usually follows standard round half away from zero.
        )?;
        let decimals = 0;

        dbg!(&df_input);
        dbg!(&decimals);
        let df_output = df_input.lazy().round_float_columns(decimals).collect()?;
        dbg!(&df_output);

        assert!(df_output.equals_missing(&df_expected));
        Ok(())
    }
}
