//! Defines custom traits, trait implementations for `egui` types, and general utility traits.
//!
//! This module centralizes extensions to existing types (`egui::Context`, `egui::Ui`, `std::path::Path`, `Vec`)
//! and defines interfaces (`Notification`, `SortableHeaderRenderer`) for common UI patterns.
//! It interacts primarily with `layout.rs` (for styling, notifications) and `container.rs` (for header rendering).

use crate::HeaderSortState; // Use the interaction enum for UI state

use egui::{
    Align, Color32, Context,
    FontFamily::Proportional,
    FontId, Frame, Layout, Response, RichText, Sense, Spacing, Stroke, Style,
    TextStyle::{self, Body, Button, Heading, Monospace, Small},
    Ui, Vec2, Visuals, Window,
    style::ScrollStyle,
};

use std::{collections::HashSet, ffi::OsStr, hash::Hash, path::Path};

/// Defines custom text styles for the egui context.
/// Overrides default `egui` font sizes for different logical text styles (Heading, Body, etc.).
/// Used by `MyStyle::set_style_init`.
pub const CUSTOM_TEXT_STYLE: [(egui::TextStyle, egui::FontId); 5] = [
    (Heading, FontId::new(18.0, Proportional)),
    (Body, FontId::new(16.0, Proportional)),
    (Button, FontId::new(16.0, Proportional)),
    (Monospace, FontId::new(15.0, Proportional)), // Adjusted size for Proportional font
    (Small, FontId::new(14.0, Proportional)),
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
        self.set_style(style);
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
    ///
    /// ### Logic
    /// 1. Define `open` state (initially `true`).
    /// 2. Create `egui::Window` named "Error", bound to `open`.
    /// 3. Configure window (e.g., non-collapsible).
    /// 4. Layout content area (fixed width, alignment).
    /// 5. Use a `Frame` with distinct styling for visual emphasis.
    /// 6. Display the error `message` within the frame.
    /// 7. Return the `open` state.
    fn show(&mut self, ctx: &Context) -> bool {
        let mut open = true; // 1. Window starts open.

        // 2. Create window.
        Window::new("Error")
            .collapsible(false) // 3. Configure.
            .open(&mut open)
            .show(ctx, |ui| {
                // 4. Layout content.
                let width_max = ui.available_width() * 0.80;
                ui.allocate_ui_with_layout(
                    Vec2::new(width_max, ui.available_height()),
                    Layout::top_down(Align::LEFT),
                    |ui| {
                        // 5. Add styled frame.
                        Frame::default()
                            .fill(Color32::from_rgb(255, 200, 200)) // Light red bg
                            .stroke(Stroke::new(1.0, Color32::DARK_RED)) // Dark red border
                            .outer_margin(2.0)
                            .inner_margin(10.0)
                            .show(ui, |ui| {
                                // 6. Display message.
                                ui.colored_label(Color32::BLACK, &self.message);
                                ui.disable(); // Optionally disable interaction within.
                            });
                    },
                );
            });

        open // 7. Return state.
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
    /// Implements header rendering for `egui::Ui`. Displays icon (with optional index) and text label horizontally.
    /// Icon/index is drawn centered within a pre-calculated sized container to minimize text shifting.
    ///
    /// ### Logic
    /// 1. Get styling info: text color based on theme, combined icon/index string using `interaction_state.get_icon(sort_index)`. Define base `TextStyle`.
    /// 2. Calculate size needed for the icon/index container using `calculate_icon_container_size_for_string` with a sample wide string (e.g., "10↕").
    /// 3. Use `ui.horizontal` for the overall cell layout.
    /// 4. Add a sized container (`ui.add_sized`) for the icon/index:
    ///    - Inside the closure, draw a centered, clickable `Label` using the icon/index string from step 1.
    ///    - Return the `Label`'s `Response` from the closure.
    /// 5. Add hover text to the `Response` captured from `add_sized`.
    /// 6. Add the column name `Label` (styling depends on `use_enhanced_style`).
    /// 7. Return the icon/index label's `Response`.
    fn render_sortable_header(
        &mut self,
        column_name: &str,
        interaction_state: &HeaderSortState, // Use the interaction enum
        sort_index: Option<usize>,           // Receive the 0-based index
        use_enhanced_style: bool,
    ) -> Response {
        // 1. Get styling info and icon string.
        let column_name_color = get_column_header_text_color(self.visuals());
        // Get icon possibly including index number (e.g., "1▲", "↕"). get_icon handles None index.
        let icon_string = interaction_state.get_icon(sort_index);
        let text_style = TextStyle::Button; // Base style for consistency

        // 2. Calculate required container size for the potentially wider icon+index string.
        let max_potential_icon_str = "99↕"; // Estimate max width needed (adjust if sort criteria > 99 expected)
        let icon_container_size =
            calculate_icon_container_size_for_string(self, &text_style, max_potential_icon_str);

        // 3. Use horizontal layout.
        let outer_response = self.horizontal_centered(|ui| {
            ui.style_mut().override_text_style = Some(text_style.clone());

            // 4. Add sized container and draw the icon/index string inside.
            let icon_response = ui
                .add_sized(icon_container_size, |ui: &mut Ui| {
                    // Draw centered label with combined icon/index, make it clickable.
                    ui.centered_and_justified(|ui| {
                        ui.add(egui::Label::new(&icon_string).sense(Sense::click()))
                    })
                    .inner // Return the Label's Response
                })
                // 5. Add hover text to the response from the sized container (which is the Label's response).
                .on_hover_text(format!("Click to sort by: {}", column_name));

            // 6. Add column name label.
            ui.add(if use_enhanced_style {
                // Enhanced: Use color and enable text wrapping.
                egui::Label::new(RichText::new(column_name).color(column_name_color)).wrap()
            } else {
                // Simple: Default color, no explicit wrapping (might wrap based on outer container).
                egui::Label::new(RichText::new(column_name))
            });

            // Return the captured icon Response from the horizontal closure.
            icon_response
        }); // End horizontal layout

        // 7. Extract and return the icon's response from the horizontal layout's inner result.
        outer_response.inner
    }
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

/// Helper: Calculates size needed for the icon container, using a sample string for max width.
/// Ensures enough space for icons potentially combined with sort order index numbers.
/// Called by `render_sortable_header`.
///
/// ### Logic
/// 1. Get text height from the provided `TextStyle`.
/// 2. Layout the `sample_str` using the `TextStyle`'s font to get its width.
/// 3. Add a small horizontal buffer to the calculated width.
/// 4. Return `Vec2` with buffered width and original height.
fn calculate_icon_container_size_for_string(
    ui: &Ui,
    text_style: &TextStyle,
    sample_str: &str,
) -> Vec2 {
    // 1. Get height.
    let text_height = ui.text_style_height(text_style);

    // 2. Calculate width based on the sample string.
    let max_width = {
        let font_id = text_style.resolve(ui.style());
        // Layout the sample string to find its rendered width.
        let galley =
            ui.fonts(|f| f.layout_no_wrap(sample_str.to_string(), font_id, Color32::PLACEHOLDER));
        // 3. Add buffer.
        (galley.size().x + 2.0).ceil() // Use ceiling to ensure enough space.
    };

    // 4. Return size.
    Vec2::new(max_width, text_height)
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

// --- Unit Tests ---

// (Tests for PathExtension and UniqueElements remain valid and unchanged)
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
