use crate::SortState;

use egui::{
    Align, Color32, Context,
    FontFamily::Proportional,
    FontId, Frame, Layout, Response, RichText, Sense, Spacing, Stroke, Style,
    TextStyle::{self, Body, Button, Heading, Monospace, Small},
    Ui, Vec2, Visuals, WidgetText, Window,
    style::ScrollStyle,
};

use std::{collections::HashSet, ffi::OsStr, hash::Hash, path::Path, sync::Arc};

/// Defines custom text styles for the egui context.
///
/// Each entry consists of a `TextStyle` (e.g., `Small`, `Body`) and a `FontId`
/// that specifies the font size and family (here, all are Proportional).
pub const CUSTOM_TEXT_STYLE: [(egui::TextStyle, egui::FontId); 5] = [
    (Heading, FontId::new(18.0, Proportional)),
    (Body, FontId::new(16.0, Proportional)),
    (Button, FontId::new(16.0, Proportional)),
    (Monospace, FontId::new(15.0, Proportional)),
    (Small, FontId::new(14.0, Proportional)),
];

/// A trait for applying custom styling to the egui context.
pub trait MyStyle {
    /// Sets the initial style for the egui context.
    fn set_style_init(&self, visuals: Visuals);
}

impl MyStyle for Context {
    /// Specifies the look and feel of egui.
    ///
    /// <https://docs.rs/egui/latest/egui/style/struct.Style.html>
    fn set_style_init(&self, visuals: Visuals) {
        let scroll = ScrollStyle {
            handle_min_length: 32.0,
            ..ScrollStyle::default()
        };

        let spacing = Spacing {
            scroll,
            item_spacing: [8.0, 6.0].into(), // Horizontal and vertical spacing between items
            ..Spacing::default()
        };

        let style = Style {
            visuals,
            spacing,
            text_styles: CUSTOM_TEXT_STYLE.into(),
            // You can customize the number formatter here if needed:
            // style.number_formatter = NumberFormatter::new(formatter);
            ..Style::default()
        };

        self.set_style(style);
    }
}

// Trait for Notification windows.
pub trait Notification: Send + Sync + 'static {
    /// Shows the notification window and returns whether it should remain open.
    fn show(&mut self, ctx: &Context) -> bool;
}

// Settings Notification struct (currently disabled).
pub struct Settings {}

impl Notification for Settings {
    /// Shows the settings Notification window.
    fn show(&mut self, ctx: &Context) -> bool {
        let mut open = true;

        // Create a window named "Settings".
        Window::new("Settings")
            .collapsible(false) // Make the window non-collapsible.
            .open(&mut open) // Control the window's open state.
            .show(ctx, |ui| {
                ctx.style_ui(ui, egui::Theme::Dark); // Apply dark theme.
                ui.disable(); // Disable user interaction.
            });

        open // Return whether the window is open.
    }
}

/// Error Notification struct.
pub struct Error {
    /// The error message to display.
    pub message: String,
}

impl Notification for Error {
    /// Shows the error Notification window.
    fn show(&mut self, ctx: &Context) -> bool {
        let mut open = true;

        // Create a window named "Error".
        Window::new("Error")
            .collapsible(false) // Make the window non-collapsible.
            .open(&mut open) // Control the window's open state.
            .show(ctx, |ui| {
                // Calculate the maximum width for the content within the window.
                let width_max = ui.available_width() * 0.80;

                // Allocate the UI space with a specific layout.
                ui.allocate_ui_with_layout(
                    Vec2::new(width_max, ui.available_height()), // Set the size of the allocated space.
                    Layout::top_down(Align::LEFT), // Arrange elements from top to bottom, aligned to the left.
                    |ui| {
                        // Use a frame to visually group the error message.
                        Frame::default()
                            .fill(Color32::from_rgb(255, 200, 200)) // Light red background for error indication
                            .stroke(Stroke::new(1.0, Color32::DARK_RED)) // Dark red border for emphasis
                            .outer_margin(2.0) // Set a margin outside the frame.
                            .inner_margin(10.0) // Set a margin inside the frame.
                            .show(ui, |ui| {
                                ui.colored_label(Color32::BLACK, &self.message); // Display the error message in black.
                                ui.disable(); // Disable user interaction within the frame.
                            });
                    },
                );
            });

        open // Return whether the window is open. The window will close if `open` is set to `false` elsewhere.
    }
}

/// Trait for selection depth, used for sort state.
/// This trait is implemented for types that represent a selectable state
/// that can be cycled through (e.g., sorting states).
pub trait SelectionDepth<Icon>: PartialEq {
    /// Increments the selection depth/state (e.g., cycles to the next sort order).
    fn inc(&self) -> Self;

    /// Resets the selection depth/state to its initial value (e.g., no sorting).
    fn reset(&self) -> Self;

    /// Formats the selection depth/state into a user-displayable representation.
    fn format(&self) -> Icon
    where
        Icon: Into<WidgetText>;
}

// Trait implementation to increment the sort state.
impl SelectionDepth<String> for SortState {
    /// Cycles through the sort states: NotSorted -> Descending -> Ascending -> NotSorted.
    fn inc(&self) -> Self {
        match self {
            SortState::NotSorted(col) => SortState::Descending(col.to_owned()), // Not Sorted -> Descending.
            SortState::Ascending(col) => SortState::Descending(col.to_owned()), // Ascending -> Descending.
            SortState::Descending(col) => SortState::Ascending(col.to_owned()), // Descending -> Ascending.
        }
    }

    /// Resets the sort state to NotSorted.
    fn reset(&self) -> Self {
        // one day, I'll be proficient enough with macros that they'll be worth the time...
        match self {
            SortState::NotSorted(col) => SortState::NotSorted(col.to_owned()), // Reset to Not Sorted.
            SortState::Ascending(col) => SortState::NotSorted(col.to_owned()), // Reset to Not Sorted.
            SortState::Descending(col) => SortState::NotSorted(col.to_owned()), // Reset to Not Sorted.
        }
    }

    /// Formats the sort state into a string with an appropriate icon.
    fn format(&self) -> String {
        match self {
            SortState::Descending(col) => format!("\u{23f7} {col}"), // ⏷
            SortState::Ascending(col) => format!("\u{23f6} {col}"),  // ⏶
            SortState::NotSorted(col) => format!("\u{2195} {col}"),  // ↕
        }
    }
}

/// Trait for extra UI interactions.  This adds custom widgets/behaviors to the `egui::Ui`.
pub trait ExtraInteractions {
    /// Creates a sort button.  This button cycles through the `SelectionDepth` states.
    ///
    /// The `current_value` is an `Option<Arc<Value>>`.  Using `Arc` here allows efficient
    /// sharing of the sort state *without* requiring a mutable reference to the entire
    /// `DataFilters` struct (or the struct containing the sort state).  This is crucial
    /// for interoperability with `eframe`, where you generally have a single mutable
    /// reference to your application state at any given time.
    ///
    /// The button updates the `current_value` to reflect the new sort state, cycling
    /// through the states defined by the `SelectionDepth` trait implementation.
    fn sort_button<Value: SelectionDepth<Icon>, Icon: Into<WidgetText>>(
        &mut self,
        current_value: &mut Option<Arc<Value>>,
        selected_value: Arc<Value>,
    ) -> Response;
}

// Implementation of ExtraInteractions for Ui.
impl ExtraInteractions for Ui {
    // Implementation of the sort button.
    fn sort_button<Value: SelectionDepth<Icon>, Icon: Into<WidgetText>>(
        &mut self,
        current_value: &mut Option<Arc<Value>>,
        selected_value: Arc<Value>,
    ) -> Response {
        // Check if the selected_value is the currently active value.
        let selected = matches!(*current_value, Some(ref val) if val == &selected_value);

        // Create a selectable label as a button. The label displays the formatted
        // representation of the selected value.
        let mut response = self.selectable_label(selected, selected_value.format());

        // If the button is clicked.
        if response.clicked() {
            // If the button was already selected (representing the current sort state).
            if selected {
                // We cycle to the *next* state using `inc()`.
                *current_value = Some(selected_value.inc().into());
            } else {
                // If the button was *not* selected, we set it as the current state
                // AND THEN cycle to its next state. This ensures that clicking
                // an unsorted column first sorts it descending, rather than requiring
                // two clicks.
                if let Some(value) = current_value {
                    value.reset(); // Reset the value.  This likely does nothing, but is good practice.
                }
                *current_value = Some(selected_value.inc().into()); // set and Increment the value.
            };
            response.mark_changed(); // Mark the response as changed.
        }
        response // Return the response.
    }
}

/// Trait defining a custom widget for rendering a sortable table header cell.
pub trait SortableHeaderWidget {
    /// Renders a table header cell with a clickable sort icon and a potentially wrapping column name.
    ///
    /// Displays: `[ICON] [COLUMN NAME]`
    /// - Icon: Represents the current sort state (↕, ⏶, ⏷) and is clickable.
    /// - Column Name: Displayed with a specific color, wraps if necessary.
    ///
    /// # Arguments
    /// * `column_name`: The text label for the column.
    /// * `column_current_sort_state`: The `SortState` associated with this column to determine the icon.
    /// * `column_name_color`: The `Color32` to apply to the `column_name` text.
    ///
    /// # Returns
    /// * `egui::Response`: The interaction response specifically from the *icon* widget.
    ///                    The caller checks `response.clicked()` to trigger the sort logic.
    fn sortable_wrapping_header(
        &mut self,
        column_name: &str,
        column_current_sort_state: &SortState,
        column_name_color: Color32,
    ) -> Response;
}

impl SortableHeaderWidget for Ui {
    /// Implementation of the custom header widget drawing logic.
    fn sortable_wrapping_header(
        &mut self,
        column_name: &str,
        column_current_sort_state: &SortState,
        column_name_color: Color32,
    ) -> Response {
        // 1. Determine the appropriate unicode character for the sort icon based on the current state.
        let icon = match column_current_sort_state {
            SortState::Ascending(_) => "⏶",
            SortState::Descending(_) => "⏷",
            SortState::NotSorted(_) => "↕",
        };

        // 2. Define the layout used *within* the header cell for arranging the icon and text.
        //    Horizontal layout, center items vertically.
        let inner_layout = Layout::left_to_right(egui::Align::Center);

        // 3. Execute drawing logic within the defined layout. The `with_layout` method handles
        //    allocating space and arranging the widgets according to `inner_layout`.
        //    Crucially, the closure returns the `icon_response`, and `.inner` extracts
        //    this specific response from the `InnerResponse` wrapper.
        let icon_response = self
            .with_layout(inner_layout, |ui| {
                // 4. Apply a base text style (e.g., bold/button look) to the content within this header cell.
                ui.style_mut().override_text_style = Some(TextStyle::Button);

                // 5. Add the Icon as a `Label` widget, making it sensitive to clicks.
                let response = ui.add(
                    egui::Label::new(icon).sense(Sense::click()), // Enable click interaction ONLY for the icon.
                );

                // 6. Attach a tooltip to the icon for user guidance.
                response
                    .clone()
                    .on_hover_text(format!("Click to sort by: {}", column_name));

                // 7. Add a small horizontal space between the icon and the column name for better readability.
                ui.add_space(ui.style().spacing.item_spacing.x * 0.5);

                // 8. Add the Column Name as a `Label` widget.
                ui.add(
                    // Use RichText to apply the specified color.
                    egui::Label::new(RichText::new(column_name).color(column_name_color))
                        // Enable text wrapping. If the name is too long for the available space,
                        // it will break onto subsequent lines, increasing the widget's (and cell's) height.
                        .wrap(),
                );
                //.on_hover_text(column_name);

                // 9. Return the `Response` object specifically from the icon widget,
                //    as this is the element whose click needs to be checked by the caller.
                response // Note: this is `response`, not `icon_response` inside this scope
            })
            .inner; // Extract the icon's response from the InnerResponse returned by with_layout

        // 10. Return the captured icon response to the caller (`render_table_header`).
        icon_response
    }
}

/// Trait to extend `Path` with a convenient method for getting the lowercase file extension.
pub trait PathExtension {
    /// Returns the file extension of the path in lowercase, or `None` if the path has no extension.
    fn extension_as_lowercase(&self) -> Option<String>;
}

impl PathExtension for Path {
    /// Extracts the file extension from a Path and converts it to lowercase.
    /// This function handles various edge cases, including hidden files, files without extensions,
    ///  and paths ending in "." or "..".
    fn extension_as_lowercase(&self) -> Option<String> {
        self.extension()
            .and_then(OsStr::to_str)
            .map(str::to_lowercase)
    }
}

/// A trait for deduplicating vectors while preserving order.
///
/// This trait adds methods to `Vec<T>` for removing duplicate elements,
/// while preserving the original order.
///
/// ### Type Parameters
///
/// * `T`: The type of elements in the vector. Must implement `Eq`, `Hash`, and `Clone`.
///     - `Eq` and `Hash` are required for elements to be used as keys in a `HashSet`
///       for efficient duplicate detection.
///     - `Clone` is necessary because elements are cloned when inserted into the `HashSet`.
///
/// ### Examples
///
/// To deduplicate while keeping the original order:
///
/// ```
/// use polars_view::UniqueElements;
///
/// let mut vec = vec![3, 2, 2, 3, 5, 3, 4, 2, 1, 5];
/// vec.unique();
/// assert_eq!(vec, vec![3, 2, 5, 4, 1]);
/// ```
pub trait UniqueElements<T> {
    /// Deduplicates elements in the vector while preserving the original order.
    fn unique(&mut self)
    where
        T: Eq + Hash + Clone;
}

impl<T> UniqueElements<T> for Vec<T> {
    /// Deduplicates elements in a vector while preserving the original order.
    ///
    /// This method iterates through the vector and keeps only the first occurrence
    /// of each element, effectively removing duplicates and maintaining the order in which
    /// elements first appear. It uses a `HashSet` to efficiently track seen elements.
    fn unique(&mut self)
    where
        T: Eq + Hash + Clone,
    {
        // `HashSet` to keep track of elements we've already encountered.
        let mut seen = HashSet::new();

        // `retain` iterates through the vector and keeps elements based on the closure's return value.
        self.retain(|x| {
            // `seen.insert(x.clone())` attempts to insert a clone of the current element `x` into the `HashSet`.
            // - If `x` is already in the `HashSet`, `insert` returns `false`.
            // - If `x` is NOT in the `HashSet`, `insert` inserts it and returns `true`.
            // We want to keep the element only if it's the first time we're seeing it (i.e., `insert` returns `true`).
            seen.insert(x.clone()) // Keep element if it's the first time we see it
        });
    }
}

#[cfg(test)]
mod tests_path_extension {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn test_extension_as_lowercase_some() {
        let path = PathBuf::from("my_file.TXT");
        assert_eq!(path.extension_as_lowercase(), Some("txt".to_string()));

        let path = PathBuf::from("path/to/file.csv");
        assert_eq!(path.extension_as_lowercase(), Some("csv".to_string()));

        let path = PathBuf::from("MyFile.Parquet");
        assert_eq!(path.extension_as_lowercase(), Some("parquet".to_string()));
    }

    #[test]
    fn test_extension_as_lowercase_none() {
        let path = PathBuf::from("myfile");
        assert_eq!(path.extension_as_lowercase(), None);

        let path = PathBuf::from("path/to/directory");
        assert_eq!(path.extension_as_lowercase(), None);

        let path = PathBuf::from(".hidden_file"); // No *extension*
        assert_eq!(path.extension_as_lowercase(), None);

        let path = PathBuf::from(""); // empty path
        assert_eq!(path.extension_as_lowercase(), None);
    }

    #[test]
    fn test_extension_as_lowercase_no_final_part() {
        let path = PathBuf::from("path/to/directory/."); // Current directory in path.
        assert_eq!(path.extension_as_lowercase(), None);

        let path = PathBuf::from("path/to/directory/.."); // Parent directory
        assert_eq!(path.extension_as_lowercase(), None);
    }

    #[test]
    fn test_extension_as_lowercase_multiple_dots() {
        let path = PathBuf::from("file.name.with.multiple.dots.ext");
        assert_eq!(path.extension_as_lowercase(), Some("ext".to_string()));

        let path = PathBuf::from("file.with..dots.ext");
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

    #[test]
    fn test_unique_empty() {
        let mut vec: Vec<i32> = vec![];
        vec.unique();
        // assert_eq!(vec, vec![] as Vec<i32>);
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
