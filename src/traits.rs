use crate::SortState;

use egui::{
    Align, Color32, Context,
    FontFamily::Proportional,
    FontId, Frame, Layout, Response, Stroke,
    TextStyle::{Body, Button, Heading, Monospace, Small},
    Ui, Vec2, WidgetText, Window,
};

use std::{ffi::OsStr, path::Path, sync::Arc};

/// A trait for applying custom styling to the egui context.
pub trait MyStyle {
    /// Sets the initial style for the egui context.
    fn set_style_init(&self);
}

impl MyStyle for Context {
    /// Specifies the look and feel of egui.
    ///
    /// <https://docs.rs/egui/latest/egui/style/struct.Style.html>
    fn set_style_init(&self) {
        // Get current context style
        let mut style = (*self.style()).clone();

        // Redefine text_styles
        style.text_styles = [
            (Small, FontId::new(12.0, Proportional)),
            (Body, FontId::new(16.0, Proportional)),
            (Monospace, FontId::new(14.0, Proportional)),
            (Button, FontId::new(14.0, Proportional)),
            (Heading, FontId::new(14.0, Proportional)),
        ]
        .into();

        style.spacing.item_spacing.x = 8.0;
        style.spacing.item_spacing.y = 6.0;

        // Mutate global style with above changes
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

#[cfg(test)]
mod tests {
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
