//! Defines the representation of sorting criteria for the table.
//! This module contains the core types for managing single and multiple sort column states.

use std::fmt::Debug;

/// Represents a single criterion for sorting (column name and direction).
/// Used within `DataFrameContainer` to store the cumulative sort order as `Vec<SortBy>`.
/// The order of criteria in the Vec determines sort precedence.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SortBy {
    /// The name of the column to sort by.
    pub column_name: String,
    /// The sort direction. `true` for ascending, `false` for descending.
    pub ascending: bool,
}

/// Represents the *interaction* state for sorting a specific column header in the UI.
///
/// This enum helps manage the click cycle (NotSorted -> Desc -> Asc -> NotSorted)
/// independently for each header. The actual applied cumulative sort state
/// (`Vec<SortBy>`) is stored and managed separately in `DataFrameContainer`.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum HeaderSortState {
    /// Column is not part of the current sort criteria list.
    NotSorted,
    /// Column is currently sorted ascending in the criteria list.
    Ascending,
    /// Column is currently sorted descending in the criteria list.
    Descending,
}

impl HeaderSortState {
    /// Calculates the next interaction state in the UI cycle for a header click.
    ///
    /// The cycle progresses as follows:
    /// 1. `NotSorted` -> `Descending` (first click applies descending)
    /// 2. `Descending` -> `Ascending` (second click changes to ascending)
    /// 3. `Ascending` -> `NotSorted` (third click removes the sort for this column)
    ///
    /// Called by `container.rs::render_table_header` when a click is detected.
    ///
    /// ### Returns
    /// The next `HeaderSortState` in the cycle.
    pub fn cycle_next(&self) -> Self {
        match self {
            HeaderSortState::NotSorted => HeaderSortState::Descending,
            HeaderSortState::Descending => HeaderSortState::Ascending,
            HeaderSortState::Ascending => HeaderSortState::NotSorted, // Cycle back to remove sort
        }
    }

    /// Returns a Unicode icon visually representing the interaction state.
    /// Optionally includes the sort precedence index (1-based) if the column is sorted.
    ///
    /// Used by the `SortableHeaderRenderer` trait implementation in `traits.rs`
    /// to display feedback in the table header (e.g., "1⏷", "2⏶", "↕").
    ///
    /// ### Arguments
    /// * `index`: `Option<usize>` - The 0-based index representing the sort precedence
    ///            (e.g., `0` for the primary sort column). `None` if not sorted.
    ///
    /// ### Returns
    /// A `String` containing the icon and optional index number.
    pub fn get_icon(&self, index: Option<usize>) -> String {
        let base_icon = match self {
            HeaderSortState::Descending => "⏷", // U+23F7 EJECT SYMBOL (Down arrow)
            HeaderSortState::Ascending => "⏶", // U+23F6 POWER ON SYMBOL OUTLINE (Looks like Up arrow)
            HeaderSortState::NotSorted => "↕", // U+2195 UP DOWN ARROW (Indicates sortable)
        };

        match index {
            // Add 1 to the 0-based index for user-friendly 1-based display.
            Some(idx) => format!("{}{}", idx + 1, base_icon),
            // If not sorted (index is None), just show the base icon.
            None => base_icon.to_string(),
        }
    }
}
