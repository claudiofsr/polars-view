//! Defines the representation of sorting criteria for the table.
//! This module contains the core types for managing single and multiple sort column states.

use std::fmt::Debug;

/// Represents a single criterion for sorting.
/// Used within `DataFrameContainer` to store the cumulative sort order as `Vec<SortBy>`.
/// The order of criteria in the Vec determines sort precedence.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SortBy {
    /// The name of the column to sort by.
    pub column_name: String,
    /// The sort direction. `true` for ascending, `false` for descending.
    pub ascending: bool,
    /// How nulls should be ordered. `true` to place nulls last, `false` for first.
    pub nulls_last: bool,
}

/// Represents the *interaction* state for sorting a specific column header in the UI.
///
/// This enum manages the click cycle:
/// NotSorted -> DescNullsFirst -> AscNullsFirst -> DescNullsLast -> AscNullsLast -> NotSorted.
/// The actual applied cumulative sort state (`Vec<SortBy>`) is stored and managed separately
/// in `DataFrameContainer`.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum HeaderSortState {
    /// Column is not part of the current sort criteria list.
    NotSorted,
    /// Column sorted descending, nulls appear first.
    DescendingNullsFirst, // State 1
    /// Column sorted ascending, nulls appear first.
    AscendingNullsFirst, // State 2
    /// Column sorted descending, nulls appear last.
    DescendingNullsLast, // State 3
    /// Column sorted ascending, nulls appear last.
    AscendingNullsLast, // State 4
}

impl HeaderSortState {
    /// Calculates the next interaction state in the UI cycle for a header click.
    ///
    /// The cycle progresses as follows:
    /// 1. `NotSorted`            -> `DescendingNullsFirst`
    /// 2. `DescendingNullsFirst` -> `AscendingNullsFirst`
    /// 3. `AscendingNullsFirst`  -> `DescendingNullsLast`
    /// 4. `DescendingNullsLast`  -> `AscendingNullsLast`
    /// 5. `AscendingNullsLast`   -> `NotSorted` (removes the sort for this column)
    ///
    /// Called by `container.rs::render_table_header` when a click is detected.
    ///
    /// ### Returns
    /// The next `HeaderSortState` in the cycle.
    pub fn cycle_next(&self) -> Self {
        match self {
            HeaderSortState::NotSorted => HeaderSortState::DescendingNullsFirst,
            HeaderSortState::DescendingNullsFirst => HeaderSortState::AscendingNullsFirst,
            HeaderSortState::AscendingNullsFirst => HeaderSortState::DescendingNullsLast,
            HeaderSortState::DescendingNullsLast => HeaderSortState::AscendingNullsLast,
            HeaderSortState::AscendingNullsLast => HeaderSortState::NotSorted,
        }
    }

    /// Returns a Unicode icon visually representing the interaction state.
    /// Optionally includes the sort precedence index (1-based) if the column is sorted.
    /// Uses different symbols to distinguish nulls placement.
    ///
    /// Used by the `SortableHeaderRenderer` trait implementation in `traits.rs`
    /// to display feedback in the table header (e.g., "1▼", "2▲", "3▽", "4△", "↕").
    ///
    /// ### Arguments
    /// * `index`: `Option<usize>` - The 0-based index representing the sort precedence. `None` if not sorted.
    ///
    /// ### Returns
    /// A `String` containing the icon and optional index number.
    pub fn get_icon(&self, index: Option<usize>) -> String {
        let base_icon = match self {
            // Nulls First States
            HeaderSortState::DescendingNullsFirst => "⏷", // U+23F7 (Down arrow)
            HeaderSortState::AscendingNullsFirst => "⏶",  // U+23F6 (Up arrow)

            // Nulls Last States
            HeaderSortState::DescendingNullsLast => "⬇",
            HeaderSortState::AscendingNullsLast => "⬆",

            // Unsorted State
            HeaderSortState::NotSorted => "↕", // U+2195 UP DOWN ARROW
        };

        match index {
            Some(idx) => format!("{}{}", idx + 1, base_icon),
            None => base_icon.to_string(),
        }
    }
}
