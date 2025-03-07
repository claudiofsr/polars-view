/// Represents the sorting state of a column.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum SortState {
    /// The column is not sorted. Holds the column name.
    NotSorted(String),
    /// The column is sorted in ascending order. Holds the column name.
    Ascending(String),
    /// The column is sorted in descending order. Holds the column name.
    Descending(String),
}

impl SortState {
    /// Checks if the given column is currently sorted (either ascending or descending).
    ///
    /// ### Arguments
    ///
    /// * `column_name`: The name of the column to check.
    ///
    /// ### Returns
    ///
    /// `true` if the column is sorted (ascending or descending), `false` otherwise.
    pub fn is_sorted_column(&self, column_name: &str) -> bool {
        match self {
            SortState::Ascending(sorted_column) | SortState::Descending(sorted_column) => {
                sorted_column == column_name
            }
            SortState::NotSorted(_) => false,
        }
    }
}
