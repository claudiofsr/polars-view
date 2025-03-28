use polars::prelude::*;

/// Formats a DataFrame by rounding all Float32 and Float64 columns to a specified number of decimal places.
///
/// Other column types remain unchanged.
///
/// The function uses lazy evaluation for efficiency.
#[allow(dead_code)]
pub fn format_dataframe_columns(
    dataframe: DataFrame,
    decimals: u32,
) -> Result<DataFrame, PolarsError> {
    dataframe
        .lazy()
        .with_columns([all() // Select all columns
            .map(
                // Apply the rounding function to each column.
                move |column| round_float_columns(column, decimals),
                // Indicate that the output type will be the same as the input type.
                GetOutput::same_type(),
            )])
        .collect() // Collect the results back into an eager DataFrame.
}

/// Rounds Float32 and Float64 Series to the specified number of decimal places.
///
/// Other Series types are returned unchanged.
pub fn round_float_columns(column: Column, decimals: u32) -> Result<Option<Column>, PolarsError> {
    match column.as_series() {
        // Attempt to get a Series from the Column
        Some(series) => {
            // Check if the series data type is a floating point type.
            if series.dtype().is_float() {
                // Round the Series to the specified number of decimals.
                series.round(decimals).map(|s| Some(s.into_column()))
            } else {
                // If it's not a floating-point series, return the original column.
                Ok(Some(column))
            }
        }
        None => Ok(Some(column)),
    }
}

/// Eliminates columns that contain only null values from a `DataFrame`.
///
/// This function efficiently removes columns that consist entirely of null values.
/// It iterates over each column (Series) in the DataFrame and checks if *any*
/// value within the column is non-null.  If all values in a column are null,
/// that column is excluded from the resulting DataFrame.
///
/// ### Arguments
///
/// *   `df` - The input `DataFrame`.
///
/// ### Returns
///
/// *   `PolarsResult<DataFrame>` - A `PolarsResult` containing the resulting
///     `DataFrame` with all-null columns removed.  If the input DataFrame
///     is empty or contains no all-null columns, the returned DataFrame will
///     be equivalent to the input.
///
/// ### Examples
///
/// ```rust
/// use polars::prelude::*;
/// use polars_view::remove_null_columns;
///
/// // Example with a DataFrame containing an all-null integer column.
/// let df = df!(
///    "col1" => &[1, 2, 3],
///    "col2" => &[None::<i32>, None::<i32>, None::<i32>], // All nulls
///    "col3" => &[Some(4), None, Some(6)],
/// ).unwrap();
///
/// let result_df = remove_null_columns(&df).unwrap();
/// assert_eq!(result_df.width(), 2); // col2 (all nulls) should be removed
/// assert_eq!(result_df.get_column_names(), vec!["col1", "col3"]);
///
/// // Example with an empty DataFrame.
/// let empty_df: DataFrame = DataFrame::default();
/// let result_empty_df = remove_null_columns(&empty_df).unwrap();
/// assert!(result_empty_df.is_empty());
///
/// // Example with a DataFrame containing NO all-null columns
/// let df_no_nulls = df!(
///     "col1" => &[Some(1), Some(2)],
///     "col2" => &[Some(3), None]
/// ).unwrap();
///
/// let result_no_nulls = remove_null_columns(&df_no_nulls).unwrap();
/// assert_eq!(result_no_nulls.get_column_names(), vec!["col1", "col2"]);
/// ```
///
/// ### See Also
///
/// *   [Polars issue #1613](https://github.com/pola-rs/polars/issues/1613) - Discussion about dropping all-null columns.
/// *   [Stack Overflow question](https://stackoverflow.com/questions/76338261/polars-and-the-lazy-api-how-to-drop-columns-that-contain-only-null-values) -  Related Stack Overflow question.
///
pub fn remove_null_columns(df: &DataFrame) -> PolarsResult<DataFrame> {
    let df_clean: DataFrame = df
        .iter() // Iterate over each Series (column) in the DataFrame.
        .filter(|series| {
            // Keep the series if it contains *any* non-null values.
            // `is_not_null()` creates a BooleanChunked indicating non-null
            // elements, and `any()` checks if at least one element is true.
            series.is_not_null().any()
        })
        .cloned() // Clone each Series to avoid borrowing issues (needed for collect).
        .collect(); // Collect the filtered Series into a new DataFrame.

    Ok(df_clean)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `cargo test -- --show-output test_format_dataframe`
    #[test]
    fn test_format_dataframe() -> Result<(), PolarsError> {
        // Create a DataFrame with various data types (string, integer, float, boolean, and optional float).
        let df = df!(
            "text_col" => &["a", "b", "c"],
            "int_col" => &[1, 2, 3],
            "float_col" => &[1.1234, 2.5650001, 3.965000],
            "bool_col" => &[true, false, true],
            "opt_float" => &[Some(1.0), None, Some(3.45677)],
        )?;

        dbg!(&df);

        // Format the DataFrame to 2 decimal places.
        let formatted_df = format_dataframe_columns(df, 2)?;

        dbg!(&formatted_df);

        // Create expected columns for comparison.
        let column1 = Column::new("float_col".into(), [1.12, 2.57, 3.97]);
        let column2 = Column::new("opt_float".into(), [Some(1.00), None, Some(3.46)]);

        // Assert that the float columns are rounded correctly.
        assert_eq!(formatted_df["float_col"], column1);
        assert_eq!(formatted_df["opt_float"], column2);
        //Other columns remains equals

        Ok(())
    }
}
