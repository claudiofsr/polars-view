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
