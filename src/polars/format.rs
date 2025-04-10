use polars::prelude::*;

/// Rounds float columns (Float32 and Float64) in a DataFrame to a specified
/// number of decimal places using optimized Polars expressions.
///
/// Columns of other data types remain unchanged.
///
/// ### Arguments
///
/// * `dataframe`: The input DataFrame.
/// * `decimals`: The number of decimal places to round to.
///
/// ### Returns
///
/// A `Result` containing the new DataFrame with rounded float columns or a `PolarsError`.
#[allow(dead_code)]
pub fn format_columns(dataframe: DataFrame, decimals: u32) -> Result<DataFrame, PolarsError> {
    // Select columns with Float32 or Float64 data types
    let float_cols_selector = dtype_cols(&[DataType::Float32, DataType::Float64]);

    dataframe
        .lazy()
        .with_columns([
            // Apply the round expression directly to the selected float columns
            float_cols_selector.round(decimals).name().keep(), // Keep original column name
        ])
        .collect() // Collect the results back into an eager DataFrame.
}

//----------------------------------------------------------------------------//
//                                   Tests                                    //
//----------------------------------------------------------------------------//

/// Run tests with:
/// `cargo test -- --show-output tests_format_columns`
#[cfg(test)]
mod tests_format_columns {
    use super::*;

    /// `cargo test -- --show-output test_format_col`
    #[test]
    fn round_float_columns() -> Result<(), PolarsError> {
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
        let df_output = format_columns(df_input, decimals)?;
        dbg!(&df_output);

        assert!(
            df_output.equals_missing(&df_expected),
            "Failed round float columns.\nOutput:\n{:?}\nExpected:\n{:?}",
            df_output,
            df_expected
        );

        Ok(())
    }

    #[test]
    fn round_no_float_columns() -> Result<(), PolarsError> {
        let df_input = df!(
            "int_col" => &[1, 2, 3],
            "str_col" => &["x", "y", "z"]
        )?;
        let df_expected = df_input.clone();
        let decimals = 2;

        dbg!(&df_input);
        dbg!(&decimals);
        let df_output = format_columns(df_input, decimals)?;
        dbg!(&df_output);

        assert!(df_output.equals(&df_expected)); // equals is fine here as no nulls involved
        Ok(())
    }

    #[test]
    fn round_with_zero_decimals() -> Result<(), PolarsError> {
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
        let df_output = format_columns(df_input, decimals)?;
        dbg!(&df_output);

        assert!(df_output.equals_missing(&df_expected));
        Ok(())
    }
}
