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
            float_cols_selector.round(decimals), // No alias needed here usually, as applying an operation like round
                                                 // to selected columns often preserves their original names automatically.
                                                 // If you encountered name clashes, you'd revert to iterating the schema
                                                 // and aliasing explicitly like in the previous example.
        ])
        .collect() // Collect the results back into an eager DataFrame.
}

/// Replaces string values with null if the value, after trimming whitespace,
/// exactly matches one of the specified `replace_with_null`.
///
/// Existing null values in the column are preserved. Uses `dtype_col` for selection
/// and `.name().keep()` to ensure original column names are preserved.
///
/// To nullify empty strings or strings containing only whitespace, ensure `""` is included
/// in the `replace_with_null` list.
///
/// ### Arguments
///
/// * `dataframe`: The input DataFrame to be processed. Must contain columns of DataType::String.
/// * `replace_with_null`: A slice of string literals (e.g., `&["NA", "N/A", ""]`)
///   that should trigger null replacement when matched against the trimmed column value.
///
/// ### Returns
///
/// A `Result` containing the new DataFrame with specified strings (based on trimmed matching)
/// replaced by null, or a `PolarsError`.
pub fn replace_strings_with_null(
    // Renamed as per user code
    dataframe: DataFrame,
    replace_with_null: &[&str],
) -> Result<DataFrame, PolarsError> {
    if replace_with_null.is_empty() {
        return Ok(dataframe);
    }

    // Selector for all String columns (adapt DataType if needed)
    let string_cols_selector = dtype_col(&DataType::String); // Target String type

    // Create a Polars Series containing the strings to be treated as null.
    // It's important that this Series contains the *exact* values to match against the trimmed strings.
    let null_values_series =
        Series::new("null_vals".into(), replace_with_null).cast(&DataType::String)?; // Cast to match target column type

    // --- Define Condition ---

    // Condition: Check if the TRIMMED string from the column matches any value in the null values list.
    let condition = string_cols_selector
        .clone() // Clone needed as selector is used again in `otherwise`
        .str()
        .strip_chars(lit(NULL)) // Trim the value in the column FIRST
        .is_in(lit(null_values_series)); // THEN check if the trimmed value is in the list

    // --- Apply Logic ---
    dataframe
        .lazy()
        .with_columns([
            when(condition) // WHEN the trimmed value matches the list
                .then(lit(NULL).cast(DataType::String)) // THEN replace with NULL
                .otherwise(string_cols_selector) // OTHERWISE keep the original value
                .name()
                .keep(), // Ensure original names are kept
        ])
        .collect()
}

/// Removes columns from the DataFrame that consist entirely of null values.
///
/// This optimized version avoids cloning full columns. It works by:
/// 1. Identifying the names (`&str`) of columns containing at least one non-null value.
/// 2. Using the efficient `DataFrame::select` method with those names.
///
/// ### Arguments
///
/// * `df`: A reference to the input DataFrame.
///
/// ### Returns
///
/// A `PolarsResult` containing the new DataFrame with only the columns
/// that have at least one non-null value, or a `PolarsError` if selection fails.
pub fn remove_null_columns(df: &DataFrame) -> PolarsResult<DataFrame> {
    // 1. Get the names of columns that have at least one non-null value.
    let columns_to_keep: Vec<&str> = df
        .iter() // Iterate over the Series (columns)
        .filter(|series| series.is_not_null().any()) // Keep series with any non-null value
        .map(|series| series.name().as_str()) // Get the series name as &str
        .collect(); // Collect the names into a Vec<&str>

    // 2. Select only those columns from the original DataFrame.
    // The select operation is highly optimized and often avoids deep data copies.
    // `select` can take an iterator or slice of items convertible to PlSmallStr, including &str.
    df.select(columns_to_keep)
}

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

        dbg!(&df_input);

        let decimals = 2;
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

        let df_output = format_columns(df_input, decimals)?;

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
        let df_output = format_columns(df_input, decimals)?;
        println!("Output DataFrame (decimals=0):\n{}", df_output);
        println!("Expected DataFrame (decimals=0):\n{}", df_expected);
        assert!(df_output.equals_missing(&df_expected));
        Ok(())
    }
}

#[cfg(test)]
mod tests_remove_null_columns {
    use super::*;

    /// `cargo test -- --show-output tests_remove_null_columns`
    #[test]
    fn remove_some_all_null_columns() -> PolarsResult<()> {
        // Create Series consisting entirely of nulls
        let all_null_int = Series::full_null("all_null_int".into(), 3, &DataType::Int32);
        let all_null_str = Series::full_null("all_null_str".into(), 3, &DataType::String); // Using String type as in previous examples

        let df_input = df!(
            "col_a" => &[Some(1), None, Some(3)],
            "col_b" => &all_null_int,       // Column with only nulls (Int32)
            "col_c" => &[None, Some("hello"), None],
            "col_d" => &all_null_str,       // Column with only nulls (String)
            "col_e" => &[Some(1.1), Some(2.2), None]
        )?;

        let df_expected = df!(
            "col_a" => &[Some(1), None, Some(3)],
            "col_c" => &[None, Some("hello"), None],
            "col_e" => &[Some(1.1), Some(2.2), None]
        )?;

        let df_output = remove_null_columns(&df_input)?;
        assert!(
            df_output.equals_missing(&df_expected),
            "Failed removing some null columns.\nOutput:\n{:?}\nExpected:\n{:?}",
            df_output,
            df_expected
        );

        Ok(())
    }

    #[test]
    fn no_null_columns_to_remove() -> PolarsResult<()> {
        let df_input = df!(
            "col_a" => &[Some(1), Some(2)],
            "col_b" => &[None, Some(true)], // Contains a non-null
            "col_c" => &[Some("a"), Some("b")]
        )?;
        let df_expected = df_input.clone(); // Should remain unchanged

        let df_output = remove_null_columns(&df_input)?;
        assert!(
            df_output.equals_missing(&df_expected),
            "Failed when no columns should be removed.\nOutput:\n{:?}\nExpected:\n{:?}",
            df_output,
            df_expected
        );

        Ok(())
    }

    #[test]
    fn empty_dataframe_input() -> PolarsResult<()> {
        let df_input = DataFrame::default(); // Creates an empty DataFrame
        let df_expected = DataFrame::default(); // Expect an empty DataFrame back

        let df_output = remove_null_columns(&df_input)?;
        // Use equals for empty DataFrames as equals_missing might behave differently
        assert!(
            df_output.equals(&df_expected),
            "Failed for empty input DataFrame.\nOutput:\n{:?}\nExpected:\n{:?}",
            df_output,
            df_expected
        );

        Ok(())
    }

    #[test]
    fn all_columns_are_all_null() -> PolarsResult<()> {
        // Create multiple Columms consisting entirely of nulls
        let all_null_col1 = Column::full_null("all_null_1".into(), 2, &DataType::Float64);
        let all_null_col2 = Column::full_null("all_null_2".into(), 2, &DataType::Boolean);

        let df_input = DataFrame::new(vec![all_null_col1, all_null_col2])?;

        // --- Corrected Expectation ---
        // Expected output is a DataFrame with 0 columns BUT the same number of rows
        // as the input (which is 2). Selecting an empty list produces this.
        let df_expected = df_input.select(Vec::<PlSmallStr>::new())?;
        // You could also write df_input.select([])? which is equivalent

        let df_output = remove_null_columns(&df_input)?;
        assert!(
            df_output.equals(&df_expected),
            "Failed when all columns are null.\nOutput:\n{:?}\nExpected:\n{:?}",
            df_output,
            df_expected
        );

        // Also check the shape explicitly if desired
        assert_eq!(df_output.shape(), (2, 0), "Output shape mismatch");
        assert_eq!(df_expected.shape(), (2, 0), "Expected shape mismatch");

        Ok(())
    }

    #[test]
    fn dataframe_with_zero_rows() -> PolarsResult<()> {
        // DataFrame with schema but no rows
        let df_input = df!(
            "col_a" => &Vec::<Option<i32>>::new(),
            "col_b" => &Vec::<Option<String>>::new()
        )?;
        // Columns are not "all null" because they don't contain *any* nulls (or any values)
        // according to the filter `is_not_null().any()` which will be false for empty series.
        // Therefore, columns should be removed.
        // Correction: An empty series `is_not_null()` is also empty, so `any()` is false.
        // Thus, columns *will* be removed. Expected output is an empty DF.
        let df_expected = DataFrame::default();

        let df_output = remove_null_columns(&df_input)?;
        assert!(
            df_output.equals(&df_expected),
            "Failed for DataFrame with zero rows.\nOutput:\n{:?}\nExpected:\n{:?}",
            df_output,
            df_expected
        );

        Ok(())
    }
}

#[cfg(test)]
mod tests_replace_strings_with_null {
    use super::*;

    // Helper function to cast columns of a DataFrame created with df! to String
    // This is crucial for testing the function which specifically targets DataType::String
    fn df_with_string_cols(
        mut df: DataFrame,
        cols_to_cast: &[&str],
    ) -> Result<DataFrame, PolarsError> {
        for &col_name in cols_to_cast {
            let col = df.column(col_name)?.cast(&DataType::String)?;
            if let Some(series) = col.as_series() {
                // Replace the original column with the casted one
                df.replace(col_name, series.clone())?;
            }
        }
        Ok(df)
    }

    /// `cargo test -- --show-output tests_replace_strings_with_null`
    #[test]
    fn test_replace_exact_and_trimmed() -> Result<(), PolarsError> {
        let df_input_initial = df![
            "col_a" =>  &[Some("Apple"), Some(""), Some("NA"), None, Some("Pear"), Some(" N/A"), Some("  "), Some(" VAL ")],
            "col_b" => &[Some(1), Some(2), Some(3), Some(4), Some(5), Some(6), Some(7), Some(8)],
            "col_c" =>  &[Some("Keep"), Some("N/A "), Some("remove "), None, Some(" "), Some("VAL"), Some("val"), Some(" N/A ")],
        ]?;
        let df_input = df_with_string_cols(df_input_initial, &["col_a", "col_c"])?;

        // Note: Including "" handles exact empty strings and strings with only whitespace (after trim)
        // Not including "VAL" - it should only be removed if its trimmed version is in the list.
        let null_targets = ["NA", "N/A", "", "val"];
        dbg!(&null_targets);

        let df_expected_initial = df![
            "col_a" => &[Some("Apple"), None, None, None, Some("Pear"), None, None, Some(" VAL ")], // "", "NA", " N/A", "  " -> None
            "col_b" => &[Some(1), Some(2), Some(3), Some(4), Some(5), Some(6), Some(7), Some(8)],    // Int untouched
            "col_c" => &[Some("Keep"), None, Some("remove "), None, None, Some("VAL"), None, None], // "N/A ", " ", "val", " N/A " -> None
        ]?;
        let df_expected = df_with_string_cols(df_expected_initial, &["col_a", "col_c"])?;

        dbg!(&df_input);
        let df_output = replace_strings_with_null(df_input, &null_targets)?;
        dbg!(&df_output);

        assert!(
            df_output.equals_missing(&df_expected),
            "DataFrames did not match for trimmed test.\nOutput:\n{:?}\nExpected:\n{:?}",
            df_output,
            df_expected
        );
        Ok(())
    }

    #[test]
    fn test_replace_with_empty_list_trimmed() -> Result<(), PolarsError> {
        // Should still be a no-op
        let df_input_initial = df![
            "col_a" =>  &[Some(" Apple "), Some(""), Some(" NA "), None, Some("Pear ")],
        ]?;
        let df_input = df_with_string_cols(df_input_initial, &["col_a"])?;

        let null_targets: [&str; 0] = []; // Empty list

        let df_expected = df_input.clone(); // Expect no changes

        let df_output = replace_strings_with_null(df_input, &null_targets)?;

        assert!(
            df_output.equals_missing(&df_expected),
            "DataFrames did not match for empty list trimmed test.\nOutput:\n{:?}\nExpected:\n{:?}",
            df_output,
            df_expected
        );
        Ok(())
    }

    #[test]
    fn test_whitespace_only_not_nullified_if_empty_not_targeted() -> Result<(), PolarsError> {
        // Test that "   " isn't nullified if "" isn't in the target list
        let df_input_initial = df![
            "col_a" => &[Some("  "), Some("val"), Some(" OK "), None],
        ]?;
        let df_input = df_with_string_cols(df_input_initial, &["col_a"])?;

        let null_targets = ["val", "OK"]; // Target "val" and "OK", but NOT ""

        // Expected: "  " trimmed is "", but "" is not targeted, so "  " remains.
        // " OK " trimmed is "OK", which IS targeted.
        // "val" matches exactly.
        let df_expected_initial = df![
            "col_a" => &[Some("  "), None, None, None],
        ]?;
        let df_expected = df_with_string_cols(df_expected_initial, &["col_a"])?;

        let df_output = replace_strings_with_null(df_input, &null_targets)?;

        assert!(
            df_output.equals_missing(&df_expected),
            "DataFrames did not match for whitespace only test.\nOutput:\n{:?}\nExpected:\n{:?}",
            df_output,
            df_expected
        );
        Ok(())
    }

    // Previous tests for non-string columns remain valid.
    #[test]
    fn test_no_string_columns_with_targets_trimmed() -> Result<(), PolarsError> {
        let df_input = df![
            "col_i" => &[Some(1), Some(2), None],
            "col_f" => &[Some(1.0), None, Some(3.5)]
        ]?;
        let null_targets = ["NA", "999", ""];
        let df_expected = df_input.clone(); // Expect no changes
        let df_output = replace_strings_with_null(df_input, &null_targets)?;
        assert!(
            df_output.equals_missing(&df_expected),
            "DataFrames did not match for no string column trimmed test.\nOutput:\n{:?}\nExpected:\n{:?}",
            df_output,
            df_expected
        );
        Ok(())
    }
}
