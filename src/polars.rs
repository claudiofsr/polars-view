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
        ])
        .collect() // Collect the results back into an eager DataFrame.
}

/// Replaces values with null based on a list of matching strings, with options
/// to apply to all columns or only string columns.
///
/// This function compares values against `null_value_list` and replaces them with `NULL`
/// upon a match. The comparison behavior depends on the `apply_to_all_columns` flag:
///
/// 1.  **If `apply_to_all_columns` is `false` (Default String Behavior):**
///     *   Operates **only** on columns with `DataType::String`.
///     *   Trims leading/trailing whitespace from the **original string** value.
///     *   Compares the trimmed string against `null_value_list`.
///     *   Non-string columns and non-matching strings are untouched.
///     *   To nullify empty/whitespace-only strings, include `""` in `null_value_list`.
///
/// 2.  **If `apply_to_all_columns` is `true` (Universal Behavior):**
///     *   Operates on **all** columns in the DataFrame.
///     *   Casts the value in each column to its **string representation** (`DataType::String`).
///     *   Trims leading/trailing whitespace from this **string representation**.
///     *   Compares the trimmed string representation against `null_value_list`.
///     *   If a match occurs, the **original value** (regardless of type) is replaced with `NULL`.
///
/// ### Important Considerations (especially when `apply_to_all_columns = true`):
///
/// *   **Trimming:** Whitespace is *always* trimmed before comparison in both modes.
///     For `apply_to_all_columns = true`, trimming occurs *after* casting to string.
/// *   **Type Casting:** The universal mode relies on Polars' default casting to String.
///     Ensure strings in `null_value_list` match the *trimmed* string representation
///     of numbers, booleans, dates, etc. (e.g., "3.45", "true", "2023-01-01", "NA").
/// *   **Ambiguity:** A string like "123" in the list might match integer `123`,
///     float `123.0` (if its string form trims to "123"), and string `" 123 "`.
/// *   **Complex Types:** Casting complex types (List, Struct, Binary) to String might
///     yield unpredictable representations or errors. Use with caution.
/// *   **Performance:** The universal mode (casting all values) can be slower than
///     the string-only mode on large datasets.
///
/// ### Arguments
///
/// *   `dataframe`: The input DataFrame to be processed.
/// *   `null_value_list`: A slice of string literals representing values to be nullified
///     *after trimming* (e.g., `&["NA", "", "999", "true"]`).
/// *   `apply_to_all_columns`: If `true`, applies the logic to all columns by casting
///     to string first. If `false`, applies only to `DataType::String` columns.
///
/// ### Returns
///
/// A `Result` containing the modified DataFrame, or a `PolarsError`.
pub fn replace_values_with_null(
    dataframe: DataFrame,
    null_value_list: &[&str],
    apply_to_all_columns: bool,
) -> Result<DataFrame, PolarsError> {
    // If the list is empty, no replacements are needed.
    if null_value_list.is_empty() {
        return Ok(dataframe);
    }

    // --- Prepare for Matching ---

    // Create a Polars Series containing the *strings* to be treated as null markers.
    // Ensure it's DataType::String for the `is_in` comparison.
    let null_values_series =
        Series::new("null_vals".into(), null_value_list).cast(&DataType::String)?;

    // --- Define Replacement Logic based on the flag ---

    let replacement_expr: Expr = if apply_to_all_columns {
        // Universal Mode: Apply to ALL columns via casting and trimming string representation
        let condition = all() // Select current column value
            .cast(DataType::String) // Cast to String
            .str()
            .strip_chars(lit(NULL)) // Trim whitespace from string representation
            .is_in(lit(null_values_series)); // Check if trimmed string is in the list

        when(condition) // WHEN the trimmed string representation matches...
            .then(lit(NULL)) // THEN replace original value with NULL
            .otherwise(all()) // OTHERWISE keep the original value
            .name()
            .keep() // Keep original column name
    } else {
        // String-Only Mode: Apply only to String columns, trim original string
        let string_cols_selector = dtype_col(&DataType::String);

        let condition = string_cols_selector // Select only string columns
            .clone() // Clone needed for use in `otherwise`
            .str()
            .strip_chars(lit(NULL)) // Trim whitespace from the original string value
            .is_in(lit(null_values_series)); // Check if trimmed string is in the list

        when(condition) // WHEN the trimmed string matches...
            // THEN replace with NULL (cast needed for type consistency within String col expr)
            .then(lit(NULL).cast(DataType::String))
            // OTHERWISE keep the original string value
            .otherwise(string_cols_selector)
            .name()
            .keep() // Keep original column name
    };

    // --- Apply Transformation ---

    dataframe
        .lazy()
        .with_columns([replacement_expr]) // Apply the selected expression
        .collect() // Execute the lazy plan
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

//----------------------------------------------------------------------------//
//                                   Tests                                    //
//----------------------------------------------------------------------------//

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
mod tests_replace_values_with_null {
    use super::*; // Import the function from the parent module

    // Helper to create a consistent test DataFrame using Option for nullability
    fn create_test_df() -> Result<DataFrame, PolarsError> {
        df!(
            // Use Option<&str> for string columns that can contain nulls
            "col_str" => &[
                Some("Keep"), Some(" N/A "), Some("<N/D>"), Some("  "), Some("Value"),
                None, // Use None for NULL
                Some("NA"), Some("999"), Some("3.45"), Some("false")
            ],
            // Use Option<i32> for integer columns, even if currently no nulls, for consistency
            "col_int" => &[
                Some(1), Some(2), Some(999), Some(4), Some(5),
                Some(6), Some(7), Some(999), Some(0), Some(10)
            ],
            // Use Option<f64> for float columns
            "col_flt" => &[
                Some(1.1), Some(2.2), Some(999.0), Some(999.1), Some(5.5),
                Some(6.6), Some(7.7), Some(8.8), Some(3.45), Some(10.1)
            ],
            // Use Option<bool> for boolean columns
            "col_bool" => &[
                Some(true), Some(false), Some(true), Some(false), Some(true),
                Some(true), Some(false), Some(true), Some(false), Some(true)
            ],
            // Use Option<&str> again for this nullable string column
            "col_str_ws" => &[
                Some(" leading"), Some("trailing "), Some(" both "), Some(""), Some("NA"),
                Some("  NA  "),
                None, // Use None for NULL
                Some("ok"), Some("999 "), Some(" 3.45")
            ]
        )
    }

    // Define the null markers used in tests
    const NULL_MARKERS: &[&str] = &["", "<N/D>", "NA", "N/A", "999", "3.45", "false"];

    /// Comprehensive test covering various data types and trimming.
    /// `cargo test -- --show-output test_universal_replacement_mixed_types`
    #[test]
    fn test_universal_replacement_mixed_types() -> Result<(), PolarsError> {
        // Input DataFrame - df! infers i32 and datetime[ms] here
        let df_input = df![
            "col_str" =>    &[Some("Keep"), Some(" NA "), Some("<N/D>"), Some("  "), None, Some("999"), Some("3.45"), Some("false"), Some("2024-01-15")],
            "col_int" =>    &[Some(123i32), Some(999i32), Some(-10i32), Some(999i32), Some(200i32), Some(0i32), Some(999i32), Some(1i32), Some(2i32)], // Explicit i32
            "col_float" =>  &[Some(1.1), Some(3.45), Some(-2.2), None, Some(999.0), Some(0.0), Some(123.456), Some(3.450), Some(5.0)], // f64 inferred
            "col_bool" =>   &[Some(true), Some(false), None, Some(true), Some(false), Some(true), Some(true), Some(false), Some(true)], // bool inferred
        ]?;

        // Define null markers - **ADJUST DATETIME MARKER**
        let null_markers = &[
            "",      // Matches "  " after trimming
            "NA",    // Matches " NA " after trimming
            "<N/D>", // Exact match
            "999",   // Will match integer 999 and string "999"
            "3.45",  // Will match float 3.45 and string "3.45"
            "false", // Matches relevant bools/strings
            "2024-01-15",
        ];

        // Expected DataFrame - let df! infer types matching input (i32, datetime[ms])
        let df_expected = df![
             "col_str" =>    &[Some("Keep"), None, None, None, None, None, None, None, None],
             "col_int" =>    &[Some(123i32), None, Some(-10i32), None, Some(200i32), Some(0i32), None, Some(1i32), Some(2i32)], // Use i32
             "col_float" =>  &[Some(1.1), None, Some(-2.2), None, Some(999.0), Some(0.0), Some(123.456), None, Some(5.0)],
             "col_bool" =>   &[Some(true), None, None, Some(true), None, Some(true), Some(true), None, Some(true)],
        ]?;

        println!("Input:\n{}", df_input);
        println!("Null Markers: {:?}", null_markers);
        let df_output = replace_values_with_null(df_input, null_markers, true)?;
        println!("Output:\n{}", df_output);
        println!("Expected:\n{}", df_expected);

        // Compare schema and values
        assert_eq!(
            df_output.schema(),
            df_expected.schema(),
            "Schemas do not match"
        );
        assert!(
            df_output.equals_missing(&df_expected),
            "DataFrames did not match for universal mixed type test."
        );

        Ok(())
    }

    /// Test whitespace-only strings specifically.
    /// `cargo test -- --show-output test_universal_whitespace_handling`
    #[test]
    fn test_universal_whitespace_handling() -> Result<(), PolarsError> {
        let df_input = df!(
            "col_a" => &[Some("   "), Some("\t\n"), Some("Keep"), Some(" Val "), None, Some("")],
        )?;

        // Case 1: Target empty string "" -> whitespace should be nullified
        let null_markers_with_empty = &["", "Val"]; // Match empty string and "Val" (after trim)
        let df_expected_with_empty = df!(
            "col_a" => &[None::<&str>, None::<&str>, Some("Keep"), None, None, None],
        )?;
        let df_output_with_empty = replace_values_with_null(
            df_input.clone(), // Clone input for the first case
            null_markers_with_empty,
            true,
        )?;
        assert!(
            df_output_with_empty.equals_missing(&df_expected_with_empty),
            "Whitespace not nullified when '' IS targeted.\nOutput:\n{:?}\nExpected:\n{:?}",
            df_output_with_empty,
            df_expected_with_empty
        );

        // Case 2: Do NOT target empty string "" -> whitespace should NOT be nullified
        let null_markers_without_empty = &["Val"]; // Only target "Val" (after trim)
        let df_expected_without_empty = df!(
            // "   ", "\t\n", "" remain because "" is not targeted after trimming them
            "col_a" => &[Some("   "), Some("\t\n"), Some("Keep"), None, None, Some("")],
        )?;
        let df_output_without_empty = replace_values_with_null(
            df_input.clone(), // Clone input for the second case
            null_markers_without_empty,
            true,
        )?;
        assert!(
            df_output_without_empty.equals_missing(&df_expected_without_empty),
            "Whitespace incorrectly nullified when '' NOT targeted.\nOutput:\n{:?}\nExpected:\n{:?}",
            df_output_without_empty,
            df_expected_without_empty
        );

        Ok(())
    }

    /// `cargo test -- --show-output test_string_columns_only`
    #[test]
    fn test_string_columns_only() -> Result<(), PolarsError> {
        let df_input = create_test_df()?;

        // Expected DataFrame now also uses Option for clarity, matching the input style
        let df_expected = df!(
            "col_str" =>    &[Some("Keep"), None, None, None, Some("Value"), None, None, None, None, None],
            "col_int" =>    &[Some(1), Some(2), Some(999), Some(4), Some(5), Some(6), Some(7), Some(999), Some(0), Some(10)],
            "col_flt" =>    &[Some(1.1), Some(2.2), Some(999.0), Some(999.1), Some(5.5), Some(6.6), Some(7.7), Some(8.8), Some(3.45), Some(10.1)],
            "col_bool" =>   &[Some(true), Some(false), Some(true), Some(false), Some(true), Some(true), Some(false), Some(true), Some(false), Some(true)],
            "col_str_ws" => &[Some(" leading"), Some("trailing "), Some(" both "), None, None, None, None, Some("ok"), None, None]
        )?;

        println!("Input:\n{}", df_input);
        println!("Null Markers: {:?}", NULL_MARKERS);
        let df_output = replace_values_with_null(df_input, NULL_MARKERS, false)?;
        println!("Output:\n{}", df_output);
        println!("Expected:\n{}", df_expected);

        assert_eq!(df_output, df_expected);
        Ok(())
    }

    /// `cargo test -- --show-output test_all_columns`
    #[test]
    fn test_all_columns() -> Result<(), PolarsError> {
        let df_input = create_test_df()?;

        let df_expected = df!(
            "col_str" =>    &[Some("Keep"), None, None, None, Some("Value"), None, None, None, None, None],
            "col_int" =>    &[Some(1), Some(2), None, Some(4), Some(5), Some(6), Some(7), None, Some(0), Some(10)],
            "col_flt" =>    &[Some(1.1), Some(2.2), Some(999.0), Some(999.1), Some(5.5), Some(6.6), Some(7.7), Some(8.8), None, Some(10.1)],
            "col_bool" =>   &[Some(true), None, Some(true), None, Some(true), Some(true), None, Some(true), None, Some(true)],
            "col_str_ws" => &[Some(" leading"), Some("trailing "), Some(" both "), None, None, None, None, Some("ok"), None, None]
        )?;

        println!("Input:\n{}", df_input);
        println!("Null Markers: {:?}", NULL_MARKERS);
        let df_output = replace_values_with_null(df_input, NULL_MARKERS, true)?;
        println!("Output:\n{}", df_output);
        println!("Expected:\n{}", df_expected);

        assert_eq!(df_output, df_expected);
        Ok(())
    }

    #[test]
    fn test_empty_null_list() -> Result<(), PolarsError> {
        let df_orig = create_test_df()?;
        println!("df_orig: {df_orig:?}");

        let result_str_only = replace_values_with_null(df_orig.clone(), &[], false)?;
        let result_all_cols = replace_values_with_null(df_orig.clone(), &[], true)?;

        assert_eq!(result_str_only, df_orig);
        assert_eq!(result_all_cols, df_orig);

        Ok(())
    }

    #[test]
    fn test_no_matches_in_list() -> Result<(), PolarsError> {
        let df_orig = create_test_df()?;
        println!("df_orig: {df_orig:?}");

        let no_match_markers = &["XYZ", "12345", "NO_MATCH"];

        let result_str_only = replace_values_with_null(df_orig.clone(), no_match_markers, false)?;
        let result_all_cols = replace_values_with_null(df_orig.clone(), no_match_markers, true)?;

        assert_eq!(result_str_only, df_orig);
        assert_eq!(result_all_cols, df_orig);

        Ok(())
    }

    #[test]
    fn test_all_nulls_input() -> Result<(), PolarsError> {
        let df = df!(
            "a" => &[Option::<i32>::None, None], // Already using Option correctly here
            "b" => &[Option::<&str>::None, None] // Already using Option correctly here
        )?;
        println!("df: {df:?}");

        let result_str_only = replace_values_with_null(df.clone(), NULL_MARKERS, false)?;
        let result_all_cols = replace_values_with_null(df.clone(), NULL_MARKERS, true)?;

        assert_eq!(result_str_only, df);
        assert_eq!(result_all_cols, df);
        Ok(())
    }
}
