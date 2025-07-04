use polars::prelude::*;

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
pub fn replace_values_with_null(
    dataframe: DataFrame,
    null_value_list: &[&str],
    apply_to_all_columns: bool,
) -> PolarsResult<DataFrame> {
    // If the list is empty, no replacements are needed.
    if null_value_list.is_empty() {
        return Ok(dataframe);
    }

    // --- Prepare for Matching ---

    let list_series: Series = Series::build_from_list(null_value_list);

    // --- Define Replacement Expr ---

    let replacement_expr: Expr = build_null_expression(list_series.lit(), apply_to_all_columns);

    // --- Apply Transformation ---

    dataframe
        .lazy()
        .with_columns([replacement_expr]) // Apply the selected expression
        .collect() // Execute the lazy plan
}

/// Define a trait to add building capabilities directly to the Series type.
pub trait SeriesExtension {
    /// Builds List<String> Series (shape (1,)) with single list row from input_slice.
    ///
    /// ### Arguments
    /// * `input_slice`: Strings for the list content.
    /// ### Returns
    /// A new List<String> Series, shape (1,).
    fn build_from_list(input_slice: &[&str]) -> Series;
}

impl SeriesExtension for Series {
    fn build_from_list(input_slice: &[&str]) -> Series {
        // Step 1: Create inner Series from slice
        // (Dtype: String, Shape: (N,)).
        let inner_series = Series::new("inner_content".into(), input_slice);

        // Step 2: Wrap the inner Series in a vector to create a single-row List Series.
        // Polars builds a List Series where each Series in the vector becomes one row's list.
        // By providing a vector with one inner Series, we get a List Series with one row.
        // (Dtype: List<String>, Shape: (1,)).
        let list_series: Series = Series::new(
            "list_string".into(), // Final Series name
            vec![inner_series],   // Vector with one Series => List Series with one row
        );

        list_series
    }
}

/// Builds a Polars Expression to replace specified string values (after trimming)
/// with NULL within selected columns of a DataFrame.
///
/// Values are replaced if they match any string in the hardcoded list
/// `null_value_list: Vec<&str>` after trimming leading/trailing whitespace.
///
pub fn build_null_expression(null_values_expr: Expr, apply_to_all_columns: bool) -> Expr {
    // --- Define Replacement Logic based on the flag ---
    let replacement_expr: Expr = if apply_to_all_columns {
        // Universal Mode: Apply to ALL columns via casting and trimming string representation
        let condition = all() // Select current column value
            .cast(DataType::String) // Cast to String
            .str()
            .strip_chars(lit(NULL)) // Trim whitespace from string representation
            .is_in(null_values_expr, true); // Check if trimmed string is in the list

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
            .is_in(null_values_expr, true); // Check if trimmed string is in the list

        when(condition) // WHEN the trimmed string matches...
            // THEN replace with NULL (cast needed for type consistency within String col expr)
            .then(lit(NULL).cast(DataType::String))
            // OTHERWISE keep the original string value
            .otherwise(string_cols_selector)
            .name()
            .keep() // Keep original column name
    };

    replacement_expr
}

//----------------------------------------------------------------------------//
//                                   Tests                                    //
//----------------------------------------------------------------------------//

/// Run tests with:
/// `cargo test -- --show-output tests_replace_values_with_null`
#[cfg(test)]
mod tests_replace_values_with_null {
    use super::*; // Import the function from the parent module
    use polars::functions::concat_df_horizontal;

    // Helper to create a consistent test DataFrame using Option for nullability
    fn create_test_df() -> PolarsResult<DataFrame> {
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

        println!("Input:\n{df_input}");
        println!("Null Markers: {null_markers:?}");
        let df_output = replace_values_with_null(df_input, null_markers, true)?;
        println!("Output:\n{df_output}");
        println!("Expected:\n{df_expected}");

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

    /// cargo test -- --show-output test_replace_values_with_null_string_only
    #[test]
    fn test_replace_values_with_null_string_only() -> PolarsResult<()> {
        let df_input = df![
        "col1" => &[" A ", "B ", " C", "D", "", " ", "NA", "KEEP", " null "],
        "col2" => &[Some(1i32), Some(2), Some(999), Some(4), Some(5), Some(6), Some(7), Some(8), Some(9)],
        "col3" => &[Some(true), Some(false), Some(true), Some(false), Some(true), Some(false), Some(true), Some(false), Some(true)],
        "col4" => &[" NA ", "ignore", "N/A", "None", "999", "", "KEEP", "other", "false"]
        ]?;

        let null_list = &["NA", "", "999", "N/A", "null"]; // Include "999" as string, "" for whitespace trimming check

        let df_expected = df![
        "col1" => &[Some(" A "), Some("B "), Some(" C"), Some("D"), None, None, None, Some("KEEP"), None],
        "col2" => &[Some(1i32), Some(2), Some(999), Some(4), Some(5), Some(6), Some(7), Some(8), Some(9)],
        "col3" => &[Some(true), Some(false), Some(true), Some(false), Some(true), Some(false), Some(true), Some(false), Some(true)],
        "col4" => &[None, Some("ignore"), None, Some("None"), None, None, Some("KEEP"), Some("other"), Some("false")]
        ]?;

        println!("Input:\n{df_input}");
        println!("Null List: {null_list:?}");
        let df_output = replace_values_with_null(df_input, null_list, false)?; // string-only
        println!("Output:\n{df_output}");
        println!("Expected:\n{df_expected}");

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
            "Whitespace not nullified when '' IS targeted.\nOutput:\n{df_output_with_empty:?}\nExpected:\n{df_expected_with_empty:?}"
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
            "Whitespace incorrectly nullified when '' NOT targeted.\nOutput:\n{df_output_without_empty:?}\nExpected:\n{df_expected_without_empty:?}"
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

        println!("Input:\n{df_input}");
        println!("Null Markers: {NULL_MARKERS:?}");
        let df_output = replace_values_with_null(df_input, NULL_MARKERS, false)?;
        println!("Output:\n{df_output}");
        println!("Expected:\n{df_expected}");

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

        println!("Input:\n{df_input}");
        println!("Null Markers: {NULL_MARKERS:?}");
        let df_output = replace_values_with_null(df_input, NULL_MARKERS, true)?;
        println!("Output:\n{df_output}");
        println!("Expected:\n{df_expected}");

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

    /// cargo test -- --show-output test_remove_leading_and_trailing_chars
    #[test]
    fn test_remove_leading_and_trailing_chars() -> Result<(), PolarsError> {
        /// Define values to be interpreted as null across all columns.
        pub static NULL_VALUES_TEST: [&str; 3] = [
            "",           // Represents empty strings --> null
            "<N/D>",      // Specific placeholder string 1
            "*DIVERSOS*", // Specific placeholder string 2
        ];

        let df_input = df! {
            "foo" => &["", " ", "hello ", " <N/D> ", " *DIVERSOS* \n ", " world", " \n\r *DIVERSOS* \n ", "<N/D>"],
        }?;

        println!("df_input: {df_input}");

        // Create a Polars Series containing the *strings* to be treated as null markers.
        let series = Series::new("null_vals".into(), NULL_VALUES_TEST);
        let null_values_expr: Expr = series.implode()?.into_series().lit();

        let condition = all() // Select current column value
            .cast(DataType::String) // Cast to String
            .str()
            .strip_chars(lit(NULL)) // Trim whitespace from string representation
            .is_in(null_values_expr.clone(), true); // Check if trimmed string is in the list
        println!("condition: {condition}");

        let replacement_expr: Expr = build_null_expression(null_values_expr, true);
        println!("replacement_expr: {replacement_expr}");

        let mut df_temp = df_input
            .clone()
            .lazy()
            .with_columns([condition.alias("other name"), replacement_expr]) // Apply the selected expression
            .collect()?;
        df_temp.set_column_names(["foo_stripped", "is_in condition"])?;

        // Concat DataFrames horizontally.
        // let df_output = df_input.hstack(df_temp.get_columns())?;
        let df_output = concat_df_horizontal(&[df_input, df_temp], true)?;

        println!("df_output: {df_output}");

        let vec_from_series: Vec<&str> = df_output
            .column("foo_stripped")?
            .str()?
            .iter() // Iterator over Option<&str>
            .map(|opt_str| opt_str.unwrap_or("null"))
            .collect();

        println!("vec_from_series: {vec_from_series:?}");

        let vec_from_series: Vec<Option<&str>> = df_output
            .column("foo_stripped")?
            .str()?
            .iter() // Iterator over Option<&str>
            .collect();

        println!("vec_from_series: {vec_from_series:?}");

        let df_expected = df! {
            "foo" => &["", " ", "hello ", " <N/D> ", " *DIVERSOS* \n ", " world", " \n\r *DIVERSOS* \n ", "<N/D>"],
            "foo_stripped" => &[None, None, Some("hello "), None, None, Some(" world"), None, None],
            "is_in condition" => &[true, true, false, true, true, false, true, true],
        }?;

        assert_eq!(
            df_output, df_expected,
            "DataFrame mismatch after schema modify and null handling"
        );

        assert_eq!(
            df_output.schema(),
            df_expected.schema(),
            "DataFrame mismatch schema"
        );

        Ok(())
    }

    #[test]
    /// cargo test -- --show-output test_create_list_series_direct_from_series
    fn test_create_list_series_direct_from_series() -> Result<(), PolarsError> {
        // Define input data for the list
        let null_value_list: &[&str] = &["", "<N/D>", "SIM"];

        // Step 1: Create the inner String Series from the data.
        let inner_series = Series::new("inner_content".into(), null_value_list);
        println!("inner_series: {inner_series}");

        assert_eq!(inner_series.len(), 3, "The length of the Series must be 3");

        // Step 2: Create the List Series using a Vec containing the inner Series.
        let list_series: Series = Series::new("list_string".into(), vec![inner_series]);
        println!("list_series: {list_series}");

        assert_eq!(list_series.len(), 1, "The length of the Series must be 1");

        assert_eq!(
            list_series.dtype(),
            &DataType::List(Box::new(DataType::String)),
            "Resulting series should be List<String>"
        );

        let result_series: Series = Series::build_from_list(null_value_list);

        assert_eq!(result_series.len(), 1); // List series itself has 1 row
        assert_eq!(result_series.name(), "list_string");
        assert_eq!(
            result_series.dtype(),
            &DataType::List(Box::new(DataType::String))
        );
        assert_eq!(result_series, list_series);

        Ok(())
    }
}
