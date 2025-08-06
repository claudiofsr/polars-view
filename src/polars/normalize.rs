use crate::{PolarsViewError, PolarsViewResult};
use polars::prelude::*;
use regex::Regex;

/// Normalizes string columns containing numeric values formatted with non-standard separators
/// (e.g., '.' for thousands, ',' for decimals) to standard numeric format
/// ('.' for decimals, no thousands separators) and then casts them to Float64.
///
/// This function identifies columns based on two criteria:
/// 1.  The column's `DataType` must be `String`.
/// 2.  The column's name must match the provided `regex_pattern`.
///
/// Columns not matching *both* criteria remain unchanged.
///
/// An error is returned if the `regex_pattern` matches a column that is *not* of type `String`.
///
/// ### Regex Pattern Requirements
///
/// The `regex_pattern` must match **entire** column names. It must be either:
///
/// *   `"*"`: A wildcard to select *all* columns that are also `DataType::String`.
/// *   A valid regex string that **starts with `^` and ends with `$`**. This ensures the pattern
///     matches the complete column name from beginning to end.
///     Examples:
///     *   `"^Value_PT_1$"`: Matches the string column "Value_PT_1" if it exists.
///     *   `"^Value_.*$"`: Matches string columns starting with "Value_" (e.g., "Value_PT_1", "Value_US").
///     *   `"^(Value_PT_1|Value_PT_2)$"`: Matches string columns named exactly "Value_PT_1" or "Value_PT_2".
///
/// If the pattern is not `"*"` and does not conform to the `^...$` requirement, or if it's an invalid regex,
/// the function will return an error (`InvalidRegexPattern`).
///
/// ### Arguments
///
/// * `df`: The input `DataFrame`.
/// * `regex_pattern`: A regex string conforming to the requirements above.
///
/// ### Returns
///
/// * `PolarsViewResult<DataFrame>`: A `PolarsViewResult` containing the transformed `DataFrame` on success.
///   Returns errors:
///     * `PolarsViewError::InvalidRegexPattern`: If the pattern is malformed or doesn't meet `^...$` (unless `*`).
///     * `PolarsViewError::InvalidDataTypeForRegex`: If the pattern matches a column that is *not* `DataType::String`.
///     * Other `PolarsError` variants (wrapped) if Polars operations fail during the lazy plan execution.
pub fn normalize_float_strings_by_regex(
    df: DataFrame,
    regex_pattern: &str,
) -> PolarsViewResult<DataFrame> {
    // --- 1. Compile Regex and Validate Pattern ---
    // Handles validation and compilation in one step.
    let compiled_regex: Option<Regex> = match regex_pattern {
        // Handle the wildcard case separately
        "*" => None,
        // Handle specific regex patterns
        pattern => {
            // Validate the required ^...$ format *before* compiling
            if !(pattern.starts_with('^') && pattern.ends_with('$')) {
                return Err(PolarsViewError::InvalidRegexPattern(pattern.to_string()));
            }
            // Attempt to compile the regex
            match Regex::new(pattern) {
                Ok(re) => Some(re),
                Err(e) => {
                    // Return specific error for invalid syntax
                    return Err(PolarsViewError::InvalidRegexSyntax {
                        pattern: pattern.to_string(),
                        error: e.to_string(),
                    });
                }
            }
        }
    };

    tracing::debug!("Compiled regex (None for wildcard): {:?}", compiled_regex);

    // --- 2. Identify Columns to Transform and Potential Errors ---
    let schema = df.schema();
    let mut columns_to_transform: Vec<PlSmallStr> = Vec::new();
    let mut error_columns_mismatched_type = Vec::new();

    for (col_name, dtype) in schema.iter() {
        // Check if the column name matches the regex pattern
        let name_matches_pattern = match &compiled_regex {
            Some(re) => re.is_match(col_name),
            None => true, // "*" matches all columns
        };

        if name_matches_pattern {
            // Name matches, now check the data type
            if dtype == &DataType::String {
                // Correct type, add to list for transformation
                columns_to_transform.push(col_name.clone());
            } else {
                // Incorrect type for transformation, record for error reporting
                error_columns_mismatched_type.push(format!("'{col_name}' (Type: {dtype})"));
            }
        }
        // If name doesn't match pattern, ignore the column
    }

    // --- 3. Check for Data Type Errors ---
    if !error_columns_mismatched_type.is_empty() {
        // Return an error if any non-string columns were matched by the regex
        return Err(PolarsViewError::InvalidDataTypeForRegex {
            pattern: regex_pattern.to_string(),
            columns: error_columns_mismatched_type,
        });
    }

    // --- 4. Handle No-Op Case ---
    if columns_to_transform.is_empty() {
        // No matching string columns found, or regex didn't match anything relevant
        tracing::debug!(
            "No string columns matching regex '{}' found for normalization.",
            regex_pattern
        );
        return Ok(df); // Return original DataFrame
    }

    tracing::debug!(
        "Applying normalization to columns: {:?}",
        columns_to_transform
    );

    // --- 5. Build and Execute Lazy Plan ---
    df.lazy()
        .with_columns([
            // Select only the *validated* string columns that matched the regex
            cols(columns_to_transform) // Select multiple columns by name
                .as_expr()
                .str()
                .replace_all(lit("."), lit(""), true) // Remove '.'
                .str()
                .replace_all(lit(","), lit("."), true) // Replace ',' with '.'
                .cast(DataType::Float64), // Cast to Float64
        ])
        .collect() // Execute the lazy plan
        .map_err(PolarsViewError::from) // Convert PolarsError to PolarsViewError
}

//----------------------------------------------------------------------------//
//                                    Tests                                   //
//----------------------------------------------------------------------------//

/// Run tests with:
/// `cargo test -- --show-output tests_normalize_float_strings`
#[cfg(test)]
mod tests_normalize_float_strings {
    use super::*; // Import the function being tested

    // Helper to check DataFrame equality including nulls, with better error message
    fn assert_df_equal(df_output: &DataFrame, df_expected: &DataFrame, context: &str) {
        assert!(
            df_output.equals_missing(df_expected),
            "\nAssertion Failed: {context}\nOutput DF:\n{df_output}\nExpected DF:\n{df_expected}\n"
        );
    }

    /// Creates a shared, complex DataFrame for use in multiple tests.
    /// Corrected version wraps all string literals in Some() for Option<&str> columns.
    fn create_shared_df() -> PolarsViewResult<DataFrame> {
        df!(
            "ID" => &[1, 2, 3, 4, 5, 6, 7, 8], // Stays as i64
            // --- Value_PT_1: All elements must be Option<&str> ---
            "Value_PT_1" => &[
                Some("1.234,56"), // Wrap literal
                Some("78,90"),    // Wrap literal
                Some("1.000"),    // Wrap literal
                Some("-10,0"),    // Wrap literal
                Some("500,"),     // Keep Some()
                None,             // Keep None
                Some("0,1"),      // Keep Some()
                Some("10")        // Wrap literal
            ],
            // --- Description: Assume descriptions can be missing -> Option<&str> ---
             "Description" => &[
                 Some("A"), Some("B"), Some("C"), Some("D"),
                 Some("E"), None,      Some("G"), Some("H") // Added a None for testing
             ],
             // --- Value_PT_2: All elements must be Option<&str> ---
            "Value_PT_2" => &[
                Some("-1,0"),     // Wrap literal
                Some("2.000,5"),  // Wrap literal
                Some("3,00"),     // Wrap literal
                Some("1."),       // Wrap literal
                Some("9.999,99"), // Keep Some()
                Some("123"),      // Keep Some()
                None,             // Keep None
                Some("")          // Keep Some() for empty string
            ],
             // --- Value_US: All elements must be Option<&str> ---
            "Value_US" => &[
                Some("1,234.56"), // Wrap literal
                Some("78.90"),    // Wrap literal
                Some("1,000"),    // Wrap literal
                Some("-10.0"),    // Wrap literal
                Some("500."),     // Keep Some()
                None,             // Keep None
                Some("0.1"),      // Keep Some()
                Some("10")        // Wrap literal
            ],
             // --- Mixed_Data: All elements must be Option<&str> ---
            "Mixed_Data" => &[
                Some("1,0"),
                Some("Invalid"),
                None,
                Some(""),
                Some("-1.000,5"),
                Some(",5"),
                Some("."),
                Some("1.2.3,4.5")
            ],
             // --- Already_F64: Stays as f64 ---
            "Already_F64" => &[10.1, 20.2, 30.3, 40.4, 50.5, 60.6, 70.7, 80.8]
        )
        .map_err(PolarsViewError::from) // Convert PolarsError to PolarsViewError
    }

    #[test]
    fn test_normalize_single_pt_column() -> PolarsViewResult<()> {
        let df_input = create_shared_df()?;
        // Expected DF needs Option<f64> for the changed column, and Option<&str> for unchanged string cols
        let df_expected = df!(
                 "ID" => &[1, 2, 3, 4, 5, 6, 7, 8],
                 // This column normalized to Option<f64>
                 "Value_PT_1" => &[Some(1234.56), Some(78.90), Some(1000.0), Some(-10.0), Some(500.0), None, Some(0.1), Some(10.0)],
                 // Unchanged columns now also defined as Option<&str>
                  "Description" => &[
                      Some("A"), Some("B"), Some("C"), Some("D"),
                      Some("E"), None,      Some("G"), Some("H")
                  ],
                 "Value_PT_2" => &[
                     Some("-1,0"), Some("2.000,5"), Some("3,00"), Some("1."),
                     Some("9.999,99"), Some("123"), None, Some("")
                 ],
                 "Value_US" => &[
                     Some("1,234.56"), Some("78.90"), Some("1,000"), Some("-10.0"),
                     Some("500."), None, Some("0.1"), Some("10")
                 ],
                 "Mixed_Data" => &[
                     Some("1,0"), Some("Invalid"), None, Some(""),
                     Some("-1.000,5"), Some(",5"), Some("."), Some("1.2.3,4.5")
                 ],
                 "Already_F64" => &[10.1, 20.2, 30.3, 40.4, 50.5, 60.6, 70.7, 80.8]
            )?
            .lazy()
            // IMPORTANT: Cast expected column AFTER df! creation to match function output type
            .with_column(col("Value_PT_1").cast(DataType::Float64))
            .collect()?;

        let regex = "^Value_PT_1$"; // Select only the first PT value column

        println!("Input DF:\n{df_input}");
        println!("regex:{regex}");
        let df_output = normalize_float_strings_by_regex(df_input.clone(), regex)?;
        println!("Output DF:\n{df_output}");
        println!("Expected DF:\n{df_expected}");

        assert_df_equal(&df_output, &df_expected, "Single PT column normalization");
        assert_eq!(df_output.schema(), df_expected.schema());

        Ok(())
    }

    #[test]
    fn test_normalize_multiple_value_columns() -> PolarsViewResult<()> {
        let df_input = create_shared_df()?;
        // Expected DF needs Option<f64> for changed columns, Option<&str> for unchanged strings
        let df_expected = df!(
             "ID" => &[1, 2, 3, 4, 5, 6, 7, 8],
             "Value_PT_1" => &[Some(1234.56), Some(78.90), Some(1000.0), Some(-10.0), Some(500.0), None, Some(0.1), Some(10.0)], // Normalized f64
             "Description" => &[ Some("A"), Some("B"), Some("C"), Some("D"), Some("E"), None,      Some("G"), Some("H")], // Unchanged Option<str>
             "Value_PT_2" => &[Some(-1.0), Some(2000.5), Some(3.0), Some(1.0), Some(9999.99), Some(123.0), None, None], // Normalized f64 (empty becomes None)
             "Value_US" => &[Some(1.23456), Some(7890.0), Some(1.0), Some(-100.0), Some(500.0), None, Some(1.0), Some(10.0)], // Normalized f64
             "Mixed_Data" => &[Some("1,0"), Some("Invalid"), None, Some(""), Some("-1.000,5"), Some(",5"), Some("."), Some("1.2.3,4.5")], // Unchanged Option<str>
             "Already_F64" => &[10.1, 20.2, 30.3, 40.4, 50.5, 60.6, 70.7, 80.8] // Unchanged f64
        )?
        .lazy()
        .with_columns(vec![
            // Cast expected changed columns AFTER df! creation
            col("Value_PT_1").cast(DataType::Float64),
            col("Value_PT_2").cast(DataType::Float64),
            col("Value_US").cast(DataType::Float64),
        ])
        .collect()?;

        let regex = "^Value_.*$"; // Select all columns starting with "Value_"

        println!("Input DF:\n{df_input}");
        println!("regex:{regex}");
        let df_output = normalize_float_strings_by_regex(df_input.clone(), regex)?;
        println!("Output DF:\n{df_output}");
        println!("Expected DF:\n{df_expected}");

        assert_df_equal(
            &df_output,
            &df_expected,
            "Multiple Value_* column normalization",
        );
        assert_eq!(df_output.schema(), df_expected.schema());

        Ok(())
    }

    #[test]
    fn test_normalize_mixed_data_column() -> PolarsViewResult<()> {
        let df_input = create_shared_df()?;
        let df_expected = df!(
             "ID" => &[1, 2, 3, 4, 5, 6, 7, 8],
             "Value_PT_1" => &[Some("1.234,56"), Some("78,90"), Some("1.000"), Some("-10,0"), Some("500,"), None, Some("0,1"), Some("10")], // Option<&str> unchanged
             "Description" => &[ Some("A"), Some("B"), Some("C"), Some("D"), Some("E"), None,      Some("G"), Some("H")], // Option<&str> unchanged
             "Value_PT_2" => &[Some("-1,0"), Some("2.000,5"), Some("3,00"), Some("1."), Some("9.999,99"), Some("123"), None, Some("")], // Option<&str> unchanged
             "Value_US" => &[Some("1,234.56"), Some("78.90"), Some("1,000"), Some("-10.0"), Some("500."), None, Some("0.1"), Some("10")], // Option<&str> unchanged
             "Mixed_Data" => &[Some(1.0), None::<f64>, None::<f64>, None::<f64>, Some(-1000.5), Some(0.5), None::<f64>, Some(123.45)], // Option<f64> normalized
             "Already_F64" => &[10.1, 20.2, 30.3, 40.4, 50.5, 60.6, 70.7, 80.8] // f64 unchanged
        )?
        .lazy()
        .with_column(col("Mixed_Data").cast(DataType::Float64)) // Cast expected changed column
        .collect()?;

        let regex = "^Mixed_Data$"; // Select only the mixed data column

        println!("Input DF:\n{df_input}");
        println!("regex:{regex}");
        let df_output = normalize_float_strings_by_regex(df_input.clone(), regex)?;
        println!("Output DF:\n{df_output}");
        println!("Expected DF:\n{df_expected}");

        assert_df_equal(&df_output, &df_expected, "Mixed_Data column normalization");
        assert_eq!(df_output.schema(), df_expected.schema());

        Ok(())
    }

    #[test]
    fn test_normalize_no_matching_columns() -> PolarsViewResult<()> {
        let df_input = create_shared_df()?;
        let df_expected = df_input.clone(); // Expect identical output

        let regex = "^NonExistent_$"; // Regex that doesn't match

        println!("Input DF:\n{df_input}");
        println!("regex:{regex}");
        let df_output = normalize_float_strings_by_regex(df_input.clone(), regex)?;
        println!("Output DF:\n{df_output}");
        println!("Expected DF:\n{df_expected}");

        assert_df_equal(&df_output, &df_expected, "No matching columns");
        assert_eq!(df_output.schema(), df_expected.schema());

        Ok(())
    }

    #[test]
    fn test_normalize_error_on_already_float() -> PolarsViewResult<()> {
        let df_input = create_shared_df()?;
        let regex = "^Already_F64$"; // Select only the f64 column

        println!("Input DF:\n{df_input}");
        println!("regex:{regex}");
        let result = normalize_float_strings_by_regex(df_input.clone(), regex); // Expect error
        println!("Result (expecting error): {result:?}");

        assert!(
            result.is_err(),
            "Expected an error when running on f64 column, but got Ok"
        );

        if let Err(e) = result {
            println!("Got expected error: {e}");
            // Check that the error is related to the string namespace function
            assert!(
                e.to_string().contains("str"),
                "Error message should indicate string function failure"
            );
        }

        Ok(()) // Test succeeds if the error occurred
    }

    #[test]
    fn test_normalize_specific_col() -> PolarsViewResult<()> {
        println!("--- Test: test_normalize_specific_col ---");
        let df_input = df!(
            "ID" => &[1, 2, 3],
            "Value_EU" => &["1.234,56", "78,90", "100"], // String
            "Value_US" => &["1,234.56", "78.90", "100.00"], // String
            "Amount" => &[1234.56, 78.90, 100.0]        // Float64
        )?;
        let regex = "^Value_EU$";
        let df_expected = df!(
             "ID" => &[1, 2, 3],
             "Value_EU" => &[1234.56, 78.90, 100.0], // Now Float64
             "Value_US" => &["1,234.56", "78.90", "100.00"], // Unchanged String
             "Amount" => &[1234.56, 78.90, 100.0]        // Unchanged Float64
        )?;

        println!("Input DF:\n{df_input}");
        println!("Regex: {regex}");
        let df_output = normalize_float_strings_by_regex(df_input.clone(), regex)?;
        println!("Output DF:\n{df_output}");
        println!("Expected DF:\n{df_expected}");

        assert!(df_output.equals_missing(&df_expected));
        Ok(())
    }

    #[test]
    fn test_normalize_regex_multi_col() -> PolarsViewResult<()> {
        println!("--- Test: test_normalize_regex_multi_col ---");
        let df_input = df!(
            "Product" => &["A", "B"],
            "Price_EU" => &["1.000,50", "25,00"],
            "Tax_EU" => &["200,10", "5,00"],
            "Cost_US" => &["900.00", "20.00"]
        )?;
        let regex = "^.*_EU$";
        let df_expected = df!(
           "Product" => &["A", "B"],
            "Price_EU" => &[1000.50, 25.0], // Float64
            "Tax_EU" => &[200.10, 5.0], // Float64
            "Cost_US" => &["900.00", "20.00"] // Unchanged
        )?;

        println!("Input DF:\n{df_input}");
        println!("Regex: {regex}");
        let df_output = normalize_float_strings_by_regex(df_input.clone(), regex)?;
        println!("Output DF:\n{df_output}");
        println!("Expected DF:\n{df_expected}");

        assert!(df_output.equals_missing(&df_expected));
        Ok(())
    }

    #[test]
    fn test_normalize_wildcard() -> PolarsViewResult<()> {
        println!("--- Test: test_normalize_wildcard ---");
        // Test case 1: Wildcard causing error due to non-string type
        let df_input_error = df!(
            "A_string" => &["1.000,50", "25,00"],
            "B_string" => &["200,10", "5,00"],
            "C_int" => &[1, 2]
        )?;
        let regex_error = "*";

        println!("Test Case 1: Wildcard with mixed types");
        println!("Input DF:\n{df_input_error}");
        println!("Regex: {regex_error}");
        let result = normalize_float_strings_by_regex(df_input_error.clone(), regex_error);
        println!("Result (expecting error): {result:?}");
        assert!(matches!(
            result,
            Err(PolarsViewError::InvalidDataTypeForRegex { .. })
        ));
        println!("--");

        // Test case 2: Wildcard working with only string types
        let df_only_strings = df!(
             "A_string" => &["1.000,50", "25,00"],
             "B_string" => &["200,10", "5,00"]
        )?;
        let regex_ok = "*";
        let expected_df = df!(
             "A_string" => &[1000.5, 25.0],
             "B_string" => &[200.1, 5.0]
        )?;

        println!("Test Case 2: Wildcard with only string types");
        println!("Input DF:\n{df_only_strings}");
        println!("Regex: {regex_ok}");
        let df_output_ok = normalize_float_strings_by_regex(df_only_strings.clone(), regex_ok)?;
        println!("Output DF:\n{df_output_ok}");
        println!("Expected DF:\n{expected_df}");
        assert!(df_output_ok.equals_missing(&expected_df));
        Ok(())
    }

    #[test]
    fn test_error_invalid_regex_pattern_format() -> PolarsViewResult<()> {
        println!("--- Test: test_error_invalid_regex_pattern_format ---");
        let df_input = df!("col_A" => &["1,23"])?;
        let regex = "Value_EU"; // Does not start with ^ or end with $

        println!("Input DF:\n{df_input}");
        println!("Regex: {regex}");
        let result = normalize_float_strings_by_regex(df_input, regex);
        println!("Result (expecting error InvalidRegexPattern): {result:?}");

        assert!(matches!(result, Err(PolarsViewError::InvalidRegexPattern(s)) if s == regex));
        Ok(())
    }

    #[test]
    fn test_error_invalid_regex_syntax() -> PolarsViewResult<()> {
        println!("--- Test: test_error_invalid_regex_syntax ---");
        let df_input = df!("col_A" => &["1,23"])?;
        let regex = "^Val[ue$"; // Invalid syntax '['

        println!("Input DF:\n{df_input}");
        println!("Regex: {regex}");
        let result = normalize_float_strings_by_regex(df_input, regex);
        println!("Result (expecting error InvalidRegexSyntax): {result:?}");

        assert!(matches!(
            result,
            Err(PolarsViewError::InvalidRegexSyntax { pattern, .. }) if pattern == regex
        ));
        Ok(())
    }

    #[test]
    fn test_error_non_string_column_match() -> PolarsViewResult<()> {
        println!("--- Test: test_error_non_string_column_match ---");
        let df_input = df!(
            "Value_EU" => &["1.000,50"], // String
            "Count_EU" => &[1000i64] // Int64 - corrected type for df!
        )?;
        let regex = "^.*_EU$"; // Matches both "Value_EU" (String) and "Count_EU" (Int64)

        println!("Input DF:\n{df_input}");
        println!("Regex: {regex}");
        let result = normalize_float_strings_by_regex(df_input, regex);
        println!("Result (expecting error InvalidDataTypeForRegex): {result:?}");

        assert!(matches!(
            result,
            Err(PolarsViewError::InvalidDataTypeForRegex{ pattern, columns })
            if pattern == regex && columns.contains(&"'Count_EU' (Type: i64)".to_string())
        ));
        Ok(())
    }

    #[test]
    fn test_empty_dataframe() -> PolarsViewResult<()> {
        println!("--- Test: test_empty_dataframe ---");
        let df_input = DataFrame::default();
        let regex_wildcard = "*";
        let regex_pattern = "^.*$";
        let df_expected = df_input.clone();

        println!("Input DF (empty):\n{df_input}");

        println!("Regex: {regex_wildcard}");
        let df_output_wild = normalize_float_strings_by_regex(df_input.clone(), regex_wildcard)?;
        println!("Output DF (wildcard):\n{df_output_wild}");
        println!("Expected DF (empty):\n{df_expected}");
        assert!(df_output_wild.equals(&df_expected));
        println!("--");

        println!("Regex: {regex_pattern}");
        let df_output_regex = normalize_float_strings_by_regex(df_input.clone(), regex_pattern)?;
        println!("Output DF (regex):\n{df_output_regex}");
        println!("Expected DF (empty):\n{df_expected}");
        assert!(df_output_regex.equals(&df_expected));
        Ok(())
    }
}
