use crate::{PolarsViewError, PolarsViewResult};
use polars::prelude::*;
use regex::Regex;

/// Drops columns from a DataFrame whose names match the provided regex pattern.
///
/// The pattern must either be the wildcard `"*"` (to drop all columns)
/// or a valid regex string enclosed in `^` and `$` anchors
/// (e.g., `"^Value_.*$"`).
///
/// # Arguments
/// * `df`: The input `DataFrame`.
/// * `regex_pattern`: The regex pattern string or `"*"` used to match column names.
///
/// # Returns
/// * `PolarsViewResult<DataFrame>`: A new `DataFrame` with the matching columns dropped,
///   or a `PolarsViewError` if the regex is invalid or a Polars operation fails.
///
/// # Errors
/// * `PolarsViewError::InvalidRegexPattern`: If `regex_pattern` is not `"*"` and does not
///   start with `^` and end with `$`.
/// * `PolarsViewError::InvalidRegexSyntax`: If `regex_pattern` has invalid regex syntax.
/// * `PolarsViewError::Polars`: If the underlying Polars `drop` or `collect` operation fails.
pub fn drop_columns_by_regex(df: DataFrame, regex_pattern: &str) -> PolarsViewResult<DataFrame> {
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

    // --- 2. Identify Columns to Drop ---
    // Efficiently collect names matching the pattern directly into the vector.
    let columns_to_drop: Vec<PlSmallStr> = df // Iterate over columns directly from the DataFrame
        .get_column_names_owned()
        .into_iter() // Convert Vec into an iterator
        .filter(|col_name| {
            // Match against the compiled regex or wildcard
            match &compiled_regex {
                Some(re) => re.is_match(col_name), // Check regex match
                None => true,                      // Wildcard "*" matches all columns
            }
        })
        .collect(); // Collect the PlSmallStr into a Vec    

    // --- 3. Handle No-Op Case ---
    // If no columns match the pattern (or the input df was empty), return the original DataFrame.
    if columns_to_drop.is_empty() {
        tracing::debug!(
            "No columns matching regex '{}' found to drop. Returning original DataFrame.",
            regex_pattern
        );
        return Ok(df);
    }

    tracing::debug!("Dropping columns: {:?}", columns_to_drop);

    // --- 4. Build and Execute Lazy Plan ---
    // Drop the identified columns using the Lazy API for potential optimizations.
    df.lazy()
        .drop(by_name(columns_to_drop, true)) // Remove the specified columns
        .collect() // Execute the lazy plan
        .map_err(PolarsViewError::from) // Convert PolarsError to PolarsViewError
}

//----------------------------------------------------------------------------//
//                                    Tests                                   //
//----------------------------------------------------------------------------//

/// Run tests with:
/// cargo test -- --show-output tests_drop_cols`
#[cfg(test)]
mod tests_drop_cols {
    use super::*; // Import the function being tested
    use crate::PolarsViewError;
    use polars::df; // Convenient macro for DataFrame creation // Make sure the error type is in scope

    // Helper to check DataFrame equality including nulls, with better error message
    fn assert_df_equal(df_output: &DataFrame, df_expected: &DataFrame, context: &str) {
        assert!(
            df_output.equals_missing(df_expected),
            "\nAssertion Failed: {}\n\nOutput DF:\n{}\nSchema: {:?}\n\nExpected DF:\n{}\nSchema: {:?}\n",
            context,
            df_output,
            df_output.schema(),
            df_expected,
            df_expected.schema()
        );
    }

    /// Creates a shared test DataFrame.
    fn create_shared_test_df() -> PolarsViewResult<DataFrame> {
        df!(
            "ID" => &[1, 2],
            "Value_A" => &["apple", "banana"],
            "Description B" => &[Some("desc 1"), None], // Nullable string
            "Value_C" => &[10.1, 20.2],
            "IgnoreMe" => &[true, false]
        )
        .map_err(PolarsViewError::from)
    }

    #[test]
    fn test_drop_single_column() -> PolarsViewResult<()> {
        let df_input = create_shared_test_df()?;
        let regex = r#"^Description B$"#; // Exact match, note raw string `r#""#` for convenience

        let df_expected = df!(
            "ID" => &[1, 2],
            "Value_A" => &["apple", "banana"],
            // "Description B" is dropped
            "Value_C" => &[10.1, 20.2],
            "IgnoreMe" => &[true, false]
        )?;

        println!("Input DF:\n{df_input}");
        println!("regex:{regex}");
        let df_output = drop_columns_by_regex(df_input, regex)?;
        println!("Output DF:\n{df_output}");
        println!("Expected DF:\n{df_expected}");

        assert_df_equal(
            &df_output,
            &df_expected,
            "Drop single column 'Description B'",
        );
        assert_eq!(df_output.width(), 4); // Verify column count
        Ok(())
    }

    #[test]
    fn test_drop_multiple_columns_pattern() -> PolarsViewResult<()> {
        let df_input = create_shared_test_df()?;
        let regex = r#"^Value_.*$"#; // Match columns starting with "Value_"

        let df_expected = df!(
            "ID" => &[1, 2],
            // Value_A dropped
            "Description B" => &[Some("desc 1"), None],
            // Value_C dropped
            "IgnoreMe" => &[true, false]
        )?;

        println!("Input DF:\n{df_input}");
        println!("regex:{regex}");
        let df_output = drop_columns_by_regex(df_input, regex)?;
        println!("Output DF:\n{df_output}");
        println!("Expected DF:\n{df_expected}");

        assert_df_equal(
            &df_output,
            &df_expected,
            "Drop columns matching '^Value_.*$'",
        );
        assert_eq!(df_output.width(), 3);
        Ok(())
    }

    #[test]
    fn test_drop_all_columns_wildcard() -> PolarsViewResult<()> {
        let df_input = create_shared_test_df()?;
        let regex = "*"; // Wildcard to drop all

        // EXPECTED: When dropping all columns with Polars lazy drop, the result is a 0x0 DataFrame.
        // The most idiomatic way to represent this is DataFrame::empty().
        let df_expected = DataFrame::empty();

        println!("Input DF:\n{df_input}");
        println!("regex:{regex}");
        let df_output = drop_columns_by_regex(df_input, regex)?;
        println!("Output DF:\n{df_output}");
        println!("Expected DF:\n{df_expected}");

        // Check schema has 0 columns AND 0 rows now
        assert_eq!(df_output.width(), 0, "Wildcard should drop all columns");
        assert_eq!(
            df_output.height(),
            0,
            "Dropping all columns via lazy drop results in 0 height"
        ); // Updated assertion

        // Compare the 0x0 output with the 0x0 expected DF
        assert_df_equal(&df_output, &df_expected, "Drop all columns using wildcard");
        Ok(())
    }

    #[test]
    fn test_drop_no_matching_columns() -> PolarsViewResult<()> {
        let df_input = create_shared_test_df()?;
        let regex = r#"^NonExistent$"#; // Pattern matches nothing

        // Expected output is the same as the input
        let df_expected = df_input.clone();

        println!("Input DF:\n{df_input}");
        println!("regex:{regex}");
        let df_output = drop_columns_by_regex(df_input, regex)?;
        println!("Output DF:\n{df_output}");
        println!("Expected DF:\n{df_expected}");

        assert_df_equal(&df_output, &df_expected, "No matching columns to drop");
        assert_eq!(df_output.width(), 5);
        Ok(())
    }

    #[test]
    fn test_invalid_regex_syntax() {
        let df_input = create_shared_test_df().unwrap();
        // Invalid regex syntax (unmatched bracket)
        let regex = r#"^Value[.*$"#;

        let result = drop_columns_by_regex(df_input, regex);

        assert!(
            result.is_err(),
            "Expected an error for invalid regex syntax"
        );
        // Check that the error is specifically InvalidRegexSyntax
        assert!(
            matches!(
                result.as_ref().unwrap_err(),
                PolarsViewError::InvalidRegexSyntax { pattern, error: _ } if pattern == regex
            ),
            "Expected InvalidRegexSyntax error, got {result:?}"
        );
    }

    #[test]
    fn test_invalid_regex_format() {
        let df_input = create_shared_test_df().unwrap();
        // Invalid format (missing ^ or $)
        let regex = "Value_.*";

        let result = drop_columns_by_regex(df_input, regex);

        assert!(
            result.is_err(),
            "Expected an error for invalid regex format"
        );
        // Check that the error is specifically InvalidRegexPattern
        assert!(
            matches!(
                result.as_ref().unwrap_err(),
                PolarsViewError::InvalidRegexPattern(pattern) if pattern == regex
            ),
            "Expected InvalidRegexPattern error, got {result:?}"
        );
    }

    #[test]
    fn test_empty_input_df() -> PolarsViewResult<()> {
        // Create an empty DataFrame with a schema but no rows
        let df_input = df!(
             "A" => Vec::<i32>::new(),
             "B" => Vec::<String>::new()
        )?;
        let regex = r#"^A$"#; // A pattern that would match if there were columns

        let df_expected = df!(
             "B" => Vec::<String>::new()
        )?;

        println!("Input DF:\n{df_input}");
        println!("regex:{regex}");
        let df_output = drop_columns_by_regex(df_input, regex)?;
        println!("Output DF:\n{df_output}");
        println!("Expected DF:\n{df_expected}");

        assert_df_equal(&df_output, &df_expected, "Empty input DataFrame");
        assert!(df_output.is_empty()); // Double check it's empty
        Ok(())
    }

    #[test]
    fn test_wildcard_on_empty_df() -> PolarsViewResult<()> {
        // Create an empty DataFrame
        let df_input = df!(
             "A" => Vec::<i32>::new(),
             "B" => Vec::<String>::new()
        )?;
        let regex = "*"; // Wildcard pattern

        let df_expected = DataFrame::empty();

        println!("Input DF:\n{df_input}");
        println!("regex:{regex}");
        let df_output = drop_columns_by_regex(df_input, regex)?;
        println!("Output DF:\n{df_output}");
        println!("Expected DF:\n{df_expected}");

        assert_df_equal(&df_output, &df_expected, "Wildcard on empty DataFrame");
        assert_eq!(df_output.width(), 0);
        assert_eq!(df_output.height(), 0);
        Ok(())
    }
}
