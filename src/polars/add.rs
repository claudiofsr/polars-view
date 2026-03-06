use polars::{io::RowIndex, prelude::*}; // Includes PolarsError and PolarsResult

/// Conditionally adds a row index column to a DataFrame based on an explicit `Option<RowIndex>`.
///
/// If a `Some(RowIndex)` configuration is provided (name already resolved for uniqueness)
/// this function adds the index column. Otherwise, it returns the original DataFrame unmodified.
///
/// This function does *not* check filter flags or resolve uniqueness; that is the responsibility
/// of the caller (typically a method in `DataFilter`).
///
/// ### Arguments
/// * `df` - The input `DataFrame`.
/// * `opt_row_index` - Optional configuration resolved by the caller.
///
/// ### Returns
/// A `PolarsResult` containing the potentially modified `DataFrame`.
// Signature takes Option<RowIndex>
pub fn add_row_index_column(
    df: DataFrame,
    opt_row_index: Option<RowIndex>,
) -> PolarsResult<DataFrame> {
    match opt_row_index {
        Some(row_index) => {
            // Config exists and name is already resolved to be unique
            tracing::debug!(
                "Adding row index column '{}' with offset {}.",
                row_index.name,
                row_index.offset
            );
            df.lazy() // Use lazy for efficiency
                .with_row_index(row_index.name, Some(row_index.offset))
                .collect() // Execute lazy plan
        }
        None => {
            // No config provided (e.g., feature disabled by filter)
            tracing::trace!(
                "Row index addition not requested or config could not be resolved by caller."
            );
            Ok(df) // No index needed, return original df
        }
    }
}

//----------------------------------------------------------------------------//
//                                   Tests                                    //
//----------------------------------------------------------------------------//

/// Run tests with:
/// cargo test -- --show-output tests_add_row_index_column`
#[cfg(test)]
mod tests_add_row_index_column {
    use super::*;
    use crate::{DataFilter, MAX_ATTEMPTS};

    // Helper to create a DataFilter with row index enabled and configured
    fn get_filter(enabled: bool, name: &str, offset: u32) -> DataFilter {
        DataFilter {
            add_row_index: enabled,
            index_column_name: name.to_string(),
            index_column_offset: offset,
            ..Default::default()
        }
    }

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

    #[test]
    fn test_add_index_col_when_disabled() -> PolarsResult<()> {
        let df_input = df! {"col_a" => &[1, 2, 3], "col_b" => &["x", "y", "z"]}?;
        let df_expected = df_input.clone();

        // Filter with add_row_index disabled
        let filter = get_filter(false, "Any Name", 1);

        let row_index = filter.get_row_index(df_input.schema())?;

        println!("df_input: {df_input}");
        println!("row_index: {row_index:?}");
        let df_output = add_row_index_column(df_input.clone(), row_index)?;
        println!("df_output: {df_output}");

        assert_df_equal(&df_output, &df_expected, "add_index_col_when_disabled");

        Ok(())
    }

    // Test case with default Polars behavior (name="index", offset=0)

    #[test]
    fn test_add_index_col_when_enabled_default_name_offset() -> PolarsResult<()> {
        let df_input = df! {"data" => &[10, 20]}?;

        // Use df! macro for comparison with known structure
        let df_expected = df! {
            "index" => &[0u32, 1],
            "data" => &[10, 20]
        }?;

        // Filter configured for default behavior
        let filter = get_filter(true, "index", 0);

        let row_index = filter.get_row_index(df_input.schema())?;

        println!("df_input: {df_input}");
        println!("row_index: {row_index:?}");
        let df_output = add_row_index_column(df_input.clone(), row_index)?;
        println!("df_output: {df_output}");

        assert_df_equal(
            &df_output,
            &df_expected,
            r#"Test case with default Polars behavior (name="index", offset=0)"#,
        );

        Ok(())
    }

    #[test]
    fn test_add_index_col_when_enabled_custom_name_offset() -> PolarsResult<()> {
        let df_input = df! {"Value" => &[1.1, 2.2, 3.3]}?; // DataFrame with 3 rows
        let custom_name = "row_num";
        let custom_offset = 1u32; // Start from 1

        // Expected data with custom name and offset
        let df_expected = df! {
            custom_name => &[custom_offset, custom_offset + 1, custom_offset + 2],
            "Value" => &[1.1, 2.2, 3.3]
        }?;

        // Filter configured for custom behavior
        let filter = get_filter(true, custom_name, custom_offset);

        let row_index = filter.get_row_index(df_input.schema())?;

        println!("df_input: {df_input}");
        println!("row_index: {row_index:?}");
        let df_output = add_row_index_column(df_input.clone(), row_index)?;
        println!("df_output: {df_output}");

        assert_df_equal(
            &df_output,
            &df_expected,
            r#"Test case with row_index {name="row_num", offset=1}"#,
        );

        Ok(())
    }

    #[test]
    fn test_add_index_col_name_conflict_base() -> PolarsResult<()> {
        // DataFrame contains "Row Number" already
        let df_input = df! {"Row Number" => &[99, 100], "data" => &["a", "b"]}?;
        let expected_name = "Row Number_1"; // Expected resolution

        let df_expected = df! {
            expected_name => &[0, 1],
            "Row Number" => &[99, 100],
            "data" => &["a", "b"],
        }?;

        // Filter configured for default name ("Row Number", offset=0), add enabled
        let filter = get_filter(true, "Row Number", 0);

        let row_index = filter.get_row_index(df_input.schema())?;

        println!("df_input: {df_input}");
        println!("row_index: {row_index:?}");
        let df_output = add_row_index_column(df_input.clone(), row_index)?;
        println!("df_output: {df_output}");

        assert_df_equal(
            &df_output,
            &df_expected,
            r#"Test case with row_index {name=Row Number", offset=1}"#,
        );

        Ok(())
    }

    #[test]
    fn test_add_index_col_name_conflict_multiple_suffixes() -> PolarsResult<()> {
        // Input dataframe contains columns that conflict with base name + common suffixes
        let df_input = df! {
             "CustomID" => &[1], // Base name
             "CustomID_1" => &[2], // Conflicts with _1
             "CustomID_2" => &[3]  // Conflicts with _2
        }?;
        let expected_name = "CustomID_3"; // Expected resolution

        let df_expected = df! {
            expected_name => &[10],
            "CustomID" => &[1], // Base name
            "CustomID_1" => &[2], // Conflicts with _1
            "CustomID_2" => &[3]  // Conflicts with _2
        }?;

        // Filter configured with base name "CustomID", offset 10
        let filter = get_filter(true, "CustomID", 10);

        let row_index = filter.get_row_index(df_input.schema())?;

        println!("df_input: {df_input}");
        println!("row_index: {row_index:?}");
        let df_output = add_row_index_column(df_input.clone(), row_index)?;
        println!("df_output: {df_output}");

        assert_df_equal(
            &df_output,
            &df_expected,
            r#"Test case with row_index {name=Row Number", offset=1}"#,
        );

        Ok(())
    }

    // Add test for the error case when unique name cannot be found
    #[test]
    fn test_add_index_col_name_conflict_max_attempts_error() -> PolarsResult<()> {
        let mut df_input = df! {"BaseName" => &[1]}?;

        for i in 0..MAX_ATTEMPTS {
            // Conflict with BaseName, BaseName_0 to BaseName_999
            // Use format!("{}_{}", base_name, suffix_counter) pattern (starts suffix at 1 in helper)
            let suffix = i + 1; // Helper logic starts suffix at 1
            let col = Column::new(format!("BaseName_{suffix}").into(), &[i as i32]);
            df_input.with_column(col)?;
        }

        println!("1. df_input: {df_input}");

        // Add conflict for the base name itself
        let col = Column::new("BaseName".into(), &[9999]);
        df_input
            .with_column(col)
            .expect("Adding base name column failed");

        println!("2. df_input: {df_input}");

        let filter = get_filter(true, "BaseName", 0); // Use "BaseName" as base

        // The helper should fail after MAX_ATTEMPTS (trying BaseName_1 to BaseName_1000)
        // Check that the function returns an error and the error message contains the expected text
        match filter.get_row_index(df_input.schema()) {
            Ok(_) => panic!("Expected error due to max attempts, but got Ok"),
            Err(e) => {
                println!("error: '{e}'");
                // Check the error type and message
                assert!(matches!(e, PolarsError::ComputeError(_)));
                assert!(
                    e.to_string()
                        .contains("Failed to find a unique column name starting with")
                );
                assert!(e.to_string().contains("BaseName"));
                assert!(e.to_string().contains("1000"));
            }
        }

        Ok(()) // Return Ok here as the test *successfully* checked the error case
    }

    #[test] // Note: No #[tokio::test] needed here
    fn test_add_index_col_empty_dataframe() -> PolarsResult<()> {
        let df_input = DataFrame::empty(); // Create an empty DataFrame
        let expected_df = df! {"RowID" => Vec::<u32>::new()}?; // Expected: empty DataFrame with the new schema

        // Test with enabled index and custom name/offset on an empty DataFrame
        let filter = get_filter(true, "RowID", 0);

        let row_index = filter.get_row_index(df_input.schema())?;

        // Call the helper SYNCHRONOUSLY using '?'
        let df_output = add_row_index_column(df_input.clone(), row_index)?;

        // For empty dataframes, compare schemas and dimensions
        assert_eq!(df_output.schema(), expected_df.schema());
        assert_eq!(df_output.height(), 0); // Height should remain 0
        assert_eq!(df_output.width(), 1); // Should have 1 column
        assert!(df_output.column("RowID").is_ok());

        Ok(())
    }
}
