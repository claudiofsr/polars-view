use polars::prelude::*;

/// Removes columns from a DataFrame that consist entirely of null values.
///
/// This optimized function identifies non-null columns without cloning them
/// and then selects only those, creating a new DataFrame efficiently.
///
/// ### Arguments
///
/// * `df`: The input `DataFrame` to be processed.
///
/// ### Returns
///
/// A `PolarsResult` with a new `DataFrame` containing only non-null columns,
/// or a `PolarsError` on failure.
pub fn remove_null_columns(df: DataFrame) -> PolarsResult<DataFrame> {
    // Partition columns into two groups: those to keep and those to remove.
    // A column is kept if it contains at least one non-null value.
    let (cols_to_keep, cols_to_remove): (Vec<&Column>, Vec<&Column>) = df
        .get_columns()
        .iter()
        // partition consumes the iterator and separates elements based on the predicate.
        // Elements for which the predicate is true go into the first collection,
        // and the rest go into the second collection.
        .partition(|col| col.is_not_null().any()); // Predicate: keep if any value is not null

    // Extract the names of the columns that will be kept.
    let columns_to_keep: Vec<PlSmallStr> =
        cols_to_keep.iter().map(|col| col.name().clone()).collect();

    // Extract the names of the columns that will be removed.
    let columns_to_remove: Vec<&PlSmallStr> = cols_to_remove.iter().map(|col| col.name()).collect();

    // Log the names of removed columns for debugging purposes.
    tracing::debug!(removed_columns = ?columns_to_remove,
        "{} columns removed for being fully null",
        cols_to_remove.len()
    );

    // Create a new DataFrame containing only the desired columns.
    df.select(columns_to_keep)
}

//----------------------------------------------------------------------------//
//                                   Tests                                    //
//----------------------------------------------------------------------------//

/// Run tests with:
/// `cargo test -- --show-output tests_remove_null_columns`
#[cfg(test)]
mod tests_remove_null_columns {
    use super::*;

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

        dbg!(&df_input);
        let df_output = remove_null_columns(df_input)?;
        dbg!(&df_output);

        assert!(
            df_output.equals_missing(&df_expected),
            "Failed removing some null columns.\nOutput:\n{df_output:?}\nExpected:\n{df_expected:?}"
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

        dbg!(&df_input);
        let df_output = remove_null_columns(df_input)?;
        dbg!(&df_output);

        assert!(
            df_output.equals_missing(&df_expected),
            "Failed when no columns should be removed.\nOutput:\n{df_output:?}\nExpected:\n{df_expected:?}"
        );

        Ok(())
    }

    #[test]
    fn empty_dataframe_input() -> PolarsResult<()> {
        let df_input = DataFrame::default(); // Creates an empty DataFrame
        let df_expected = DataFrame::default(); // Expect an empty DataFrame back

        dbg!(&df_input);
        let df_output = remove_null_columns(df_input)?;
        dbg!(&df_output);

        // Use equals for empty DataFrames as equals_missing might behave differently
        assert!(
            df_output.equals(&df_expected),
            "Failed for empty input DataFrame.\nOutput:\n{df_output:?}\nExpected:\n{df_expected:?}"
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

        dbg!(&df_input);
        let df_output = remove_null_columns(df_input)?;
        dbg!(&df_output);

        assert!(
            df_output.equals(&df_expected),
            "Failed when all columns are null.\nOutput:\n{df_output:?}\nExpected:\n{df_expected:?}"
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

        dbg!(&df_input);
        let df_output = remove_null_columns(df_input)?;
        dbg!(&df_output);

        assert!(
            df_output.equals(&df_expected),
            "Failed for DataFrame with zero rows.\nOutput:\n{df_output:?}\nExpected:\n{df_expected:?}"
        );

        Ok(())
    }
}
