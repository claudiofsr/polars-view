use polars::prelude::*;

use crate::DataFilter;

/// Conditionally adds a row index column to a DataFrame based on filter settings.
///
/// If the `DataFilter` indicate that a row index should be added (`get_row_index` returns `Some`),
/// this function uses Polars' `with_row_index` on a lazy frame to efficiently add the index column
/// with the specified name and offset. Otherwise, it returns the original DataFrame unmodified.
///
/// ### Arguments
/// * `df` - The input `DataFrame`.
/// * `filter` - Configuration determining if and how to add the row index.
///
/// ### Returns
/// A `PolarsResult` containing the potentially modified `DataFrame`.
pub fn add_row_index_column(df: DataFrame, filter: &DataFilter) -> PolarsResult<DataFrame> {
    // Determine if an index column needs to be added based on filter.
    match filter.get_row_index() {
        // If configuration exists for the index column:
        Some(row_index) => df
            .lazy() // Operate lazily for potential performance benefits.
            // Add the index column using the provided name and offset.
            .with_row_index(row_index.name, Some(row_index.offset))
            .collect(), // Execute the lazy plan and return the new DataFrame.
        // If no index column configuration is found (e.g., feature disabled):
        None => Ok(df), // Return the original DataFrame unmodified.
    }
}

//----------------------------------------------------------------------------//
//                                   Tests                                    //
//----------------------------------------------------------------------------//

/// Run tests with:
/// cargo test -- --show-output tests_add_row_index_column`
#[cfg(test)]
mod tests_add_row_index_column {
    use super::*; // Import items from parent module
    use std::sync::Arc;

    // Helper to create DataFilter for tests
    fn create_test_filters(
        add_index: bool,
        base_name: &str,
        offset: u32,
        existing_cols: &[&str], // Names of columns already in the schema
    ) -> DataFilter {
        let fields = existing_cols
            .iter()
            .map(|&name| Field::new(name.into(), DataType::Int32)); // Dummy type

        let schema = Schema::from_iter(fields);

        DataFilter {
            add_row_index: add_index,
            index_column_name: base_name.to_string(),
            index_column_offset: offset,
            schema_without_index: Arc::new(schema),
            ..Default::default() // For other fields if they exist
        }
    }

    // === Tests for add_row_index_column ===

    #[test]
    fn test_add_index_col_when_disabled() -> PolarsResult<()> {
        // Return PolarsResult
        let df_input = df! {"col_a" => &[1, 2, 3], "col_b" => &["x", "y", "z"]}?;
        let df_expected = df_input.clone(); // Expected output is the same as input
        let filters = create_test_filters(false, "Index", 0, &["col_a", "col_b"]);

        println!("Input DF:\n{}", df_input);
        let df_output = add_row_index_column(df_input.clone(), &filters)?;
        println!("Output DF:\n{}", df_output);
        println!("Expected DF:\n{}", df_expected);

        // DataFrame should be unchanged
        assert!(df_output.equals(&df_expected));
        assert_eq!(df_output.schema(), df_expected.schema());

        Ok(()) // Return Ok on success
    }

    #[test]
    fn test_add_index_col_when_enabled_default() -> PolarsResult<()> {
        // Return PolarsResult
        let df_input = df! {"data" => &[10, 20]}?;
        // Filters enable index, base name "Index" is unique in original df's schema
        let schema_ref = df_input.schema();
        let mut filters = create_test_filters(true, "Index", 0, &[]); // Conceptually no existing "Index"
        filters.schema_without_index = schema_ref.clone(); // Provide actual schema for uniqueness check

        let df_expected = df! {
            "Index" => &[0u32, 1], // Offset 0 is default Polars row index
            "data" => &[10, 20]
        }?;

        println!("Input DF:\n{}", df_input);
        let df_output = add_row_index_column(df_input.clone(), &filters)?;
        println!("Output DF:\n{}", df_output);
        println!("Expected DF:\n{}", df_expected);

        // Polars adds the new column first by default with `with_row_index`
        assert!(df_output.equals(&df_expected)); // `equals` is usually sufficient
        assert!(df_output.column("Index").is_ok());
        assert_eq!(df_output.width(), 2);
        assert_eq!(
            df_output.column("Index")?.dtype(),
            &DataType::UInt32 // Default row index type
        );

        Ok(()) // Return Ok on success
    }

    #[test]
    fn test_add_index_col_when_enabled_with_offset_and_conflict() -> PolarsResult<()> {
        // Return PolarsResult
        let df_input = df! {"Index" => &["a", "b", "c"], "Value" => &[1.1, 2.2, 3.3]}?;
        // Filters enable index, base name "Index" exists, offset 1
        let schema_ref = df_input.schema();
        let mut filters = create_test_filters(true, "Index", 1, &[]); // Conceptually no "Index_1" yet
        // Tell the filter about the *actual* existing columns before adding the index
        filters.schema_without_index = schema_ref.clone();

        // Expected column name is "Index_1", values start from 1
        let df_expected = df! {
            "Index_1" => &[1u32, 2, 3], // Offset 1 means index starts at 1
            "Index" => &["a", "b", "c"],
            "Value" => &[1.1, 2.2, 3.3]
        }?
        // Explicitly select columns in the order Polars likely produces
        .select(["Index_1", "Index", "Value"])?;

        println!("Input DF:\n{}", df_input);
        let df_output = add_row_index_column(df_input.clone(), &filters)?;
        println!("Output DF:\n{}", df_output);
        println!("Expected DF:\n{}", df_expected);

        // Use equals_ordered for strict comparison including column order
        // Note: equals_missing compares ignoring null differences, equals checks strictly
        assert!(df_output.equals_missing(&df_expected));
        assert!(df_output.column("Index_1").is_ok());
        assert_eq!(df_output.width(), 3);

        Ok(()) // Return Ok on success
    }

    #[test]
    fn test_add_index_col_empty_dataframe() -> PolarsResult<()> {
        // Return PolarsResult
        let df_input = DataFrame::empty(); // Create an empty DataFrame
        // Enable index, base name is unique
        let schema_ref = df_input.schema();
        let mut filters = create_test_filters(true, "RowID", 0, &[]);
        filters.schema_without_index = schema_ref.clone();

        // Expected output: empty dataframe with just the index column
        let df_expected = df! {"RowID" => Vec::<u32>::new()}?;

        println!("Input DF:\n{}", df_input);
        let df_output = add_row_index_column(df_input.clone(), &filters)?;
        println!("Output DF:\n{}", df_output);
        println!("Expected DF:\n{}", df_expected);

        // Compare schemas and dimensions for empty dataframes
        assert_eq!(df_output.schema(), df_expected.schema());
        assert_eq!(df_output.height(), 0);
        assert_eq!(df_output.width(), 1);
        assert!(df_output.column("RowID").is_ok());

        Ok(()) // Return Ok on success
    }
}
