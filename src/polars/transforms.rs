use crate::{
    DataFilter, PolarsViewError, PolarsViewResult, add_row_index_column, drop_columns_by_regex,
    normalize_float_strings_by_regex, remove_null_columns, replace_values_with_null,
};
use polars::{prelude::*, sql::SQLContext};

/// Represents a single data transformation step applied to a DataFrame.
/// Implementors define a specific transformation and apply it.
/// The decision whether to include a specific transformation is made by the caller
/// (e.g., `DataContainer::load_data`) based on the `DataFilter`.
pub trait DataFrameTransform: Send + Sync {
    /// Applies the transformation to the input DataFrame.
    ///
    /// This method assumes the transformation is intended to be applied and performs its logic directly.
    /// The DataFrame is taken by value and returned by value.
    /// Access to the `DataFilter` is provided for configuration details (like regex, table names, etc.).
    fn apply(&self, df: DataFrame, filter: &DataFilter) -> PolarsViewResult<DataFrame>;
}

// --- Concrete Transformation Strategy Structs ---

/// Transformation strategy to drop columns matching a regex.
/// Active when `filter.drop` is true.
pub struct DropColumnsTransform;

impl DataFrameTransform for DropColumnsTransform {
    /// Applies the column dropping logic based on the filter's regex.
    /// Assumes this transform is active.
    fn apply(&self, df: DataFrame, filter: &DataFilter) -> PolarsViewResult<DataFrame> {
        tracing::debug!(
            "Applying DropColumnsTransform (regex: '{}')",
            filter.drop_regex
        );
        drop_columns_by_regex(df, &filter.drop_regex)
    }
}

/// Transformation strategy to normalize Euro-style float strings via regex.
/// Active when `filter.normalize` is true.
pub struct NormalizeTransform;

impl DataFrameTransform for NormalizeTransform {
    /// Applies the normalization logic based on the filter's regex.
    /// Assumes this transform is active.
    fn apply(&self, df: DataFrame, filter: &DataFilter) -> PolarsViewResult<DataFrame> {
        tracing::debug!(
            "Applying NormalizeTransform (regex: '{}')",
            filter.normalize_regex
        );
        normalize_float_strings_by_regex(df, &filter.normalize_regex)
    }
}

/// Transformation strategy to replace specific string values with nulls.
/// Always added to the pipeline, but the actual replacement logic inside checks
/// if there are any values configured in `filter.null_values`.
pub struct ReplaceNullsTransform;

impl DataFrameTransform for ReplaceNullsTransform {
    /// Applies value replacement logic based on configured null values.
    /// Performs the operation only if the null values list is not empty.
    fn apply(&self, df: DataFrame, filter: &DataFilter) -> PolarsViewResult<DataFrame> {
        let null_value_list: Vec<&str> = filter.parse_null_values();

        if null_value_list.is_empty() {
            tracing::trace!("ReplaceNullsTransform skipped: no null values configured.");
            // Pass through if no values configured to replace.
            return Ok(df);
        }

        tracing::debug!(
            "Applying ReplaceNullsTransform with values: {:?}",
            null_value_list
        );
        replace_values_with_null(df, &null_value_list, false).map_err(crate::PolarsViewError::from)
    }
}

/// Transformation strategy to execute a SQL query.
/// Active when `filter.apply_sql` is true.
pub struct SqlTransform;

impl DataFrameTransform for SqlTransform {
    /// Executes the SQL query configured in the filter.
    /// Assumes this transform is active.
    fn apply(&self, df: DataFrame, filter: &DataFilter) -> PolarsViewResult<DataFrame> {
        tracing::debug!("Applying SqlTransform...");
        let mut ctx = SQLContext::new();
        ctx.register(&filter.table_name, df.lazy());
        ctx.execute(&filter.query)?
            .collect()
            .map_err(crate::PolarsViewError::from)
    }
}

/// Transformation strategy to remove columns containing only null values.
/// Active when `filter.exclude_null_cols` is true.
pub struct RemoveNullColumnsTransform;

impl DataFrameTransform for RemoveNullColumnsTransform {
    /// Removes columns with all nulls.
    /// Assumes this transform is active.
    fn apply(&self, df: DataFrame, filter: &DataFilter) -> PolarsViewResult<DataFrame> {
        let _ = filter;
        let initial_width = df.width();
        let result_df = remove_null_columns(df)?;
        tracing::debug!(
            "RemoveNullColumnsTransform applied. Width {} -> {}",
            initial_width,
            result_df.width()
        );
        Ok(result_df)
    }
}

/// Transformation strategy to add a row index column.
/// Active when `filter.add_row_index` is true.
pub struct AddRowIndexTransform;

impl DataFrameTransform for AddRowIndexTransform {
    /// Adds a row index column based on filter configuration.
    /// Assumes this transform is active.
    fn apply(&self, df: DataFrame, filter: &DataFilter) -> PolarsViewResult<DataFrame> {
        tracing::debug!("Applying AddRowIndexTransform...");
        let row_index = filter.get_row_index(df.schema())?;
        add_row_index_column(df, row_index).map_err(PolarsViewError::from)
    }
}
