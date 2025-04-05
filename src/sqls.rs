use polars::prelude::{DataType, Schema};

// Polars SQL documentation:
// - General: https://docs.pola.rs/api/python/stable/reference/sql/index.html
// - Clauses: https://docs.pola.rs/api/python/stable/reference/sql/clauses.html
// - Functions: https://docs.pola.rs/api/python/stable/reference/sql/functions/index.html

/// The default SQL query, selected when the application starts or when examples are unavailable.
pub const DEFAULT_QUERY: &str = "\
-- Select all columns and rows

SELECT * FROM AllData;";

/// Columns explicitly excluded by name in some helper functions (`get_col_name`, `get_cols_by_type`).
/// These might be temporary or intermediate columns added during processing steps like date formatting or grouping.
const COLS_TEMP: [&str; 5] = [
    "Frequency",
    "Base_de_Calculo",
    "Pis",
    "Cofins",
    "DMY (day/month/year)",
];

/// Safely retrieves the name of a column at a specific index, excluding temporary columns.
///
/// 1. Accesses schema at `index`.
/// 2. Checks if the retrieved column name is in `COLS_TEMP`.
/// 3. Returns `Some(column_name)` if found and not temporary, else `None`.
pub fn get_col_name(schema: &Schema, index: usize) -> Option<&str> {
    // 1. Attempt to get column name and type at the given index.
    schema.get_at_index(index).and_then(|(col_name, _dtype)| {
        // 2. Check if the column name is one of the temporary ones to exclude.
        let cols = COLS_TEMP.contains(&col_name.as_str());
        // 3. If not temporary, return its name, otherwise None.
        if !cols { Some(col_name.as_str()) } else { None }
    })
}

/// Safely retrieves the name of a column at a specific index *if* it matches the required data type,
/// excluding temporary columns.
///
/// 1. Accesses schema at `index`.
/// 2. Checks if the column name is in `COLS_TEMP`.
/// 3. Compares the column's `DataType` (`dtype_a`) with the required `dtype_b`.
/// 4. Returns `Some(column_name)` if found, not temporary, and types match, else `None`.
pub fn get_col_name_with_dtype<'a>(
    schema: &'a Schema,
    index: usize,
    dtype_b: &'a DataType,
) -> Option<&'a str> {
    // 1. Attempt to get column name and type at the index.
    schema.get_at_index(index).and_then(|(col_name, dtype_a)| {
        // 2. Check if it's a temporary column.
        let cols = COLS_TEMP.contains(&col_name.as_str());
        // 3. Check if it's not temporary AND the data types match.
        if !cols && dtype_a == dtype_b {
            // 4. Return the name if checks pass.
            Some(col_name.as_str())
        } else {
            // Otherwise, return None.
            None
        }
    })
}

/// Attempts to retrieve the names of the first one or two columns matching a specified data type,
/// excluding temporary columns.
///
/// 1. Initializes an empty vector `v` to store matching names.
/// 2. Iterates through the schema `(name, dtype)`.
/// 3. Skips temporary columns (`COLS_TEMP`).
/// 4. Calls the `target_dtype` closure to check if `dtype` matches the desired type.
/// 5. If it matches, adds `name` to `v`.
/// 6. If `v` now contains two names, returns `Some(v)` immediately.
/// 7. After iterating, if `v` is not empty (found at least one), returns `Some(v)`.
/// 8. Otherwise (no matching columns found), returns `None`.
pub fn get_cols_by_type<F>(schema: &Schema, target_dtype: F) -> Option<Vec<&str>>
where
    F: Fn(&DataType) -> bool, // Closure defines the type check logic.
{
    // 1. Store matching column names.
    let mut v: Vec<&str> = Vec::new();
    // 2. Iterate through schema.
    for (name, dtype) in schema.iter() {
        // 3. Skip temporary columns.
        if COLS_TEMP.contains(&name.as_str()) {
            continue;
        }
        // 4. Check data type using the provided closure.
        if target_dtype(dtype) {
            // 5. Add name if type matches.
            v.push(name.as_str());
            // 6. Return early if two columns are found.
            if v.len() >= 2 {
                return Some(v);
            }
        }
    }

    // 7. Return if at least one column was found.
    // 8. Returns None implicitly if v remains empty.
    if !v.is_empty() { Some(v) } else { None }
}

/// Generates a list of example SQL commands based on the provided DataFrame schema.
/// Used to populate the "SQL Command Examples" section in the UI.
///
/// 1. Retrieves column names based on common data types (String, Integer, Date, Float) using helpers.
/// 2. Initializes `commands` vector with the `DEFAULT_QUERY`.
/// 3. Adds basic SELECT examples (first two columns, exclude first two columns).
/// 4. Adds examples filtering by `IS NULL`, `IS NOT NULL`, and empty strings for string columns.
/// 5. Adds filtering examples for integer columns, potentially combined with string/float conditions.
/// 6. Adds a basic frequency counting (GROUP BY/COUNT) example if a 6th column exists.
/// 7. Adds a combined `LIKE` and `IS NULL` example if string and float columns exist.
/// 8. Adds a date formatting example if a date column exists.
/// 9. Adds more complex examples (creating subsets, excluding specific columns, complex group/order)
///    using helper functions (`new_table`, `exclude_cols`, `group_by_and_order`).
/// 10. Returns the populated `Vec<String>` of example queries.
pub fn sql_commands(schema: &Schema) -> Vec<String> {
    // 1. Get columns by type for common use cases in examples.
    let col_str = get_cols_by_type(schema, |dtype| dtype.is_string());
    let col_int = get_cols_by_type(schema, |dtype| dtype.is_integer());
    let col_date = get_cols_by_type(schema, |dtype| dtype.is_date());
    let col_float = get_cols_by_type(schema, |dtype| dtype.is_float());

    // 2. Start with the default query.
    let mut commands: Vec<String> = vec![DEFAULT_QUERY.to_string()];

    // 3. Add null filtering examples.
    if let (Some(col0), Some(col1)) = (get_col_name(schema, 0), get_col_name(schema, 1)) {
        commands.push(format!(
            "\
-- Filter rows where column value:
-- IS NULL or IS NOT NULL

SELECT * 
FROM AllData 
WHERE 
    `{col0}` IS NULL
OR
    `{col1}` IS NOT NULL;
"
        ));
    }

    // 4. Add basic SELECT examples.
    if let (Some(col0), Some(col1)) = (get_col_name(schema, 0), get_col_name(schema, 1)) {
        commands.push(format!(
            "\
-- Select specific columns by name
-- Use LIMIT to specify records to return.

SELECT
    `{col0}`,
    \"{col1}\"
FROM AllData
Limit 10;
"
        ));

        commands.push(format!(
            "\
-- Select all columns *except* specific ones
-- Exclude columns using SELECT * Except(colA, colB, ...) FROM AllData

SELECT * 
EXCEPT(
    `{col0}`, 
    \"{col1}\"
) 
FROM AllData;
"
        ));
    }

    // 5. Add integer filtering examples.
    if let Some(col_int_vec) = &col_int {
        if let Some(col_int_first) = col_int_vec.first() {
            commands.push(format!(
                "\
-- Filter rows where an integer column equals a value

SELECT * 
FROM AllData 
WHERE 
    `{col_int_first}` = 2020;
"
            ));

            // Combine integer and string filters
            if let Some(col_str_vec) = &col_str {
                if let Some(col_str_first) = col_str_vec.first() {
                    commands.push(format!(
                        "\
-- Filter rows using AND with integer and string columns

SELECT * 
FROM AllData 
WHERE 
    `{col_int_first}` = 2020 
AND 
    `{col_str_first}` = 'aa bb';
"
                    ));
                }
            }

            // Combine integer and float filters
            if let Some(col_float_vec) = &col_float {
                if let Some(col_float_first) = col_float_vec.first() {
                    commands.push(format!(
                        "\
-- Filter rows using AND with integer and float comparison

SELECT * 
FROM AllData 
WHERE 
    `{col_int_first}` = 2020 
AND 
    `{col_float_first}` > 1.0;
"
                    ));
                }
            }
        }
    }

    // 6. Add basic GROUP BY example.
    if let (Some(col1), Some(col5)) = (get_col_name(schema, 1), get_col_name(schema, 5)) {
        commands.push(format!(
            "\
-- Count occurrences of values in a column (frequency)

SELECT
    `{col1}`,
    `{col5}`,
    COUNT(*) AS Frequency
FROM AllData
GROUP BY
    `{col1}`,
    `{col5}`
ORDER BY
    Frequency DESC;
"
        ));
    }

    // 7. Add combined LIKE and IS NULL example.
    if let (Some(col_str_vec), Some(col_float_vec)) = (&col_str, &col_float) {
        if let (Some(col_str_first), Some(col_float_first)) =
            (col_str_vec.first(), col_float_vec.first())
        {
            commands.push(format!(
                "\
-- Filter using LIKE for pattern matching and checking for NULL

SELECT *
FROM AllData
WHERE
    `{col_str_first}` LIKE 'Saldo%'
AND
    `{col_float_first}` IS NULL;
"
            ));
        }
    }

    // 8. Add date formatting example.
    if let Some(col_date_vec) = &col_date {
        if let Some(col_date_first) = col_date_vec.first() {
            commands.push(date_format(col_date_first));
        }
    }

    // 9. Add more complex examples generated by helper functions.
    if let Some(sql_cmd) = new_table(schema) {
        commands.push(sql_cmd)
    }
    if let Some(sql_cmd) = exclude_cols(schema) {
        commands.push(sql_cmd)
    }
    if let Some(sql_cmd) = group_by_and_order(schema) {
        commands.push(sql_cmd)
    }

    // 10. Return all collected examples.
    commands
}

/// Helper function to generate a date formatting SQL example.
fn date_format(col_name: &str) -> String {
    format!(
        "\
-- Format a date column into DD/MM/YYYY using STRFTIME

SELECT
    AllData.*, -- Select all original columns
    STRFTIME(`{col_name}`, '%d/%m/%Y') AS `DMY (day/month/year)` -- Add formatted column
FROM
    AllData;
",
    )
}

/// Helper function to generate an SQL example that creates a new table (subset of columns and rows).
/// Only generates if specific column indices/types exist in the schema.
fn new_table(schema: &Schema) -> Option<String> {
    // 1.1 Try to find potential string columns.
    let col_str_vec = get_cols_by_type(schema, |dtype| dtype.is_string());
    let mut col_str_0: Option<&str> = None;
    let mut col_str_1: Option<&str> = None;
    if let Some(ref vec) = col_str_vec {
        col_str_0 = vec.first().copied(); // .copied() converts Option<&&str> to Option<&str>
        col_str_1 = vec.get(1).copied();
    }

    // 1.2 Try to find potential integer columns.
    let col_float_vec = get_cols_by_type(schema, |dtype| dtype.is_float());
    let mut col_float_0: Option<&str> = None;
    if let Some(ref vec) = col_float_vec {
        col_float_0 = vec.first().copied(); // .copied() converts Option<&&str> to Option<&str>
    }

    // 2. Use `if let` to safely extract required columns by index or fallback to found string columns.
    if let (
        Some(col1),  // Column at index 1 (any type)
        Some(col2),  // Column at index 2 (any type)
        Some(col3),  // Column at index 3 (prefer String, fallback to first found String)
        Some(col9),  // Column at index 9 (prefer String, fallback to second found String)
        Some(col10), // Column at index 10 (any type)
    ) = (
        get_col_name(schema, 1),
        get_col_name(schema, 2),
        get_col_name_with_dtype(schema, 3, &DataType::String).or(col_str_0),
        get_col_name_with_dtype(schema, 9, &DataType::String).or(col_str_1),
        get_col_name_with_dtype(schema, 10, &DataType::Float64).or(col_float_0),
    ) {
        // 3. If all required columns are present, build the SQL query string.
        Some(format!(
            "\
-- Select specific columns based on conditions
-- SubStr: Extract a substring from a string
-- (start at position 1, extract 16 characters)

SELECT
    `{col1}`,
    \"{col2}\",
    SubStr(`{col9}`,1,16),
    -- Use Modulo Operator % to hide actual values
    `{col10}` % 117
FROM AllData
WHERE
    `{col9}` LIKE '%Saldo%' -- Filter condition 1
AND ( -- Filter condition 2 (using OR)
    `{col3}` = ''
    OR
    `{col3}` IS NULL
);
"
        ))
    } else {
        // 4. If any required column is missing, return None.
        None
    }
}

/// Helper function to generate an SQL example excluding several specific columns.
/// Only generates if specific column indices/types exist in the schema.
fn exclude_cols(schema: &Schema) -> Option<String> {
    // 1. Find potential string columns for fallbacks.
    let opt_cols_str = get_cols_by_type(schema, |dtype| dtype.is_string());
    let col_str_0 = opt_cols_str.as_ref().and_then(|v| v.first().copied());

    // 2. Find potential integer columns for fallbacks.
    let opt_cols_int = get_cols_by_type(schema, |dtype| dtype.is_integer());
    let mut col_int_0: Option<&str> = None;
    let mut col_int_1: Option<&str> = None;
    if let Some(ref vec) = opt_cols_int {
        col_int_0 = vec.first().copied();
        col_int_1 = vec.get(1).copied();
    }

    // 3. Safely extract required column names using fallbacks.
    if let (
        Some(col1), // Col at index 1 (prefer Int64, fallback to first Int)
        Some(col2), // Col at index 2 (prefer Int64, fallback to second Int)
        Some(col3), // Col at index 3 (prefer String, fallback to first String)
        Some(col14),
        Some(col15),
        Some(col16), // Specific indices
    ) = (
        get_col_name_with_dtype(schema, 1, &DataType::Int64).or(col_int_0),
        get_col_name_with_dtype(schema, 2, &DataType::Int64).or(col_int_1),
        get_col_name_with_dtype(schema, 3, &DataType::String).or(col_str_0),
        get_col_name(schema, 14),
        get_col_name(schema, 15),
        get_col_name(schema, 16),
    ) {
        // 4. If all columns found, generate the SQL example.
        Some(format!(
            "\
-- Select all columns except a specific list, with filtering

SELECT *
EXCEPT( -- List columns to exclude
    `{col3}`,
    `{col14}`,
    `{col15}`,
    `{col16}`
)
FROM AllData
WHERE -- Apply filters
    `{col1}` = 2023
AND
    `{col2}` = 1
AND
    `{col3}` LIKE '%jan%';
"
        ))
    } else {
        // 5. If any required column is missing, return None.
        None
    }
}

/// Helper function to generate a GROUP BY and ORDER BY example with SUM aggregations and CASE statement.
/// Only generates if specific column indices exist.
fn group_by_and_order(schema: &Schema) -> Option<String> {
    // 1. Safely extract required column names by index.
    if let (
        Some(col5),
        Some(col6),
        Some(col8),
        Some(col9),
        Some(col12),
        Some(col33),
        Some(col36),
        Some(col37),
    ) = (
        get_col_name(schema, 5),
        get_col_name(schema, 6),
        get_col_name(schema, 8),
        get_col_name(schema, 9),
        get_col_name(schema, 12),
        get_col_name(schema, 33),
        get_col_name(schema, 36),
        get_col_name(schema, 37),
    ) {
        // 2. If all columns exist, generate the SQL query.
        Some(format!(
            "\
-- Group data by multiple columns, calculate SUMs, and order results

SELECT
    `{col5}`, -- Grouping column 1
    `{col6}`, -- Grouping column 2
    `{col8}`, -- Grouping column 3
    `{col9}`, -- Grouping column 4 (also used in ORDER BY CASE)
    `{col12}`,-- Grouping column 5
    SUM(`{col33}`) AS Base_de_Calculo, -- Aggregation 1
    SUM(`{col36}`) AS Pis,    -- Aggregation 2
    SUM(`{col37}`) AS Cofins  -- Aggregation 3
FROM
    AllData
GROUP BY -- List all non-aggregated selected columns
    `{col5}`,
    `{col6}`,
    `{col8}`,
    `{col9}`,
    `{col12}`
ORDER BY
    `{col5}`, -- Primary sort key
    CASE `{col9}` -- Secondary sort key using CASE for custom order
        WHEN 'Saída' THEN 1
        WHEN 'Entrada' THEN 2
        WHEN 'Detalhamento' THEN 3
        WHEN 'Descontos' THEN 4
        ELSE 5 -- Handle unexpected values
    END;
"
        ))
    } else {
        // 3. If any column is missing, return None.
        None
    }
}
