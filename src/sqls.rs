use polars::prelude::{DataType, Schema};

// https://docs.pola.rs/api/python/stable/reference/sql/index.html
// https://docs.pola.rs/api/python/stable/reference/sql/clauses.html
// https://docs.pola.rs/api/python/stable/reference/sql/functions/index.html

pub const DEFAULT_QUERY: &str = "SELECT * FROM AllData;";

#[allow(dead_code)]
const DATE_FORMAT: &str = "\
SELECT
    AllData.*,
    STRFTIME(`Período de Apuração`, '%d/%m/%Y') AS `PA (day/month/year)`
FROM
    AllData;\
";

#[allow(dead_code)]
const NEW_TABLE: &str = "\
-- The following SQL statement copies only a few columns into a NewTable:

SELECT
`Ano do Período de Apuração`,
`Trimestre do Período de Apuração`,
`Natureza da Base de Cálculo dos Créditos`,
`Valor da Base de Cálculo das Contribuições`,
INTO NewTable
FROM AllData
WHERE 
`Natureza da Base de Cálculo dos Créditos` LIKE 'Saldo de Crédito%' 
AND 
(
`Mês do Período de Apuração` = ''
OR
`Mês do Período de Apuração` IS NULL
);\
";

#[allow(dead_code)]
const GROUP_BY_AND_ORDER: &str = "\
SELECT
    `Período de Apuração`,
    `Ano do Período de Apuração`,
    `Mês do Período de Apuração`,
    `Tipo de Operação`,
    `Tipo de Crédito`,
    SUM(`Valor da Base de Cálculo das Contribuições`) AS Base_de_Calculo,
    SUM(`Valor de PIS/PASEP`) AS Pis,
    SUM(`Valor de COFINS`) AS Cofins
FROM
    AllData
GROUP BY
    `Período de Apuração`,
    `Ano do Período de Apuração`,
    `Mês do Período de Apuração`,
    `Tipo de Operação`,
    `Tipo de Crédito`
ORDER BY
    `Período de Apuração`,
    CASE `Tipo de Operação`
        WHEN 'Saída' THEN 1
        WHEN 'Entrada' THEN 2
        WHEN 'Detalhamento' THEN 3
        WHEN 'Descontos' THEN 4
        ELSE 5  -- Lidar com outros valores inesperados
    END;\
";

// Predefined SQL commands for easy selection.
#[allow(dead_code)]
const SQL_COMMANDS: [&str; 14] = [
    DEFAULT_QUERY,
    "SELECT `Valor da Base de Cálculo das Contribuições` FROM AllData;",
    "SELECT * FROM AllData WHERE `Data 1ª DCOMP Ativa` IS NOT NULL;",
    "SELECT * FROM AllData WHERE `Mês do Período de Apuração` IS NULL;",
    "SELECT * FROM AllData WHERE `Mês do Período de Apuração` = '';",
    "SELECT * FROM AllData WHERE `Ano do Período de Apuração` = 2020;",
    "SELECT * FROM AllData WHERE `Ano do Período de Apuração` = 2020 AND `Mês do Período de Apuração` = 'março';",
    "SELECT * FROM AllData WHERE `Ano do Período de Apuração` = 2020 AND `Trimestre do Período de Apuração` = 3;",
    "SELECT `Tipo de Crédito`, COUNT(*) AS Frequencia FROM AllData GROUP BY `Tipo de Crédito` ORDER BY Frequencia DESC;",
    "SELECT `Tipo de Operação`, `Tipo de Crédito`, COUNT(*) AS Frequencia FROM AllData GROUP BY `Tipo de Operação`, `Tipo de Crédito`;",
    "SELECT * FROM AllData WHERE `Natureza da Base de Cálculo dos Créditos` LIKE 'Saldo de Crédito%' AND `Mês do Período de Apuração` IS NULL;",
    DATE_FORMAT,
    NEW_TABLE,
    GROUP_BY_AND_ORDER,
];

/// Helper function to safely get a column name by index.
pub fn get_col_name(schema: &Schema, index: usize) -> Option<&str> {
    schema
        .get_at_index(index)
        .map(|(col_name, _dtype)| col_name.as_str())
}

/// Attempts to retrieve the first column of a specified data type.
///
/// ### Arguments
///
/// * `schema` - The schema to search within.
/// * `target_dtype` - A closure that takes a `DataType` and returns `true` if
///                    the data type matches the desired type, and `false` otherwise.
///
/// ### Returns
///
/// * `Some(&str)` - The name of the first column that matches the specified data type.
/// * `None` - If no column matching the specified data type is found.
pub fn get_col_by_type<F>(schema: &Schema, target_dtype: F) -> Option<&str>
where
    F: Fn(&DataType) -> bool,
{
    for (name, dtype) in schema.iter() {
        if target_dtype(dtype) {
            return Some(name.as_str());
        }
    }

    None // Return None if no matching column is found.
}

// Predefined SQL commands for easy selection. Examples:
pub fn sql_commands(schema: &Schema) -> Vec<String> {
    let mut commands: Vec<String> = vec![DEFAULT_QUERY.to_string()];

    let col_str = get_col_by_type(schema, |dtype| dtype.is_string());
    let col_int = get_col_by_type(schema, |dtype| dtype.is_integer());
    let col_date = get_col_by_type(schema, |dtype| dtype.is_date());
    let col_float = get_col_by_type(schema, |dtype| dtype.is_float());

    if let (Some(col0), Some(col1)) = (get_col_name(schema, 0), get_col_name(schema, 1)) {
        commands.push(format!("SELECT `{col0}`, \"{col1}\" FROM AllData;"));
    }

    // Build queries based on schema, handling potential missing columns.
    if let Some(col_str) = col_str {
        commands.push(format!(
            "SELECT * FROM AllData WHERE `{col_str}` IS NOT NULL;"
        ));
        commands.push(format!("SELECT * FROM AllData WHERE `{col_str}` IS NULL;"));
        commands.push(format!("SELECT * FROM AllData WHERE `{col_str}` = '';"));
    }

    if let Some(col_int) = col_int {
        commands.push(format!("SELECT * FROM AllData WHERE `{col_int}` = 2020;"));

        if let Some(col_str) = col_str {
            commands.push(format!(
                "SELECT * FROM AllData WHERE `{col_int}` = 2020 AND `{col_str}` = 'aa bb';"
            ));
        }

        if let Some(col_float) = col_float {
            commands.push(format!(
                "SELECT * FROM AllData WHERE `{col_int}` = 2020 AND `{col_float}` > 1.0;"
            ));
        }
    }

    if let Some(col5) = get_col_name(schema, 5) {
        commands.push(format!(
            "SELECT `{col5}`, COUNT(*) AS Frequency FROM AllData GROUP BY `{col5}` ORDER BY Frequency DESC;"
        ));
    }

    if let (Some(col_str), Some(col_float)) = (col_str, col_float) {
        commands.push(format!(
            "SELECT * FROM AllData WHERE `{col_str}` LIKE 'Saldo de Crédito%' AND `{col_float}` IS NULL;"
        ));
    }

    if let Some(col_date) = col_date {
        commands.push(date_format(col_date));
    }

    if schema.len() >= 11 {
        commands.push(new_table(schema));
    }

    if schema.len() >= 38 {
        commands.push(group_by_and_order(schema));
    }

    commands
}

fn date_format(col_name: &str) -> String {
    format!(
        "\
SELECT
    AllData.*,
    STRFTIME(`{col_name}`, '%d/%m/%Y') AS `DMY (day/month/year)`
FROM
    AllData;\
",
    )
}

fn new_table(schema: &Schema) -> String {
    // Use `if let Some(...) = ...` to safely extract column names.  If *any*
    // of the required columns are missing, we return a default/fallback query.
    if let (Some(col1), Some(col2), Some(col3), Some(col9), Some(col10)) = (
        get_col_name(schema, 1),
        get_col_name(schema, 2),
        get_col_name(schema, 3),
        get_col_name(schema, 9),
        get_col_name(schema, 10),
    ) {
        // All required columns are present. Build the dynamic SQL query.
        format!(
            "\
-- The following SQL statement copies only a few columns into a NewTable:

SELECT
`{col1}`,
\"{col2}\",
`{col9}`,
`{col10}`,
INTO NewTable
FROM AllData
WHERE 
`{col9}` LIKE 'Saldo de Crédito%' 
AND 
(
`{col3}` = ''
OR
`{col3}` IS NULL
);\
"
        )
    } else {
        // One or more required columns are missing. Return a default/fallback query.
        // This prevents the application from crashing.
        "SELECT * FROM AllData; -- Fallback query: select all".to_string()
    }
}

fn group_by_and_order(schema: &Schema) -> String {
    // Use `if let Some(...) = ...` to safely extract column names.  If *any*
    // of the required columns are missing, we return a default/fallback query.
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
        // All required columns are present. Build the dynamic SQL query.
        format!(
            "\
SELECT
    `{col5}`,
    `{col6}`,
    `{col8}`,
    `{col9}`,
    `{col12}`,
    SUM(`{col33}`) AS Base_de_Calculo,
    SUM(`{col36}`) AS Pis,
    SUM(`{col37}`) AS Cofins
FROM
    AllData
GROUP BY
    `{col5}`,
    `{col6}`,
    `{col8}`,
    `{col9}`,
    `{col12}`,
ORDER BY
    `{col5}`,
    CASE `{col9}`
        WHEN 'Saída' THEN 1
        WHEN 'Entrada' THEN 2
        WHEN 'Detalhamento' THEN 3
        WHEN 'Descontos' THEN 4
        ELSE 5 -- Dealing with unexpected values
    END;\
"
        )
    } else {
        // One or more required columns are missing. Return a default/fallback query.
        // This prevents the application from crashing.
        "SELECT * FROM AllData; -- Fallback query: select all".to_string()
    }
}
