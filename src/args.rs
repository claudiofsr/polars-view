use crate::{DEFAULT_CSV_DELIMITER, NULL_VALUES, PolarsViewError, PolarsViewResult};

use clap::Parser;
use regex::Regex;
use std::path::PathBuf;

// https://stackoverflow.com/questions/74068168/clap-rs-not-printing-colors-during-help
fn get_styles() -> clap::builder::Styles {
    let cyan = anstyle::Color::Ansi(anstyle::AnsiColor::Cyan);
    let green = anstyle::Color::Ansi(anstyle::AnsiColor::Green);
    let yellow = anstyle::Color::Ansi(anstyle::AnsiColor::Yellow);

    clap::builder::Styles::styled()
        .placeholder(anstyle::Style::new().fg_color(Some(yellow)))
        .usage(anstyle::Style::new().fg_color(Some(cyan)).bold())
        .header(
            anstyle::Style::new()
                .fg_color(Some(cyan))
                .bold()
                .underline(),
        )
        .literal(anstyle::Style::new().fg_color(Some(green)))
}

// https://docs.rs/clap/latest/clap/struct.Command.html#method.help_template
const APPLET_TEMPLATE: &str = "\
{before-help}
{about-with-newline}
{usage-heading} {usage}

{all-args}
{after-help}";

const EX1: &str = r#" polars-view data.csv"#;
const EX2: &str = r#" polars-view data.csv -f "^(Chave|Key).*$""#;
const EX3: &str =
    r#" polars-view data.csv -q "SELECT * FROM AllData WHERE \"Col Name\" Like '%ABC%'""#;
const EX4: &str = r#" polars-view -q "SELECT * FROM AllData WHERE \"Valor Total\" > 5000" -r "^Val.*$" data.parquet"#;

/// Command-line arguments for the PolarsView application.
#[derive(Parser, Debug, Clone)]
#[command(
    // Read from `Cargo.toml`.
    author, version, about,
    long_about = None,
    next_line_help = true,
    help_template = APPLET_TEMPLATE,
    styles=get_styles(),
    after_help = format!("EXAMPLES:\n{EX1}\n{EX2}\n{EX3}\n{EX4}")
)]
pub struct Arguments {
    /// CSV delimiter character. [Default: ';']
    #[arg(
        short = 'd',
        long,
        default_value = DEFAULT_CSV_DELIMITER,
        help = "CSV delimiter character",
        long_help = "Sets the CSV delimiter.\n\
        Auto-detect tries common separators (, ; | \\t) if initial parse fails.",
        requires = "path"
    )]
    pub delimiter: String,

    /// Exclude columns containing only null values [requires data file].
    #[arg(
        short = 'e',
        long,
        help = "Exclude columns containing only null values [requires FILE_PATH]",
        long_help = "If present, drops columns with only NULLs after load/query.",
        action = clap::ArgAction::SetTrue,
        // requires path implicitly holds
    )]
    pub exclude_null_cols: bool,

    /// Regex pattern(s) matching columns to force read as String type [requires FILE_PATH].
    #[arg(
        short = 'f',
        long = "force-string-cols",
        value_name = "REGEX_PATTERN",      // Indicate expected value format
        help = "Regex matching columns to force read as String (overrides inference)",
        long_help = "\
Forces columns whose names match the provided REGEX_PATTERN to be read as String type.
Crucial for columns with large numeric IDs/keys often misinterpreted by type inference.

REGEX_PATTERN Requirements:
- Matching is case-sensitive by default (depends on regex engine).
- Example 1: --force-string-cols \"^Chave.*$\"
- Example 2: --force-string-cols \"^(Chave|ID|Code).*$\"

[NOTE] Primarily affects CSV/JSON reading where type inference occurs.
",
        requires = "path",
        value_parser = validate_force_string_argument_regex
    )]
    pub force_string_patterns: Option<String>,

    /// Comma-separated values to treat as NULL. [Default: \"\", <N/D>]
    #[arg(
        short = 'n',
        long,
        value_name = "NULL_LIST",
        default_value = NULL_VALUES,
        help = "Comma-separated values interpreted as NULL",
        long_help = "Specify custom null strings. Whitespace trimmed.\n\
        Use quotes for values with commas/spaces (e.g., \"NA\",\"-\").",
        requires = "path"
    )]
    pub null_values: String,

    /// Optional path to the data file (CSV, JSON, NDJSON, Parquet).
    #[arg(
        value_name = "FILE_PATH",
        default_value = ".",
        required = false,
        help = "Path to data file (CSV/JSON/NDJSON/Parquet) [Optional]",
        long_help = "Path to the input data file.\n\
        If omitted, opens the UI to load a file manually (menu or drag-drop)."
    )]
    pub path: PathBuf,

    /// SQL query to apply after loading data [requires data file].
    #[arg(
        short = 'q',
        long,
        value_name = "SQL_QUERY",
        help = "SQL query to apply to loaded data (use quotes) [requires FILE_PATH]",
        long_help = "Optional Polars SQL query to execute after loading.\n\
        Example: -q \"SELECT * FROM AllData WHERE count > 10\"",
        requires = "path"
    )]
    pub query: Option<String>,

    /// Apply regex to normalize specific string columns containing European-style numbers.
    #[arg(
        short = 'r',
        long,
        value_name = "REGEX_PATTERN", // Indicate expected value format
        help = "Normalize Euro-style number strings in selected columns (via regex) to Float64",
        long_help = "\
    Selects string columns using the provided regex pattern and converts their contents
    from a European-style numeric format (e.g., '1.234,56') to standard Float64 values
    (e.g., 1234.56).
    
    The normalization removes '.' (thousands separators) and replaces ',' with '.' (decimal separator)
    before casting to Float64.
    
    REGEX_PATTERN Requirements:
    - Must match *entire* column names.
    - Must be '*' (wildcard for ALL string columns - CAUTION!) OR
    - Must be a regex starting with '^' and ending with '$'.
    Examples: \"^Amount_EUR$\", \"^Value_.*$\", \"^(Total|Subtotal)$\"
    
    [WARNING] Applying to non-string columns via '*' or incorrect regex will likely cause errors.
    Invalid regex patterns (e.g., '^Val[') can also cause errors.
    
    Application example:
        polars-view data.csv -a \"^Val.*$\"
    ",
        requires = "path",
        value_parser = validate_normalize_argument_regex
    )]
    pub regex: Option<String>,

    /// Table name for SQL queries [requires -q/--query]. [Default: AllData]
    #[arg(
        short = 't',
        long,
        value_name = "TABLE_NAME",
        default_value = "AllData",
        help = "Table name for SQL queries [Default: AllData; requires -q]",
        long_help = "Sets the table name used in the FROM clause of the SQL query (--query).",
        requires = "query"
    )]
    pub table_name: String,
}

impl Arguments {
    /// Build `Arguments` struct.
    pub fn build() -> Arguments {
        Arguments::parse()
    }
}

// --- Regex Validation Functions ---

/// Validates command-line regex pattern: must be '*' or '^...$' format AND syntactically correct.
fn validate_cli_regex(pattern: &str, arg_name: &str) -> PolarsViewResult<String> {
    // 1. Check Format Constraint
    let is_wildcard = pattern == "*";
    let is_formatted_regex = pattern.starts_with('^') && pattern.ends_with('$');

    if !is_wildcard && !is_formatted_regex {
        let reason = "Pattern must be '*' or (start with '^' and end with '$')".to_string();
        return Err(PolarsViewError::InvalidArgument {
            arg_name: arg_name.to_string(),
            reason,
        });
    }

    if !is_wildcard && !pattern.is_empty() {
        match Regex::new(pattern) {
            Ok(_) => Ok(pattern.to_string()),
            Err(e) => {
                let reason = format!("Invalid regex syntax: {e}");
                Err(PolarsViewError::InvalidArgument {
                    arg_name: arg_name.to_string(),
                    reason,
                })
            }
        }
    } else {
        Ok(pattern.to_string())
    }
}

// --- Wrapper Validator Functions for specific arguments ---

/// clap validator specifically for the '--regex' (normalization) argument.
fn validate_normalize_argument_regex(s: &str) -> PolarsViewResult<String> {
    validate_cli_regex(s, "--regex")
}

/// clap validator specifically for the '--force-string-cols' argument.
fn validate_force_string_argument_regex(s: &str) -> PolarsViewResult<String> {
    validate_cli_regex(s, "--force-string-cols")
}

//----------------------------------------------------------------------------//
//                                   Tests                                    //
//----------------------------------------------------------------------------//

/// Run tests with:
/// cargo test -- --show-output tests_args`
#[cfg(test)]
mod tests_args {
    use super::*;
    use crate::{DEFAULT_CSV_DELIMITER, NULL_VALUES};
    use std::path::PathBuf;

    // Helper to create a dummy PathBuf for testing command line parsing.
    // clap doesn't need the file to exist for basic parsing tests.
    fn test_path(name: &str) -> PathBuf {
        PathBuf::from(name)
    }

    #[test]
    fn test_args_basic_path_only() {
        let path_str = "data.csv";
        let args = Arguments::parse_from(["polars-view", path_str]);

        assert_eq!(args.path, test_path(path_str));
        // Check defaults
        assert_eq!(args.delimiter, DEFAULT_CSV_DELIMITER);
        assert_eq!(args.null_values, NULL_VALUES);
        assert_eq!(args.query, None); // Query is optional, defaults to None
        assert_eq!(args.table_name, "AllData"); // Table name defaults even without query
        assert!(!args.exclude_null_cols); // Flag defaults to false
        assert_eq!(args.regex, None); // Optional, defaults to None
    }

    #[test]
    fn test_args_defaults_with_dot_path() {
        let args = Arguments::parse_from(["polars-view", "."]); // Explicitly use default path

        assert_eq!(args.path, test_path("."));
        // Check defaults
        assert_eq!(args.delimiter, DEFAULT_CSV_DELIMITER);
        assert_eq!(args.null_values, NULL_VALUES);
        assert_eq!(args.query, None);
        assert_eq!(args.table_name, "AllData");
        assert!(!args.exclude_null_cols);
        assert_eq!(args.regex, None);
    }
    #[test]
    fn test_args_all_options_short() {
        let path_str = "input.parquet";
        let query_str = "SELECT c1 FROM MyData WHERE c2 > 0";
        let regex_str = "^Col_\\d+$";
        let nulls_str = "NA,-99";
        let table_str = "MyData";
        let delim_str = ",";

        let args = Arguments::parse_from([
            "polars-view",
            "-d",
            delim_str,
            "-n",
            nulls_str,
            "-q",
            query_str,
            "-t",
            table_str, // requires -q
            "-e",      // exclude_null_cols flag
            "-r",
            regex_str,
            path_str, // Path comes last usually
        ]);

        assert_eq!(args.path, test_path(path_str));
        assert_eq!(args.delimiter, delim_str);
        assert_eq!(args.null_values, nulls_str);
        assert_eq!(args.query, Some(query_str.to_string()));
        assert_eq!(args.table_name, table_str);
        assert!(args.exclude_null_cols);
        assert_eq!(args.regex, Some(regex_str.to_string()));
    }

    #[test]
    fn test_args_all_options_long() {
        let path_str = "log.ndjson";
        let query_str = "SELECT *";
        let regex_str = "*";
        let nulls_str = "\"-\", \"?\"";
        let table_str = "LogData";
        let delim_str = ";"; // Delimiter specified but won't be used for ndjson

        let args = Arguments::parse_from([
            "polars-view",
            "--delimiter",
            delim_str,
            "--null-values",
            nulls_str,
            "--query",
            query_str,
            "--table-name",
            table_str,
            "--exclude-null-cols", // Long flag
            "--regex",
            regex_str,
            path_str,
        ]);

        assert_eq!(args.path, test_path(path_str));
        assert_eq!(args.delimiter, delim_str); // Value is captured even if not used for this format
        assert_eq!(args.null_values, nulls_str);
        assert_eq!(args.query, Some(query_str.to_string()));
        assert_eq!(args.table_name, table_str);
        assert!(args.exclude_null_cols);
        assert_eq!(args.regex, Some(regex_str.to_string()));
    }

    #[test]
    fn test_args_no_path_provided_uses_default() {
        // No path provided, clap should use the default_value "."
        let args = Arguments::parse_from(["polars-view"]); // Use default path "."

        assert_eq!(args.path, test_path("."));
        // Defaults for others
        assert_eq!(args.delimiter, DEFAULT_CSV_DELIMITER);
        assert_eq!(args.null_values, NULL_VALUES);
        assert_eq!(args.query, None);
        assert_eq!(args.table_name, "AllData");
        assert!(!args.exclude_null_cols);
        assert_eq!(args.regex, None);
    }

    #[test]
    fn test_args_query_without_tablename() {
        // Should use default table_name 'AllData'
        let path_str = "metrics.csv";
        let query_str = "SELECT count(*) FROM AllData";
        let args = Arguments::parse_from(["polars-view", "-q", query_str, path_str]);

        assert_eq!(args.path, test_path(path_str));
        assert_eq!(args.query, Some(query_str.to_string()));
        assert_eq!(args.table_name, "AllData"); // Default table name used
        // Check other defaults
        assert_eq!(args.delimiter, DEFAULT_CSV_DELIMITER);
        assert_eq!(args.null_values, NULL_VALUES);
        assert!(!args.exclude_null_cols);
        assert_eq!(args.regex, None);
    }

    #[test]
    fn test_args_flags_only() {
        let path_str = "config.json";
        let args = Arguments::parse_from(["polars-view", "-e", path_str]); // Just the path and remove flag

        assert_eq!(args.path, test_path(path_str));
        assert!(args.exclude_null_cols); // Flag sets it to true
        // Check other defaults
        assert_eq!(args.delimiter, DEFAULT_CSV_DELIMITER);
        assert_eq!(args.null_values, NULL_VALUES);
        assert_eq!(args.query, None);
        assert_eq!(args.table_name, "AllData");
        assert_eq!(args.regex, None);
    }
}
