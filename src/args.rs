use crate::{DEFAULT_QUERY, NULL_VALUES};

use clap::Parser;
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

/// Command-line arguments for the PolarsView application.
#[derive(Parser, Debug, Clone)]
#[command(
    // Read from `Cargo.toml`.
    author, version, about,
    long_about = None,
    next_line_help = true,
    help_template = APPLET_TEMPLATE,
    styles=get_styles(),
)]
pub struct Arguments {
    /// CSV delimiter.
    #[arg(short, long, default_value = ";", help = "CSV delimiter character")]
    pub delimiter: String,

    /// Null Values.
    #[arg(
        short,
        long,
        default_value = NULL_VALUES,
        help = "Custom null values",
        requires = "path"
    )]
    pub null_values: String,

    /// Path to data file (CSV, Json, NDJson or Parquet).
    #[arg(
        default_value = ".",
        help = "Path to the data file (CSV, Json, NDJson or Parquet)"
    )]
    pub path: PathBuf,

    /// SQL query.
    #[arg(
        short,
        long,
        default_value = DEFAULT_QUERY,
        help = "SQL query to apply",
        requires = "path"
    )]
    pub query: String,

    /// Remove columns containing only null values.
    #[arg(
        short,
        long,
        help = "Flag to indicate whether to remove columns containing only null values",
        requires = "path"
    )]
    pub remove_null_cols: bool,

    /// Table name for SQL.
    #[arg(
        short,
        long,
        default_value = "AllData",
        help = "Table name for SQL queries",
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

// Existing code in args.rs ...
#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;
    use std::path::PathBuf;

    #[test]
    fn test_arguments_build() {
        let args = Arguments::parse_from([
            "polars-view",
            "--delimiter",
            ",",
            "--query",
            "SELECT * FROM mytable",
            "--table-name",
            "mytable",
            "/tmp/data.csv",
        ]);

        assert_eq!(args.delimiter, ",");
        assert_eq!(args.path, PathBuf::from("/tmp/data.csv"));
        assert_eq!(args.query, "SELECT * FROM mytable".to_string());
        assert_eq!(args.table_name, "mytable");
    }

    #[test]
    fn test_arguments_build_with_short_options() {
        let args = Arguments::parse_from([
            "polars-view",
            "-d",
            "|",
            "my_file.parquet", // Positional argument for path
            "-q",
            "SELECT foo FROM bar",
            "-t",
            "tablename",
        ]);

        assert_eq!(args.delimiter, "|");
        assert_eq!(args.path, PathBuf::from("my_file.parquet"));
        assert_eq!(args.query, "SELECT foo FROM bar".to_string());
        assert_eq!(args.table_name, "tablename");
    }

    #[test]
    fn test_arguments_csv_file() {
        let args = Arguments::parse_from(["polars-view", "/tmp/data.csv"]);

        assert_eq!(args.delimiter, ";"); // Should still have default
        assert_eq!(args.path, PathBuf::from("/tmp/data.csv"));
        assert_eq!(args.query, DEFAULT_QUERY); // Check default query
        assert_eq!(args.table_name, "AllData"); // and default table_name
    }

    #[test]
    fn test_arguments_parquet_file() {
        let args = Arguments::parse_from(["polars-view", "data.parquet"]); //Only the mandatory 'path'

        assert_eq!(args.path, PathBuf::from("data.parquet")); // Path is what we provided
        assert_eq!(args.delimiter, ";"); // delimiter defaults.
        assert_eq!(args.query, DEFAULT_QUERY); // query defaults.
        assert_eq!(args.table_name, "AllData"); // table_name defaults
    }
}
