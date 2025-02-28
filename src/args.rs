use clap::Parser;

use crate::SQL_COMMANDS;

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

#[derive(Parser, Debug, Clone)]
#[command(
    // Read from `Cargo.toml`
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

    /// Set the parquet filename.
    #[arg(help = "Path to the data file (Parquet or CSV)")]
    pub filename: Option<String>,

    /// Set the query.
    #[arg(
        short,
        long,
        default_value = SQL_COMMANDS[0],
        help = "SQL query to apply to the data",
        requires = "filename"
    )]
    pub query: Option<String>,

    /// Set the table_name.
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
    /// Build Arguments struct
    pub fn build() -> Arguments {
        Arguments::parse()
    }
}
