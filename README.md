# PolarsView

[![Crates.io](https://img.shields.io/crates/v/polars-view)](https://crates.io/crates/polars-view)
[![Documentation](https://docs.rs/polars-view/badge.svg)](https://docs.rs/polars-view)
[![License](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/Rust-1.85+-orange.svg)](https://www.rust-lang.org)

![Polars View](polars-view.png)

A fast and interactive viewer for CSV, JSON (including Newline-Delimited JSON - NDJSON), and Apache Parquet files, built with [Polars](https://www.pola.rs/) and [egui](https://github.com/emilk/egui).

This project is inspired by and initially forked from the [parqbench](https://github.com/Kxnr/parqbench) project.

## Features

*   **Fast Data Handling:** Leverages the high-performance Polars DataFrame library for efficient loading, processing, and querying.
*   **Multiple File Format Support:**
    *   Load data from: CSV, JSON, NDJSON, Parquet.
    *   Save data as: CSV, JSON, NDJSON, Parquet (via "Save As...").
*   **Interactive Table View:**
    *   Displays data in a scrollable and resizable table using `egui_extras::TableBuilder`.
    *   **Sorting:** Click column headers to sort the *entire* DataFrame (cycles through Not Sorted → Descending ↔ Ascending). Sort operations are performed asynchronously.
    *   **Column Header Styles (Compile-Time Features):**
        *   `header-wrapping` (Default): Uses a custom header widget with wrapping text for long names and makes only the sort icon clickable. Provides colored text.
        *   `header-simple`: Uses a standard `egui` button style for headers; the entire button is clickable for sorting, text does not wrap.
    *   **Column Sizing:** Choose between automatically sizing columns to content (`Format > Expand Cols` = true) or using faster initial calculated widths (`Expand Cols` = false). Switching requires a table layout reset managed internally.
*   **SQL Querying:** Filter and transform data using Polars' SQL interface. Specify the query in the "Query" panel and click "Apply SQL Commands". Queries execute asynchronously.
*   **Configuration Panels:**
    *   **Metadata:** Displays file information (row count, column count).
    *   **Schema:** Shows column names and their Polars data types (right-click column name to copy).
    *   **Format:**
        *   **Alignment:** Customize text alignment (Left, Center, Right) for different data types.
        *   **Decimals:** Control the number of decimal places displayed for float columns.
        *   **Expand Cols:** Toggle column auto-sizing behavior.
        *   **Cell Formatting (Compile-Time Features):**
            *   `format-simple` (Default): Basic alignment and decimal formatting.
            *   `format-special`: Applies special alignment/decimal rules to specific columns (e.g., "Alíquota").
    *   **Query:**
        *   **SQL Query:** Enter SQL commands (default table name: `AllData`).
        *   **Remove Null Cols:** Option to automatically drop columns containing only null values upon loading or applying SQL.
        *   **Schema Inference Length:** (CSV/JSON/NDJSON) Control rows used for schema detection.
        *   **CSV Delimiter:** Specify the delimiter (auto-detection attempted on load).
        *   **Null Values (CSV):** Define custom strings (comma-separated) to be interpreted as nulls (e.g., `"", "NA", <N/A>`).
        *   **SQL Examples:** Provides context-aware SQL command suggestions based on the loaded data schema.
*   **Drag and Drop:** Load files by dragging and dropping them onto the application window.
*   **Asynchronous Operations:** Uses Tokio for non-blocking file loading, saving, sorting, and SQL execution, keeping the UI responsive. Data state updates (`load`, `sort`, `format`) happen asynchronously and results are seamlessly integrated back into the UI.
*   **Robust Error Handling:** Utilizes a custom `PolarsViewError` enum and displays errors clearly in a non-blocking notification window.
*   **Theming:** Switch between Light and Dark themes.

## Basic Architecture

PolarsView uses `eframe` and `egui` for the immediate-mode GUI. Data operations are powered by `polars` and executed asynchronously using `tokio` to avoid blocking the UI thread. State updates from background tasks are managed through `tokio::sync::oneshot` channels.

## Building and Running

1.  **Prerequisites:**
    *   Rust and Cargo (latest stable version recommended, minimum version 1.85 as defined in `Cargo.toml`).

2.  **Clone the Repository:**

    ```bash
    git clone https://github.com/claudiofsr/polars-view.git
    cd polars-view
    ```

3.  **Build and Install (Release Mode):**

    ```bash
    # Build with default features (header-wrapping, format-simple)
    cargo b -r && cargo install --path=.

    # --- Feature Examples ---
    # Build with specific header style (simple) and specific formatting (special)
    # (Note: only one header-* and one format-* feature should typically be active)
    cargo b -r && cargo install --path=. --no-default-features --features header-simple,format-special

    # Build enabling 'simple' header while keeping the default 'simple' format
    cargo b -r && cargo install --path=. --no-default-features --features header-simple
    ```
    This compiles the application in release mode (optimized) and installs the binary (`polars-view`) into your Cargo bin directory (`~/.cargo/bin/` by default), making it available in your PATH.

4.  **Run:**

    ```bash
    polars-view [path_to_file] [options]
    ```

    *   If `[path_to_file]` is provided, the application will attempt to load it on startup. Supported formats: `.csv`, `.json`, `.ndjson`, `.parquet`.
    *   Use `polars-view --help` for a detailed list of available command-line options (e.g., `--delimiter`, `--query`, `--table-name`, `--null-values`, `--remove-null-cols`).
    *   **Logging/Tracing:** Control log output using the `RUST_LOG` environment variable:
        *   `export RUST_LOG=info` (General information)
        *   `export RUST_LOG=debug` (Detailed information for debugging)
        *   `export RUST_LOG=trace` (Very detailed, for granular debugging)
        *   Combine levels: `export RUST_LOG=polars_view=debug,polars=info`
        *   Run directly: `RUST_LOG=debug polars-view data.parquet`

    *   **Examples:**
        ```bash
        polars-view sales_data.parquet
        polars-view --delimiter="|" transactions.csv --null-values="N/A,-"
        polars-view data.csv -q "SELECT category, SUM(value) AS total FROM AllData WHERE date > '2023-01-01' GROUP BY category"
        # Using backticks or double quotes for column names with spaces:
        polars-view items.csv -q "SELECT \`Item Name\`, Price FROM AllData WHERE Price > 100.0"
        polars-view logs.ndjson -q 'SELECT timestamp, level, message FROM AllData WHERE level = "ERROR"'
        RUST_LOG=info polars-view big_dataset.parquet --remove-null-cols
        ```

## Usage Guide

*   **Opening Files:**
    *   Provide the file path as a command-line argument.
    *   Use the "File" > "Open File..." menu (Ctrl+O).
    *   Drag and drop a supported file onto the application window.
*   **Viewing Data:**
    *   The main panel displays the data in a table. Use horizontal and vertical scrollbars if needed.
    *   Column headers show the column names. Click the sort icon (or the whole header, depending on build feature) to sort.
    *   Adjust column widths by dragging the separators between headers.
*   **Configuring View & Data:**
    *   Expand the panels on the left ("Metadata", "Schema", "Format", "Query") to view information and change settings.
    *   **Format Panel:** Adjust alignment per data type, set float precision, and toggle column expansion (`Expand Cols`). Changes trigger an efficient asynchronous update.
    *   **Query Panel:** Set CSV options (delimiter, nulls, schema inference), toggle `Remove Null Cols`, define and apply SQL queries. Applying SQL or changing most query settings triggers an asynchronous data reload/requery.
*   **Applying SQL:**
    *   Enter your query in the "SQL Query" text area (using `AllData` as the default table name unless changed via CLI or config).
    *   Click "Apply SQL Commands". The table will update after the query executes asynchronously. Refer to [Polars SQL documentation](https://docs.pola.rs/api/python/stable/reference/sql/index.html).
    *   Check the dynamically generated "SQL Command Examples" for syntax relevant to your data.
*   **Saving Data:**
    *   **Save (Ctrl+S):** Attempts to overwrite the original file (if applicable) with the currently displayed data (after filtering/sorting). This happens asynchronously. *Use with caution*.
    *   **Save As... (Ctrl+A):** Opens a dialog to save the currently displayed data to a new file. You can choose the output format (CSV, JSON, NDJSON, Parquet) and location. This happens asynchronously.
*   **Exiting:** Use "File" > "Exit" or close the window.

## Core Dependencies

*   [Polars](https://www.pola.rs/): High-performance DataFrame library (CSV, JSON, Parquet, SQL features enabled).
*   [eframe](https://crates.io/crates/eframe) / [egui](https://github.com/emilk/egui): Immediate-mode GUI framework.
*   [egui_extras](https://docs.rs/egui_extras/latest/egui_extras/): Additional widgets for egui (`TableBuilder`).
*   [tokio](https://tokio.rs/): Asynchronous runtime for background tasks.
*   [clap](https://crates.io/crates/clap): Command-line argument parsing.
*   [tracing](https://crates.io/crates/tracing) / [tracing-subscriber](https://crates.io/crates/tracing-subscriber): Application logging.
*   [thiserror](https://crates.io/crates/thiserror): Error handling boilerplate.
*   [rfd](https://crates.io/crates/rfd): Native file dialogs.
*   [cfg-if](https://crates.io/crates/cfg-if): Conditional compilation helpers.

## License

This project is licensed under the [MIT License](LICENSE).
