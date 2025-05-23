# PolarsView

[![Crates.io](https://img.shields.io/crates/v/polars-view)](https://crates.io/crates/polars-view)
[![Documentation](https://docs.rs/polars-view/badge.svg)](https://docs.rs/polars-view)
[![License](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/Rust-1.85+-orange.svg)](https://www.rust-lang.org)

![Polars View](polars-view.png)

A fast and interactive viewer for CSV, JSON (including Newline-Delimited JSON - NDJSON), and [Apache Parquet](https://parquet.apache.org) files, built with [Polars](https://www.pola.rs/) and [egui](https://github.com/emilk/egui).

This project is inspired by and initially forked from the [parqbench](https://github.com/Kxnr/parqbench) project.

## Features

*   **Fast Data Handling:** Uses the [Polars](https://www.pola.rs/) DataFrame library for efficient data loading, processing, and querying.
*   **Multiple File Format Support:**
    *   Load data from: CSV, JSON, NDJSON (Newline-Delimited JSON), Parquet.
    *   Save data as: CSV, JSON, NDJSON, Parquet (via "Save As..." [Ctrl+A]).
*   **Interactive Table View:**
    *   **Supports sorting by multiple columns simultaneously:** Click column header *icons* to sort the *entire* DataFrame asynchronously. The *order* of clicks determines sort precedence. The 5-state cycle for each column controls direction and null placement:
        - `↕`:  Not Sorted
        - `⏷`:  Descending, Nulls First
        - `⏶`:  Ascending, Nulls First
        - `⬇`:  Descending, Nulls Last
        - `⬆`:  Ascending, Nulls Last
        - `↕`:  Back to Not Sorted
        
        *(Numbers indicate sort precedence if multiple columns are sorted)*
    *   **Customizable Header:** Toggle visual style ("Enhanced Header"), adjust vertical padding ("Header Padding").
    *   **Column Sizing:** Choose automatic content-based sizing ("Auto Col Width": true) or faster fixed initial widths ("Auto Col Width": false). Manually resize columns by dragging separators.
*   **SQL Querying:** Filter and transform data using Polars' SQL interface. Execute queries asynchronously via the "Query" panel.
*   **String Column Number Normalization (CLI):** Use the `--regex` (`-r`) argument to select string columns (via wildcard `*` or a `^...$` regex pattern matching column names) containing European-style numbers (e.g., '1.234,56') and convert them to standard Float64 format (e.g., 1234.56) on load.
*   **Configuration Panels (Side Bar):**
    *   **Info:** Displays file dimensions (rows, columns).
    *   **Format:** Set text alignment, float decimal places, column width strategy, header style, and header padding.
    *   **Query:** Configure SQL query, add optional row index column (with custom name/offset), normalize columns, null column removal, remove columns by regex, schema inference rows (CSV/JSON/NDJSON), CSV delimiter, custom CSV null values, and view SQL examples.
    *   **Columns:** Shows column names and Polars data types. **Right-click a column name to copy it.**
*   **Asynchronous Operations:** Utilizes Tokio for non-blocking file I/O, sorting, and SQL execution, keeping the UI responsive. Shows a spinner during processing.
*   **Drag and Drop:** Load files by dropping them onto the application window.
*   **Robust Error Handling:** Displays errors (file loading, parsing, SQL, etc.) in a non-blocking notification window.
*   **Theming:** Switch between Light and Dark themes via the menu bar.
*   **Persistence:** Remembers window size and position between sessions.

## Building and Running

1.  **Prerequisites:**
    *   Rust and Cargo (latest stable version recommended, minimum version 1.86 and edition 2024).

2.  **Clone the Repository:**

    ```bash
    git clone https://github.com/claudiofsr/polars-view.git
    cd polars-view
    ```

3.  **Build and Install (Release Mode):**

    ```bash
    # Build with default features (uses 'format-simple')
    cargo b -r && cargo install --path=.

    # --- OR Build with Specific Features ---
    # Example: Build with 'format-special' (formats 'Alíq'/'Aliq' columns differently)
    cargo b -r && cargo install --path=. --features format-special
    ```
    This compiles optimized code and installs the `polars-view` binary to `~/.cargo/bin/`.

4.  **Run:**

    ```bash
    polars-view [path_to_file] [options]
    ```

    *   If `[path_to_file]` is provided (CSV, JSON, NDJSON, Parquet), it's loaded on startup.
    *   Run `polars-view --help` for command-line options (`--delimiter`, `--exclude-null-cols`, `--null-values`, `--query`, `--regex`, `--table-name`).
    *   **Logging/Tracing:** Control log detail using the `RUST_LOG` environment variable (values: `error`, `warn`, `info`, `debug`, `trace`). **Remember to `export` it before running:**
        ```bash
        # Example: Run with debug level logging
        export RUST_LOG=debug
        polars-view data.parquet
        ```

    *   **Examples:**
        ```bash
        polars-view sales_data.parquet
        polars-view --delimiter="|" transactions.csv --null-values="N/A,-"
        polars-view data.csv -q "SELECT category, SUM(value) AS total FROM AllData GROUP BY category"
        # Normalize Euro numbers in columns matching "^Value.*$"
        polars-view data.csv --regex "^Value.*$"
        # Normalize Euro numbers in ALL string columns (Use with caution!)
        polars-view data.csv -r "*"
        # Use backticks/quotes for names with spaces/special chars
        polars-view items.csv -q "SELECT \`Item Name\`, Price FROM AllData WHERE Price > 100.0"
        polars-view logs.ndjson -q 'SELECT timestamp, message FROM AllData WHERE level = "ERROR"'
        # Exclude all null columns on load
        polars-view big_dataset.parquet --exclude-null-cols
        ```

## Usage Guide

*   **Opening Files:** Use the command line, "File" > "Open File..." (Ctrl+O), or drag & drop.
*   **Viewing Data:** Scroll the table. Click header icons to apply/cycle sorting (supports multiple columns; order matters). Drag header separators to resize columns.
*   **Configuring View & Data:** Use left-side panels ("Info", "Format", "Query", "Columns"). Format changes update the view efficiently; Query/Filter changes trigger an asynchronous data reload/requery.
*   **Applying SQL:** Enter query in "Query" panel (default table: `AllData`). Click "Apply SQL Commands". See examples or [Polars SQL docs](https://docs.pola.rs/api/python/stable/reference/sql/index.html).
*   **Saving Data:**
    *   **Save (Ctrl+S):** *Overwrites* the original file path.
    *   **Save As... (Ctrl+A):** Saves current view to a *new* file. Choose format (CSV, JSON, NDJSON, Parquet) via dialog.
*   **Exiting:** Use "File" > "Exit" or close the window.

## Core Dependencies

*   **GUI Framework:** `eframe`, `egui`, `egui_extras`
*   **Data Handling:** `polars` (with features like `lazy`, `csv`, `json`, `parquet`, `sql`)
*   **Asynchronous Runtime:** `tokio` (with features like `rt`, `sync`, `rt-multi-thread`)
*   **Command Line:** `clap`, `anstyle`
*   **File Dialogs:** `rfd`
*   **Logging/Diagnostics:** `tracing`, `tracing-subscriber`
*   **Utilities:** `regex`, `thiserror`, `cfg-if`, `env_logger` (non-wasm)

## License

This project is licensed under the [MIT License](LICENSE).
