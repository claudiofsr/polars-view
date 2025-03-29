# PolarsView

[![Crates.io](https://img.shields.io/crates/v/polars-view)](https://crates.io/crates/polars-view)
[![Documentation](https://docs.rs/polars-view/badge.svg)](https://docs.rs/polars-view)
[![License](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/Rust-1.85+-orange.svg)](https://www.rust-lang.org)

![Polars View](polars-view.png)

A fast and interactive viewer for CSV, JSON (including Newline-Delimited JSON - NDJSON), and Apache Parquet files, built with [Polars](https://www.pola.rs/) and [egui](https://github.com/emilk/egui).

This project is inspired by and initially forked from the [parqbench](https://github.com/Kxnr/parqbench) project.

## Features

*   **Fast Data Handling:** Leverages the high-performance Polars DataFrame library (`v0.46`) for efficient loading, processing, and querying (including lazy operations where applicable).
*   **Multiple File Format Support:**
    *   Load data from: CSV, JSON, NDJSON, Parquet.
    *   Save data as: CSV, JSON, NDJSON, Parquet (via "Save As..."). Automatic CSV delimiter detection on load.
*   **Interactive Table View:**
    *   **Sorting:** Click column headers to sort the *entire* DataFrame (cycles through Not Sorted → Descending ↔ Ascending). Sort operations are performed asynchronously using `tokio`.
    *   **Column Header Styles (Compile-Time Features):**
        *   `header-wrapping` (Enabled via `DataFormat` setting, default: `true`): Uses a custom header widget with wrapping text for long names and makes only the sort icon clickable. Provides colored text.
        *   `header-simple` (Enabled if `header-wrapping=false`): Uses a standard `egui` button style for headers; the entire button is clickable for sorting, text does not wrap.
    *   **Column Sizing:** Choose between automatically sizing columns to content (`Format > Auto Col Width` = true) or using faster initial calculated widths (`Auto Col Width` = false). The table view adapts dynamically.
*   **SQL Querying:** Filter and transform data using Polars' SQL interface. Specify the query in the "Query" panel and click "Apply SQL Commands". Queries execute asynchronously.
*   **Configuration Panels (Side Panel):**
    *   **Metadata:** Displays file information (row count, column count).
    *   **Schema:** Shows column names and their Polars data types (right-click column name to copy).
    *   **Format:**
        *   **Alignment:** Customize text alignment (Left, Center, Right) for different data types (`Float`, `Int`, `Date`, `Bool`, `String`, etc.).
        *   **Decimals:** Control the number of decimal places displayed for `Float32`/`Float64` columns.
        *   **Auto Col Width:** Toggle automatic column width adjustment based on content.
        *   **Header Wrapping:** Toggle between the wrapping/colored header and the simple/single-line header.
    *   **Query:**
        *   **SQL Query:** Enter SQL commands (default table name: `AllData`, configurable via CLI).
        *   **Apply SQL Button:** Executes the entered query asynchronously.
        *   **Remove Null Cols:** Option to automatically drop columns containing only null values upon loading or applying SQL.
        *   **Schema Inference Length:** (CSV/JSON/NDJSON) Control rows used for schema detection.
        *   **CSV Delimiter:** Specify the delimiter (auto-detection attempted on load, reflects detected delimiter).
        *   **Null Values (CSV):** Define custom strings (comma-separated) to be interpreted as nulls (e.g., `"", "NA", <N/A>`).
        *   **SQL Table Name:** View/edit the table name used in SQL (set via CLI).
        *   **SQL Examples:** Provides context-aware SQL command suggestions based on the loaded data schema.
*   **Drag and Drop:** Load files by dragging and dropping them onto the application window.
*   **Asynchronous Operations:** Uses Tokio (`v1.44`) for non-blocking file loading, saving, sorting, and SQL execution, keeping the UI responsive. Data state updates (`load`, `sort`, `format`) happen asynchronously, and results are seamlessly integrated back into the UI using `tokio::sync::oneshot` channels.
*   **Robust Error Handling:** Utilizes a custom `PolarsViewError` enum (`thiserror`) and displays errors clearly in a non-blocking notification window.
*   **Theming:** Switch between Light and Dark themes.
*   **Persistence:** Window position and size persisted between sessions (`eframe` feature).

## Building and Running

1.  **Prerequisites:**
    *   Rust and Cargo (Minimum Rust version 1.85).

2.  **Clone the Repository:**

    ```bash
    git clone https://github.com/claudiofsr/polars-view.git
    cd polars-view
    ```

3.  **Build and Install (Release Mode):**

    ```bash
    # Build with default features (format-simple)
    cargo b -r && cargo install --path=.

    # --- Feature Examples ---
    # Build with special formatting rules active:
    cargo b -r && cargo install --path=. --features format-special
    ```
    This compiles the application in release mode (optimized with LTO, stripped symbols) and installs the binary (`polars-view`) into your Cargo bin directory (`~/.cargo/bin/` by default), making it available in your PATH.

4.  **Run:**

    ```bash
    polars-view [path_to_file] [options]
    ```

    *   If `[path_to_file]` is provided, the application will attempt to load it on startup. Supported formats: `.csv`, `.json`, `.ndjson`, `.parquet`.
    *   Use `polars-view --help` for a detailed list of available command-line options (e.g., `-d` / `--delimiter`, `-q` / `--query`, `-t` / `--table-name`, `-n` / `--null-values`, `-r` / `--remove-null-cols`).
    *   **Logging/Tracing:** Control log output using the `RUST_LOG` environment variable (uses `tracing-subscriber`):
        *   `export RUST_LOG=info` (General information)
        *   `export RUST_LOG=debug` (Detailed information for debugging)
        *   `export RUST_LOG=trace` (Very detailed, for granular debugging)
        *   Combine levels: `export RUST_LOG=polars_view=debug,polars=info`
        *   Run directly: `RUST_LOG=debug polars-view data.parquet`

    *   **Examples:**
        ```bash
        polars-view sales_data.parquet
        polars-view -d "|" transactions.csv -n "N/A,-"
        polars-view data.csv -q "SELECT category, SUM(value) AS total FROM AllData WHERE date > '2023-01-01' GROUP BY category"
        # Using backticks or double quotes for column names with spaces or special chars:
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
    *   Column headers show the column names. Click the sort icon (part of the header) to sort. Sorting applies to the full dataset and runs asynchronously.
    *   Adjust column widths by dragging the separators between headers. Toggle `Format > Auto Col Width` to automatically fit content (may be slower) or use uniform initial widths.
*   **Configuring View & Data:**
    *   Expand the panels on the left ("Metadata", "Schema", "Format", "Query") to view information and change settings.
    *   **Format Panel:** Adjust alignment per data type, set float precision, toggle column auto-width, and toggle header wrapping style. Changes trigger an efficient asynchronous update.
    *   **Query Panel:** Set CSV options (delimiter, nulls, schema inference), toggle `Remove Null Cols`, define/edit the SQL query, and apply it using the "Apply SQL Commands" button. Applying SQL or changing CSV settings/Null Col removal triggers an asynchronous data reload/requery. View SQL examples tailored to your data.
*   **Applying SQL:**
    *   Enter your query in the "SQL Query" text area (using `AllData` as the default table name unless changed via CLI). Use backticks `` ` `` or double quotes `"` for identifiers with spaces or special characters.
    *   Click "Apply SQL Commands". The table will update after the query executes asynchronously. Refer to [Polars SQL documentation](https://docs.pola.rs/api/python/stable/reference/sql/index.html).
    *   Check the dynamically generated "SQL Command Examples" for syntax relevant to your data.
*   **Saving Data:**
    *   **Save (Ctrl+S):** Attempts to overwrite the original file (if applicable) with the currently displayed data (after filtering/sorting). This happens asynchronously. **Use with caution, as this overwrites the source file.**
    *   **Save As... (Ctrl+A):** Opens a dialog to save the currently displayed data to a new file. You can choose the output format (CSV, JSON, NDJSON, Parquet) and location. This happens asynchronously.
*   **Exiting:** Use "File" > "Exit" or close the window.

## Core Dependencies

*   [Polars (`v0.46`)](https://www.pola.rs/): High-performance DataFrame library (features: `lazy`, `csv`, `json`, `parquet`, `sql`, `round_series`, `strings`, `temporal` enabled).
*   [eframe (`v0.31`)](https://crates.io/crates/eframe) / [egui (`v0.31`)](https://github.com/emilk/egui): Immediate-mode GUI framework.
*   [egui_extras (`v0.31`)](https://docs.rs/egui_extras/latest/egui_extras/): Additional widgets for egui (`TableBuilder`).
*   [tokio (`v1.44`)](https://tokio.rs/): Asynchronous runtime for background tasks (features: `rt`, `sync`, `rt-multi-thread`).
*   [clap (`v4.5`)](https://crates.io/crates/clap): Command-line argument parsing.
*   [tracing (`v0.1`)](https://crates.io/crates/tracing) / [tracing-subscriber (`v0.3`)](https://crates.io/crates/tracing-subscriber): Application logging.
*   [thiserror (`v2.0`)](https://crates.io/crates/thiserror): Error handling boilerplate.
*   [rfd (`v0.15`)](https://crates.io/crates/rfd): Native file dialogs.
*   [cfg-if (`v1.0`)](https://crates.io/crates/cfg-if): Conditional compilation helpers.

## License

This project is licensed under the [MIT License](LICENSE).
