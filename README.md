# PolarsView

[![License](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/Rust-1.85+-orange.svg)](https://www.rust-lang.org)

A fast viewer for Parquet and CSV files, built with Rust, Polars, and egui.

This project is inspired by and forked from the [parqbench](https://github.com/Kxnr/parqbench) project.

## Features

* **Fast Loading:** Leverages Polars for efficient data loading and processing.
* **Parquet and CSV Support:** Handles both Parquet and CSV file formats.
* **SQL Querying:** Apply SQL queries to filter and transform data.
* **Interactive Table:** Displays data in a sortable, scrollable table.
* **Data Filtering:** Provides UI elements for setting CSV delimiters, schema inference length, and decimal precision.
* **Metadata Display:** Shows file metadata (column count, row count) and schema information.
* **Asynchronous Operations:** Uses Tokio for non-blocking file loading and query execution.
* **Customizable UI:**  Uses egui for a responsive and customizable user interface.
* **Error Handling**: Custom error enum type for robust error handling.
* **Sorting Capabilities**: Interactive column sorting (ascending/descending).

## Building and Running

1.  **Prerequisites:**
    *   Rust and Cargo (latest stable version recommended).

2.  **Clone the Repository:**

    ```bash
    git clone https://github.com/claudiofsr/polars-view.git
    cd polars-view
    ```

3.  **Build and Install:**

    ```bash
    cargo b -r && cargo install --path=.
    ```

4.  **Run:**

    ```bash
    polars-view [path_to_file] [options]
    ```

    *   Replace `[path_to_file]` with the actual path to your Parquet or CSV file.
    *   Use `polars-view --help` for a list of available options (delimiter, query, table name).
    *   **Tracing Options (for logging):**  To enable detailed logging, use the `RUST_LOG` environment variable:

        ```bash
        RUST_LOG=info polars-view [path_to_file] [options]  # General info logs
        RUST_LOG=debug polars-view [path_to_file] [options] # More detailed logs
        RUST_LOG=trace polars-view [path_to_file] [options] # Very detailed logs (for debugging)
        ```
        You can also specify the log level for specific modules:
        ```bash
        RUST_LOG=polars_view=debug,polars=info polars-view [path_to_file] [options]
        ```

    *   Examples:
        ```bash
        polars-view data.parquet
        polars-view --delimiter ; data.csv --query "SELECT * FROM mytable WHERE x > 10"
        RUST_LOG=info polars-view data.parquet
        ```

## Usage

*   **Open a File:**
    *   Run the application with a file path as an argument.
    *   Use the "File" -> "Open" menu option.
    *   Drag and drop a Parquet or CSV file onto the application window.

*   **Filtering:**
    *   Use the "Query" panel to set CSV delimiter, schema inference length, and decimal places.
    *   Enter SQL queries in the "SQL Query" field.
    *   Click "Apply" to apply the filters.

*   **Sorting:**
    *   Click the column headers in the table to sort by that column (ascending/descending).

*   **Metadata:**
    *   The "Metadata" panel shows file metadata.
    *   The "Schema" panel displays the data schema.

*   **SQL Examples:**

    The interface includes predefined SQL command for easy reference.  
    These cover various common filtering and aggregation operations.
    See [Polars SQL documentation](https://docs.pola.rs/api/python/stable/reference/sql/index.html).

    ```sql
    SELECT * FROM AllData;
    SELECT * FROM AllData WHERE column_name > value;
    SELECT column1, COUNT(*) FROM AllData GROUP BY column1;
    ```

## Dependencies

*   [Polars](https://www.pola.rs/): High-performance DataFrame library.
*   [eframe](https://github.com/emilk/egui): Immediate-mode GUI library.
*   [egui](https://github.com/emilk/egui): Immediate-mode GUI library.
*   [egui_extras](https://docs.rs/egui_extras/latest/egui_extras/): Complement egui features
*   [clap](https://crates.io/crates/clap): Command-line argument parser.
*   [tokio](https://tokio.rs/): Asynchronous runtime.
*   [tracing](https://crates.io/crates/tracing): Application-level tracing framework.
*   [thiserror](https://crates.io/crates/thiserror): Library for deriving the `Error` trait.
*   [rfd](https://crates.io/crates/rfd): Native file dialogs.
*   [parquet](https://crates.io/crates/parquet): Crate for reading and writing parquet.

## Contributing

Contributions are welcome! Please open an issue or submit a pull request.

## License

This project is licensed under the [MIT License](LICENSE).
