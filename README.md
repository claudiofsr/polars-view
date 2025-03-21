# PolarsView

[![Crates.io](https://img.shields.io/crates/v/polars-view)](https://crates.io/crates/polars-view)
[![Documentation](https://docs.rs/polars-view/badge.svg)](https://docs.rs/polars-view)
[![License](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/Rust-1.85+-orange.svg)](https://www.rust-lang.org)

![Polars View](polars-view.png)

A fast and interactive viewer for CSV, Json and Parquet data, built with Polars and egui.

This project is inspired by and forked from the [parqbench](https://github.com/Kxnr/parqbench) project.

## Features

* **Fast Loading:** Leverages Polars for efficient data loading and processing.
* **CSV, Json and Parquet Support:** Handles CSV, Json (and Newline-Delimited Json) and Parquet file formats.
* **Save as:**  Save to file and select `CSV`, `Json`, `NDJson` or `Parquet` format.
* **Metadata Display:** Shows file metadata (column count, row count) and schema information.
* **Sorting Capabilities**: Interactive column sorting (ascending/descending).
* **Drag and Drop**:  Supports dragging and dropping files directly onto the application window.
* **Asynchronous Operations:** Uses Tokio for non-blocking file loading, saving, and query execution.
* **Error Handling**: Custom error enum (`PolarsViewError`) for robust error handling and informative error messages.  Errors are displayed in a dedicated notification window.

* **Query Panel Configuration Options:**
    *   **CSV Delimiter:** Specify the delimiter character for CSV files (automatic detection is also available).
    *   **Schema Inference Length:** Control the number of rows used to infer the data schema.
    *   **Decimal Places:** Set the desired decimal precision for floating-point numbers.
    *   **Remove All-Null Columns**: Option to automatically drop columns where all values are null, cleaning up the dataset.
    *   **Null Values (CSV):**  Allows you to specify custom strings that should be interpreted as null or missing values when loading CSV files. This is useful when your CSV data uses non-standard representations for missing data (e.g., "N/A", "NULL", "--", or other custom placeholders).  You can enter a comma-separated list of these values.

## Building and Running

1.  **Prerequisites:**
    *   Rust and Cargo (latest stable version recommended, minimum version 1.85).

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

    *   Replace `[path_to_file]` with the actual path to your CSV, Json or Parquet file.
    *   Use `polars-view --help` for a list of available options (delimiter, query, table name).
    *   **Tracing Options (for logging):**  
        * To enable detailed logging, use the `RUST_LOG` environment variable:

        ```bash
        RUST_LOG=info polars-view [path_to_file] [options]  # General info logs
        RUST_LOG=debug polars-view [path_to_file] [options] # More detailed logs
        RUST_LOG=trace polars-view [path_to_file] [options] # Very detailed logs (for debugging)
        ```
        * You can also specify the log level for specific modules:
        ```bash
        RUST_LOG=polars_view=debug,polars=info polars-view [path_to_file] [options]
        ```

        * Persistent Setting (for the current shell session):
        
        In your shell (bash, zsh, etc.), use `export`:
        ```bash
        export RUST_LOG=debug # Set the desired log level
        polars-view [path_to_file] [options]
        ```

    *   Examples:
        ```bash
        polars-view data.parquet
        polars-view --delimiter ';' data.csv --query "SELECT * FROM AllData WHERE Col_With_Integers > 10"
        polars-view data.csv -q "Select Col_A, \`Col B\` From AllData Where Col_A < 30"
        polars-view data.csv -q "Select Col_A, \"Col B\" From AllData Where Col_A < 30 And \`Col C\` > 5"
        polars-view data.csv -q "Select Linhas, \"Valor Total do Item\" From AllData Where Linhas < 30"
        RUST_LOG=info polars-view data.parquet
        ```

## Usage

*   **Open a File:**
    *   Run the application with a file path as an argument.
    *   Use the "File" -> "Open" menu option.
    *   Drag and drop a CSV, Json or Parquet file onto the application window.

*   **Filtering:**
    *   Use the "Query" panel to set CSV delimiter, schema inference length, and decimal places.  The application will automatically attempt to detect the CSV delimiter.
    *   Enter SQL queries in the "SQL Query" field.  The default table name is "AllData," but you can change this with the `--table-name` argument or in the "Query" panel.
    *   Click "Apply SQL Commands" to apply the filters and execute the SQL query (if provided).
    *   Check "Drop Null Cols" to remove any columns that contain only null values.
    *   Null Values (CSV): In the "Query" panel, enter a comma-separated list of strings you want to treat as null values in the "Null Values" field (e.g.,  `"", " ", NA, NULL, --`).

*   **Sorting:**
    *   Click the column headers in the table to sort by that column (ascending/descending). The sort state cycles through Not Sorted ⇒ Descending ⇔ Ascending.  The entire DataFrame is sorted, not just the visible rows.

*   **Metadata:**
    *   The "Metadata" panel shows file metadata.
    *   The "Schema" panel displays the data schema.

*   **SQL Examples:**

    The interface includes a "SQL Command Examples" section that provides pre-defined SQL commands based on the loaded data's schema. These cover various common filtering and aggregation operations. See [Polars SQL documentation](https://docs.pola.rs/api/python/stable/reference/sql/index.html).  The examples are dynamically generated to reflect the column names and data types of your specific file.

    ```sql
    SELECT * FROM AllData;
    SELECT * FROM AllData WHERE column_name > value;
    SELECT column1, COUNT(*) FROM AllData GROUP BY column1;
    ```
    
*   **Save as:**
   *   Use the "File" -> "Save As..." menu option.
   *    Select one of supported formats to save `CSV`, `Json`, `NDJson` or `Parquet`.
   
## Dependencies

*   [Polars](https://www.pola.rs/): High-performance DataFrame library.
*   [eframe](https://crates.io/crates/eframe): Immediate-mode GUI library.
*   [egui](https://github.com/emilk/egui): Immediate-mode GUI library.
*   [egui_extras](https://docs.rs/egui_extras/latest/egui_extras/): Complement egui features
*   [clap](https://crates.io/crates/clap): Command-line argument parser.
*   [tokio](https://tokio.rs/): Asynchronous runtime.
*   [tracing](https://crates.io/crates/tracing): Application-level tracing framework.
*   [thiserror](https://crates.io/crates/thiserror): Library for deriving the `Error` trait.
*   [rfd](https://crates.io/crates/rfd): Native file dialogs.
*   [anstyle](https://crates.io/crates/anstyle): Print styled output to terminal.

## License

This project is licensed under the [MIT License](LICENSE).
