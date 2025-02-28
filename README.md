# Polars View

[![License](https://img.shields.io/badge/License-GPL--3.0-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/Rust-1.85+-orange.svg)](https://www.rust-lang.org)

**A fast, cross-platform viewer for Parquet and CSV files, powered by Polars and egui.**

## Overview

Polars View is a lightweight and efficient tool for inspecting and exploring Parquet and CSV datasets. Built with the [Polars](https://www.pola.rs/) data processing library and the [egui](https://www.egui.rs/) immediate mode GUI framework, Polars View offers a user-friendly interface for viewing, filtering, and sorting tabular data. It also supports querying data using SQL.

**This project is a fork of [parqbench](https://github.com/Kxnr/parqbench), reimagined to leverage the power of Polars instead of DataFusion.**

## Features

*   **Fast Loading:** Leverages Polars for efficient data loading and processing.
*   **Cross-Platform:** Runs on Windows, macOS, and Linux.
*   **Parquet and CSV Support:** Handles both popular data formats.
*   **User-Friendly Interface:** Uses egui for a responsive and intuitive GUI.
*   **Filtering:** Easily filter data using SQL queries.
*   **Sorting:** Sort data by one or more columns in ascending or descending order.
*   **Metadata Display:** View file metadata and schema information.
*   **SQL Querying:** Search and filter data using SQL syntax.
*   **Flexible Usage:** Can be used via command-line arguments or through the graphical interface.

## Installation

1.  **Install Rust:** If you don't have Rust installed, download it from [https://www.rust-lang.org/](https://www.rust-lang.org/) using `rustup`. Polars View requires Rust version 1.85 or higher.

2.  **Clone the Repository:**

    ```bash
    git clone https://github.com/claudiofsr/polars-view.git
    cd polars-view
    ```

3.  **Build the Project:**

    ```bash
    cargo build --release
    ```

## Usage

You can use Polars View in two primary ways:

**1. Graphical Interface:**

1.  **Run the Executable:** The compiled executable will be located in the `target/release` directory.

    ```bash
    ./target/release/polars-view
    ```

2.  **Open a File:**
    *   Drag and drop a Parquet or CSV file onto the application window.
    *   Alternatively, use the "File > Open" menu option.

3.  **Explore the Data:**
    *   View the data in a tabular format.
    *   Use the "Query" panel to apply SQL-like filters.
    *   Click on column headers to sort the data.
    *   View file metadata and schema information in the side panel.

**2. Command-Line Arguments:**

You can also specify a file and query directly from the command line:

```bash
./target/release/polars-view /path/to/your/data.parquet -q "SELECT * FROM AllData WHERE column1 > 100"
