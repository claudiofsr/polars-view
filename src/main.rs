#![warn(clippy::all)]
#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")] // hide console window on Windows in release

use polars_view::{Arguments, DataContainer, DataFilter, DataFormat, PolarsViewApp};
use tracing::error;

/*
cargo fmt
cargo test -- --nocapture
cargo test -- --show-output remove_null_col
cargo run -- --help
cargo run --features format-special -- data.csv
cargo doc --open
cargo b -r && cargo install --path=.
cargo b -r && cargo install --path=. --features format-special
*/

#[cfg(not(target_arch = "wasm32"))]
fn main() -> eframe::Result<()> {
    // Initialize the tracing subscriber for logging.
    // The log level can be controlled via the RUST_LOG environment variable (e.g., export RUST_LOG=info).
    tracing_subscriber::fmt::init();

    // Parse command-line arguments into the Arguments struct.
    let args = Arguments::build();

    // Configure the native options for the eframe/egui application.
    let native_options = eframe::NativeOptions {
        centered: true,
        persist_window: true,
        vsync: true,
        viewport: egui::ViewportBuilder::default()
            .with_drag_and_drop(true)
            .with_active(true)
            .with_visible(true),
        ..Default::default()
    };

    // Start the native eframe application.
    eframe::run_native(
        "PolarsView",
        native_options,
        Box::new(move |creation_context| {
            // Determine the application's initial state based on provided command-line arguments.
            let app_result = match &args.path {
                // If a path was provided and it points to a valid file, initiate immediate loading.
                Some(path) if path.is_file() => {
                    tracing::info!(target: "polars_view", "Loading path: {}", path.display());

                    // Initialize data filters from command line arguments (e.g., delimiter, null values).
                    // The '?' operator propagates errors to the outer Result block.
                    let data_filter = DataFilter::new(&args)?;

                    tracing::debug!("Initialization DataFilter state: {data_filter:#?}");

                    // Initialize the DataContainer and prepare the asynchronous loading future.
                    let dc = DataContainer::default();
                    let future = dc.load_data(data_filter, DataFormat::default());

                    // Create the application instance with the pending data loading task.
                    PolarsViewApp::new_with_future(creation_context, Box::new(Box::pin(future)))
                }
                // Default case: Open the application with an empty state (no file loaded).
                _ => {
                    tracing::info!(target: "polars_view", "No valid file path provided. Opening empty application.");
                    PolarsViewApp::new(creation_context)
                }
            };

            // Handle the result of the application initialization.
            match app_result {
                // On success, box the app and return it to eframe.
                Ok(app) => Ok(Box::new(app)),
                // On failure, log the error and return it. eframe will handle the graceful shutdown.
                Err(err) => {
                    error!("Failed to initialize PolarsViewApp: {}", err);
                    // Convert the custom error into a boxed dynamic error for eframe.
                    Err(Box::new(err))
                }
            }
        }),
    )
}
