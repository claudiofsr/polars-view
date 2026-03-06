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
    // Use RUST_LOG environment variable to set logging level.  eg `export RUST_LOG=info`
    tracing_subscriber::fmt::init();

    // Parse command-line arguments.
    let args = Arguments::build();

    // Configure the native options for the eframe application.
    let native_options = eframe::NativeOptions {
        centered: true,
        persist_window: true,
        vsync: true,
        viewport: egui::ViewportBuilder::default().with_drag_and_drop(true),
        ..Default::default()
    };

    // Run the eframe application.
    eframe::run_native(
        "PolarsView",
        native_options,
        Box::new(move |creation_context| {
            // Create a new PolarsViewApp. If a path is provided, load the data.
            let app = if args.path.is_file() {
                // Create data filters from command line arguments
                let data_filter = DataFilter::new(&args)?;

                // RUST_LOG=debug cargo run -- data.csv
                tracing::debug!("main()\nDataFilter: {data_filter:#?}");

                // Load the data from the specified path.
                let dc = DataContainer::default();
                let future = dc.load_data(data_filter, DataFormat::default());

                // Create a new PolarsViewApp with the data loading future.
                PolarsViewApp::new_with_future(creation_context, Box::new(Box::pin(future)))
            } else {
                // Create a new PolarsViewApp without loading data.
                PolarsViewApp::new(creation_context)
            };

            match app {
                Ok(app) => Ok(Box::new(app)),
                Err(err) => {
                    error!("Failed to initialize PolarsViewApp: {}", err); //Log
                    panic!("Failed to initialize PolarsViewApp: {err}"); //Panic
                }
            }
        }),
    )
}
