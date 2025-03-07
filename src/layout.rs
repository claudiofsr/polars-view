use crate::{
    DataFrameContainer, Error, FileExtension, MyStyle, PolarsViewError, PolarsViewResult, Popover,
    Settings,
    filters::{DataFilters, DataFuture},
    metadata::FileMetadata,
};

use egui::{
    CentralPanel, Color32, Context, Direction, FontId, Frame, Grid, Hyperlink, Layout, RichText,
    ScrollArea, SidePanel, Stroke, TopBottomPanel, ViewportCommand, menu, style::Visuals,
    warn_if_debug_build, widgets,
};
use rfd::AsyncFileDialog;
use std::{path::PathBuf, sync::Arc};
use tokio::sync::oneshot::{self, error::TryRecvError};
use tracing::error;

/// Opens a file dialog asynchronously.
pub async fn file_dialog() -> PolarsViewResult<PathBuf> {
    // Open the file dialog.
    let opt_file = AsyncFileDialog::new().pick_file().await;

    opt_file
        .map(|file| file.path().to_path_buf())
        .ok_or_else(|| PolarsViewError::FileNotFound(PathBuf::new()))
}

/// The main application struct for PolarsView.
pub struct PolarsViewApp {
    /// The `DataFrameContainer` holds the loaded data (Parquet, CSV, etc.).  Using `Arc` for shared ownership.
    pub df: Arc<Option<DataFrameContainer>>,
    /// Component for managing data filters (SQL queries, sorting, etc.).
    pub data_filters: DataFilters,
    /// Metadata extracted from the loaded file (if available).
    pub metadata: Option<FileMetadata>,
    /// Optional popover window for displaying errors, settings, or other notifications.
    pub popover: Option<Box<dyn Popover>>,

    /// Tokio runtime for asynchronous operations (file loading, queries).
    runtime: tokio::runtime::Runtime,
    /// Channel for receiving the result of asynchronous data loading.
    pipe: Option<tokio::sync::oneshot::Receiver<PolarsViewResult<DataFrameContainer>>>,

    /// Vector of active asynchronous tasks. Used to prevent the app from hanging.
    tasks: Vec<tokio::task::JoinHandle<()>>,
}

impl Default for PolarsViewApp {
    fn default() -> Self {
        Self {
            df: Arc::new(None),
            data_filters: DataFilters::default(),
            runtime: tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .expect("Failed to build Tokio runtime"),
            pipe: None,
            popover: None,
            metadata: None,
            tasks: Vec::new(),
        }
    }
}

impl PolarsViewApp {
    /// Creates a new `PolarsViewApp` instance.
    pub fn new(cc: &eframe::CreationContext<'_>) -> PolarsViewResult<Self> {
        cc.egui_ctx.set_visuals(Visuals::dark()); // Set dark theme.
        cc.egui_ctx.set_style_init(); // Apply custom styles.
        Ok(Default::default())
    }

    /// Creates a new `PolarsViewApp` with a pre-existing `DataFuture`.
    pub fn new_with_future(
        cc: &eframe::CreationContext<'_>,
        future: DataFuture,
    ) -> PolarsViewResult<Self> {
        let mut app: Self = Default::default();
        cc.egui_ctx.set_visuals(Visuals::dark());
        cc.egui_ctx.set_style_init();
        app.run_data_future(future, &cc.egui_ctx);
        Ok(app)
    }

    /// Checks if a popover is active and displays it.
    fn check_popover(&mut self, ctx: &Context) {
        if let Some(popover) = &mut self.popover {
            if !popover.show(ctx) {
                self.popover = None; // Remove closed popover.
            }
        }
    }

    /// Checks if there is a data loading operation pending (asynchronous).
    fn check_data_pending(&mut self) -> bool {
        let Some(mut output) = self.pipe.take() else {
            return false;
        };

        match output.try_recv() {
            Ok(data) => {
                match data {
                    Ok(data) => {
                        // Data loaded successfully!
                        let path = data.filters.absolute_path.clone();

                        // Update data filters
                        self.data_filters = data.filters.as_ref().clone();
                        dbg!(&data.filters);

                        // Load metadata
                        self.metadata = match data.extension {
                            FileExtension::Parquet => {
                                FileMetadata::from_path(path, "parquet", None, None).ok()
                            }
                            FileExtension::Csv => {
                                let arc_schema = data.df.schema().clone();
                                let row_count = data.df.height();
                                FileMetadata::from_path(
                                    path,
                                    "csv",
                                    Some(arc_schema),
                                    Some(row_count),
                                )
                                .ok()
                            }
                            _ => None,
                        };

                        self.df = Arc::new(Some(data));
                        false // Data loading complete.
                    }
                    Err(err) => {
                        // An error occurred during data loading.
                        let error_message = match &err {
                            PolarsViewError::FileNotFound(path) => {
                                format!("File not found: {}", path.display())
                            }
                            PolarsViewError::Io(io_err) => format!("IO error: {io_err}"),
                            PolarsViewError::Polars(polars_err) => {
                                format!("Polars error: {polars_err}")
                            }
                            PolarsViewError::CsvParsing(msg) => format!("CSV parsing error: {msg}"),
                            PolarsViewError::FileType(msg) => format!("File type error: {msg}"),
                            PolarsViewError::TokioJoin(join_err) => {
                                format!("Tokio JoinError: {join_err}")
                            }
                            PolarsViewError::ChannelReceive(msg) => {
                                format!("Channel receive error: {msg}")
                            }
                            PolarsViewError::Other(msg) => format!("Other error: {msg}"),
                            PolarsViewError::InvalidDelimiter(msg) => {
                                format!("Invalid CSV delimiter: {msg}")
                            }
                            PolarsViewError::UnsupportedFileType(msg) => {
                                format!("Unsupported file type: {msg}")
                            }
                            PolarsViewError::SqlQueryError(msg) => {
                                format!("SQL query error: {msg}")
                            }
                            PolarsViewError::Parquet(parquet_err) => {
                                format!("Parquet error: {parquet_err}")
                            }
                        };
                        self.popover = Some(Box::new(Error {
                            message: error_message.clone(), // Capture for the closure.
                        }));
                        error!("Data loading failed: {}", error_message); // Log
                        false // Data loading complete (with error).
                    }
                }
            }
            Err(error) => match error {
                TryRecvError::Empty => {
                    self.pipe = Some(output);
                    true
                }
                TryRecvError::Closed => {
                    let err_msg = "Data operation terminated without response.".to_string();
                    self.popover = Some(Box::new(Error {
                        message: err_msg.clone(),
                    }));
                    error!("{}", err_msg);
                    false
                }
            },
        }
    }

    /// Runs a `DataFuture` to load data asynchronously.
    ///
    /// This function takes a future, spawns a Tokio task, and sets up a channel to receive the result.
    fn run_data_future(&mut self, future: DataFuture, ctx: &Context) {
        // Before scheduling a new future, ensure no tasks are stuck
        self.tasks.retain(|task| !task.is_finished());

        // Create a oneshot channel for sending the data from the async task to the UI thread.
        let (tx, rx) = oneshot::channel::<PolarsViewResult<DataFrameContainer>>();
        self.pipe = Some(rx);

        // Clone the context for use within the asynchronous task (to request repaints).
        let ctx_clone = ctx.clone();

        // Spawn an async task to load the data.
        let handle = self.runtime.spawn(async move {
            let data = future.await;
            // Handle potential error if the receiver is dropped.
            if tx.send(data).is_err() {
                error!("Receiver dropped before data could be sent.");
            }

            // Request a repaint of the UI to display the loaded data.
            ctx_clone.request_repaint();
        });

        self.tasks.push(handle); // Track the task.
    }
}

// See
// https://github.com/emilk/egui/blob/master/examples/custom_window_frame/src/main.rs
// https://rodneylab.com/trying-egui/

impl eframe::App for PolarsViewApp {
    fn update(&mut self, ctx: &Context, _frame: &mut eframe::Frame) {
        // Check and display any active popovers (errors, settings, etc.).
        self.check_popover(ctx);

        // Handle dropped files.
        if let Some(dropped_file) = ctx.input(|i| i.raw.dropped_files.last().cloned()) {
            if let Some(path) = &dropped_file.path {
                // Update PolarsViewApp
                self.data_filters.set_path(path);
                let future = DataFrameContainer::load_data(self.data_filters.clone(), false);
                self.run_data_future(Box::new(Box::pin(future)), ctx);
            }
        }

        // Define the main UI layout.
        //
        // Using static layout until I put together a TabTree that can make this dynamic
        //
        //  | menu_bar        widgets |
        //  ---------------------------
        //  |         |               |
        //  | Data    |     main      |
        //  | Filters |     table     |
        //  |         |               |
        //  ---------------------------
        //  | notification footer     |

        TopBottomPanel::top("top_panel").show(ctx, |ui| {
            menu::bar(ui, |ui| {
                ui.horizontal(|ui| {
                    ui.menu_button("File", |ui| {
                        if ui.button("Open").clicked() {
                            // Open a file dialog to select a file.
                            if let Ok(path) = self.runtime.block_on(file_dialog()) {
                                // Update PolarsViewApp
                                self.data_filters.set_path(&path);
                                dbg!(&self.data_filters);
                                let future =
                                    DataFrameContainer::load_data(self.data_filters.clone(), false);
                                self.run_data_future(Box::new(Box::pin(future)), ctx);
                            }
                            ui.close_menu();
                        }

                        if ui.button("Settings").clicked() {
                            // Show the settings popover.
                            self.popover = Some(Box::new(Settings {}));
                            ui.close_menu();
                        }

                        ui.menu_button("About", |ui| {
                            // Display application information.
                            Frame::default()
                                .stroke(Stroke::new(1.0, Color32::GRAY)) // Thin gray border for visual separation.
                                .outer_margin(2.0) // Set a margin outside the frame.
                                .inner_margin(10.0) // Set a margin inside the frame.
                                .show(ui, |ui| {
                                    let version = env!("CARGO_PKG_VERSION");
                                    let authors = env!("CARGO_PKG_AUTHORS");

                                    Grid::new("about_grid")
                                        .num_columns(1)
                                        .spacing([10.0, 4.0])
                                        .show(ui, |ui| {
                                            ui.with_layout(
                                                Layout::centered_and_justified(Direction::LeftToRight),
                                                |ui| {
                                                    ui.label(
                                                        RichText::new("Polars View")
                                                            .font(FontId::proportional(30.0)),
                                                    );
                                                },
                                            );
                                            ui.end_row();

                                            ui.with_layout(
                                                Layout::centered_and_justified(Direction::LeftToRight),
                                                |ui| {
                                                    ui.label(format!("Version: {version}"));
                                                },
                                            );
                                            ui.end_row();
                                            ui.end_row();

                                            ui.with_layout(
                                                Layout::centered_and_justified(Direction::LeftToRight),
                                                |ui| {
                                                    ui.label(
                                                        RichText::new("A fast viewer for Parquet and CSV files")
                                                            .font(FontId::proportional(20.0)),
                                                    );
                                                },
                                            );
                                            ui.end_row();
                                            ui.end_row();

                                            ui.horizontal(|ui| {
                                                let url = "https://github.com/pola-rs/polars";
                                                let heading =
                                                    Hyperlink::from_label_and_url("Polars", url);

                                                ui.label("Powered by ");
                                                ui.add(heading).on_hover_text(url);
                                            });
                                            ui.end_row();

                                            ui.horizontal(|ui| {
                                                let url = "https://github.com/emilk/egui";
                                                let heading =
                                                    Hyperlink::from_label_and_url("egui", url);

                                                ui.label("Built with ");
                                                ui.add(heading).on_hover_text(url);
                                            });
                                            ui.end_row();

                                            ui.horizontal(|ui| {
                                                let url = "https://github.com/Kxnr/parqbench";
                                                let heading =
                                                    Hyperlink::from_label_and_url("parqbench", url);

                                                ui.label("A fork of ");
                                                ui.add(heading).on_hover_text(url);
                                            });
                                            ui.end_row();
                                            ui.end_row();

                                            ui.label(format!("Author: {authors}"));
                                            ui.end_row();
                                        });
                                });
                        });

                        if ui.button("Quit").clicked() {
                            // Close the application.
                            ui.ctx().send_viewport_cmd(ViewportCommand::Close);
                        }
                    });

                    // Add spacing to align theme switch to the right.
                    let delta = ui.available_width() - 15.0;
                    if delta > 0.0 {
                        ui.add_space(delta);
                        widgets::global_theme_preference_switch(ui);
                    }
                });
            });
        });

        SidePanel::left("side_panel")
            .resizable(true)
            .show(ctx, |ui| {
                ScrollArea::vertical().show(ui, |ui| {
                    // Add Metadata section
                    if let Some(metadata) = &self.metadata {
                        ui.collapsing("Metadata", |ui| {
                            metadata.render_metadata(ui);
                        });
                    }

                    // Add Query section
                    ui.collapsing("Query", |ui| {
                        if let Some(filters) = self.data_filters.render_filter(ui) {
                            let future = DataFrameContainer::load_data(filters, true);
                            self.run_data_future(Box::new(Box::pin(future)), ctx);
                        }
                    });

                    // Add Schema section
                    if let Some(metadata) = &self.metadata {
                        ui.collapsing("Schema", |ui| {
                            metadata.render_schema(ui);
                        });
                    }
                });
            });

        TopBottomPanel::bottom("bottom_panel").show(ctx, |ui| {
            // Display the path of the loaded data.
            ui.horizontal(|ui| match &*self.df {
                Some(table) => {
                    ui.label(format!("{:#?}", table.filters.absolute_path));
                }
                None => {
                    ui.label("no file set");
                }
            });
        });

        // Main table display area.
        // CentralPanel must be added after all other panels!
        CentralPanel::default().show(ctx, |ui| {
            warn_if_debug_build(ui);

            match self.df.as_ref().clone() {
                Some(df_container) if df_container.df.width() > 0 => {
                    // Data loaded successfully, display the table.
                    ScrollArea::horizontal().show(ui, |ui| {
                        ui.style_mut().spacing.scroll.handle_min_length = 32.0;
                        ui.style_mut().spacing.scroll.allocated_width();

                        let opt_filters = df_container.render_table(ui);
                        if let Some(filters) = opt_filters {
                            let future = df_container.sort(Some(filters));
                            self.run_data_future(Box::new(Box::pin(future)), ctx);
                        }
                    });
                }
                _ => {
                    // No data loaded yet, show a prompt.
                    ui.centered_and_justified(|ui| {
                        ui.label("Drag and drop parquet file here.");
                    });
                }
            };

            // Show a loading spinner if data is currently being loaded.
            if self.check_data_pending() {
                ui.disable(); // Disable UI interaction while loading.
                if self.df.as_ref().is_none() {
                    ui.centered_and_justified(|ui| {
                        // Show spinner while loading initial data.
                        ui.spinner();
                    });
                }
            }
        });
    }
}
