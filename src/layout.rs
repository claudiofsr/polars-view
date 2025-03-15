use crate::{
    DataFilters, DataFrameContainer, Error, FileMetadata, MyStyle, Notification, PolarsViewResult,
    Settings, file_dialog, save_file_dialog,
};

use egui::{
    CentralPanel, Color32, Context, Direction, FontId, Frame, Grid, Hyperlink, Layout, RichText,
    ScrollArea, SidePanel, Stroke, TopBottomPanel, ViewportCommand, menu, style::Visuals,
    warn_if_debug_build, widgets,
};
use std::sync::Arc;
use tokio::sync::oneshot::{self, Receiver, error::TryRecvError};
use tracing::error;

/// Type alias for a Result with a `DataFrameContainer`.
pub type ContainerResult = PolarsViewResult<DataFrameContainer>;
/// Type alias for a boxed, dynamically dispatched Future that returns a `ContainerResult`.
pub type DataFuture = Box<dyn Future<Output = ContainerResult> + Unpin + Send + 'static>;

/// The main application struct for PolarsView.
pub struct PolarsViewApp {
    /// The `DataFrameContainer` holds the loaded data (Parquet, CSV, etc.).
    /// Using Option<Arc> it is more efficient for sharing data across the UI.
    pub data_container: Option<Arc<DataFrameContainer>>,
    /// Component for managing data filters (SQL queries, sorting, etc.).
    pub data_filters: DataFilters,
    /// Metadata extracted from the loaded file (if available).
    pub metadata: Option<FileMetadata>,
    /// Optional Notification window for displaying errors or settings.
    pub notification: Option<Box<dyn Notification>>,

    /// Tokio runtime for asynchronous operations (file loading, queries).
    runtime: tokio::runtime::Runtime,
    /// Channel for receiving the result of asynchronous data loading.
    pipe: Option<Receiver<PolarsViewResult<DataFrameContainer>>>,
    /// Vector of active asynchronous tasks.  Used to prevent the app from hanging.
    tasks: Vec<tokio::task::JoinHandle<()>>,
}

impl Default for PolarsViewApp {
    fn default() -> Self {
        Self {
            data_container: None,
            data_filters: DataFilters::default(),
            runtime: tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .expect("Failed to build Tokio runtime"),
            pipe: None,
            notification: None,
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

    /// Checks if a Notification is active and displays it.
    fn check_notification(&mut self, ctx: &Context) {
        if let Some(notification) = &mut self.notification {
            if !notification.show(ctx) {
                self.notification = None; // Remove closed Notification.
            }
        }
    }

    /// Checks if there is a pending data loading operation (asynchronous).
    /// If data is available or an error occurred, process it.  If the operation is still
    /// in progress, keeps it in the `pipe`.  Returns `true` if loading is pending,
    /// and `false` if loading is complete (either with data or an error).
    fn check_data_pending(&mut self) -> bool {
        // Attempt to take ownership of the receiver.  If it's None (no pending operation), return false.
        let Some(mut output) = self.pipe.take() else {
            return false;
        };

        // Try to receive a value from the channel without blocking.
        match output.try_recv() {
            // Successfully received data (Ok) or an error (Err) from the background task.
            Ok(data_result) => {
                match data_result {
                    // Data loaded successfully.
                    Ok(container) => {
                        // Update application state with the new data.

                        // 1. Update data filters:
                        self.data_filters = container.filters.as_ref().clone();

                        // 2. Load metadata (for display in the UI):
                        self.metadata = FileMetadata::from_container(&container);

                        // 3. Store the DataFrameContainer (wrapped in Arc for shared ownership):
                        self.data_container = Some(Arc::new(container));

                        false // Indicate that data loading is complete.
                    }
                    // An error occurred during data loading.
                    Err(err) => {
                        // Create the error message string directly from the error.  Because
                        // PolarsViewError implements Display, we can just use `to_string()`.
                        let error_message = err.to_string();

                        // Create and display the error Notification (to the user).
                        self.notification = Some(Box::new(Error {
                            message: error_message,
                        }));
                        error!("Data loading failed: {}", err); // Log full error details.
                        false // Indicate that data loading is complete (with error).
                    }
                }
            }
            // An error occurred while trying to receive from the channel.
            Err(try_recv_error) => match try_recv_error {
                // The channel is empty (data not yet available).  This is the normal "pending" state.
                TryRecvError::Empty => {
                    // Put the receiver back into `self.pipe` to check again later.
                    self.pipe = Some(output);
                    true // Indicate that data loading is still pending.
                }
                // The channel is closed (the sender was dropped). This is an unexpected error state.
                TryRecvError::Closed => {
                    let err_msg = "Data operation terminated without response.".to_string();
                    // Notify the user and log the error.
                    self.notification = Some(Box::new(Error {
                        message: err_msg.clone(),
                    }));
                    error!("{}", err_msg);
                    false // Indicate data loading is complete (with error).
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
        // Check and display any active Notifications (errors, settings, etc.).
        self.check_notification(ctx);

        // Handle dropped files.
        if let Some(dropped_file) = ctx.input(|i| i.raw.dropped_files.last().cloned()) {
            if let Some(path) = &dropped_file.path {
                // Update PolarsViewApp
                self.data_filters.set_path(path);
                let future = DataFrameContainer::load_data(self.data_filters.clone());
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
                                let future =
                                    DataFrameContainer::load_data(self.data_filters.clone());
                                self.run_data_future(Box::new(Box::pin(future)), ctx);
                            }
                            ui.close_menu();
                        }

                        // Add the "Save as" option to the File menu.
                        if ui.button("Save as").clicked() {
                            if let Some(container) = &self.data_container {
                                // Clone the Arc for shared ownership.  This is a *cheap* clone, just incrementing
                                // the reference count, not deep-copying the data.
                                let container_clone = container.clone();
                                let ctx_clone = ctx.clone(); //Clone the context too

                                // Spawn an async task so saving doesn't block the UI.
                                self.runtime.spawn(async move {
                                    if let Err(err) =
                                        save_file_dialog(container_clone, ctx_clone).await
                                    {
                                        error!("Failed to save file: {}", err); // Log or show to user.
                                    }
                                });
                            }
                            ui.close_menu();
                        }

                        if ui.button("Settings").clicked() {
                            // Show the settings Notification.
                            self.notification = Some(Box::new(Settings {}));
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
                                    let description = env!("CARGO_PKG_DESCRIPTION");

                                    Grid::new("about_grid")
                                        .num_columns(1)
                                        .spacing([10.0, 4.0])
                                        .show(ui, |ui| {
                                            ui.with_layout(
                                                Layout::centered_and_justified(
                                                    Direction::LeftToRight,
                                                ),
                                                |ui| {
                                                    ui.label(
                                                        RichText::new("Polars View")
                                                            .font(FontId::proportional(30.0)),
                                                    );
                                                },
                                            );
                                            ui.end_row();

                                            ui.with_layout(
                                                Layout::centered_and_justified(
                                                    Direction::LeftToRight,
                                                ),
                                                |ui| {
                                                    ui.label(format!("Version: {version}"));
                                                },
                                            );
                                            ui.end_row();
                                            ui.end_row();

                                            ui.with_layout(
                                                Layout::centered_and_justified(
                                                    Direction::LeftToRight,
                                                ),
                                                |ui| {
                                                    ui.label(
                                                        RichText::new(description)
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
                            let future = DataFrameContainer::load_data(filters);
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
            ui.horizontal(|ui| match &self.data_container {
                Some(container) => {
                    ui.label(format!("{:#?}", container.filters.absolute_path));
                }
                None => {
                    ui.label("no file set");
                }
            });
        });

        // Main table display area.
        // CentralPanel must be added after all other panels in your egui layout!
        CentralPanel::default().show(ctx, |ui| {
            // Display a warning message if the application is built in debug mode.
            warn_if_debug_build(ui);

            // Disable UI interaction while data is being loaded or processed (data_pending is true).
            if self.check_data_pending() {
                ui.disable();
            }

            match &self.data_container {
                Some(container) => {
                    // Store the optional filters here, *before* the ScrollArea.
                    let mut opt_filters = None;

                    // Dataframe is loaded and available in dt_container. Display the table.
                    ScrollArea::horizontal()
                        .auto_shrink([false, false]) // Prevent the scroll area from shrinking.
                        .show(ui, |ui| {
                            // Customize the minimum length of the scrollbar handle for better user interaction.
                            ui.style_mut().spacing.scroll.handle_min_length = 32.0;
                            ui.style_mut().spacing.scroll.allocated_width();

                            // Render the data table using render_table method.
                            // Returns optional filters applied by the user in the table UI.
                            opt_filters = container.render_table(ui);
                        }); // Close ScrollArea *before* using run_data_future.

                    // If filters were applied by the user (opt_filters is Some), initiate sorting.
                    if let Some(filters) = opt_filters {
                        // Create a future for the sorting operation, passing the applied filters.
                        let future = container.as_ref().clone().sort(filters);
                        // Run the data future to execute the sorting operation asynchronously.
                        self.run_data_future(Box::new(Box::pin(future)), ctx);
                    }
                }
                None => {
                    // Check if data loading is pending (e.g., initial load in progress).
                    if self.check_data_pending() {
                        // Data loading is pending, show a loading spinner in the center of the panel.
                        ui.centered_and_justified(|ui| {
                            ui.spinner();
                        });
                    } else {
                        // No data loaded and no data loading pending.
                        // Display a prompt message to the user.
                        ui.centered_and_justified(|ui| {
                            ui.label("Drag and drop CSV or Parquet file here.");
                        });
                    }
                }
            }
        });
    }
}
