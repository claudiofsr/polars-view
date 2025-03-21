use crate::{
    DataFilters, DataFrameContainer, Error, FileMetadata, MyStyle, Notification, PolarsViewResult,
    Settings, open_file, save, save_as,
};

use egui::{
    CentralPanel, Color32, Context, Direction, FontId, Frame, Grid, Hyperlink, Key,
    KeyboardShortcut, Layout, Modifiers, RichText, ScrollArea, SidePanel, Stroke, TopBottomPanel,
    ViewportCommand, menu, style::Visuals,
};
use std::sync::Arc;
use tokio::sync::oneshot::{self, Receiver, error::TryRecvError};
use tracing::error;

/// Type alias for a Result with a `DataFrameContainer`.
pub type ContainerResult = PolarsViewResult<DataFrameContainer>;
/// Type alias for a boxed, dynamically dispatched Future that returns a `ContainerResult`.
pub type DataFuture = Box<dyn Future<Output = ContainerResult> + Unpin + Send + 'static>;

// Define keyboard shortcuts for common actions.
const CTRL_O: KeyboardShortcut = KeyboardShortcut::new(Modifiers::CTRL, Key::O); // Ctrl+O for Open
const CTRL_S: KeyboardShortcut = KeyboardShortcut::new(Modifiers::CTRL, Key::S); // Ctrl+S for Save
const CTRL_A: KeyboardShortcut = KeyboardShortcut::new(Modifiers::CTRL, Key::A); // Ctrl+A for Save As

/// The main application struct for PolarsView.
pub struct PolarsViewApp {
    /// The `DataFrameContainer` holds the loaded data (Parquet, CSV, etc.).
    /// Using `Option<Arc>` it is more efficient for sharing data across the UI.  An `Option` allows
    /// for the absence of data (e.g., before a file is loaded). The `Arc` enables shared
    /// ownership, making it easy to pass the data around without deep copying.
    pub data_container: Option<Arc<DataFrameContainer>>,
    /// Stores the *previous* state of the filters, used for change detection.
    /// This allows the application to determine when the filters have been modified and
    /// a reload of the data is necessary.
    pub filters_previous: DataFilters,
    /// Metadata extracted from the loaded file (if available). This includes information
    /// like row count, column count, and schema.  The `Option` allows for cases where
    /// metadata might not be available (e.g., before a file is loaded or if there's an error).
    pub metadata: Option<FileMetadata>,
    /// Optional Notification window for displaying errors or settings.
    /// A `Box<dyn Notification>` allows for different types of notifications
    /// (e.g., error messages, settings dialog) to be displayed using dynamic dispatch.
    /// The `Option` indicates that a notification might not be active at all times.
    pub notification: Option<Box<dyn Notification>>,

    /// Tokio runtime for asynchronous operations (file loading, queries).
    /// The runtime is responsible for spawning and managing asynchronous tasks,
    /// allowing for non-blocking I/O operations.
    runtime: tokio::runtime::Runtime,
    /// Channel for receiving the result of asynchronous data loading.
    /// The `Receiver` part of a oneshot channel is used to get the result
    /// (either a `DataFrameContainer` or an error) from the background data loading task.
    /// The `Option` allows for the channel to be consumed after receiving the result.
    pipe: Option<Receiver<PolarsViewResult<DataFrameContainer>>>,
    /// Vector of active asynchronous tasks. Used to prevent the app from hanging.
    /// This stores the `JoinHandle`s of spawned Tokio tasks. Keeping track of these handles
    /// allows the application to check their status and prevent resource leaks.
    tasks: Vec<tokio::task::JoinHandle<()>>,
}

impl Default for PolarsViewApp {
    fn default() -> Self {
        Self {
            data_container: None,
            filters_previous: DataFilters::default(),
            runtime: tokio::runtime::Builder::new_multi_thread() // Use a multi-threaded runtime.
                .enable_all() // Enable all Tokio features (I/O, time, etc.).
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
    ///
    /// Initializes the application with default settings and sets up the dark theme.
    /// It uses the creation context (`cc`) from `eframe` to configure the initial UI.
    pub fn new(cc: &eframe::CreationContext<'_>) -> PolarsViewResult<Self> {
        cc.egui_ctx.set_style_init(Visuals::dark()); // Apply custom styles and dark theme (defined in `MyStyle` trait).
        Ok(Default::default()) // Return a new PolarsViewApp with default settings.
    }

    /// Creates a new `PolarsViewApp` with a pre-existing `DataFuture`.
    ///
    /// This allows starting the application with data already being loaded. It is useful
    /// when the data loading process is initiated outside the main application,
    /// for instance, through command-line arguments that specify a file to load on startup.
    /// Initializes the application similarly to `new`, but also immediately starts running
    /// the provided future to load data.
    ///
    /// The future will be executed using the internal `tokio` runtime.
    pub fn new_with_future(
        cc: &eframe::CreationContext<'_>,
        future: DataFuture,
    ) -> PolarsViewResult<Self> {
        let mut app: Self = Default::default(); // Create a new app with default settings.
        cc.egui_ctx.set_style_init(Visuals::dark()); // Apply custom styles and dark theme (defined in `MyStyle` trait).
        app.run_data_future(future, &cc.egui_ctx); // Start running the provided data loading future.
        Ok(app) // Return the initialized app.
    }

    /// Checks if a Notification is active and displays it.
    fn check_notification(&mut self, ctx: &Context) {
        if let Some(notification) = &mut self.notification {
            // If a notification is present,
            if !notification.show(ctx) {
                // try to display it.
                self.notification = None; // If `show` returns false, the notification is closed, so remove it.
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

                        // 1. Get data filters:
                        self.filters_previous = container.filters.as_ref().clone();

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
        self.pipe = Some(rx); // Store the receiving end of the channel.

        // Clone the context for use within the asynchronous task (to request repaints).
        let ctx_clone = ctx.clone();

        // Spawn an async task to load the data.
        let handle = self.runtime.spawn(async move {
            let data = future.await; // Await the result of the DataFuture.
            // Handle potential error if the receiver is dropped.
            if tx.send(data).is_err() {
                error!("Receiver dropped before data could be sent.");
            }

            // Request a repaint of the UI to display the loaded data or any error message.
            ctx_clone.request_repaint();
        });

        self.tasks.push(handle); // Track the task.
    }
}

// See
// https://github.com/emilk/egui/blob/master/examples/custom_window_frame/src/main.rs
// https://rodneylab.com/trying-egui/

impl eframe::App for PolarsViewApp {
    /// Main update function for the `eframe` application. This is called on each frame.
    fn update(&mut self, ctx: &Context, _frame: &mut eframe::Frame) {
        // Check and display any active Notifications (errors, settings, etc.).
        self.check_notification(ctx);

        // Handle dropped files.  This allows the user to drag and drop files onto the application.
        if let Some(dropped_file) = ctx.input(|i| i.raw.dropped_files.last().cloned()) {
            if let Some(path) = &dropped_file.path {
                // Update absolute path
                if let Err(error_message) = self.filters_previous.set_path(path) {
                    // If setting the path fails (e.g., invalid path), show an error notification.
                    self.notification = Some(Box::new(Error {
                        message: error_message.to_string(),
                    }))
                }

                // Update PolarsViewApp
                // Create a future to load the data from the newly dropped file.
                let future = DataFrameContainer::load_data(self.filters_previous.clone());
                self.run_data_future(Box::new(Box::pin(future)), ctx); // Start loading the data asynchronously.
            }
        }

        // Check for global keyboard shortcuts (Ctrl+O, Ctrl+S, Ctrl+A) *before* drawing any menus.
        // This ensures the shortcuts work even if the menu isn't open.
        ctx.input_mut(|i| {
            if i.consume_shortcut(&CTRL_O) {
                // Open: Ctrl + O
                self.handle_open_file(ctx);
            }

            if i.consume_shortcut(&CTRL_S) {
                // Save: Ctrl + S
                self.handle_save_file(ctx);
            }

            if i.consume_shortcut(&CTRL_A) {
                // Save As: Ctrl + A
                self.handle_save_as(ctx);
            }
        });

        // Define the main UI layout structure using `TopBottomPanel`, `SidePanel`, and `CentralPanel`.
        //
        // Using static layout until I put together a TabTree that can make this dynamic
        //
        //  | menu_bar help   widgets |
        //  ---------------------------
        //  |         |               |
        //  | Data    |     main      |
        //  | Filters |     table     |
        //  |         |               |
        //  ---------------------------
        //  | notification footer     |

        // Top panel for the menu bar.
        self.render_menu_bar(ctx);

        // Side panel for data filters and metadata.
        self.render_side_panel(ctx);

        // Bottom panel for displaying file path information.
        self.render_bottom_panel(ctx);

        // Main table display area.
        // CentralPanel must be added after all other panels in your egui layout!
        self.render_central_panel(ctx);
    }
}

impl PolarsViewApp {
    /// Handles the "Open File" action, triggered by menu or Ctrl+O.
    fn handle_open_file(&mut self, ctx: &Context) {
        // Open a file dialog to select a file.
        if let Ok(path) = self.runtime.block_on(open_file()) {
            // Update absolute path
            if let Err(error_message) = self.filters_previous.set_path(&path) {
                // If setting the path fails, display an error.
                self.notification = Some(Box::new(Error {
                    message: error_message.to_string(),
                }))
            }

            // Update PolarsViewApp
            // If a file is selected, create a future to load the data.
            let future = DataFrameContainer::load_data(self.filters_previous.clone());
            self.run_data_future(Box::new(Box::pin(future)), ctx); // Start loading asynchronously.
        }
    }

    /// Handles the "Save" action (Ctrl+S), saving to the original file if possible.
    fn handle_save_file(&mut self, ctx: &Context) {
        if let Some(container) = &self.data_container {
            let container_clone = container.clone();
            // Clone the context for use within the asynchronous task (to request repaints).
            let ctx_clone = ctx.clone();
            // Spawn an async task so saving doesn't block the UI.
            self.runtime.spawn(async move {
                if let Err(err) = save(container_clone, ctx_clone).await {
                    error!("Failed to save file: {}", err); // Log the error
                }
                // No need for ctx_clone.request_repaint() here; save() now handles it.
            });
        }
    }

    /// Handles the "Save As" action (Ctrl+A), prompting the user for a new file location.
    fn handle_save_as(&mut self, ctx: &Context) {
        if let Some(container) = &self.data_container {
            // Clone the Arc for shared ownership. This is a *cheap* clone.
            let container_clone = container.clone();
            let ctx_clone = ctx.clone(); // Clone the context too.

            // Spawn an async task so saving doesn't block the UI.
            self.runtime.spawn(async move {
                if let Err(err) = save_as(container_clone, ctx_clone).await {
                    error!("Failed to save file: {}", err); // Log or show to user.
                }
            });
        }
    }

    /// Renders the top menu bar.
    fn render_menu_bar(&mut self, ctx: &Context) {
        TopBottomPanel::top("top_panel").show(ctx, |ui| {
            menu::bar(ui, |ui| {
                ui.horizontal(|ui| {
                    // "File" menu.
                    self.render_file_menu(ui);

                    // "Help" menu.
                    self.render_help_menu(ui);

                    // Light/Dark theme
                    self.render_theme(ui);
                });
            });
        });
    }

    /// Renders the "File" menu within the menu bar.
    fn render_file_menu(&mut self, ui: &mut egui::Ui) {
        ui.menu_button("File", |ui| {
            // Display the information in a grid layout.
            Grid::new("open_file_grid")
                .num_columns(2)
                .spacing([20.0, 10.0])
                .show(ui, |ui| {
                    // "Open File..." option.
                    if ui.button("Open File...").clicked() {
                        self.handle_open_file(ui.ctx());
                        ui.close_menu();
                    }
                    ui.label("Ctrl + O");
                    ui.end_row();

                    // "Save" option.  Saves the *original* file, if available.
                    if ui.button("Save").clicked() {
                        self.handle_save_file(ui.ctx());
                        ui.close_menu();
                    }
                    ui.label("Ctrl + S");
                    ui.end_row();

                    // Add the "Save as" option to the File menu.
                    if ui.button("Save As...").clicked() {
                        self.handle_save_as(ui.ctx());
                        ui.close_menu();
                    }
                    ui.label("Ctrl + A");
                    ui.end_row();
                });

            // Add separator line HERE
            ui.add(egui::Separator::default().horizontal());

            // Display the information in a grid layout.
            Grid::new("settings_grid")
                .num_columns(2)
                .spacing([20.0, 10.0])
                .show(ui, |ui| {
                    // "Settings" option (currently non-functional, but shows how to create a notification).
                    if ui.button("Settings").clicked() {
                        // Show the settings Notification.
                        self.notification = Some(Box::new(Settings {}));
                        ui.close_menu();
                    }
                    ui.label("");
                    ui.end_row();

                    // "Exit" option.
                    if ui.button("Exit").clicked() {
                        // Close the application.
                        ui.ctx().send_viewport_cmd(ViewportCommand::Close);
                    }
                    ui.label("");
                    ui.end_row();
                });
        });
    }

    /// Renders the "Help" menu within the menu bar.
    fn render_help_menu(&mut self, ui: &mut egui::Ui) {
        ui.menu_button("Help", |ui| {
            // Documentation link.
            ui.horizontal(|ui| {
                let url = "https://docs.rs/polars-view";
                ui.hyperlink_to("Documentation", url).on_hover_text(url);
            });

            // Add separator line HERE
            ui.add(egui::Separator::default().horizontal());

            ui.menu_button("About", |ui| {
                // Display application information.
                Frame::default()
                    .stroke(Stroke::new(1.0, Color32::GRAY)) // Thin gray border
                    .outer_margin(2.0) // Set a margin outside the frame.
                    .inner_margin(10.0) // Set a margin inside the frame.
                    .show(ui, |ui| {
                        // Load version, authors, and description from Cargo.toml.
                        let version = env!("CARGO_PKG_VERSION");
                        let authors = env!("CARGO_PKG_AUTHORS");
                        let description = env!("CARGO_PKG_DESCRIPTION");

                        // Display the information in a grid layout.
                        Grid::new("about_grid")
                            .num_columns(1)
                            .spacing([20.0, 10.0])
                            .show(ui, |ui| {
                                ui.with_layout(
                                    Layout::centered_and_justified(Direction::LeftToRight),
                                    |ui| {
                                        // Application title with a larger font.
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
                                        // Display the version number.
                                        ui.label(format!("Version: {version}"));
                                    },
                                );
                                ui.end_row();
                                ui.end_row();

                                ui.with_layout(
                                    Layout::centered_and_justified(Direction::LeftToRight),
                                    |ui| {
                                        // Display a short description.
                                        ui.label(
                                            RichText::new(description)
                                                .font(FontId::proportional(20.0)),
                                        );
                                    },
                                );
                                ui.end_row();
                                ui.end_row();

                                // Hyperlink to the Polars library.
                                ui.horizontal(|ui| {
                                    let url = "https://github.com/pola-rs/polars";
                                    let heading = Hyperlink::from_label_and_url("Polars", url);

                                    ui.label("Powered by ");
                                    ui.add(heading).on_hover_text(url);
                                });
                                ui.end_row();

                                // Hyperlink to the egui library.
                                ui.horizontal(|ui| {
                                    let url = "https://github.com/emilk/egui";
                                    let heading = Hyperlink::from_label_and_url("egui", url);

                                    ui.label("Built with ");
                                    ui.add(heading).on_hover_text(url);
                                });
                                ui.end_row();

                                // Hyperlink to the parqbench project.
                                ui.horizontal(|ui| {
                                    let url = "https://github.com/Kxnr/parqbench";
                                    let heading = Hyperlink::from_label_and_url("parqbench", url);

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
        });
    }

    /// Light/Dark theme
    fn render_theme(&mut self, ui: &mut egui::Ui) {
        // Add spacing to align theme switch to the right.
        // Calculate based on the button size.
        let button_size = ui.style().spacing.interact_size.x; // Width of a standard button.
        let delta = ui.available_width() - button_size;

        if delta > 0.0 {
            ui.add_space(delta);

            // Light/Dark theme switch (radio buttons).
            ui.with_layout(Layout::right_to_left(egui::Align::Center), |ui| {
                let mut dark_mode = ui.ctx().style().visuals.dark_mode;

                if ui.radio_value(&mut dark_mode, true, "🌙").clicked() {
                    ui.ctx().set_style_init(Visuals::dark());
                }

                if ui.radio_value(&mut dark_mode, false, "🔆").clicked() {
                    ui.ctx().set_style_init(Visuals::light());
                }
            });
        }
    }

    /// Renders the side panel for data filters and metadata.
    fn render_side_panel(&mut self, ctx: &Context) {
        SidePanel::left("side_panel")
            .resizable(true) // Allow resizing the side panel.
            .show(ctx, |ui| {
                ScrollArea::vertical().show(ui, |ui| {
                    // Add Metadata section
                    if let Some(metadata) = &self.metadata {
                        ui.collapsing("Metadata", |ui| {
                            metadata.render_metadata(ui); // Display file metadata.
                        });
                    }

                    // Add Query section
                    ui.collapsing("Query", |ui| {
                        // Render the data filters UI.  If filters are changed,
                        // `render_filter` returns a new `DataFilters` instance.
                        if let Some(filters) = self.filters_previous.render_filter(ui) {
                            // If new filters are returned, create a future to reload
                            // with the updated filters.
                            let future = DataFrameContainer::load_data(filters);
                            self.run_data_future(Box::new(Box::pin(future)), ctx); // Run asynchronously.
                        }
                    });

                    // Add Schema section
                    if let Some(metadata) = &self.metadata {
                        ui.collapsing("Schema", |ui| {
                            metadata.render_schema(ui); // Display the data schema.
                        });
                    }
                });
            });
    }

    /// Renders the bottom panel, which displays file path information.
    fn render_bottom_panel(&mut self, ctx: &Context) {
        TopBottomPanel::bottom("bottom_panel").show(ctx, |ui| {
            // Display the path of the loaded data.
            ui.horizontal(|ui| match &self.data_container {
                Some(container) => {
                    // If data is loaded, display the file path.
                    ui.label(format!("{:#?}", container.filters.absolute_path));
                }
                None => {
                    // If no data is loaded, display "no file set".
                    ui.label("no file set");
                }
            });
        });
    }

    /// Renders the central panel, which contains the main data table.
    /// CentralPanel must be added after all other panels in your egui layout!
    fn render_central_panel(&mut self, ctx: &Context) {
        CentralPanel::default().show(ctx, |ui| {
            // Display a warning if the application is built in debug mode.
            egui::warn_if_debug_build(ui);

            // Disable UI interaction while data loading/processing (data_pending is true).
            if self.check_data_pending() {
                ui.disable();
            }

            match &self.data_container {
                Some(container) => {
                    // Dataframe is loaded. Display the table.
                    // Store optional sort filters, *before* the ScrollArea.
                    let mut opt_filters = None;

                    ScrollArea::horizontal()
                        .auto_shrink([false, false]) // Prevent shrinking.
                        .show(ui, |ui| {
                            // Render data, get sort filters.
                            opt_filters = container.render_table(ui);
                        }); // Close ScrollArea *before* using run_data_future.

                    // If sort filters applied, initiate sorting.
                    if let Some(filters) = opt_filters {
                        let future = container.as_ref().clone().sort(filters);
                        self.run_data_future(Box::new(Box::pin(future)), ctx);
                    }
                }
                None => {
                    // Check for data loading pending (initial load in progress).
                    if self.check_data_pending() {
                        // Data loading is pending, show a loading spinner in the center of the panel.
                        ui.centered_and_justified(|ui| {
                            ui.spinner(); // Show a loading spinner.
                        });
                    } else {
                        // No data, no loading. Display prompt message.
                        ui.centered_and_justified(|ui| {
                            ui.label("Drag and drop CSV or Parquet file here.");
                        });
                    }
                }
            }
        });
    }
}
