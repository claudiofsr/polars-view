use crate::{
    DataFilters, DataFormat, DataFrameContainer, Error, FileMetadata, MyStyle, Notification,
    PolarsViewError, PolarsViewResult, Settings, open_file, save, save_as,
};

use egui::{
    CentralPanel, Color32, Context, FontId, Frame, Grid, Key, KeyboardShortcut, Layout, Modifiers,
    RichText, ScrollArea, SidePanel, Stroke, TopBottomPanel, ViewportCommand, menu, style::Visuals,
};
use std::{future::Future, sync::Arc}; // Added `Future` import for clarity in DataFuture.
use tokio::sync::oneshot::{self, Receiver, error::TryRecvError};
use tracing::error;

// --- Type Aliases ---

/// Type alias for a `Result` specifically wrapping a `DataFrameContainer` on success.
/// Simplifies function signatures involving potential data loading/processing errors.
pub type ContainerResult = PolarsViewResult<DataFrameContainer>;

/// Type alias for a boxed, dynamically dispatched Future that yields a `ContainerResult`.
/// This allows storing and managing different asynchronous operations (load, sort, format)
/// that all eventually produce a `DataFrameContainer` or an error.
/// - `dyn Future`: Dynamic dispatch for different future types.
/// - `Output = ContainerResult`: The future resolves to our specific result type.
/// - `+ Unpin`: Required for `async`/`await` usage in certain contexts.
/// - `+ Send + 'static`: Necessary bounds for futures used across threads (like with `tokio::spawn`).
pub type DataFuture = Box<dyn Future<Output = ContainerResult> + Unpin + Send + 'static>;

// --- Constants ---

// Define keyboard shortcuts for common actions using `egui`'s `KeyboardShortcut`.
const CTRL_O: KeyboardShortcut = KeyboardShortcut::new(Modifiers::CTRL, Key::O); // Ctrl+O for Open File
const CTRL_S: KeyboardShortcut = KeyboardShortcut::new(Modifiers::CTRL, Key::S); // Ctrl+S for Save File
const CTRL_A: KeyboardShortcut = KeyboardShortcut::new(Modifiers::CTRL, Key::A); // Ctrl+A for Save As...

// --- Main Application Struct ---

/// The main application struct for PolarsView, holding the entire application state.
pub struct PolarsViewApp {
    /// Holds the currently loaded data (`DataFrameContainer`).
    /// - `Option`: Allows representing the state where no data is loaded yet.
    /// - `Arc`: Enables cheap cloning and sharing of the (potentially large) `DataFrameContainer`
    ///   between the UI thread and background tasks without deep copies.
    pub data_container: Option<Arc<DataFrameContainer>>,

    /// Stores the state of the filters *as they were last applied or loaded*.
    /// Used primarily in the side panel UI (`render_query`) to detect changes made by the user.
    /// When the current UI state differs from `applied_filters`, it signifies that the user
    /// has made modifications, triggering a data reload/requery.
    pub applied_filters: DataFilters,

    /// Stores the state of the data formatting *as it was last applied*.
    /// Similar to `applied_filters`, used in `render_format` to detect user changes
    /// (e.g., toggling `expand_cols`, changing decimal places) and trigger an update.
    pub applied_format: DataFormat,

    /// Metadata extracted from the loaded file (e.g., row count, column count, schema).
    /// `Option` because it's only available when data is loaded.
    pub metadata: Option<FileMetadata>,

    /// Optional Notification window for displaying errors or settings dialogs.
    /// - `Option`: A notification might not always be active.
    /// - `Box<dyn Notification>`: Allows displaying different notification types
    ///   polymorphically using dynamic dispatch (see `traits.rs`).
    pub notification: Option<Box<dyn Notification>>,

    /// Tokio runtime instance for managing all asynchronous operations (file I/O, sorting, etc.).
    /// Using a multi-threaded runtime allows concurrent task execution for better performance.
    runtime: tokio::runtime::Runtime,

    /// The receiving end of a `tokio::sync::oneshot` channel.
    /// Used to receive the `ContainerResult` from a completed asynchronous `DataFuture` task
    /// back on the main UI thread.
    /// `Option` because a data operation might not be in progress.
    pipe: Option<Receiver<PolarsViewResult<DataFrameContainer>>>,

    /// Vector to keep track of active `tokio` task handles (`JoinHandle`).
    /// Used primarily to prevent the application from potentially hanging if tasks are
    /// spawned but never awaited or properly managed (though direct joining isn't typical here).
    /// Can be used for cleanup or cancellation if needed in the future.
    tasks: Vec<tokio::task::JoinHandle<()>>,
}

impl Default for PolarsViewApp {
    /// Creates a default `PolarsViewApp` instance. Initializes the runtime and sets initial state.
    fn default() -> Self {
        Self {
            data_container: None,                    // No data loaded initially.
            applied_filters: DataFilters::default(), // Start with default filter settings.
            applied_format: DataFormat::default(),   // Start with default format settings.
            metadata: None,                          // No metadata initially.
            notification: None,                      // No notification initially.
            runtime: tokio::runtime::Builder::new_multi_thread() // Essential: Use a multi-threaded runtime.
                .enable_all() // Enable necessary Tokio features (I/O, time, etc.).
                .build()
                .expect("Failed to build Tokio runtime"), // Runtime creation is critical.
            pipe: None,                              // No async operation pending at start.
            tasks: Vec::new(),                       // No tasks running at start.
        }
    }
}

impl PolarsViewApp {
    /// Creates a new `PolarsViewApp` instance.
    /// Sets the initial UI style (theme).
    pub fn new(cc: &eframe::CreationContext<'_>) -> PolarsViewResult<Self> {
        // Apply custom styles and dark theme (defined via `MyStyle` trait in `traits.rs`).
        cc.egui_ctx.set_style_init(Visuals::dark());

        cc.egui_ctx.memory_mut(|mem| {
            mem.data.clear();
        });

        Ok(Default::default()) // Return a new app with default settings.
    }

    /// Creates a new `PolarsViewApp` and immediately starts loading data using a provided `DataFuture`.
    /// Useful for loading data specified via command-line arguments on startup.
    /// `future`: The asynchronous operation (e.g., `DataFrameContainer::load_data`) to run.
    pub fn new_with_future(
        cc: &eframe::CreationContext<'_>,
        future: DataFuture,
    ) -> PolarsViewResult<Self> {
        cc.egui_ctx.set_style_init(Visuals::dark()); // Apply style.

        cc.egui_ctx.memory_mut(|mem| {
            mem.data.clear();
        });

        let mut app: Self = Default::default(); // Create default app instance.
        // Initiate the asynchronous data loading process.
        app.run_data_future(future, &cc.egui_ctx);
        Ok(app) // Return the app (data loading will happen in the background).
    }

    /// Checks if a `Notification` is active and renders it using `egui::Window`.
    /// Removes the notification if its `show` method returns `false` (indicating it was closed).
    fn check_notification(&mut self, ctx: &Context) {
        if let Some(notification) = &mut self.notification {
            if !notification.show(ctx) {
                // If `show` returns false (window closed by user or logic),
                // clear the notification state.
                self.notification = None;
            }
        }
    }

    /// Checks the `oneshot` channel (`pipe`) for the result of a pending async data operation.
    /// This function is called repeatedly in the `update` loop.
    ///
    /// Returns:
    /// - `true`: If an operation is still pending (channel was empty).
    /// - `false`: If a result was received (success or error) or the channel was closed.
    fn check_data_pending(&mut self) -> bool {
        // Take the receiver out of the Option to check it.
        let Some(mut output) = self.pipe.take() else {
            return false; // No receiver means no operation is pending.
        };

        // Try to receive the result without blocking.
        match output.try_recv() {
            // --- Result Received ---
            Ok(data_result) => {
                match data_result {
                    // --- Async Operation Succeeded ---
                    Ok(container) => {
                        // A new `DataFrameContainer` was successfully produced.
                        // Update the application state:

                        // 1. Update `applied_filters` to match the filters *used* in the new container.
                        //    This ensures the UI reflects the state of the currently displayed data.
                        self.applied_filters = container.filters.as_ref().clone();

                        // 2. Update `applied_format` similarly. Crucial for changes like `expand_cols`.
                        self.applied_format = container.format.as_ref().clone();

                        // 3. Regenerate metadata based on the new container.
                        self.metadata = FileMetadata::from_container(&container);

                        // 4. Store the new `DataFrameContainer`, wrapped in `Arc`.
                        self.data_container = Some(Arc::new(container));

                        false // Indicate loading/update is complete.
                    }
                    // --- Async Operation Failed ---
                    Err(err) => {
                        // The async task returned an error.
                        let error_message = err.to_string();
                        // Display the error in a notification window.
                        self.notification = Some(Box::new(Error {
                            message: error_message,
                        }));
                        error!("Async data operation failed: {}", err); // Log the error.

                        false // Indicate loading/update is complete (though failed).
                    }
                }
            }
            // --- No Result Yet or Channel Closed ---
            Err(try_recv_error) => match try_recv_error {
                // --- Channel Empty (Operation Still Running) ---
                TryRecvError::Empty => {
                    // The async task hasn't finished yet.
                    // Put the receiver back into the `Option` so we can check again next frame.
                    self.pipe = Some(output);
                    true // Indicate operation is still pending.
                }
                // --- Channel Closed (Sender Dropped Prematurely) ---
                TryRecvError::Closed => {
                    // This indicates an issue, likely the sending task panicked or exited unexpectedly.
                    let err_msg = "Async data operation channel closed unexpectedly.".to_string();
                    self.notification = Some(Box::new(Error {
                        message: err_msg.clone(),
                    }));
                    error!("{}", err_msg); // Log the error.

                    false // Operation is effectively complete (failed unexpectedly).
                }
            },
        }
    }

    /// Spawns a `DataFuture` onto the shared `tokio` runtime.
    /// Sets up the `oneshot` channel to receive the result.
    /// `future`: The async operation (boxed Future) to execute.
    /// `ctx`: The `egui::Context` used to request repaints from the background task.
    fn run_data_future(&mut self, future: DataFuture, ctx: &Context) {
        // Basic cleanup: remove completed task handles (optional but good practice).
        self.tasks.retain(|task| !task.is_finished());

        // Create the single-use channel for sending the result back to the UI thread.
        let (tx, rx) = oneshot::channel::<PolarsViewResult<DataFrameContainer>>();
        // Store the receiving end in `self.pipe` so `check_data_pending` can poll it.
        self.pipe = Some(rx);

        // Clone the egui context so the background task can request UI repaints.
        let ctx_clone = ctx.clone();

        // Spawn the future onto the application's Tokio runtime.
        // The task runs in the background, managed by the runtime's thread pool.
        let handle = self.runtime.spawn(async move {
            // Await the completion of the provided async operation.
            let data = future.await;

            // Send the result (Ok or Err) back through the oneshot channel.
            // Ignore the result of `send`; if it fails, the receiver (`pipe`) was dropped,
            // which `check_data_pending` handles anyway.
            if tx.send(data).is_err() {
                // Log if the receiver was dropped before sending - indicates UI might have closed/restarted.
                error!("Receiver dropped before data could be sent from async task.");
            }

            // Request a repaint of the UI thread. This is crucial to ensure the UI
            // updates immediately after the async operation completes, especially
            // if `check_data_pending` doesn't run in the *exact* same frame.
            ctx_clone.request_repaint();
        });

        // Store the task handle (optional, mainly for potential future management).
        self.tasks.push(handle);
    }

    /// Handles the "Open File" action (triggered by menu or Ctrl+O).
    fn handle_open_file(&mut self, ctx: &Context) {
        // Use the runtime to block on the async file dialog function.
        // This is acceptable here as it's a direct user action expecting a pause.
        match self.runtime.block_on(open_file()) {
            Ok(path) => {
                // If a path was successfully selected:
                // Update the path in the filters state. Show error if it fails.
                if let Err(error) = self.applied_filters.set_path(&path) {
                    self.notification = Some(Box::new(Error {
                        message: error.to_string(),
                    }));
                } else {
                    // If path is valid, create and run the async loading future.
                    let future = DataFrameContainer::load_data(
                        self.applied_filters.clone(),
                        self.applied_format.clone(),
                    );
                    self.run_data_future(Box::new(Box::pin(future)), ctx);
                }
            }
            Err(PolarsViewError::FileNotFound(_)) => {
                // User cancelled the dialog, do nothing. Log potentially?
                tracing::debug!("File open dialog cancelled by user.");
            }
            Err(e) => {
                // Other error opening the dialog itself.
                self.notification = Some(Box::new(Error {
                    message: e.to_string(),
                }));
            }
        }
    }

    /// Handles the "Save" action (Ctrl+S). Saves to the *original* file path.
    fn handle_save_file(&mut self, ctx: &Context) {
        // Only proceed if data is loaded.
        if let Some(container) = &self.data_container {
            // Clone the Arc (cheap) to pass to the async task.
            let container_clone = container.clone();
            // Clone context for repaint request within the task.
            let ctx_clone = ctx.clone();
            // Spawn the save operation onto the runtime to avoid blocking the UI.
            self.runtime.spawn(async move {
                if let Err(err) = save(container_clone, ctx_clone).await {
                    // Log error if saving fails. Error notification could also be added here
                    // via a channel back to the main thread if more user feedback is desired.
                    error!("Failed to save file: {}", err);
                }
                // Note: `save` itself now requests repaint upon completion/error.
            });
        }
    }

    /// Handles the "Save As..." action (Ctrl+A). Prompts user for a new location/format.
    fn handle_save_as(&mut self, ctx: &Context) {
        // Only proceed if data is loaded.
        if let Some(container) = &self.data_container {
            // Clone Arc and context for the async task.
            let container_clone = container.clone();
            let ctx_clone = ctx.clone();
            // Spawn the save_as operation onto the runtime.
            self.runtime.spawn(async move {
                if let Err(err) = save_as(container_clone, ctx_clone).await {
                    // Log error if saving fails. Similar notification strategy as `handle_save_file` applies.
                    error!("Failed to save file using 'Save As': {}", err);
                }
                // Note: `save_as` itself now requests repaint upon completion/error.
            });
        }
    }

    /// Renders the top menu bar (`TopBottomPanel`).
    fn render_menu_bar(&mut self, ctx: &Context) {
        TopBottomPanel::top("top_panel").show(ctx, |ui| {
            // Use egui's built-in menu bar layout.
            menu::bar(ui, |ui| {
                // Arrange menu buttons horizontally.
                ui.horizontal(|ui| {
                    self.render_file_menu(ui); // "File" menu
                    self.render_help_menu(ui); // "Help" menu
                    // Add space and theme switch aligned to the right.
                    ui.with_layout(Layout::right_to_left(egui::Align::Center), |ui| {
                        self.render_theme(ui);
                    });
                });
            });
        });
    }

    /// Renders the "File" menu contents.
    fn render_file_menu(&mut self, ui: &mut egui::Ui) {
        ui.menu_button("File", |ui| {
            // --- File Operations ---
            // Use a Grid for alignment of buttons and shortcuts.
            Grid::new("file_ops_grid")
                .num_columns(2)
                .spacing([20.0, 10.0]) // spacing [horizontal, vertical]
                .show(ui, |ui| {
                    // "Open File..." button
                    if ui.button("Open File...").clicked() {
                        self.handle_open_file(ui.ctx()); // Trigger the action.
                        ui.close_menu(); // Close the menu after clicking.
                    }
                    ui.label("Ctrl + O");
                    ui.end_row();

                    // "Save" button (enabled only if data is loaded)
                    let save_enabled = self.data_container.is_some();
                    if ui
                        .add_enabled(save_enabled, egui::Button::new("Save"))
                        .clicked()
                    {
                        self.handle_save_file(ui.ctx());
                        ui.close_menu();
                    }
                    ui.label("Ctrl + S");
                    ui.end_row();

                    // "Save As..." button (enabled only if data is loaded)
                    let save_as_enabled = self.data_container.is_some();
                    if ui
                        .add_enabled(save_as_enabled, egui::Button::new("Save As..."))
                        .clicked()
                    {
                        self.handle_save_as(ui.ctx());
                        ui.close_menu();
                    }
                    ui.label("Ctrl + A");
                    ui.end_row();
                });

            ui.separator(); // Visual separator.

            // --- Application Settings & Exit ---
            Grid::new("app_ops_grid")
                .num_columns(2) // Simplified for fewer items.
                .spacing([20.0, 10.0])
                .show(ui, |ui| {
                    // "Settings" button (Placeholder - shows a basic notification)
                    if ui.button("Settings").clicked() {
                        self.notification = Some(Box::new(Settings {})); // Show placeholder.
                        ui.close_menu();
                    }
                    ui.label(""); // Placeholder for alignment.
                    ui.end_row();

                    // "Exit" button
                    if ui.button("Exit").clicked() {
                        // Send command to close the application window.
                        ui.ctx().send_viewport_cmd(ViewportCommand::Close);
                    }
                    ui.label("");
                    ui.end_row();
                });
        });
    }

    /// Renders the "Help" menu contents.
    fn render_help_menu(&mut self, ui: &mut egui::Ui) {
        ui.menu_button("Help", |ui| {
            // Link to documentation.
            let url = "https://docs.rs/polars-view";
            ui.hyperlink_to("Documentation", url).on_hover_text(url);

            ui.separator();

            // "About" submenu.
            ui.menu_button("About", |ui| {
                // Display application info within a styled Frame.
                Frame::default()
                    .stroke(Stroke::new(1.0, Color32::GRAY))
                    .outer_margin(2.0)
                    .inner_margin(10.0)
                    .show(ui, |ui| {
                        // Retrieve package info from Cargo environment variables (set at build time).
                        let version = env!("CARGO_PKG_VERSION");
                        let authors = env!("CARGO_PKG_AUTHORS");
                        let description = env!("CARGO_PKG_DESCRIPTION");
                        let name = env!("CARGO_PKG_NAME"); // Use package name for title

                        // Use a Grid for structured layout.
                        Grid::new("about_grid")
                            .num_columns(1) // Single column layout.
                            .spacing([10.0, 8.0]) // Tighter spacing.
                            .show(ui, |ui| {
                                ui.set_min_width(400.0); // Enforce minimum width.

                                // Centered Title
                                ui.with_layout(Layout::top_down(egui::Align::Center), |ui| {
                                    ui.label(
                                        RichText::new(name)
                                            .font(FontId::proportional(28.0))
                                            .strong(),
                                    );
                                    ui.label(
                                        RichText::new(description).font(FontId::proportional(20.0)),
                                    );
                                    ui.label("");
                                    ui.label(format!("Version: {version}"));
                                    ui.label(format!("Author: {authors}"));
                                });
                                ui.end_row();

                                ui.separator();
                                ui.end_row();

                                // Links - Use horizontal layouts for label + link.
                                ui.horizontal(|ui| {
                                    ui.label("Powered by");
                                    let url = "https://github.com/pola-rs/polars";
                                    ui.hyperlink_to("Polars", url).on_hover_text(url);
                                });
                                ui.end_row();

                                ui.horizontal(|ui| {
                                    ui.label("Built with");
                                    let url = "https://github.com/emilk/egui";
                                    ui.hyperlink_to("egui", url).on_hover_text(url);
                                    ui.label("&");
                                    let url_eframe =
                                        "https://github.com/emilk/egui/tree/master/crates/eframe";
                                    ui.hyperlink_to("eframe", url_eframe)
                                        .on_hover_text(url_eframe);
                                });
                                ui.end_row();

                                ui.horizontal(|ui| {
                                    ui.label("Inspired by");
                                    let url_parq = "https://github.com/Kxnr/parqbench";
                                    ui.hyperlink_to("parqbench", url_parq)
                                        .on_hover_text(url_parq);
                                });
                                ui.end_row();
                            });
                    });
            });
        });
    }

    /// Renders the Light/Dark theme selection radio buttons.
    fn render_theme(&mut self, ui: &mut egui::Ui) {
        // Determine current theme.
        let mut dark_mode = ui.ctx().style().visuals.dark_mode; // Get boolean dark_mode state.

        // Use radio buttons to select the theme.
        // The `radio_value` function updates the `dark_mode` variable directly if clicked.
        let dark_changed = ui
            .radio_value(&mut dark_mode, true, "🌙")
            .on_hover_text("Dark Theme")
            .changed();

        let light_changed = ui
            .radio_value(&mut dark_mode, false, "🔆")
            .on_hover_text("Light Theme")
            .changed();

        // If the theme selection changed, apply the new visuals.
        if dark_changed {
            ui.ctx().set_style_init(Visuals::dark()); // Switch to dark theme.
        }

        if light_changed {
            ui.ctx().set_style_init(Visuals::light()); // Switch to light theme.
        }
    }

    /// Renders the left side panel (`SidePanel`) containing collapsible sections
    /// for Metadata, Format, Query, and Schema.
    fn render_side_panel(&mut self, ctx: &Context) {
        SidePanel::left("side_panel")
            .resizable(true) // Allow user resizing.
            .default_width(300.0) // Sensible default width.
            .show(ctx, |ui| {
                // Use a ScrollArea in case content exceeds panel height.
                ScrollArea::vertical().show(ui, |ui| {
                    // --- Metadata Section ---
                    // Only show if metadata is available.
                    if let Some(metadata) = &self.metadata {
                        ui.collapsing("Metadata", |ui| {
                            metadata.render_metadata(ui); // Delegate rendering to FileMetadata.
                        });
                    }

                    // --- Format Section ---
                    ui.collapsing("Format", |ui| {
                        // Render the format UI. `render_format` takes a mutable reference
                        // to `self.applied_format` and updates it directly based on UI interaction.
                        // It returns `Some(new_format)` *only if* a change was detected
                        // compared to its state *before* rendering the widgets.
                        if let Some(new_format) = self.applied_format.render_format(ui) {
                            // A format setting changed (e.g., checkbox toggled, decimal changed).
                            // Only proceed if data is actually loaded.
                            if let Some(data_container) = &self.data_container {
                                tracing::debug!(
                                    "render_side_panel: Format change detected, triggering update_format with: {:#?}",
                                    new_format
                                );

                                // Create the async future to apply the format change.
                                // `update_format` is lightweight, just creating a new container state.
                                let future = DataFrameContainer::update_format(
                                    Arc::clone(data_container), // Pass current container Arc.
                                    Arc::new(new_format),     // Pass the new format state detected by render_format.
                                );

                                // Schedule the async update using the standard mechanism.
                                self.run_data_future(Box::new(Box::pin(future)), ctx);
                            }
                         }
                    });

                    // --- Query Section ---
                    ui.collapsing("Query", |ui| {
                        // Render the query/filter UI. Similar to format, `render_query`
                        // modifies `self.applied_filters` and returns `Some(new_filters)`
                        // if changes were detected (e.g., SQL edited, delimiter changed, checkbox toggled).
                        if let Some(new_filters) = self.applied_filters.render_query(ui) {
                             tracing::debug!(
                                "render_side_panel: Filter change detected, triggering load_data with: {:#?}",
                                new_filters
                            );
                            // Filter changes require reloading/requerying the data.
                            // Create the async future for `load_data` using the new filters
                            // and the *current* format settings.
                            let future = DataFrameContainer::load_data(
                                new_filters,              // The changed filters state.
                                self.applied_format.clone(), // Keep the current format.
                            );
                            // Schedule the async reload/requery.
                            self.run_data_future(Box::new(Box::pin(future)), ctx);
                        }
                    });

                    // --- Schema Section ---
                    // Only show if metadata (which contains schema) is available.
                    if let Some(metadata) = &self.metadata {
                        ui.collapsing("Schema", |ui| {
                             metadata.render_schema(ui); // Delegate rendering to FileMetadata.
                        });
                    }
                }); // End ScrollArea
            }); // End SidePanel
    }

    /// Renders the bottom status bar (`TopBottomPanel`).
    fn render_bottom_panel(&mut self, ctx: &Context) {
        TopBottomPanel::bottom("bottom_panel").show(ctx, |ui| {
            ui.horizontal(|ui| {
                // Display the file path if data is loaded, otherwise show a default message.
                match &self.data_container {
                    Some(container) => {
                        // Display path losslessly if possible, otherwise debug representation.
                        ui.label(container.filters.absolute_path.to_string_lossy());
                    }
                    None => {
                        ui.label("No file loaded.");
                    }
                }
                // Optional: Could add more status info here (e.g., loading state).
            });
        });
    }

    /// Renders the central panel (`CentralPanel`), primarily for the data table.
    /// NOTE: `CentralPanel` must be the last panel added in the `eframe::App::update` method.
    fn render_central_panel(&mut self, ctx: &Context) {
        CentralPanel::default().show(ctx, |ui| {
            // Show a warning overlay if running a debug build (egui helper).
            egui::warn_if_debug_build(ui);

            // Check if an async operation is pending.
            let is_pending = self.check_data_pending();

            // Disable the central panel's UI elements while loading/processing.
            ui.add_enabled_ui(!is_pending, |ui| {
                match &self.data_container {
                    // --- Data is Loaded ---
                    Some(container) => {
                        // Variable to capture potential filter changes from the table (sorting).
                        let mut opt_sort_filters = None;

                        // Use a horizontal ScrollArea for potentially wide tables.
                        ScrollArea::horizontal()
                            .auto_shrink([false, false]) // Prevent shrinking axes.
                            .show(ui, |ui| {
                                // Delegate table rendering to the DataFrameContainer.
                                // `render_table` returns `Some(DataFilters)` if a sort action occurred.
                                opt_sort_filters = container.render_table(ui);
                            }); // ScrollArea scope ends here.

                        // If `render_table` indicated a sort change:
                        if let Some(filters_with_new_sort) = opt_sort_filters {
                            // Create the async future for the sort operation.
                            let future = DataFrameContainer::sort(
                                Arc::clone(container),       // Pass current container Arc.
                                filters_with_new_sort, // Pass filters containing the new sort state.
                            );
                            // Schedule the async sort.
                            self.run_data_future(Box::new(Box::pin(future)), ctx);
                        }
                    }
                    // --- No Data Loaded ---
                    None => {
                        // Center content within the panel.
                        ui.centered_and_justified(|ui| {
                            if is_pending {
                                // If no data is present *but* an operation is pending, show a spinner.
                                ui.spinner();
                            } else {
                                // If no data and nothing is pending, show a help message.
                                ui.label("Open a file (File > Open or Ctrl+O) or drag & drop CSV, JSON, or Parquet files here.");
                            }
                        });
                    }
                }
            }); // End of `add_enabled_ui`. If pending, UI above was disabled.
        }); // End CentralPanel
    }
}

// --- eframe::App Implementation ---

impl eframe::App for PolarsViewApp {
    /// The main update function called by `eframe` on each frame (the "render loop").
    /// Responsible for handling events, updating state, and drawing the UI.
    fn update(&mut self, ctx: &Context, _frame: &mut eframe::Frame) {
        // 1. Check and display any active Notification windows (errors, etc.).
        self.check_notification(ctx);

        // 2. Handle file drag-and-drop events.
        // Check if any files were dropped onto the window in this frame.
        if let Some(dropped_file) = ctx.input(|i| i.raw.dropped_files.last().cloned()) {
            if let Some(path) = &dropped_file.path {
                // Try to set the path in the filters state.
                if let Err(error) = self.applied_filters.set_path(path) {
                    // If setting the path fails (e.g., non-existent), show an error.
                    self.notification = Some(Box::new(Error {
                        message: error.to_string(),
                    }));
                } else {
                    // If path is valid, create a future to load the data.
                    let future = DataFrameContainer::load_data(
                        self.applied_filters.clone(), // Use current filters state.
                        self.applied_format.clone(),  // Use current format state.
                    );
                    // Run the loading task asynchronously.
                    self.run_data_future(Box::new(Box::pin(future)), ctx);
                }
            }
        }

        // 3. Handle global keyboard shortcuts *before* drawing UI elements that might consume input.
        ctx.input_mut(|i| {
            if i.consume_shortcut(&CTRL_O) {
                // Open File
                self.handle_open_file(ctx);
            }
            if i.consume_shortcut(&CTRL_S) {
                // Save File
                self.handle_save_file(ctx);
            }
            if i.consume_shortcut(&CTRL_A) {
                // Save As...
                self.handle_save_as(ctx);
            }
        });

        // 4. Define the main UI layout using `egui` panels.
        // The order matters: Top/Bottom/Left/Right panels are defined *before* the CentralPanel.

        // 4a. Top panel for the menu bar.
        self.render_menu_bar(ctx);

        // 4b. Left side panel for configuration (Filters, Format) and info (Metadata, Schema).
        self.render_side_panel(ctx);

        // 4c. Bottom panel for displaying status info (e.g., loaded file path).
        self.render_bottom_panel(ctx);

        // 4d. Central panel: The main content area, primarily for the data table.
        // Must be added *last*.
        self.render_central_panel(ctx);
    }
}
