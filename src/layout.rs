use crate::{
    DataContainer, DataFilter, DataFormat, Error, FileInfo, MyStyle, Notification, PolarsViewError,
    PolarsViewResult, Settings, SortBy, open_file, save, save_as,
};

use egui::{
    CentralPanel, Color32, Context, FontId, Frame, Grid, Key, KeyboardShortcut, Layout, Modifiers,
    RichText, ScrollArea, SidePanel, Stroke, TopBottomPanel, ViewportCommand, menu, style::Visuals,
};
use std::{future::Future, sync::Arc};
use tokio::sync::oneshot::{self, Receiver, error::TryRecvError};
use tracing::error;

// --- Type Aliases ---

/// Type alias for a `Result` specifically wrapping a `DataContainer` on success.
/// Simplifies function signatures involving potential data loading/processing errors.
pub type ContainerResult = PolarsViewResult<DataContainer>;

/// Type alias for a boxed, dynamically dispatched Future that yields a `ContainerResult`.
/// This allows storing and managing different asynchronous operations (load, sort, format)
/// that all eventually produce a `DataContainer` or an error.
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

/// The main application struct for PolarsView, holding the entire UI and async state.
pub struct PolarsViewApp {
    /// Holds the currently loaded data and its associated view state (`DataContainer`).
    /// `None` if no data is loaded. `Arc` allows efficient sharing with async tasks.
    pub data_container: Option<Arc<DataContainer>>,

    /// Stores the state of the data loading/filtering parameters *last applied*.
    /// Used by the side panel UI (`render_query`) to detect changes made by the user
    /// to settings like SQL query, delimiter, etc. **Does not contain sort info.**
    pub applied_filter: DataFilter,

    /// Stores the state of the data formatting settings *last applied*.
    /// Used by the side panel UI (`render_format`) to detect changes to display settings.
    pub applied_format: DataFormat,

    /// Info extracted from the currently loaded file.
    pub file_info: Option<FileInfo>,

    /// Optional Notification window for displaying errors or settings dialogs.
    pub notification: Option<Box<dyn Notification>>,

    /// Tokio runtime instance for managing asynchronous operations.
    runtime: tokio::runtime::Runtime,

    /// Receiving end of a `tokio::sync::oneshot` channel used to get results
    /// back from async `DataFuture` tasks onto the UI thread.
    pipe: Option<Receiver<ContainerResult>>,

    /// Vector to keep track of active `tokio` task handles. (Mainly for potential future management)
    tasks: Vec<tokio::task::JoinHandle<()>>,
}

impl Default for PolarsViewApp {
    /// Creates a default `PolarsViewApp` instance. Initializes the runtime and sets initial state.
    fn default() -> Self {
        Self {
            data_container: None,                  // No data loaded initially.
            applied_filter: DataFilter::default(), // Start with default filter settings.
            applied_format: DataFormat::default(), // Start with default format settings.
            file_info: None,                       // No file_info initially.
            notification: None,                    // No notification initially.
            runtime: tokio::runtime::Builder::new_multi_thread() // Essential: Use a multi-threaded runtime.
                .enable_all() // Enable necessary Tokio features (I/O, time, etc.).
                .build()
                .expect("Failed to build Tokio runtime"), // Runtime creation is critical.
            pipe: None,                            // No async operation pending at start.
            tasks: Vec::new(),                     // No tasks running at start.
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
    /// `future`: The asynchronous operation (e.g., `DataContainer::load_data`) to run.
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
                        // A new `DataContainer` was successfully produced.
                        // Update the application state:

                        // 1. Update `applied_filter` to match the filter *used* in the new container.
                        //    This ensures the UI reflects the state of the currently displayed data.
                        self.applied_filter = container.filter.as_ref().clone();

                        // 2. Update `applied_format` similarly. Crucial for changes like `expand_cols`.
                        self.applied_format = container.format.as_ref().clone();

                        // 3. Regenerate file_info based on the new container.
                        self.file_info = FileInfo::from_container(&container);

                        // 4. Store the new `DataContainer`, wrapped in `Arc`.
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
        let (tx, rx) = oneshot::channel::<PolarsViewResult<DataContainer>>();
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

    // --- Event Handlers ---

    /// Handles the "Open File" action (triggered by menu or Ctrl+O).
    fn handle_open_file(&mut self, ctx: &Context) {
        // Use the runtime to block on the async file dialog function.
        // This is acceptable here as it's a direct user action expecting a pause.
        match self.runtime.block_on(open_file()) {
            Ok(path) => {
                // If a path was successfully selected:
                // Update the path in the filter state.
                match self.applied_filter.set_path(&path) {
                    // Case 1: Path was successfully set and canonicalized.
                    Ok(()) => {
                        // Log the successful operation and the canonical path now stored.
                        tracing::debug!(
                            "Open File: Path set successfully: {:?}. Triggering load_data.",
                            self.applied_filter.absolute_path // Log the verified, absolute path
                        );

                        // Create the asynchronous future to load the data using the updated filter.
                        let future = DataContainer::load_data(
                            self.applied_filter.clone(), // Clone state needed for the async task
                            self.applied_format.clone(), // Clone format state as well
                        );

                        // Run the data loading task in the background.
                        // Box::pin is necessary for futures that might be !Unpin.
                        // Box::new creates the DataFuture trait object.
                        self.run_data_future(Box::new(Box::pin(future)), ctx);
                    }
                    // Case 2: Failed to set the path (e.g., path doesn't exist, canonicalization failed).
                    Err(error) => {
                        // Log the specific error encountered.
                        tracing::error!(
                            "Open File: Failed to set or canonicalize path {:?}: {}",
                            path, // Log the original path that caused the error
                            error
                        );

                        // Show an error notification to the user.
                        // Format a user-friendly message including the error and the problematic path.
                        self.notification = Some(Box::new(Error {
                            message: format!(
                                "Error opening file path:\n{}\n\nPath: {}",
                                error,
                                path.display() // Use .display() for a cleaner path representation in UI
                            ),
                        }));
                    }
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
                    // TODO: Notify user of save failure via channel?
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

    // --- UI Rendering Methods ---

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

    /// Renders the left side panel containing collapsible sections for configuration and info.
    fn render_side_panel(&mut self, ctx: &Context) {
        SidePanel::left("side_panel")
            .resizable(true)
            .default_width(300.0)
            .show(ctx, |ui| {
                // Use a ScrollArea in case content exceeds panel height.
                ScrollArea::vertical().show(ui, |ui| {
                    // --- Info Section ---
                    // Only show if file_info is available.
                    if let Some(file_info) = &self.file_info {
                        ui.collapsing("Info", |ui| {
                            file_info.render_metadata(ui); // Delegate rendering to FileInfo.
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
                                let future = DataContainer::update_format(
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
                        // modifies `self.applied_filter` and returns `Some(new_filter)`
                        // if changes were detected (e.g., SQL edited, delimiter changed, checkbox toggled).
                        if let Some(new_filter) = self.applied_filter.render_query(ui) {
                             tracing::debug!(
                                "render_side_panel: Filter change detected, triggering load_data with: {:#?}",
                                new_filter
                            );
                            // Filter changes require reloading/requerying the data.
                            // Create the async future for `load_data` using the new filter
                            // and the *current* format settings.
                            let future = DataContainer::load_data(
                                new_filter,              // The changed filter state.
                                self.applied_format.clone(), // Keep the current format.
                            );
                            // Schedule the async reload/requery.
                            self.run_data_future(Box::new(Box::pin(future)), ctx);
                        }
                    });

                    // --- Columns Section ---
                    // Only show if file_info (which contains schema) is available.
                    if let Some(file_info) = &self.file_info {
                        ui.collapsing("Columns", |ui| {
                             file_info.render_schema(ui); // Delegate rendering to FileInfo.
                        });
                    }
                });
            });
    }

    /// Renders the bottom status bar.
    fn render_bottom_panel(&mut self, ctx: &Context) {
        TopBottomPanel::bottom("bottom_panel").show(ctx, |ui| {
            ui.horizontal(|ui| {
                // Display file path and sort status if data is loaded
                if let Some(container) = &self.data_container {
                    // Use lossy conversion as fallback for non-UTF8 paths
                    ui.label(format!(
                        "File: {}",
                        container.filter.absolute_path.to_string_lossy()
                    ));
                    ui.separator();
                    // Show how many columns are involved in the sort
                    ui.label(format!("Sort: {} active criteria", container.sort.len()));
                } else {
                    ui.label("No file loaded."); // Default message
                }

                // Show spinner and text if an async operation is pending
                if self.pipe.is_some() {
                    ui.with_layout(Layout::right_to_left(egui::Align::Center), |ui| {
                        ui.spinner();
                        ui.label("Processing... "); // Indicate background work
                    });
                }
            });
        });
    }

    /// Renders the central panel, primarily displaying the data table.
    /// Handles triggering the `apply_sort` async operation based on header clicks.
    fn render_central_panel(&mut self, ctx: &Context) {
        CentralPanel::default().show(ctx, |ui| {
            egui::warn_if_debug_build(ui); // Debug build reminder overlay

            // Check async task status BEFORE rendering UI potentially disabled by it
            let is_pending = self.check_data_pending();

            // Disable central panel interaction while loading/sorting/etc.
            ui.add_enabled_ui(!is_pending, |ui| {
                match &self.data_container {
                    Some(container) => {
                        // Variable to capture the new sort criteria requested by header clicks
                        let mut opt_new_sort_criteria: Option<Vec<SortBy>> = None;

                        // Use horizontal scroll area for wide tables
                        ScrollArea::horizontal()
                            .id_salt("central_scroll") // Add ID for state persistence
                            .auto_shrink([false, false]) // Don't shrink if content fits
                            .show(ui, |ui| {
                                // `render_table` handles drawing and captures header interactions.
                                // Returns `Some(Vec<SortBy>)` if a click occurred
                                // that modified the sort order (add, remove, change direction).
                                opt_new_sort_criteria = container.render_table(ui);
                            });

                        // --- Handle Sort Action Triggered from Table Header ---
                        if let Some(new_criteria) = opt_new_sort_criteria {
                            // A header click signaled a request to change the sort state.
                            tracing::debug!(
                                "render_central_panel: Sort action requested by header click. Triggering apply_sort. New criteria: {:#?}",
                                new_criteria // Log the requested criteria
                            );

                            // Trigger the `apply_sort` async operation.
                            // This handles both applying new sorts and resetting (if new_criteria is empty).
                            let future = DataContainer::apply_sort(
                                Arc::clone(container), // Pass current container Arc
                                new_criteria,          // Pass the new requested sort criteria Vec
                            );
                            self.run_data_future(Box::new(Box::pin(future)), ctx);
                        }
                    }
                    None => { // No data container is loaded
                        ui.centered_and_justified(|ui| {
                            if is_pending {
                                ui.spinner(); // Show loading indicator if async task is running
                            } else {
                                // Show help message if idle and no data
                                ui.label("Open a file (File > Open or Ctrl+O) or drag & drop CSV, JSON, or Parquet files here.");
                            }
                        });
                    }
                }
            }); // End of add_enabled_ui block
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
        // Take the first dropped file only
        if let Some(dropped_file) = ctx.input(|i| i.raw.dropped_files.first().cloned()) {
            if let Some(path) = &dropped_file.path {
                // Attempt to set the path in the filter state, handling potential errors.
                match self.applied_filter.set_path(path) {
                    // Case 1: Path was successfully set and canonicalized.
                    Ok(()) => {
                        // Log the successful operation and the canonical path now stored.
                        tracing::debug!(
                            "Drag-n-drop: Path set successfully: {:?}. Triggering load_data.",
                            self.applied_filter.absolute_path // Log the verified, absolute path
                        );

                        // Create the asynchronous future to load the data using the updated filter.
                        let future = DataContainer::load_data(
                            self.applied_filter.clone(), // Clone state needed for the async task
                            self.applied_format.clone(), // Clone format state as well
                        );

                        // Run the data loading task in the background.
                        // Box::pin is necessary for futures that might be !Unpin.
                        // Box::new creates the DataFuture trait object.
                        self.run_data_future(Box::new(Box::pin(future)), ctx);
                    }
                    // Case 2: Failed to set the path (e.g., path doesn't exist, canonicalization failed).
                    Err(error) => {
                        // Log the specific error encountered.
                        tracing::error!(
                            "Drag-n-drop: Failed to set or canonicalize path {:?}: {}",
                            path, // Log the original path that caused the error
                            error
                        );

                        // Show an error notification to the user.
                        // Format a user-friendly message including the error and the problematic path.
                        self.notification = Some(Box::new(Error {
                            message: format!(
                                "Error processing dropped file path:\n{}\n\nPath: {}",
                                error,
                                path.display() // Use .display() for a cleaner path representation in UI
                            ),
                        }));
                    }
                }
            } else {
                // Optional: Log if a dropped item lacked a path.
                tracing::warn!(
                    "Drag-n-drop: Ignored dropped item without a path: {:?}",
                    dropped_file
                );
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

        // 4b. Left side panel for configuration (Filters, Format) and info (Info, Columns).
        self.render_side_panel(ctx);

        // 4c. Bottom panel for displaying status info (e.g., loaded file path).
        self.render_bottom_panel(ctx);

        // 4d. Central panel: The main content area, primarily for the data table.
        // Must be added *last*.
        self.render_central_panel(ctx);
    }
}
