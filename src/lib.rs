//! `gmail-auto-label` library entrypoint.
//!
//! This crate powers the `gmail-auto-label` CLI workflow.
//! Most users run it through the binary, while integrators can
//! invoke [`main_entry`] directly.

mod app;
mod cache;
mod classify;
mod command;
mod errors;
mod gog;
mod models;
mod utils;

/// Runs the full CLI application flow.
///
/// This function parses CLI args and executes the end-to-end process.
///
/// # Examples
///
/// ```no_run
/// gmail_auto_label::main_entry();
/// ```
pub fn main_entry() {
    if let Err(e) = app::run() {
        eprintln!("{e}");
        std::process::exit(1);
    }
}
