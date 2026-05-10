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
mod llm;
mod models;
mod sync;
mod utils;

use serde::Serialize;

use crate::errors::AppError;
use crate::models::{Args, OutputFormat};

#[derive(Debug, Serialize)]
struct AppRunErrorOutput {
    ok: bool,
    code: String,
    message: String,
}

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
    let args: Args = clap::Parser::parse();
    let output_format = args.output;
    match app::run_with_args(args) {
        Ok(summary) => {
            if output_format == OutputFormat::Json {
                match serde_json::to_string(&summary) {
                    Ok(payload) => println!("{payload}"),
                    Err(err) => {
                        eprintln!("Failed to serialize JSON output: {err}");
                        std::process::exit(1);
                    }
                }
            }
        }
        Err(err) => {
            if output_format == OutputFormat::Json {
                let output = if let Some(app_err) = err.downcast_ref::<AppError>() {
                    AppRunErrorOutput {
                        ok: false,
                        code: app_err.code().to_string(),
                        message: app_err.to_string(),
                    }
                } else {
                    AppRunErrorOutput {
                        ok: false,
                        code: "internal_error".to_string(),
                        message: err.to_string(),
                    }
                };
                match serde_json::to_string(&output) {
                    Ok(payload) => println!("{payload}"),
                    Err(serr) => eprintln!("Failed to serialize JSON error output: {serr}"),
                }
            } else {
                eprintln!("{err}");
            }
            std::process::exit(1);
        }
    }
}
