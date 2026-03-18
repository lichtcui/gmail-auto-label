mod app;
mod cache;
mod classify;
mod command;
mod errors;
mod gog;
mod models;
mod utils;

pub fn main_entry() {
    if let Err(e) = app::run() {
        eprintln!("{e}");
        std::process::exit(1);
    }
}
