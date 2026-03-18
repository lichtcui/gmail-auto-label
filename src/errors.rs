use std::fmt::{Display, Formatter};

#[derive(Debug)]
pub(crate) enum AppError {
    RateLimit(String),
    Config(String),
    Command(String),
    Parse(String),
    Other(String),
}

impl Display for AppError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            AppError::RateLimit(msg)
            | AppError::Config(msg)
            | AppError::Command(msg)
            | AppError::Parse(msg)
            | AppError::Other(msg) => write!(f, "{}", msg),
        }
    }
}

impl std::error::Error for AppError {}

impl From<anyhow::Error> for AppError {
    fn from(value: anyhow::Error) -> Self {
        AppError::Other(value.to_string())
    }
}
