use polars::prelude::*;

#[derive(Debug)]
pub enum DeltaError {
    IOError(std::io::Error),
    JsonError(serde_json::Error),
    PolarsError(PolarsError),
    InvalidType,
    InvalidTable,
    TableAlreadyExists,
}

impl From<std::io::Error> for DeltaError {
    fn from(value: std::io::Error) -> DeltaError {
        DeltaError::IOError(value)
    }
}

impl From<serde_json::Error> for DeltaError {
    fn from(value: serde_json::Error) -> DeltaError {
        DeltaError::JsonError(value)
    }
}

impl From<PolarsError> for DeltaError {
    fn from(value: PolarsError) -> Self {
        DeltaError::PolarsError(value)
    }
}
