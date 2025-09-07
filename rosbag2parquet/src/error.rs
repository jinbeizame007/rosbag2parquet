use thiserror::Error;

#[non_exhaustive]
#[derive(Debug, Error)]
pub enum Rosbag2ParquetError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    #[error("Parquet error: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),

    #[error("UTF-8 conversion error: {0}")]
    Utf8(#[from] std::str::Utf8Error),

    #[error("Parse error in topic '{topic}' at message {index}: {message}")]
    ParseError { topic: String, index: usize, message: String },

    #[error("Schema error for type '{type_name}': {message}")]
    SchemaError { type_name: String, message: String },

    #[error("Topic '{topic}' not found")]
    TopicNotFound { topic: String },

    #[error("Type '{type_name}' not found for topic '{topic}'")]
    TypeNotFound { type_name: String, topic: String },

    #[error("Invalid configuration: {message}")]
    ConfigError { message: String },
}

pub type Result<T> = std::result::Result<T, Rosbag2ParquetError>;

impl From<anyhow::Error> for Rosbag2ParquetError {
    fn from(err: anyhow::Error) -> Self {
        Rosbag2ParquetError::ConfigError {
            message: err.to_string(),
        }
    }
}
