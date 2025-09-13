use std::collections::HashSet;
use std::str::FromStr;

use crate::error::{Result, Rosbag2ParquetError};
use camino::Utf8PathBuf;
use mcap::Message;
use parquet::basic::Compression;

#[derive(Debug, Clone, Copy)]
pub struct CompressionSetting {
    kind: Compression,
    level: Option<i32>,
}

impl CompressionSetting {
    pub fn new(kind: Compression, level: Option<i32>) -> Self {
        Self { kind, level }
    }

    pub fn kind(&self) -> Compression {
        self.kind
    }

    pub fn level(&self) -> Option<i32> {
        self.level
    }
}

#[derive(Debug, Clone)]
pub struct Config {
    message_filter: MessageFilter,
    output_dir: Option<Utf8PathBuf>,
    compression: CompressionSetting,
    threads: Option<usize>,
}

impl Config {
    pub fn new(
        message_filter: MessageFilter,
        output_dir: Option<Utf8PathBuf>,
        compression: Compression,
        threads: Option<usize>,
    ) -> Self {
        Self {
            message_filter,
            output_dir,
            compression: CompressionSetting::new(compression, None),
            threads,
        }
    }

    pub fn message_filter(&self) -> &MessageFilter {
        &self.message_filter
    }

    pub fn output_dir(&self) -> Option<&Utf8PathBuf> {
        self.output_dir.as_ref()
    }

    pub fn threads(&self) -> Option<usize> {
        self.threads
    }

    pub fn set_include_topic_names(mut self, include_topic_names: Option<HashSet<String>>) -> Self {
        self.message_filter
            .set_include_topic_names(include_topic_names);
        self
    }

    pub fn set_exclude_topic_names(mut self, exclude_topic_names: Option<HashSet<String>>) -> Self {
        self.message_filter
            .set_exclude_topic_names(exclude_topic_names);
        self
    }

    pub fn set_start_time(mut self, start_time: Option<u64>) -> Self {
        self.message_filter.set_start_time(start_time);
        self
    }

    pub fn set_end_time(mut self, end_time: Option<u64>) -> Self {
        self.message_filter.set_end_time(end_time);
        self
    }

    pub fn set_output_dir(mut self, output_dir: Option<Utf8PathBuf>) -> Self {
        self.output_dir = output_dir;
        self
    }

    pub fn set_threads(mut self, threads: Option<usize>) -> Self {
        self.threads = threads;
        self
    }

    pub fn set_compression(mut self, compression: Compression) -> Self {
        self.compression = CompressionSetting::new(compression, None);
        self
    }

    pub fn set_compression_from_str(mut self, compression: &str) -> Self {
        if let Ok(kind) = Compression::from_str(compression) {
            self.compression = CompressionSetting::new(kind, None);
        }
        self
    }

    pub fn try_set_compression_from_str(mut self, compression: &str) -> crate::error::Result<Self> {
        let kind = Compression::from_str(compression).map_err(|e| {
            crate::error::Rosbag2ParquetError::ConfigError {
                message: format!("Invalid compression: {}", e),
            }
        })?;
        self.compression = CompressionSetting::new(kind, None);
        Ok(self)
    }

    pub fn set_compression_with_level(mut self, kind: Compression, level: Option<i32>) -> Self {
        self.compression = CompressionSetting::new(kind, level);
        self
    }

    pub fn compression(&self) -> CompressionSetting {
        self.compression
    }

    pub fn validate(&self) -> Result<()> {
        self.message_filter.validate()?;
        if let Some(output_dir) = &self.output_dir {
            if output_dir.as_str().is_empty() {
                return Err(Rosbag2ParquetError::ConfigError {
                    message: "output_dir must not be empty".to_string(),
                });
            }
        }
        Ok(())
    }
}

impl Default for Config {
    fn default() -> Self {
        Self::new(MessageFilter::default(), None, Compression::SNAPPY, None)
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct MessageFilter {
    include_topic_names: Option<HashSet<String>>,
    exclude_topic_names: Option<HashSet<String>>,
    start_time: Option<u64>,
    end_time: Option<u64>,
}

impl MessageFilter {
    pub fn set_include_topic_names(&mut self, include_topic_names: Option<HashSet<String>>) {
        self.include_topic_names = include_topic_names;
    }

    pub fn set_exclude_topic_names(&mut self, exclude_topic_names: Option<HashSet<String>>) {
        self.exclude_topic_names = exclude_topic_names;
    }

    pub fn set_start_time(&mut self, start_time: Option<u64>) {
        self.start_time = start_time;
    }

    pub fn set_end_time(&mut self, end_time: Option<u64>) {
        self.end_time = end_time;
    }

    pub fn matches(&self, message: &Message) -> bool {
        if let Some(include_topic_names) = &self.include_topic_names {
            if !include_topic_names.contains(&message.channel.topic) {
                return false;
            }
        }
        if let Some(exclude_topic_names) = &self.exclude_topic_names {
            if exclude_topic_names.contains(&message.channel.topic) {
                return false;
            }
        }
        if let Some(start_time) = self.start_time {
            if message.log_time < start_time {
                return false;
            }
        }
        if let Some(end_time) = self.end_time {
            if message.log_time > end_time {
                return false;
            }
        }
        true
    }

    pub fn validate(&self) -> Result<()> {
        if let (Some(start), Some(end)) = (self.start_time, self.end_time) {
            if start > end {
                return Err(Rosbag2ParquetError::ConfigError {
                    message: format!("start_time ({start}) must be <= end_time ({end})"),
                });
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_message_filter_validate_ok() {
        let mut f = MessageFilter::default();
        f.set_start_time(Some(1));
        f.set_end_time(Some(1));
        assert!(f.validate().is_ok());
    }

    #[test]
    fn test_message_filter_validate_err() {
        let mut f = MessageFilter::default();
        f.set_start_time(Some(2));
        f.set_end_time(Some(1));
        let err = f.validate().unwrap_err();
        let s = err.to_string();
        assert!(s.contains("start_time"));
    }

    #[test]
    fn test_config_validate_output_dir_ok() {
        let c = Config::default().set_output_dir(Some(Utf8PathBuf::from("out")));
        assert!(c.validate().is_ok());
    }
}
