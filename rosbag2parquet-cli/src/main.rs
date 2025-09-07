use camino::Utf8PathBuf;
use clap::{Parser, ValueEnum};
use rosbag2parquet::Config;

#[derive(Parser)]
#[command(name = "rosbag2parquet")]
#[command(version, about = "Convert ROS2 bag files to Parquet format")]
struct Cli {
    /// Path to the MCAP file
    #[arg()]
    input: Utf8PathBuf,

    /// Space-separated list of topic to include
    #[arg(
            long,
            num_args = 1..,
        )]
    topics: Vec<String>,

    /// Space-separated list of topic to exclude
    #[arg(
            long,
            num_args = 1..,
        )]
    exclude: Vec<String>,

    /// Start time [ns] of the messages to include
    #[arg(long)]
    start_time: Option<u64>,

    /// End time [ns] of the messages to include
    #[arg(long)]
    end_time: Option<u64>,

    /// Output directory for the converted Parquet files
    #[arg(long)]
    output_dir: Option<Utf8PathBuf>,

    /// Compression algorithm to use
    #[arg(long, value_enum, default_value = "snappy")]
    compression: CompressionType,

    /// Compression level (only for gzip, brotli, zstd)
    ///
    /// Valid ranges:
    /// - gzip: 0-9 (default: 6)
    /// - brotli: 0-11 (default: 6)
    /// - zstd: 1-22 (default: 3)
    #[arg(long, verbatim_doc_comment)]
    compression_level: Option<u32>,
}

/// Compression types supported by the Parquet format
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
enum CompressionType {
    /// No compression
    Uncompressed,
    /// Snappy compression ( no level support)
    Snappy,
    /// Gzip compression (levels 0-9)
    Gzip,
    /// LZO compression (no level support)
    Lzo,
    /// Brotli compression (levels 0-11)
    Brotli,
    /// LZ4 compression (no level support)
    Lz4,
    /// Zstandard compression (levels 1-22)
    Zstd,
    /// Raw LZ4 compression (no level support)
    Lz4Raw,
}

impl CompressionType {
    fn supports_level(&self) -> bool {
        matches!(
            self,
            CompressionType::Gzip | CompressionType::Brotli | CompressionType::Zstd
        )
    }

    fn level_range(&self) -> Option<(u32, u32, u32)> {
        match self {
            CompressionType::Gzip => Some((0, 9, 6)),
            CompressionType::Brotli => Some((0, 11, 6)),
            CompressionType::Zstd => Some((1, 22, 3)),
            _ => None,
        }
    }

    fn validate_level(&self, level: Option<u32>) -> Result<Option<u32>, String> {
        match (self.supports_level(), level) {
            (false, Some(_)) => Err(format!(
                "Compression type '{:?}' does not support compression levels. \
                    Only gzip, brotli, and zstd support level configuration.",
                self
            )),
            (true, Some(l)) => {
                if let Some((min, max, _)) = self.level_range() {
                    if l < min || l > max {
                        Err(format!(
                            "Invalid compression level {} for {:?}. Valid range is {}-{}.",
                            l, self, min, max
                        ))
                    } else {
                        Ok(Some(l))
                    }
                } else {
                    Ok(None)
                }
            }
            _ => Ok(None),
        }
    }

    fn to_parquet_string(self, level: Option<u32>) -> Result<String, String> {
        let validated_level = self.validate_level(level)?;

        Ok(match self {
            CompressionType::Uncompressed => "UNCOMPRESSED".to_string(),
            CompressionType::Snappy => "SNAPPY".to_string(),
            CompressionType::Gzip => {
                if let Some(l) = validated_level {
                    format!("GZIP({})", l)
                } else {
                    "GZIP(6)".to_string()
                }
            }
            CompressionType::Lzo => "LZO".to_string(),
            CompressionType::Brotli => {
                if let Some(l) = validated_level {
                    format!("BROTLI({})", l)
                } else {
                    "BROTLI(1)".to_string()
                }
            }
            CompressionType::Lz4 => "LZ4".to_string(),
            CompressionType::Zstd => {
                if let Some(l) = validated_level {
                    format!("ZSTD({})", l)
                } else {
                    "ZSTD(3)".to_string()
                }
            }
            CompressionType::Lz4Raw => "LZ4_RAW".to_string(),
        })
    }
}

fn main() {
    let cli = Cli::parse();

    let topics_set = match cli.topics.is_empty() {
        true => None,
        false => Some(cli.topics.into_iter().collect()),
    };
    let exclude_set = match cli.exclude.is_empty() {
        true => None,
        false => Some(cli.exclude.into_iter().collect()),
    };

    let mut config = Config::default()
        .set_include_topic_names(topics_set)
        .set_exclude_topic_names(exclude_set)
        .set_start_time(cli.start_time)
        .set_end_time(cli.end_time)
        .set_output_dir(cli.output_dir);

    match cli.compression.to_parquet_string(cli.compression_level) {
        Ok(compression_str) => {
            config = config.set_compression_from_str(&compression_str);
        }
        Err(e) => {
            eprintln!("Error: {}", e);
            std::process::exit(1);
        }
    }

    rosbag2parquet::rosbag2parquet(&cli.input, config);
    println!("Conversion completed successfully!");
}
