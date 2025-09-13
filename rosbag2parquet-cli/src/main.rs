use camino::Utf8PathBuf;
use clap::{Parser, ValueEnum};
use parquet::basic::{BrotliLevel, Compression as PCompression, GzipLevel, ZstdLevel};
use rosbag2parquet::Config;

#[derive(Parser)]
#[command(name = "rosbag2parquet")]
#[command(version, about = "Convert ROS2 bag files to Parquet format")]
struct Cli {
    /// MCAP file paths or a directory containing MCAP files
    #[arg(num_args = 1..)]
    inputs: Vec<Utf8PathBuf>,

    /// A space-separated list of topics to include
    #[arg(
            long,
            num_args = 1..,
        )]
    topics: Vec<String>,

    /// A space-separated list of topics to exclude
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

    /// Number of threads to use
    ///
    /// If not specified, the number of threads will be automatically determined.
    #[arg(long)]
    threads: Option<usize>,
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
        .set_output_dir(cli.output_dir)
        .set_threads(cli.threads);

    let validated_level = match cli.compression.validate_level(cli.compression_level) {
        Ok(level) => level,
        Err(e) => {
            eprintln!("Error: {}", e);
            std::process::exit(1);
        }
    };

    let kind = match cli.compression {
        CompressionType::Uncompressed => PCompression::UNCOMPRESSED,
        CompressionType::Snappy => PCompression::SNAPPY,
        CompressionType::Gzip => {
            let l: u32 = validated_level.unwrap_or(6);
            let lvl = GzipLevel::try_new(l).unwrap_or_else(|_| GzipLevel::try_new(6).unwrap());
            PCompression::GZIP(lvl)
        }
        CompressionType::Lzo => PCompression::LZO,
        CompressionType::Brotli => {
            let l: u32 = validated_level.unwrap_or(6);
            let lvl = BrotliLevel::try_new(l).unwrap_or_else(|_| BrotliLevel::try_new(6).unwrap());
            PCompression::BROTLI(lvl)
        }
        CompressionType::Lz4 => PCompression::LZ4,
        CompressionType::Zstd => {
            let l: i32 = validated_level.unwrap_or(3) as i32;
            let lvl = ZstdLevel::try_new(l).unwrap_or_else(|_| ZstdLevel::try_new(3).unwrap());
            PCompression::ZSTD(lvl)
        }
        CompressionType::Lz4Raw => PCompression::LZ4_RAW,
    };
    config = config.set_compression(kind);

    match rosbag2parquet::rosbag2parquet(&cli.inputs, config) {
        Ok(()) => println!("Conversion completed successfully!"),
        Err(e) => {
            eprintln!("Error: {}", e);
            std::process::exit(1);
        }
    }
}
