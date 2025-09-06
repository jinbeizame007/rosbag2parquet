use camino::Utf8PathBuf;
use clap::{Parser, Subcommand};
use rosbag2parquet::Config;

#[derive(Parser)]
#[command(name = "rosbag2parquet")]
#[command(version, about = "Convert ROS2 bag files to Parquet format")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Convert {
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
        ///
        /// Available options are:
        /// - UNCOMPRESSED
        /// - SNAPPY
        /// - GZIP(GzipLevel({0 - 9}))
        /// - LZO
        /// - BROTLI(BrotliLevel({0 - 11}))
        /// - LZ4
        /// - ZSTD(ZstdLevel({1 - 22})):
        /// - LZ4_RAW
        #[arg(long, verbatim_doc_comment, default_value = "SNAPPY")]
        compression: Option<String>,
    },
}

fn main() {
    let cli = Cli::parse();

    match cli.command {
        Commands::Convert {
            input,
            topics,
            exclude,
            start_time,
            end_time,
            output_dir,
            compression,
        } => {
            let topics_set = match topics.is_empty() {
                true => None,
                false => Some(topics.into_iter().collect()),
            };
            let exclude_set = match exclude.is_empty() {
                true => None,
                false => Some(exclude.into_iter().collect()),
            };

            let mut config = Config::default()
                .set_include_topic_names(topics_set)
                .set_exclude_topic_names(exclude_set)
                .set_start_time(start_time)
                .set_end_time(end_time)
                .set_output_dir(output_dir);
            if let Some(compression) = compression {
                config = config.set_compression_from_str(&compression);
            }
            rosbag2parquet::rosbag2parquet(&input, config);
            println!("Conversion completed successfully!");
        }
    }
}
