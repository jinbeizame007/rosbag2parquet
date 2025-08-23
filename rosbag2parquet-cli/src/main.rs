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
        #[arg(help = "Path to the MCAP file")]
        input: Utf8PathBuf,

        #[arg(
            long,
            help = "Space-separated list of topics to include",
            num_args = 1..,
        )]
        topics: Vec<String>,

        #[arg(
            long,
            help = "Space-separated list of topics to exclude",
            num_args = 1..,
        )]
        exclude: Vec<String>,

        #[arg(long, help = "Start time of the messages to include")]
        start_time: Option<u64>,

        #[arg(long, help = "End time of the messages to include")]
        end_time: Option<u64>,

        #[arg(long, help = "Output directory for the converted Parquet files")]
        output_dir: Option<Utf8PathBuf>,

        #[arg(long, help = "Compression algorithm to use")]
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
