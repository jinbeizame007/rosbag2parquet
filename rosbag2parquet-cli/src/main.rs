use camino::Utf8PathBuf;
use clap::{Parser, Subcommand};
use rosbag2parquet::{Config, TopicFilter};

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
            help = "Space-separated list of topics to include (alternative to multiple --include)",
            num_args = 1..,
        )]
        topic: Vec<String>,

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
            topic,
            output_dir,
            compression,
        } => {
            let topic_filter = if !topic.is_empty() {
                TopicFilter::include(topic)
            } else {
                TopicFilter::all()
            };

            let mut config = Config::default()
                .set_topic_filter(topic_filter)
                .set_output_dir(output_dir);
            if let Some(compression) = compression {
                config = config.set_compression_from_str(&compression);
            }
            rosbag2parquet::rosbag2parquet(&input, config);
            println!("Conversion completed successfully!");
        }
    }
}
