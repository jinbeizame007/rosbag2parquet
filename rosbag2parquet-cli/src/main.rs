use camino::Utf8PathBuf;
use clap::{Parser, Subcommand};
use rosbag2parquet::TopicFilter;

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
    },
}

fn main() {
    let cli = Cli::parse();

    match cli.command {
        Commands::Convert { input, topic } => {
            let topic_filter = if !topic.is_empty() {
                TopicFilter::include(topic)
            } else {
                TopicFilter::all()
            };

            rosbag2parquet::rosbag2parquet_with_filter(&input, topic_filter);
            println!("Conversion completed successfully!");
        }
    }
}
