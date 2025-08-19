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
    },
}

fn main() {
    let cli = Cli::parse();

    match cli.command {
        Commands::Convert {
            input,
            topic,
            output_dir,
        } => {
            let topic_filter = if !topic.is_empty() {
                TopicFilter::include(topic)
            } else {
                TopicFilter::all()
            };

            let config = Config::new(topic_filter, output_dir);
            rosbag2parquet::rosbag2parquet(&input, config);
            println!("Conversion completed successfully!");
        }
    }
}
