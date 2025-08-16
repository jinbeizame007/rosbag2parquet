use std::collections::HashSet;

use camino::Utf8PathBuf;
use clap::{Parser, Subcommand};

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
        
        #[arg(short, long, help = "Topic filter (can be specified multiple times)")]
        topic: Vec<String>,
        
        #[arg(
            long,
            help = "Space-separated list of topics (alternative to multiple --topic)",
            num_args = 1..,
            value_delimiter = ' '
        )]
        topics: Vec<String>,
    },
}

fn main() {
    let cli = Cli::parse();

    match cli.command {
        Commands::Convert { input, topic, topics } => {
            let mut all_topics = topic;
            all_topics.extend(topics);
            
            let topic_filter = if all_topics.is_empty() {
                None
            } else {
                Some(all_topics.into_iter().collect::<HashSet<_>>())
            };

            rosbag2parquet::rosbag2parquet(&input, topic_filter);
            println!("Conversion completed successfully!");
        }
    }
}