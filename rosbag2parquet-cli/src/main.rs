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
        
        #[arg(short, long, help = "Include only these topics (can be specified multiple times)")]
        include: Vec<String>,
        
        #[arg(short, long, help = "Exclude these topics (can be specified multiple times)")]
        exclude: Vec<String>,
        
        #[arg(
            long,
            help = "Space-separated list of topics to include (alternative to multiple --include)",
            num_args = 1..,
            value_delimiter = ' '
        )]
        topics: Vec<String>,
    },
}

fn main() {
    let cli = Cli::parse();

    match cli.command {
        Commands::Convert { input, include, exclude, topics } => {
            let mut all_include_topics = include;
            all_include_topics.extend(topics);
            
            if !all_include_topics.is_empty() && !exclude.is_empty() {
                eprintln!("Error: Cannot use both --include/--topics and --exclude at the same time");
                std::process::exit(1);
            }
            
            let topic_filter = if !all_include_topics.is_empty() {
                TopicFilter::include(all_include_topics)
            } else if !exclude.is_empty() {
                TopicFilter::exclude(exclude)
            } else {
                TopicFilter::all()
            };

            rosbag2parquet::rosbag2parquet_with_filter(&input, topic_filter);
            println!("Conversion completed successfully!");
        }
    }
}