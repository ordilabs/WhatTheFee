use anyhow::Result;
use clap::{Parser, Subcommand};
use tracing::{info, metadata::LevelFilter};
use tracing_subscriber::EnvFilter;
use wtf::record::Record;

#[derive(Parser)]
struct Cli {
    /// Data directory for mempool recording
    #[arg(short, long, default_value_t = String::from("data"))]
    data_dir: String,
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Begin recording mempool data
    Record {
        /// Bitcoin Core REST endpoint
        #[arg(short, long, default_value_t = String::from("http://localhost:8332/rest/"))]
        bitcoin_core_endpoint: String,
    },
    /// Calculate the fee
    Calc {
        /// Confidence percentage
        #[arg(short, long, default_value_t = 0.95)]
        confidence: f64,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let filter = EnvFilter::builder()
        .with_default_directive(LevelFilter::INFO.into())
        .from_env_lossy();
    tracing_subscriber::fmt().with_env_filter(filter).init();

    let cli = Cli::parse();

    match cli.command {
        Commands::Record {
            bitcoin_core_endpoint,
        } => {
            Record::record(cli.data_dir, bitcoin_core_endpoint).await?;
        }
        Commands::Calc { confidence } => {
            info!("calc confidence {confidence}");
        }
    }

    Ok(())
}
