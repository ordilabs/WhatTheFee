use anyhow::Result;
use clap::{Parser, Subcommand};
use tracing::metadata::LevelFilter;
use tracing_subscriber::EnvFilter;
use wtf::record::Record;

#[derive(Parser)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Record,
}

#[tokio::main]
async fn main() -> Result<()> {
    let filter = EnvFilter::builder()
        .with_default_directive(LevelFilter::INFO.into())
        .from_env_lossy();
    tracing_subscriber::fmt().with_env_filter(filter).init();

    let cli = Cli::parse();

    match &cli.command {
        Commands::Record => {
            Record::record().await?;
        }
    }

    Ok(())
}
