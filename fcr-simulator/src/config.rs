use clap::Parser;
use std::path::PathBuf;

#[derive(Parser, Debug, Clone)]
#[command(name = "fcr-simulator")]
#[command(about = "Simulate Ethereum's Fast Confirmation Rule against historical data")]
pub struct Config {
    /// Beacon node URL for fetching the starting state
    #[arg(long)]
    pub beacon_node_url: String,

    /// Start epoch (inclusive)
    #[arg(long)]
    pub start_epoch: u64,

    /// End epoch (exclusive)
    #[arg(long)]
    pub end_epoch: u64,

    /// ERA file download URL
    #[arg(long, default_value = "https://mainnet.era.nimbus.team")]
    pub era_url: String,

    /// Local cache directory for ERA files and beacon states
    #[arg(long, default_value = "~/.cache/fcr-simulator")]
    pub cache_dir: PathBuf,

    /// Output file path
    #[arg(long, default_value = "results.csv")]
    pub output: PathBuf,

    /// Output format
    #[arg(long, default_value = "csv")]
    pub output_format: OutputFormat,

    /// Byzantine threshold percentage (0-25)
    #[arg(long, default_value = "25")]
    pub byzantine_threshold: u64,

    /// Number of warmup epochs before each worker's start_epoch.
    /// More warmup = more reliable FCR results. Attestation weight
    /// falls off with distance, so 10 epochs covers the meaningful range.
    #[arg(long, default_value = "10")]
    pub warmup_epochs: u64,

    /// Number of parallel workers. Each worker processes a chunk of the
    /// epoch range independently with its own BeaconChain instance.
    /// Each worker uses ~2GB RAM for the BeaconState.
    #[arg(long, default_value = "1")]
    pub parallel: u64,

    /// Use real attestation timing data from xatu instead of next-block attestations.
    /// Downloads daily parquet files from R2 with per-validator timing information.
    #[arg(long)]
    pub use_xatu_attestations: bool,
}

impl Config {
    pub fn resolved_cache_dir(&self) -> PathBuf {
        let path = self.cache_dir.to_string_lossy();
        if path.starts_with("~/")
            && let Some(home) = dirs_fallback()
        {
            return home.join(&path[2..]);
        }
        self.cache_dir.clone()
    }

    #[allow(dead_code)]
    pub fn start_slot(&self) -> u64 {
        self.start_epoch * 32
    }

    #[allow(dead_code)]
    pub fn end_slot(&self) -> u64 {
        self.end_epoch * 32
    }

    #[allow(dead_code)]
    pub fn warmup_start_slot(&self) -> u64 {
        self.start_epoch.saturating_sub(self.warmup_epochs) * 32
    }

    /// Split the epoch range into N chunks for parallel workers.
    /// Returns (start_epoch, end_epoch) for each chunk.
    pub fn split_ranges(&self) -> Vec<(u64, u64)> {
        let total_epochs = self.end_epoch.saturating_sub(self.start_epoch);
        let n = self.parallel.max(1);

        if total_epochs == 0 || n == 0 {
            return vec![];
        }

        let chunk_size = total_epochs / n;
        let remainder = total_epochs % n;

        let mut ranges = Vec::with_capacity(n as usize);
        let mut cursor = self.start_epoch;

        for i in 0..n {
            let extra = if i < remainder { 1 } else { 0 };
            let end = cursor + chunk_size + extra;
            ranges.push((cursor, end));
            cursor = end;
        }

        ranges
    }
}

fn dirs_fallback() -> Option<PathBuf> {
    std::env::var("HOME").ok().map(PathBuf::from)
}

#[derive(Debug, Clone, clap::ValueEnum)]
pub enum OutputFormat {
    Csv,
    Json,
}
