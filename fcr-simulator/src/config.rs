use std::path::{Path, PathBuf};

use anyhow::{Result, bail};
use clap::{Parser, ValueEnum};

#[derive(Parser, Debug, Clone)]
#[command(name = "fcr-lighthouse")]
#[command(about = "Lighthouse FCR simulation engine")]
pub struct Config {
    /// Orchestrator HTTP server URL.
    #[arg(long)]
    pub beacon_node_url: Option<String>,

    /// First sim slot to record, inclusive.
    #[arg(long)]
    pub start_slot: Option<u64>,

    /// Sim slot exclusive upper bound.
    #[arg(long)]
    pub end_slot: Option<u64>,

    /// Slot at which to bootstrap from the checkpoint state.
    #[arg(long)]
    pub warmup_start_slot: Option<u64>,

    /// Ethereum network identifier.
    #[arg(long, value_enum)]
    pub network: Option<Network>,

    /// FCR byzantine threshold in percent.
    #[arg(long, default_value_t = 25)]
    pub byzantine_threshold: u64,

    /// Accepted for orchestrator compatibility; execution uses the v2 plan.
    #[arg(long)]
    pub attestation_source_mode: Option<String>,

    /// Accepted for orchestrator compatibility; execution uses the v2 plan.
    #[arg(long)]
    pub lookahead_cap: Option<u64>,

    /// Path to write JSONL output.
    #[arg(long)]
    pub output: Option<PathBuf>,

    /// Print the engine manifest JSON to stdout and exit.
    #[arg(long)]
    pub manifest_json: bool,
}

impl Config {
    pub fn validate(&self) -> Result<()> {
        if self.manifest_json {
            return Ok(());
        }

        let start_slot = self.required_u64(self.start_slot, "--start-slot")?;
        let end_slot = self.required_u64(self.end_slot, "--end-slot")?;
        let warmup_start_slot = self.required_u64(self.warmup_start_slot, "--warmup-start-slot")?;

        self.required_str(self.beacon_node_url.as_deref(), "--beacon-node-url")?;
        self.required(self.network, "--network")?;
        self.required_path(self.output.as_deref(), "--output")?;

        if warmup_start_slot > start_slot {
            bail!("--warmup-start-slot must be <= --start-slot");
        }

        if start_slot >= end_slot {
            bail!("--start-slot must be < --end-slot");
        }

        Ok(())
    }

    pub fn beacon_node_url(&self) -> &str {
        self.beacon_node_url
            .as_deref()
            .expect("validated config should include --beacon-node-url")
    }

    pub fn start_slot(&self) -> u64 {
        self.start_slot
            .expect("validated config should include --start-slot")
    }

    pub fn end_slot(&self) -> u64 {
        self.end_slot
            .expect("validated config should include --end-slot")
    }

    pub fn warmup_start_slot(&self) -> u64 {
        self.warmup_start_slot
            .expect("validated config should include --warmup-start-slot")
    }

    pub fn network(&self) -> Network {
        self.network
            .expect("validated config should include --network")
    }

    pub fn output(&self) -> &Path {
        self.output
            .as_deref()
            .expect("validated config should include --output")
    }

    fn required_str<'a>(&self, value: Option<&'a str>, flag: &str) -> Result<&'a str> {
        value.ok_or_else(|| anyhow::anyhow!("missing required flag {flag}"))
    }

    fn required_path<'a>(&self, value: Option<&'a Path>, flag: &str) -> Result<&'a Path> {
        value.ok_or_else(|| anyhow::anyhow!("missing required flag {flag}"))
    }

    fn required<T: Copy>(&self, value: Option<T>, flag: &str) -> Result<T> {
        value.ok_or_else(|| anyhow::anyhow!("missing required flag {flag}"))
    }

    fn required_u64(&self, value: Option<u64>, flag: &str) -> Result<u64> {
        value.ok_or_else(|| anyhow::anyhow!("missing required flag {flag}"))
    }
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum Network {
    Mainnet,
}
