pub mod fetcher;
pub mod slot;

pub use fetcher::BeaconFetcher;
pub use slot::{PlanAttestation, PlanBlockImport, fetch_slot_instruction};

use anyhow::{Context, Result};

fn build_client() -> Result<reqwest::Client> {
    reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(600))
        .build()
        .context("failed to build HTTP client")
}

fn base_url(base_url: &str) -> String {
    base_url.trim_end_matches('/').to_string()
}
