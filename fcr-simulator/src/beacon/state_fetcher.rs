use std::fs;
use std::path::Path;

use anyhow::{bail, Context, Result};
use tracing::info;
use types::{BeaconState, ChainSpec, MainnetEthSpec, SignedBeaconBlock, Slot};

fn http_client() -> Result<reqwest::blocking::Client> {
    reqwest::blocking::Client::builder()
        .timeout(std::time::Duration::from_secs(600))
        .build()
        .context("failed to build HTTP client")
}

fn fetch_ssz(url: &str) -> Result<Vec<u8>> {
    let client = http_client()?;
    let response = client
        .get(url)
        .header("Accept", "application/octet-stream")
        .send()
        .with_context(|| format!("failed to fetch {}", url))?;

    if !response.status().is_success() {
        bail!("HTTP {} from {}", response.status(), url);
    }

    Ok(response.bytes().context("failed to read body")?.to_vec())
}

fn cached_or_fetch(cache_path: &Path, url: &str, label: &str) -> Result<Vec<u8>> {
    if cache_path.exists() {
        info!(path = %cache_path.display(), "Loading cached {}", label);
        return fs::read(cache_path)
            .with_context(|| format!("failed to read cached {}", label));
    }

    info!(url = %url, "Fetching {} (this may take a while)", label);
    let data = fetch_ssz(url)?;
    info!(size_mb = data.len() / (1024 * 1024), "Downloaded {}", label);

    if let Some(parent) = cache_path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(cache_path, &data)
        .with_context(|| format!("failed to cache {} to {}", label, cache_path.display()))?;

    Ok(data)
}

/// Fetch a beacon state from a beacon node, using local cache if available.
pub fn fetch_beacon_state(
    beacon_node_url: &str,
    slot: Slot,
    cache_dir: &Path,
    spec: &ChainSpec,
) -> Result<BeaconState<MainnetEthSpec>> {
    let cache_path = cache_dir.join("states").join(format!("state-{}.ssz", slot));
    let url = format!(
        "{}/eth/v2/debug/beacon/states/{}",
        beacon_node_url.trim_end_matches('/'),
        slot
    );

    let data = cached_or_fetch(&cache_path, &url, &format!("beacon state at slot {}", slot))?;

    BeaconState::from_ssz_bytes(&data, spec)
        .map_err(|e| anyhow::anyhow!("failed to decode state SSZ: {:?}", e))
}

/// Fetch the genesis beacon state from a beacon node, using local cache if available.
pub fn fetch_genesis_state(
    beacon_node_url: &str,
    cache_dir: &Path,
    spec: &ChainSpec,
) -> Result<BeaconState<MainnetEthSpec>> {
    let cache_path = cache_dir.join("states").join("state-genesis.ssz");
    let url = format!(
        "{}/eth/v2/debug/beacon/states/genesis",
        beacon_node_url.trim_end_matches('/'),
    );

    let data = cached_or_fetch(&cache_path, &url, "genesis beacon state")?;

    BeaconState::from_ssz_bytes(&data, spec)
        .map_err(|e| anyhow::anyhow!("failed to decode genesis state SSZ: {:?}", e))
}

/// Fetch a beacon block from a beacon node, using local cache if available.
pub fn fetch_beacon_block(
    beacon_node_url: &str,
    slot: Slot,
    cache_dir: &Path,
    spec: &ChainSpec,
) -> Result<SignedBeaconBlock<MainnetEthSpec>> {
    let cache_path = cache_dir.join("blocks").join(format!("block-{}.ssz", slot));
    let url = format!(
        "{}/eth/v2/beacon/blocks/{}",
        beacon_node_url.trim_end_matches('/'),
        slot
    );

    let data = cached_or_fetch(&cache_path, &url, &format!("beacon block at slot {}", slot))?;

    SignedBeaconBlock::from_ssz_bytes(&data, spec)
        .map_err(|e| anyhow::anyhow!("failed to decode block SSZ: {:?}", e))
}
