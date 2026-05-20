use std::sync::Arc;

use anyhow::{Context, Result, bail};
use types::{BeaconState, ChainSpec, Hash256, MainnetEthSpec, SignedBeaconBlock, Slot};

use crate::http::{base_url, build_client};

#[derive(Clone)]
pub struct BeaconFetcher {
    base_url: String,
    client: reqwest::Client,
    spec: Arc<ChainSpec>,
}

impl BeaconFetcher {
    pub fn new(beacon_node_url: &str, spec: Arc<ChainSpec>) -> Result<Self> {
        Ok(Self {
            base_url: base_url(beacon_node_url),
            client: build_client()?,
            spec,
        })
    }

    pub async fn fetch_checkpoint_state(&self, slot: Slot) -> Result<BeaconState<MainnetEthSpec>> {
        let url = format!("{}/eth/v2/debug/beacon/states/{}", self.base_url, slot);
        let data = self.fetch_ssz(&url).await?;

        BeaconState::from_ssz_bytes(&data, self.spec.as_ref())
            .map_err(|e| anyhow::anyhow!("failed to decode checkpoint state SSZ: {:?}", e))
    }

    pub async fn fetch_genesis_state(&self) -> Result<BeaconState<MainnetEthSpec>> {
        let url = format!("{}/eth/v2/debug/beacon/states/genesis", self.base_url);
        let data = self.fetch_ssz(&url).await?;

        BeaconState::from_ssz_bytes(&data, self.spec.as_ref())
            .map_err(|e| anyhow::anyhow!("failed to decode genesis state SSZ: {:?}", e))
    }

    pub async fn fetch_block_by_root(
        &self,
        root: Hash256,
    ) -> Result<SignedBeaconBlock<MainnetEthSpec>> {
        self.fetch_block_by_root_optional(root)
            .await?
            .with_context(|| format!("block {:?} was not found", root))
    }

    pub async fn fetch_block_by_root_optional(
        &self,
        root: Hash256,
    ) -> Result<Option<SignedBeaconBlock<MainnetEthSpec>>> {
        let url = format!("{}/eth/v2/beacon/blocks/{:?}", self.base_url, root);
        let response = self
            .client
            .get(&url)
            .header("Accept", "application/octet-stream")
            .send()
            .await
            .with_context(|| format!("failed to fetch {}", url))?;

        if response.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }

        let data = read_success_body(response, &url).await?;

        SignedBeaconBlock::from_ssz_bytes(&data, self.spec.as_ref())
            .map_err(|e| anyhow::anyhow!("failed to decode block SSZ for {:?}: {:?}", root, e))
            .map(Some)
    }

    pub async fn fetch_block_at_slot(
        &self,
        slot: Slot,
    ) -> Result<Option<SignedBeaconBlock<MainnetEthSpec>>> {
        let url = format!("{}/eth/v2/beacon/blocks/{}", self.base_url, slot);
        let response = self
            .client
            .get(&url)
            .header("Accept", "application/octet-stream")
            .send()
            .await
            .with_context(|| format!("failed to fetch {}", url))?;

        if response.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }

        let data = read_success_body(response, &url).await?;
        let block = SignedBeaconBlock::from_ssz_bytes(&data, self.spec.as_ref())
            .map_err(|e| anyhow::anyhow!("failed to decode block SSZ at slot {}: {:?}", slot, e))?;

        Ok(Some(block))
    }

    async fn fetch_ssz(&self, url: &str) -> Result<Vec<u8>> {
        let response = self
            .client
            .get(url)
            .header("Accept", "application/octet-stream")
            .send()
            .await
            .with_context(|| format!("failed to fetch {}", url))?;

        read_success_body(response, url).await
    }
}

async fn read_success_body(response: reqwest::Response, url: &str) -> Result<Vec<u8>> {
    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        bail!("HTTP {} from {}: {}", status, url, trim_body(&body));
    }

    Ok(response
        .bytes()
        .await
        .context("failed to read body")?
        .to_vec())
}

fn trim_body(body: &str) -> &str {
    const MAX_LEN: usize = 512;

    if body.len() <= MAX_LEN {
        body
    } else {
        let end = body
            .char_indices()
            .map(|(idx, _)| idx)
            .take_while(|idx| *idx <= MAX_LEN)
            .last()
            .unwrap_or(0);
        &body[..end]
    }
}
