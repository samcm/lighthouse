use std::collections::HashMap;
use std::str::FromStr;

use anyhow::{Context, Result, bail};
use serde::Deserialize;
use types::{Hash256, Slot};

use crate::http::{base_url, build_client};

pub type AttestationPlan = HashMap<Slot, PlanEntry>;

#[derive(Clone)]
pub struct PlanEntry {
    pub eval_slot: Slot,
    pub import_blocks: Vec<PlanBlockImport>,
    pub attestation_sources: Vec<PlanAttestationSource>,
    pub source_block_slot: Option<Slot>,
}

#[derive(Clone)]
pub struct PlanBlockImport {
    pub slot: Slot,
    pub root: Hash256,
    pub canonical: bool,
}

#[derive(Clone)]
pub struct PlanAttestationSource {
    pub slot: Slot,
    pub max_attestation_slot: Option<Slot>,
}

#[derive(Deserialize)]
struct PlanResponse {
    version: Option<u64>,
    entries: Vec<PlanEntryWire>,
}

#[derive(Deserialize)]
struct PlanEntryWire {
    sim_slot: u64,
    eval_slot: u64,
    import_blocks: Vec<PlanBlockImportWire>,
    attestation_sources: Vec<PlanAttestationSourceWire>,
    source_block_slot: Option<u64>,
}

#[derive(Deserialize)]
struct PlanBlockImportWire {
    slot: u64,
    root: String,
    canonical: bool,
}

#[derive(Deserialize)]
struct PlanAttestationSourceWire {
    slot: u64,
    max_attestation_slot: Option<u64>,
}

pub async fn fetch_attestation_plan(
    beacon_node_url: &str,
    from: Slot,
    to: Slot,
) -> Result<AttestationPlan> {
    let client = build_client()?;
    let base_url = base_url(beacon_node_url);
    let url = format!(
        "{}/fcr-sim/v1/plan?from={}&to={}",
        base_url,
        from.as_u64(),
        to.as_u64()
    );

    let response = client
        .get(&url)
        .header("Accept", "application/json")
        .send()
        .await
        .with_context(|| format!("failed to fetch {}", url))?;

    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        bail!("HTTP {} from {}: {}", status, url, trim_body(&body));
    }

    let plan_response = response
        .json::<PlanResponse>()
        .await
        .context("failed to decode attestation plan JSON")?;

    let version = plan_response
        .version
        .context("attestation plan response is missing version")?;
    if version != 2 {
        bail!(
            "unsupported attestation plan version {}; expected 2",
            version
        );
    }

    let mut plan = HashMap::with_capacity(plan_response.entries.len());
    for entry in plan_response.entries {
        let sim_slot = Slot::new(entry.sim_slot);
        if plan
            .insert(
                sim_slot,
                PlanEntry {
                    eval_slot: Slot::new(entry.eval_slot),
                    import_blocks: entry
                        .import_blocks
                        .into_iter()
                        .map(|block| {
                            Ok(PlanBlockImport {
                                slot: Slot::new(block.slot),
                                root: parse_root(&block.root).with_context(|| {
                                    format!(
                                        "invalid import block root for sim_slot {} slot {}",
                                        sim_slot, block.slot
                                    )
                                })?,
                                canonical: block.canonical,
                            })
                        })
                        .collect::<Result<Vec<_>>>()?,
                    attestation_sources: entry
                        .attestation_sources
                        .into_iter()
                        .map(|source| PlanAttestationSource {
                            slot: Slot::new(source.slot),
                            max_attestation_slot: source.max_attestation_slot.map(Slot::new),
                        })
                        .collect(),
                    source_block_slot: entry.source_block_slot.map(Slot::new),
                },
            )
            .is_some()
        {
            bail!("attestation plan contains duplicate sim_slot {}", sim_slot);
        }
    }

    Ok(plan)
}

fn parse_root(root: &str) -> Result<Hash256> {
    let root = root.strip_prefix("0x").unwrap_or(root);
    if root.len() != 64 {
        bail!(
            "expected 32-byte root hex string, got {} hex chars",
            root.len()
        );
    }
    Hash256::from_str(root).map_err(|e| anyhow::anyhow!("invalid root hex: {:?}", e))
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
