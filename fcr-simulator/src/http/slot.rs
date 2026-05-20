use std::str::FromStr;

use anyhow::{Context, Result, bail};
use serde::Deserialize;
use types::{Epoch, Hash256, Slot};

use crate::http::{base_url, build_client};

#[derive(Clone)]
pub struct SlotInstruction {
    pub sim_slot: Slot,
    pub eval_slot: Slot,
    pub import_blocks: Vec<PlanBlockImport>,
    pub attestations: Vec<PlanAttestation>,
}

#[derive(Clone)]
pub struct PlanBlockImport {
    pub slot: Slot,
    pub root: Hash256,
    pub canonical: bool,
}

#[derive(Clone)]
pub struct PlanAttestation {
    pub aggregation_bits: Vec<u8>,
    pub committee_bits: Option<Vec<u8>>,
    pub data: PlanAttestationData,
}

#[derive(Clone)]
pub struct PlanAttestationData {
    pub slot: Slot,
    pub index: u64,
    pub beacon_block_root: Hash256,
    pub source: PlanCheckpoint,
    pub target: PlanCheckpoint,
}

#[derive(Clone)]
pub struct PlanCheckpoint {
    pub epoch: Epoch,
    pub root: Hash256,
}

#[derive(Deserialize)]
struct SlotResponse {
    version: Option<u64>,
    slot: SlotInstructionWire,
}

#[derive(Deserialize)]
struct SlotInstructionWire {
    sim_slot: u64,
    eval_slot: u64,
    import_blocks: Vec<PlanBlockImportWire>,
    attestations: Vec<PlanAttestationWire>,
}

#[derive(Deserialize)]
struct PlanBlockImportWire {
    slot: u64,
    root: String,
    canonical: bool,
}

#[derive(Deserialize)]
struct PlanAttestationWire {
    aggregation_bits: String,
    committee_bits: Option<String>,
    data: PlanAttestationDataWire,
}

#[derive(Deserialize)]
struct PlanAttestationDataWire {
    slot: u64,
    index: u64,
    beacon_block_root: String,
    source: PlanCheckpointWire,
    target: PlanCheckpointWire,
}

#[derive(Deserialize)]
struct PlanCheckpointWire {
    epoch: u64,
    root: String,
}

pub async fn fetch_slot_instruction(
    beacon_node_url: &str,
    sim_slot: Slot,
    warmup_start_slot: Slot,
) -> Result<SlotInstruction> {
    let client = build_client()?;
    let base_url = base_url(beacon_node_url);
    let url = format!(
        "{}/fcr-sim/v1/slot/{}?warmup_start_slot={}",
        base_url,
        sim_slot.as_u64(),
        warmup_start_slot.as_u64()
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

    let slot_response = response
        .json::<SlotResponse>()
        .await
        .context("failed to decode slot instruction JSON")?;

    let version = slot_response
        .version
        .context("slot instruction response is missing version")?;
    if version != 3 {
        bail!(
            "unsupported slot instruction version {}; expected 3",
            version
        );
    }

    let slot = slot_response.slot;
    if slot.sim_slot != sim_slot.as_u64() {
        bail!(
            "slot instruction response sim_slot mismatch: requested {}, got {}",
            sim_slot,
            slot.sim_slot
        );
    }

    Ok(SlotInstruction {
        sim_slot: Slot::new(slot.sim_slot),
        eval_slot: Slot::new(slot.eval_slot),
        import_blocks: slot
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
        attestations: slot
            .attestations
            .into_iter()
            .map(parse_attestation)
            .collect::<Result<Vec<_>>>()?,
    })
}

fn parse_attestation(attestation: PlanAttestationWire) -> Result<PlanAttestation> {
    Ok(PlanAttestation {
        aggregation_bits: parse_hex_bytes(&attestation.aggregation_bits)
            .context("invalid attestation aggregation_bits")?,
        committee_bits: attestation
            .committee_bits
            .as_deref()
            .map(parse_hex_bytes)
            .transpose()
            .context("invalid attestation committee_bits")?,
        data: parse_attestation_data(attestation.data)?,
    })
}

fn parse_attestation_data(data: PlanAttestationDataWire) -> Result<PlanAttestationData> {
    Ok(PlanAttestationData {
        slot: Slot::new(data.slot),
        index: data.index,
        beacon_block_root: parse_root(&data.beacon_block_root)
            .context("invalid attestation beacon_block_root")?,
        source: parse_checkpoint(data.source).context("invalid attestation source checkpoint")?,
        target: parse_checkpoint(data.target).context("invalid attestation target checkpoint")?,
    })
}

fn parse_checkpoint(checkpoint: PlanCheckpointWire) -> Result<PlanCheckpoint> {
    Ok(PlanCheckpoint {
        epoch: Epoch::new(checkpoint.epoch),
        root: parse_root(&checkpoint.root)?,
    })
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

fn parse_hex_bytes(value: &str) -> Result<Vec<u8>> {
    let value = value.strip_prefix("0x").unwrap_or(value);
    if !value.len().is_multiple_of(2) {
        bail!("expected even-length hex string");
    }
    hex::decode(value).map_err(|e| anyhow::anyhow!("invalid hex: {:?}", e))
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
