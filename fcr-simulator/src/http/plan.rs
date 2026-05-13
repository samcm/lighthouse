use std::collections::HashMap;

use anyhow::{Context, Result, bail};
use serde::Deserialize;
use types::Slot;

use crate::http::{base_url, build_client};

pub type AttestationPlan = HashMap<Slot, Option<Slot>>;

#[derive(Deserialize)]
struct PlanResponse {
    entries: Vec<PlanEntry>,
}

#[derive(Deserialize)]
struct PlanEntry {
    sim_slot: u64,
    source_block_slot: Option<u64>,
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

    Ok(plan_response
        .entries
        .into_iter()
        .map(|entry| {
            (
                Slot::new(entry.sim_slot),
                entry.source_block_slot.map(Slot::new),
            )
        })
        .collect())
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
