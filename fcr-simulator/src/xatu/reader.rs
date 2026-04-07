use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use arrow::array::{
    Array, ListArray, StringArray, StructArray, UInt8Array, UInt32Array, UInt64Array,
};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use tracing::{debug, warn};
use types::Hash256;

use super::downloader::XatuDownloader;

/// A distinct vote tuple from xatu data.
#[derive(Debug, Clone)]
pub struct Vote {
    pub head_root: Hash256,
    pub target_epoch: u32,
}

/// Per-slot attestation timing data from xatu.
#[derive(Debug, Clone)]
pub struct SlotAttestationData {
    /// One entry per committee position. 255 = not seen.
    /// Otherwise N = attestation first seen N slots after duty slot.
    pub slot_offsets: Vec<u8>,
    /// One entry per committee position. 255 = no vote. Otherwise index into `votes`.
    pub vote_ids: Vec<u8>,
    /// Distinct vote tuples for this slot.
    pub votes: Vec<Vote>,
}

/// Reads and caches xatu attestation timing data from parquet files.
pub struct XatuReader {
    downloader: XatuDownloader,
    /// Loaded data keyed by slot.
    slot_data: HashMap<u64, SlotAttestationData>,
    /// Currently loaded date string to avoid re-parsing.
    loaded_date: Option<String>,
}

impl XatuReader {
    pub fn new(cache_dir: &Path) -> Result<Self> {
        let downloader = XatuDownloader::new(cache_dir)?;
        Ok(Self {
            downloader,
            slot_data: HashMap::new(),
            loaded_date: None,
        })
    }

    /// Get attestation timing data for a given slot.
    /// Downloads and parses the appropriate daily parquet file on demand.
    pub fn get_slot_data(&mut self, slot: u64) -> Result<Option<&SlotAttestationData>> {
        let date = slot_to_date(slot);

        // Load the file if we haven't loaded this date yet
        if self.loaded_date.as_deref() != Some(&date) {
            self.load_date(&date)?;
        }

        Ok(self.slot_data.get(&slot))
    }

    fn load_date(&mut self, date: &str) -> Result<()> {
        let path = match self.downloader.get_parquet(date) {
            Ok(p) => p,
            Err(e) => {
                warn!(date, error = %e, "Failed to get xatu data for date, will have no xatu attestations for this day");
                self.slot_data.clear();
                self.loaded_date = Some(date.to_string());
                return Ok(());
            }
        };

        self.slot_data = parse_parquet_file(&path)
            .with_context(|| format!("failed to parse xatu parquet for {}", date))?;

        debug!(
            date,
            slots = self.slot_data.len(),
            "Loaded xatu attestation data"
        );
        self.loaded_date = Some(date.to_string());
        Ok(())
    }
}

/// Convert a slot number to the date string (YYYY-MM-DD) for the parquet file.
/// Slots map to dates based on the Beacon Chain genesis time (Dec 1, 2020 12:00:07 UTC).
fn slot_to_date(slot: u64) -> String {
    const GENESIS_TIME: i64 = 1606824023; // Dec 1, 2020 12:00:23 UTC
    const SLOT_DURATION: i64 = 12;

    let timestamp = GENESIS_TIME + (slot as i64) * SLOT_DURATION;
    let dt =
        chrono::DateTime::from_timestamp(timestamp, 0).expect("slot timestamp should be valid");
    dt.format("%Y-%m-%d").to_string()
}

/// Parse a parquet file and extract all slot attestation data.
fn parse_parquet_file(path: &Path) -> Result<HashMap<u64, SlotAttestationData>> {
    let file = std::fs::File::open(path)
        .with_context(|| format!("failed to open parquet file: {}", path.display()))?;

    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .context("failed to create parquet reader")?;
    let reader = builder.build().context("failed to build parquet reader")?;

    let mut slot_data = HashMap::new();

    for batch_result in reader {
        let batch = batch_result.context("failed to read parquet batch")?;
        let num_rows = batch.num_rows();
        let schema = batch.schema();

        // Get column indices
        let slot_idx = schema.index_of("slot").context("missing 'slot' column")?;
        let offsets_idx = schema
            .index_of("slot_offsets")
            .context("missing 'slot_offsets' column")?;
        let vote_ids_idx = schema
            .index_of("vote_ids")
            .context("missing 'vote_ids' column")?;
        let votes_idx = schema.index_of("votes").context("missing 'votes' column")?;

        let slot_col = batch
            .column(slot_idx)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .context("'slot' column is not UInt64")?;

        let offsets_col = batch
            .column(offsets_idx)
            .as_any()
            .downcast_ref::<ListArray>()
            .context("'slot_offsets' column is not a List")?;

        let vote_ids_col = batch
            .column(vote_ids_idx)
            .as_any()
            .downcast_ref::<ListArray>()
            .context("'vote_ids' column is not a List")?;

        let votes_col = batch
            .column(votes_idx)
            .as_any()
            .downcast_ref::<ListArray>()
            .context("'votes' column is not a List")?;

        for row in 0..num_rows {
            let slot = slot_col.value(row);

            // Parse slot_offsets: list[u8]
            let offsets_arr = offsets_col.value(row);
            let slot_offsets = extract_u8_list(&offsets_arr)
                .with_context(|| format!("failed to parse slot_offsets for slot {}", slot))?;

            // Parse vote_ids: list[u8]
            let vote_ids_arr = vote_ids_col.value(row);
            let vote_ids = extract_u8_list(&vote_ids_arr)
                .with_context(|| format!("failed to parse vote_ids for slot {}", slot))?;

            // Parse votes: list[struct{head_root, source_epoch, source_root, target_epoch, target_root}]
            let votes_arr = votes_col.value(row);
            let votes = extract_votes(&votes_arr)
                .with_context(|| format!("failed to parse votes for slot {}", slot))?;

            slot_data.insert(
                slot,
                SlotAttestationData {
                    slot_offsets,
                    vote_ids,
                    votes,
                },
            );
        }
    }

    Ok(slot_data)
}

/// Extract a Vec<u8> from an Arrow array that should be a UInt8Array.
fn extract_u8_list(arr: &Arc<dyn Array>) -> Result<Vec<u8>> {
    let u8_arr = arr
        .as_any()
        .downcast_ref::<UInt8Array>()
        .context("expected UInt8Array for list elements")?;
    Ok(u8_arr.values().to_vec())
}

/// Extract vote structs from an Arrow StructArray.
fn extract_votes(arr: &Arc<dyn Array>) -> Result<Vec<Vote>> {
    let struct_arr = arr
        .as_any()
        .downcast_ref::<StructArray>()
        .context("expected StructArray for votes")?;

    let num_votes = struct_arr.len();
    if num_votes == 0 {
        return Ok(vec![]);
    }

    // Get the head_root column
    let head_root_col = struct_arr
        .column_by_name("head_root")
        .context("votes struct missing 'head_root' field")?;
    let head_roots = head_root_col
        .as_any()
        .downcast_ref::<StringArray>()
        .context("'head_root' field is not String")?;

    // Get the target_epoch column
    let target_epoch_col = struct_arr
        .column_by_name("target_epoch")
        .context("votes struct missing 'target_epoch' field")?;
    let target_epochs = target_epoch_col
        .as_any()
        .downcast_ref::<UInt32Array>()
        .context("'target_epoch' field is not UInt32")?;

    let mut votes = Vec::with_capacity(num_votes);
    for i in 0..num_votes {
        let head_root_str = head_roots.value(i);
        let head_root = parse_hex_hash(head_root_str)
            .with_context(|| format!("failed to parse head_root: {}", head_root_str))?;

        votes.push(Vote {
            head_root,
            target_epoch: target_epochs.value(i),
        });
    }

    Ok(votes)
}

/// Parse a hex string (with or without 0x prefix) into a Hash256.
fn parse_hex_hash(s: &str) -> Result<Hash256> {
    let s = s.strip_prefix("0x").unwrap_or(s);
    if s.len() != 64 {
        bail!("expected 64 hex chars, got {}: '{}'", s.len(), s);
    }
    let bytes = hex::decode(s).context("invalid hex")?;
    Ok(Hash256::from_slice(&bytes))
}
