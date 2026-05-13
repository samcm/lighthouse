use serde::Serialize;

#[derive(Debug, Serialize)]
pub struct SlotResult {
    pub slot: u64,
    pub epoch: u64,
    pub has_block: bool,
    pub block_root: Option<String>,

    // FCR
    pub head_root: String,
    pub confirmed_root: String,
    pub confirmed_slot: u64,
    pub confirmation_delay_slots: u64,
    pub fast_confirmed: bool,
    pub strict_one_slot_confirmed: bool,
    pub finalized_epoch: u64,
    pub justified_epoch: u64,

    // Attestation plan
    pub source_block_slot: Option<u64>,
    pub num_attestations_injected: u64,

    // Meta
    pub is_epoch_boundary: bool,
    pub is_missed_slot: bool,
    pub fcr_eval_duration_us: u64,
}
