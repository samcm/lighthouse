use serde::Serialize;

#[derive(Debug, Serialize)]
pub struct SlotResult {
    pub slot: u64,
    pub epoch: u64,
    pub has_block: bool,
    pub block_root: String,

    // FCR
    pub confirmed: bool,
    pub confirmed_root: String,
    pub confirmed_slot: u64,
    pub confirmation_delay_slots: u64,

    // Fork choice
    pub head_root: String,
    pub finalized_epoch: u64,
    pub justified_epoch: u64,

    // Coverage
    pub num_attestations_injected: u64,

    // Meta
    pub is_epoch_boundary: bool,
    pub is_missed_slot: bool,
    pub fcr_eval_duration_us: u64,
    pub attestation_source: String,
}
