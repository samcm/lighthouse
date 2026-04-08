use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use anyhow::{Context, Result};
use beacon_chain::builder::BeaconChainBuilder;
use beacon_chain::chain_config::ChainConfig;
use beacon_chain::migrate::MigratorConfig;
use beacon_chain::test_utils::DiskHarnessType;
use beacon_chain::{BeaconChain, NotifyExecutionLayer};
use fork_choice::AttestationFromBlock;
use rand::SeedableRng;
use slot_clock::{SlotClock, TestingSlotClock};
use state_processing::ConsensusContext;
use store::{HotColdDB, StoreConfig};
use task_executor::test_utils::TestRuntime;
use tracing::{debug, info, warn};
use types::{
    BlockImportSource, ChainSpec, Epoch, EthSpec, Hash256, MainnetEthSpec, SignedBeaconBlock, Slot,
};

use crate::beacon;
use crate::config::Config;
use crate::era::{EraBlockIterator, EraDownloader};
use crate::output::{OutputWriter, SlotResult};
use crate::xatu::XatuReader;

type T = DiskHarnessType<MainnetEthSpec>;

pub struct WorkerResult {
    pub total_slots: u64,
    pub duration_secs: f64,
}

/// Shared progress state for a single worker. Updated atomically by the worker,
/// read by the progress reporter.
pub struct WorkerProgress {
    pub worker_id: AtomicU64,
    pub current_slot: AtomicU64,
    pub start_slot: AtomicU64,
    pub end_slot: AtomicU64,
    pub confirmed: AtomicU64,
    pub total_recorded: AtomicU64,
}

impl WorkerProgress {
    pub fn new(worker_id: u64, start_slot: u64, end_slot: u64) -> Self {
        Self {
            worker_id: AtomicU64::new(worker_id),
            current_slot: AtomicU64::new(start_slot),
            start_slot: AtomicU64::new(start_slot),
            end_slot: AtomicU64::new(end_slot),
            confirmed: AtomicU64::new(0),
            total_recorded: AtomicU64::new(0),
        }
    }
}

/// A pending attestation to inject at a future slot.
struct PendingAttestation {
    validator_index: usize,
    block_root: Hash256,
    target_epoch: Epoch,
}

pub struct Engine {
    chain: Arc<BeaconChain<T>>,
    era_blocks: EraBlockIterator,
    output: OutputWriter,
    config: EngineConfig,
    progress: Option<Arc<WorkerProgress>>,
    /// Xatu attestation reader, present when --use-xatu-attestations is set
    xatu_reader: Option<XatuReader>,
    /// Buffer of pending xatu attestations: maps injection_slot -> attestations
    pending_xatu_attestations: BTreeMap<Slot, Vec<PendingAttestation>>,
    _runtime: TestRuntime,
    _db_dir: tempfile::TempDir,
}

struct EngineConfig {
    start_slot: Slot,
    end_slot: Slot,
    warmup_start_slot: Slot,
    use_xatu_attestations: bool,
}

impl Engine {
    /// Create an engine for a specific epoch range (used by parallel workers).
    pub async fn new_for_range(
        config: &Config,
        start_epoch: u64,
        end_epoch: u64,
        output_path: std::path::PathBuf,
        progress: Option<Arc<WorkerProgress>>,
    ) -> Result<Self> {
        let spec = Arc::new(MainnetEthSpec::default_spec());
        let cache_dir = config.resolved_cache_dir();

        let start_slot = Slot::new(start_epoch * 32);
        let end_slot = Slot::new(end_epoch * 32);
        let warmup_start_slot = Slot::new(start_epoch.saturating_sub(config.warmup_epochs) * 32);

        info!(
            warmup_start_slot = %warmup_start_slot,
            start_slot = %start_slot,
            end_slot = %end_slot,
            warmup_slots = warmup_start_slot.as_u64().abs_diff(start_slot.as_u64()),
            use_xatu = config.use_xatu_attestations,
            "Initializing worker"
        );

        // Fetch checkpoint state, block, and genesis state from beacon node
        let checkpoint_state = beacon::fetch_beacon_state(
            &config.beacon_node_url,
            warmup_start_slot,
            &cache_dir,
            &spec,
        )
        .context("failed to fetch checkpoint state")?;

        let checkpoint_block = beacon::fetch_beacon_block(
            &config.beacon_node_url,
            warmup_start_slot,
            &cache_dir,
            &spec,
        )
        .context("failed to fetch checkpoint block")?;

        let genesis_state = beacon::fetch_genesis_state(&config.beacon_node_url, &cache_dir, &spec)
            .context("failed to fetch genesis state")?;

        // Build headless BeaconChain
        info!("Building headless BeaconChain");
        let worker_id = progress
            .as_ref()
            .map(|p| p.worker_id.load(Ordering::Relaxed))
            .unwrap_or(0);
        let (chain, runtime, db_dir) = build_chain(
            checkpoint_state,
            checkpoint_block,
            genesis_state,
            &spec,
            config,
            &cache_dir,
            worker_id,
        )
        .context("failed to build BeaconChain")?;

        // Set up ERA block iterator
        let downloader = EraDownloader::new(&config.era_url, &cache_dir)
            .context("failed to create ERA downloader")?;
        let era_blocks = EraBlockIterator::new(downloader, (*spec).clone());

        // Set up output writer
        let output = OutputWriter::new(&output_path, &config.output_format)
            .context("failed to create output writer")?;

        // Set up xatu reader if requested
        let xatu_reader = if config.use_xatu_attestations {
            Some(XatuReader::new(&cache_dir).context("failed to create xatu reader")?)
        } else {
            None
        };

        Ok(Self {
            chain,
            era_blocks,
            output,
            config: EngineConfig {
                start_slot,
                end_slot,
                warmup_start_slot,
                use_xatu_attestations: config.use_xatu_attestations,
            },
            progress,
            xatu_reader,
            pending_xatu_attestations: BTreeMap::new(),
            _runtime: runtime,
            _db_dir: db_dir,
        })
    }

    pub async fn run(&mut self) -> Result<WorkerResult> {
        let total_slots = self.config.end_slot.as_u64() - self.config.warmup_start_slot.as_u64();
        let recording_start = self.config.start_slot;

        let mut slot = self.config.warmup_start_slot + 1;
        let mut processed = 0u64;
        let mut total_recorded = 0u64;
        let batch_start = Instant::now();

        while slot < self.config.end_slot {
            let is_recording = slot >= recording_start;

            self.chain.slot_clock.set_slot(slot.as_u64());

            // Get block for this slot from ERA files
            let block_at_slot = self.era_blocks.block_at_slot(slot)?.cloned();
            let has_block = block_at_slot.is_some();

            // Process block if it exists
            if let Some(ref block) = block_at_slot
                && let Err(e) = self.process_block(block).await
            {
                tracing::error!(
                    slot = %slot,
                    error = %e,
                    "Failed to process block, skipping"
                );
            }

            // Inject attestations based on the configured source.
            // During warmup, always use ERA attestations to avoid injecting votes
            // for non-canonical block roots that corrupt fork choice state.
            let num_injected = if self.config.use_xatu_attestations && is_recording {
                self.inject_xatu_attestations(slot)?
            } else {
                self.inject_next_block_attestations(slot)?
            };

            // The spec's on_tick_per_slot_after_attestations_applied evaluates FCR
            // at slot N+1 with attestations from slot N applied. This matters because
            // is_one_confirmed computes:
            //   maximum_support = estimate_committee_weight(parent_slot+1, current_slot-1)
            // If current_slot == block_slot, that range is empty, maximum_support = 0,
            // and everything trivially confirms. Evaluating at slot+1 gives the correct
            // non-zero denominator.
            //
            // Note: inject_next_block_attestations already advances fork choice time to
            // slot+1 via on_attestation, so epoch boundary snapshots are handled correctly.
            self.chain.recompute_head_at_slot(slot + 1).await;

            if is_recording {
                let attestation_source = if self.config.use_xatu_attestations {
                    "xatu"
                } else {
                    "era"
                };
                let result =
                    self.build_slot_result(slot, has_block, num_injected, attestation_source);
                total_recorded += 1;
                self.output.write(&result)?;
                self.output.flush_if_needed(total_recorded)?;

                // Update shared progress for the reporter
                if let Some(ref progress) = self.progress {
                    progress
                        .current_slot
                        .store(slot.as_u64(), Ordering::Relaxed);
                    progress.confirmed.store(0, Ordering::Relaxed);
                    progress
                        .total_recorded
                        .store(total_recorded, Ordering::Relaxed);
                }
            }

            processed += 1;

            if processed.is_multiple_of(1000) {
                let elapsed = batch_start.elapsed();
                let slots_per_sec = processed as f64 / elapsed.as_secs_f64();
                let remaining = total_slots.saturating_sub(processed);
                let eta_secs = remaining as f64 / slots_per_sec;
                info!(
                    slot = %slot,
                    processed,
                    total = total_slots,
                    slots_per_sec = format!("{:.1}", slots_per_sec),
                    eta_mins = format!("{:.1}", eta_secs / 60.0),
                    total_recorded,
                    "Progress"
                );
            }

            slot += 1;
        }

        self.output.flush()?;

        info!(
            total_slots = total_recorded,
            duration_secs = format!("{:.1}", batch_start.elapsed().as_secs_f64()),
            "Worker complete"
        );

        Ok(WorkerResult {
            total_slots: total_recorded,
            duration_secs: batch_start.elapsed().as_secs_f64(),
        })
    }

    async fn process_block(&self, block: &SignedBeaconBlock<MainnetEthSpec>) -> Result<()> {
        let block_root = block.canonical_root();
        let block_arc = Arc::new(block.clone());

        // Create AvailableBlock without DA checks - we're replaying historical data
        // without blob sidecars. Safe because we only care about consensus/FCR.
        let available_block =
            beacon_chain::data_availability_checker::AvailableBlock::new_without_da_check(
                block_arc,
                self.chain.spec.clone(),
            );
        let range_sync_block =
            beacon_chain::block_verification_types::RangeSyncBlock::from_available_block(
                available_block,
            );

        self.chain
            .process_block(
                block_root,
                range_sync_block,
                NotifyExecutionLayer::No,
                BlockImportSource::RangeSync,
                || Ok(()),
            )
            .await
            .map_err(|e| anyhow::anyhow!("block import failed: {:?}", e))?;

        Ok(())
    }

    /// Inject attestations from the next block into fork choice (ERA-based approach).
    ///
    /// This simulates what a real node would see: the next block's attestations
    /// arriving during the current slot. If the next slot is missed, we skip
    /// ahead to find the next actual block (handling consecutive missed slots).
    fn inject_next_block_attestations(&mut self, current_slot: Slot) -> Result<u64> {
        // Find the next block. Usually N+1, but skip missed slots.
        let max_missed = 4u64; // handle up to 4 consecutive missed slots
        let mut next_block = None;

        for offset in 1..=max_missed {
            let peek_slot = current_slot + offset;
            if let Some(block) = self.era_blocks.peek_next_block(peek_slot)? {
                next_block = Some((peek_slot, block.clone()));
                break;
            }
        }

        let Some((block_slot, block)) = next_block else {
            return Ok(0);
        };

        let attestation_count = block.message().body().attestations_len();
        if attestation_count == 0 {
            return Ok(0);
        }

        let head_snapshot = self.chain.head_snapshot();
        let head_state = &head_snapshot.beacon_state;
        let mut ctxt = ConsensusContext::new(block_slot);

        // Inject ALL attestations from the next block - this is what a real node
        // would see arriving during the current slot. No slot filtering.
        let mut indexed_attestations = Vec::with_capacity(attestation_count);
        for attestation in block.message().body().attestations() {
            match ctxt.get_indexed_attestation(head_state, attestation) {
                Ok(indexed_ref) => {
                    indexed_attestations.push(indexed_ref.clone_as_indexed_attestation());
                }
                Err(e) => {
                    debug!(error = ?e, "Failed to get indexed attestation");
                }
            }
        }
        drop(head_snapshot);

        if indexed_attestations.is_empty() {
            return Ok(0);
        }

        let inject_slot = current_slot + 1;
        let mut fc = self.chain.canonical_head.fork_choice_write_lock();
        let mut injected = 0u64;

        for indexed in &indexed_attestations {
            if let Err(e) =
                fc.on_attestation(inject_slot, indexed.to_ref(), AttestationFromBlock::True)
            {
                let target_root = indexed.to_ref().data().target.root;
                let target_in_fc = fc.proto_array().contains_block(&target_root);
                let head_root = indexed.to_ref().data().beacon_block_root;
                let head_in_fc = fc.proto_array().contains_block(&head_root);
                let finalized = fc.finalized_checkpoint();
                warn!(
                    error = ?e,
                    %current_slot,
                    ?target_root,
                    target_in_fc,
                    ?head_root,
                    head_in_fc,
                    finalized_epoch = %finalized.epoch,
                    finalized_root = ?finalized.root,
                    "Failed to inject attestation"
                );
            } else {
                injected += 1;
            }
        }

        Ok(injected)
    }

    /// Inject attestations using xatu timing data.
    ///
    /// For each slot, we:
    /// 1. Drain any pending attestations that should arrive at this slot
    /// 2. Look up xatu data for this slot's committee assignments
    /// 3. For offset=0 attestations, inject immediately
    /// 4. For offset>0 attestations, buffer them for injection at slot + offset
    fn inject_xatu_attestations(&mut self, current_slot: Slot) -> Result<u64> {
        let mut injected = 0u64;

        // Step 1: Drain pending attestations for this slot
        let inject_slot = current_slot + 1;
        if let Some(pending) = self.pending_xatu_attestations.remove(&current_slot) {
            let mut fc = self.chain.canonical_head.fork_choice_write_lock();
            if let Err(e) = fc.update_time(inject_slot) {
                debug!(error = ?e, "Failed to update fork choice time for pending attestations");
            }
            for att in &pending {
                fc.proto_array_mut()
                    .process_attestation(att.validator_index, att.block_root, att.target_epoch)
                    .map_err(|e| {
                        debug!(error = ?e, validator = att.validator_index, "Failed to inject pending xatu attestation");
                    })
                    .ok();
                injected += 1;
            }
            drop(fc);
        }

        // Step 2: Look up xatu data for this slot
        let Some(xatu_reader) = self.xatu_reader.as_mut() else {
            return Ok(injected);
        };

        let slot_data = match xatu_reader.get_slot_data(current_slot.as_u64())? {
            Some(data) => data.clone(),
            None => return Ok(injected),
        };

        // Step 3: Get committee assignments for this slot from the beacon state
        let head_snapshot = self.chain.head_snapshot();
        let head_state = &head_snapshot.beacon_state;

        let committees = match head_state.get_beacon_committees_at_slot(current_slot) {
            Ok(c) => c,
            Err(e) => {
                warn!(
                    slot = %current_slot,
                    error = ?e,
                    "Failed to get committees for xatu attestation injection"
                );
                return Ok(injected);
            }
        };

        // Build flat validator list matching the xatu parquet committee ordering.
        // v1 parquets sorted committee_index lexicographically ("0","1","10",...,"2","20",...),
        // so we must iterate committees in the same order.
        let mut indexed_committees: Vec<(usize, &[usize])> = committees
            .iter()
            .map(|c| (c.index as usize, c.committee))
            .collect();
        indexed_committees.sort_by(|a, b| a.0.to_string().cmp(&b.0.to_string()));

        let mut validators_in_order: Vec<usize> = Vec::new();
        for (_, committee) in &indexed_committees {
            validators_in_order.extend_from_slice(committee);
        }
        drop(head_snapshot);

        // Verify lengths match
        if validators_in_order.len() != slot_data.slot_offsets.len() {
            warn!(
                slot = %current_slot,
                committee_size = validators_in_order.len(),
                xatu_size = slot_data.slot_offsets.len(),
                "Committee size mismatch with xatu data, skipping slot"
            );
            return Ok(injected);
        }

        // Step 4: Process each committee position
        let mut immediate = Vec::new();
        for (pos, &validator_index) in validators_in_order.iter().enumerate() {
            let offset = match slot_data.slot_offsets.get(pos) {
                Some(&255) | None => continue,
                Some(&o) => o,
            };

            let vote_id = match slot_data.vote_ids.get(pos) {
                Some(&255) | None => continue,
                Some(&id) => id as usize,
            };

            let Some(vote) = slot_data.votes.get(vote_id) else {
                continue;
            };

            let target_epoch = Epoch::new(vote.target_epoch as u64);
            let block_root = vote.head_root;

            if offset == 0 {
                // Inject immediately
                immediate.push(PendingAttestation {
                    validator_index,
                    block_root,
                    target_epoch,
                });
            } else {
                // Buffer for later injection
                let future_slot = current_slot + offset as u64;
                self.pending_xatu_attestations
                    .entry(future_slot)
                    .or_default()
                    .push(PendingAttestation {
                        validator_index,
                        block_root,
                        target_epoch,
                    });
            }
        }

        // Inject immediate attestations
        if !immediate.is_empty() {
            let mut fc = self.chain.canonical_head.fork_choice_write_lock();
            if let Err(e) = fc.update_time(inject_slot) {
                debug!(error = ?e, "Failed to update fork choice time for xatu attestations");
            }
            for att in &immediate {
                fc.proto_array_mut()
                    .process_attestation(att.validator_index, att.block_root, att.target_epoch)
                    .map_err(|e| {
                        debug!(error = ?e, validator = att.validator_index, "Failed to inject xatu attestation");
                    })
                    .ok();
                injected += 1;
            }
        }

        Ok(injected)
    }

    fn build_slot_result(
        &mut self,
        slot: Slot,
        has_block: bool,
        num_attestations_injected: u64,
        attestation_source: &str,
    ) -> SlotResult {
        let head = self.chain.head();
        let head_root = head.head_block_root();
        let finalized_epoch = head.finalized_checkpoint().epoch.as_u64();
        let justified_epoch = head.justified_checkpoint().epoch.as_u64();

        // Read FCR state
        let (confirmed_root, confirmed_slot) =
            if let Some(ref fcr_mutex) = self.chain.canonical_head.fast_confirmation {
                let fcr = fcr_mutex.lock();
                let root = fcr.confirmed_root;
                let fc = self.chain.canonical_head.fork_choice_read_lock();
                let c_slot = fc.get_block(&root).map(|b| b.slot.as_u64()).unwrap_or(0);

                (root, c_slot)
            } else {
                (Hash256::ZERO, 0)
            };

        let delay = slot.as_u64().saturating_sub(confirmed_slot);
        let epoch = slot.as_u64() / 32;

        SlotResult {
            slot: slot.as_u64(),
            epoch,
            has_block,
            block_root: format!("{:?}", head_root),
            confirmed_root: format!("{:?}", confirmed_root),
            confirmed_slot,
            confirmation_delay_slots: delay,
            head_root: format!("{:?}", head_root),
            finalized_epoch,
            justified_epoch,
            num_attestations_injected,
            is_epoch_boundary: slot.as_u64().is_multiple_of(32),
            is_missed_slot: !has_block,
            fcr_eval_duration_us: 0,
            attestation_source: attestation_source.to_string(),
        }
    }
}

fn build_chain(
    checkpoint_state: types::BeaconState<MainnetEthSpec>,
    checkpoint_block: SignedBeaconBlock<MainnetEthSpec>,
    genesis_state: types::BeaconState<MainnetEthSpec>,
    spec: &Arc<ChainSpec>,
    config: &Config,
    cache_dir: &std::path::Path,
    _worker_id: u64,
) -> Result<(Arc<BeaconChain<T>>, TestRuntime, tempfile::TempDir)> {
    let runtime = TestRuntime::default();

    let mut spec_mut = (**spec).clone();
    spec_mut.confirmation_byzantine_threshold = config.byzantine_threshold;
    let spec_arc = Arc::new(spec_mut);

    // Use cache dir for DB storage, not system temp (macOS cleans /tmp aggressively)
    let db_base = cache_dir.join("db");
    std::fs::create_dir_all(&db_base)?;
    let db_dir = tempfile::tempdir_in(&db_base).context("failed to create DB directory")?;
    let hot_path = db_dir.path().join("hot");
    let cold_path = db_dir.path().join("cold");
    let blobs_path = db_dir.path().join("blobs");

    let store = HotColdDB::open(
        &hot_path,
        &cold_path,
        &blobs_path,
        |_, _, _| Ok(()),
        StoreConfig {
            skip_disk_writes: true,
            ..StoreConfig::default()
        },
        spec_arc.clone(),
    )
    .map_err(|e| anyhow::anyhow!("failed to create disk store: {:?}", e))?;

    let (shutdown_tx, _shutdown_rx) = futures::channel::mpsc::channel(1);

    let chain_config = ChainConfig {
        enable_fast_confirmation: true,
        ignore_ws_check: true,
        ..ChainConfig::default()
    };

    let kzg = kzg::Kzg::new_from_trusted_setup(&kzg::trusted_setup::get_trusted_setup())
        .map_err(|e| anyhow::anyhow!("failed to create KZG: {:?}", e))?;

    let checkpoint_slot = checkpoint_state.slot();

    let builder = BeaconChainBuilder::new(MainnetEthSpec, Arc::new(kzg))
        .custom_spec(spec_arc)
        .store(store)
        .store_migrator_config(MigratorConfig::default().blocking())
        .task_executor(runtime.task_executor.clone())
        .shutdown_sender(shutdown_tx)
        .chain_config(chain_config)
        .rng(Box::new(rand::rngs::StdRng::seed_from_u64(42)))
        .ordered_custody_column_indices(vec![])
        .weak_subjectivity_state(checkpoint_state, checkpoint_block, None, genesis_state)
        .map_err(|e| anyhow::anyhow!("weak_subjectivity_state failed: {}", e))?;

    // Create slot clock at checkpoint slot - build() calls slot_clock.now() internally
    let slot_clock = TestingSlotClock::new(
        checkpoint_slot,
        std::time::Duration::from_secs(0),
        spec.get_slot_duration(),
    );

    let chain = builder
        .slot_clock(slot_clock)
        .build()
        .map_err(|e| anyhow::anyhow!("BeaconChain build failed: {}", e))?;

    info!(checkpoint_slot = %checkpoint_slot, "BeaconChain initialized from checkpoint");

    Ok((Arc::new(chain), runtime, db_dir))
}
