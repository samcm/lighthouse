use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
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
use tracing::{debug, info};
use types::{
    BlockImportSource, ChainSpec, EthSpec, Hash256, MainnetEthSpec, SignedBeaconBlock, Slot,
};

use crate::beacon;
use crate::config::Config;
use crate::era::{EraBlockIterator, EraDownloader};
use crate::output::{OutputWriter, SlotResult};

type T = DiskHarnessType<MainnetEthSpec>;

pub struct WorkerResult {
    pub total_slots: u64,
    pub confirmed: u64,
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

pub struct Engine {
    chain: Arc<BeaconChain<T>>,
    era_blocks: EraBlockIterator,
    output: OutputWriter,
    config: EngineConfig,
    progress: Option<Arc<WorkerProgress>>,
    _runtime: TestRuntime,
    _db_dir: tempfile::TempDir,
}

struct EngineConfig {
    start_slot: Slot,
    end_slot: Slot,
    warmup_start_slot: Slot,
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
        let warmup_start_slot =
            Slot::new(start_epoch.saturating_sub(config.warmup_epochs) * 32);

        info!(
            warmup_start_slot = %warmup_start_slot,
            start_slot = %start_slot,
            end_slot = %end_slot,
            warmup_slots = warmup_start_slot.as_u64().abs_diff(start_slot.as_u64()),
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

        let genesis_state =
            beacon::fetch_genesis_state(&config.beacon_node_url, &cache_dir, &spec)
                .context("failed to fetch genesis state")?;

        // Build headless BeaconChain
        info!("Building headless BeaconChain");
        let (chain, runtime, db_dir) =
            build_chain(checkpoint_state, checkpoint_block, genesis_state, &spec, config)
                .context("failed to build BeaconChain")?;

        // Set up ERA block iterator
        let downloader = EraDownloader::new(&config.era_url, &cache_dir)
            .context("failed to create ERA downloader")?;
        let era_blocks = EraBlockIterator::new(downloader, (*spec).clone());

        // Set up output writer
        let output = OutputWriter::new(&output_path, &config.output_format)
            .context("failed to create output writer")?;

        Ok(Self {
            chain,
            era_blocks,
            output,
            config: EngineConfig {
                start_slot,
                end_slot,
                warmup_start_slot,
            },
            progress,
            _runtime: runtime,
            _db_dir: db_dir,
        })
    }

    pub async fn run(&mut self) -> Result<WorkerResult> {
        let total_slots = self.config.end_slot.as_u64() - self.config.warmup_start_slot.as_u64();
        let recording_start = self.config.start_slot;

        let mut slot = self.config.warmup_start_slot + 1;
        let mut processed = 0u64;
        let mut confirmed_count = 0u64;
        let mut total_recorded = 0u64;
        let batch_start = Instant::now();

        while slot < self.config.end_slot {
            let is_recording = slot >= recording_start;

            self.chain.slot_clock.set_slot(slot.as_u64());

            // Get block for this slot from ERA files
            let block_at_slot = self.era_blocks.block_at_slot(slot)?.cloned();
            let has_block = block_at_slot.is_some();

            // Process block if it exists
            if let Some(ref block) = block_at_slot {
                self.process_block(block)
                    .await
                    .with_context(|| format!("failed to process block at slot {}", slot))?;
            }

            // Recompute head after block processing
            self.chain.recompute_head_at_current_slot().await;

            // Inject attestations from the next block (simulates them arriving during this slot)
            let num_injected = self.inject_next_block_attestations(slot)?;

            // Advance clock and recompute head to trigger FCR with injected attestations
            if num_injected > 0 {
                self.chain.slot_clock.set_slot(slot.as_u64() + 1);
                self.chain.recompute_head_at_current_slot().await;
            }

            if is_recording {
                let result = self.build_slot_result(slot, has_block, num_injected);
                if result.confirmed {
                    confirmed_count += 1;
                }
                total_recorded += 1;
                self.output.write(&result)?;
                self.output.flush_if_needed(total_recorded)?;

                // Update shared progress for the reporter
                if let Some(ref progress) = self.progress {
                    progress.current_slot.store(slot.as_u64(), Ordering::Relaxed);
                    progress.confirmed.store(confirmed_count, Ordering::Relaxed);
                    progress.total_recorded.store(total_recorded, Ordering::Relaxed);
                }
            }

            processed += 1;

            if processed % 1000 == 0 {
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
                    confirmed = confirmed_count,
                    total_recorded,
                    "Progress"
                );
            }

            slot += 1;
        }

        self.output.flush()?;

        let confirm_rate = if total_recorded > 0 {
            confirmed_count as f64 / total_recorded as f64 * 100.0
        } else {
            0.0
        };

        info!(
            total_slots = total_recorded,
            confirmed = confirmed_count,
            confirmation_rate = format!("{:.2}%", confirm_rate),
            duration_secs = format!("{:.1}", batch_start.elapsed().as_secs_f64()),
            "Worker complete"
        );

        Ok(WorkerResult {
            total_slots: total_recorded,
            confirmed: confirmed_count,
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

    /// Inject attestations from the next block into fork choice.
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

        let (block_slot, block) = match next_block {
            Some(b) => b,
            None => return Ok(0),
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
            if let Err(e) = fc.on_attestation(
                inject_slot,
                indexed.to_ref(),
                AttestationFromBlock::True,
            ) {
                debug!(error = ?e, "Failed to inject attestation");
            } else {
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
                let c_slot = fc
                    .get_block(&root)
                    .map(|b| b.slot.as_u64())
                    .unwrap_or(0);

                (root, c_slot)
            } else {
                (Hash256::ZERO, 0)
            };

        // "confirmed" = the head (or within 1 slot of it) has been confirmed by FCR.
        // confirmation_delay_slots == 0 means the head itself is confirmed.
        // confirmation_delay_slots == 1 is normal (FCR confirms the previous slot's block
        // once attestations arrive in the current slot).
        let delay = slot.as_u64().saturating_sub(confirmed_slot);
        let confirmed = confirmed_root != Hash256::ZERO && delay <= 1;

        let epoch = slot.as_u64() / 32;

        SlotResult {
            slot: slot.as_u64(),
            epoch,
            has_block,
            block_root: format!("{:?}", head_root),
            confirmed,
            confirmed_root: format!("{:?}", confirmed_root),
            confirmed_slot,
            confirmation_delay_slots: delay,
            head_root: format!("{:?}", head_root),
            finalized_epoch,
            justified_epoch,
            num_attestations_injected,
            is_epoch_boundary: slot.as_u64() % 32 == 0,
            is_missed_slot: !has_block,
            fcr_eval_duration_us: 0,
        }
    }
}

fn build_chain(
    checkpoint_state: types::BeaconState<MainnetEthSpec>,
    checkpoint_block: SignedBeaconBlock<MainnetEthSpec>,
    genesis_state: types::BeaconState<MainnetEthSpec>,
    spec: &Arc<ChainSpec>,
    config: &Config,
) -> Result<(Arc<BeaconChain<T>>, TestRuntime, tempfile::TempDir)> {
    let runtime = TestRuntime::default();

    let mut spec_mut = (**spec).clone();
    spec_mut.confirmation_byzantine_threshold = config.byzantine_threshold;
    let spec_arc = Arc::new(spec_mut);

    let db_dir = tempfile::tempdir().context("failed to create temp DB directory")?;
    let hot_path = db_dir.path().join("hot");
    let cold_path = db_dir.path().join("cold");
    let blobs_path = db_dir.path().join("blobs");

    let store = HotColdDB::open(
        &hot_path,
        &cold_path,
        &blobs_path,
        |_, _, _| Ok(()),
        StoreConfig::default(),
        spec_arc.clone(),
    )
    .map_err(|e| anyhow::anyhow!("failed to create disk store: {:?}", e))?;

    let (shutdown_tx, _shutdown_rx) = futures::channel::mpsc::channel(1);

    let mut chain_config = ChainConfig::default();
    chain_config.enable_fast_confirmation = true;
    chain_config.ignore_ws_check = true;

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
