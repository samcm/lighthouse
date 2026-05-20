use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, Result, bail};
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
    BeaconState, BlockImportSource, ChainSpec, EthSpec, Hash256, MainnetEthSpec, SignedBeaconBlock,
    Slot,
};

use crate::config::{Config, Network};
use crate::http::{
    AttestationPlan, BeaconFetcher, PlanAttestationSource, PlanBlockImport, fetch_attestation_plan,
};
use crate::output::{OutputWriter, SlotResult};

type T = DiskHarnessType<MainnetEthSpec>;

pub struct Engine {
    chain: Arc<BeaconChain<T>>,
    block_fetcher: BeaconFetcher,
    block_cache: HashMap<Slot, Option<SignedBeaconBlock<MainnetEthSpec>>>,
    plan: AttestationPlan,
    output: OutputWriter,
    config: EngineConfig,
    _runtime: TestRuntime,
    _db_dir: tempfile::TempDir,
}

struct EngineConfig {
    start_slot: Slot,
    end_slot: Slot,
    warmup_start_slot: Slot,
}

impl Engine {
    pub async fn new(config: &Config) -> Result<Self> {
        let spec = Arc::new(match config.network() {
            Network::Mainnet => MainnetEthSpec::default_spec(),
        });

        let start_slot = Slot::new(config.start_slot());
        let end_slot = Slot::new(config.end_slot());
        let warmup_start_slot = Slot::new(config.warmup_start_slot());

        info!(
            %warmup_start_slot,
            %start_slot,
            %end_slot,
            "Initializing Lighthouse FCR engine"
        );

        let block_fetcher = BeaconFetcher::new(config.beacon_node_url(), spec.clone())
            .context("failed to create HTTP block fetcher")?;

        let mut checkpoint_state = block_fetcher
            .fetch_checkpoint_state(warmup_start_slot)
            .await
            .context("failed to fetch checkpoint state")?;
        let checkpoint_state_root = checkpoint_state
            .canonical_root()
            .map_err(|e| anyhow::anyhow!("failed to calculate checkpoint state root: {:?}", e))?;
        let checkpoint_block_root = checkpoint_state.get_latest_block_root(checkpoint_state_root);

        let checkpoint_block = block_fetcher
            .fetch_block_by_root(checkpoint_block_root)
            .await
            .with_context(|| {
                format!(
                    "failed to fetch checkpoint block by root {:?}",
                    checkpoint_block_root
                )
            })?;

        let genesis_state = block_fetcher
            .fetch_genesis_state()
            .await
            .context("failed to fetch genesis state")?;

        info!("Building headless BeaconChain");
        let (chain, runtime, db_dir) = build_chain(
            checkpoint_state,
            checkpoint_block,
            genesis_state,
            &spec,
            config,
        )
        .context("failed to build BeaconChain")?;

        let plan =
            fetch_attestation_plan(config.beacon_node_url(), warmup_start_slot + 1, end_slot)
                .await
                .context("failed to fetch attestation plan")?;

        let mut sim_slot = warmup_start_slot + 1;
        while sim_slot < end_slot {
            if !plan.contains_key(&sim_slot) {
                bail!(
                    "attestation plan is missing sim_slot {}; orchestrator served an incomplete plan",
                    sim_slot
                );
            }
            sim_slot += 1;
        }

        let output =
            OutputWriter::new(config.output()).context("failed to create JSONL output writer")?;

        Ok(Self {
            chain,
            block_fetcher,
            block_cache: HashMap::new(),
            plan,
            output,
            config: EngineConfig {
                start_slot,
                end_slot,
                warmup_start_slot,
            },
            _runtime: runtime,
            _db_dir: db_dir,
        })
    }

    pub async fn run(&mut self) -> Result<()> {
        let mut slot = self.config.warmup_start_slot + 1;
        let mut total_recorded = 0u64;
        let run_start = Instant::now();

        while slot < self.config.end_slot {
            let is_recording = slot >= self.config.start_slot;

            self.chain.slot_clock.set_slot(slot.as_u64());

            let plan_entry = self
                .plan
                .get(&slot)
                .cloned()
                .with_context(|| format!("attestation plan is missing sim_slot {}", slot))?;

            let (has_block, block_root) = self
                .import_planned_blocks(slot, &plan_entry.import_blocks)
                .await
                .with_context(|| {
                    format!("failed to import planned blocks for sim slot {}", slot)
                })?;

            let source_slot = plan_entry.source_block_slot.or_else(|| {
                plan_entry
                    .attestation_sources
                    .first()
                    .map(|source| source.slot)
            });
            let num_attestations_injected = self
                .inject_attestations_from_sources(slot, &plan_entry.attestation_sources)
                .await
                .with_context(|| {
                    let source_slots = plan_entry
                        .attestation_sources
                        .iter()
                        .map(|source| source.slot.as_u64())
                        .collect::<Vec<_>>();
                    format!(
                        "failed to inject attestations for sim slot {} from source blocks {:?}",
                        slot, source_slots
                    )
                })?;

            let fcr_eval_start = Instant::now();
            self.chain
                .recompute_head_at_slot(plan_entry.eval_slot)
                .await;
            let fcr_eval_duration_us = micros_since(fcr_eval_start);

            if is_recording {
                let result = self.build_slot_result(
                    slot,
                    has_block,
                    block_root,
                    source_slot,
                    num_attestations_injected,
                    fcr_eval_duration_us,
                );
                total_recorded += 1;
                self.output.write(&result)?;
                self.output.flush_if_needed(total_recorded)?;
            }

            slot += 1;
        }

        self.output.flush()?;

        info!(
            total_slots = total_recorded,
            duration_secs = format!("{:.1}", run_start.elapsed().as_secs_f64()),
            "Simulation complete"
        );

        Ok(())
    }

    async fn get_block_at_slot(
        &mut self,
        slot: Slot,
    ) -> Result<Option<SignedBeaconBlock<MainnetEthSpec>>> {
        if let Some(block) = self.block_cache.get(&slot) {
            return Ok(block.clone());
        }

        let block = self.block_fetcher.fetch_block_at_slot(slot).await?;
        self.block_cache.insert(slot, block.clone());

        Ok(block)
    }

    async fn import_planned_blocks(
        &mut self,
        sim_slot: Slot,
        imports: &[PlanBlockImport],
    ) -> Result<(bool, Option<Hash256>)> {
        let canonical_root = imports
            .iter()
            .find(|import| import.canonical && import.slot == sim_slot)
            .map(|import| import.root);

        for import in imports {
            let Some(block) = self.fetch_planned_block(import).await? else {
                continue;
            };

            let block_root = block.canonical_root();
            if block_root != import.root {
                bail!(
                    "planned block root mismatch for slot {}: plan {:?}, fetched {:?}",
                    import.slot,
                    import.root,
                    block_root
                );
            }
            if block.slot() != import.slot {
                bail!(
                    "planned block slot mismatch for root {:?}: plan {}, fetched {}",
                    import.root,
                    import.slot,
                    block.slot()
                );
            }

            if let Err(e) = self.process_block(&block).await {
                if import.canonical {
                    return Err(e).with_context(|| {
                        format!("failed to process canonical block at {}", sim_slot)
                    });
                }
                warn!(
                    block_root = ?import.root,
                    slot = %import.slot,
                    error = %format!("{e:#}"),
                    "planned non-canonical block import failed"
                );
            }
        }

        Ok((canonical_root.is_some(), canonical_root))
    }

    async fn fetch_planned_block(
        &mut self,
        import: &PlanBlockImport,
    ) -> Result<Option<SignedBeaconBlock<MainnetEthSpec>>> {
        if import.canonical {
            let block = self.get_block_at_slot(import.slot).await?;
            return block.map(Some).with_context(|| {
                format!(
                    "planned canonical block at slot {} was not found",
                    import.slot
                )
            });
        }

        match self
            .block_fetcher
            .fetch_block_by_root_optional(import.root)
            .await?
        {
            Some(block) => Ok(Some(block)),
            None => {
                warn!(
                    block_root = ?import.root,
                    slot = %import.slot,
                    "planned non-canonical block was not found; skipping"
                );
                Ok(None)
            }
        }
    }

    async fn process_block(&self, block: &SignedBeaconBlock<MainnetEthSpec>) -> Result<()> {
        let block_root = block.canonical_root();
        let block_arc = Arc::new(block.clone());

        // Historical replay skips DA and EL integration. Blocks are canonical-chain inputs.
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

    async fn inject_attestations_from_sources(
        &mut self,
        sim_slot: Slot,
        sources: &[PlanAttestationSource],
    ) -> Result<u64> {
        let mut injected = 0;
        for source in sources {
            injected += self
                .inject_attestations_from_source(sim_slot, source)
                .await?;
        }
        Ok(injected)
    }

    async fn inject_attestations_from_source(
        &mut self,
        sim_slot: Slot,
        source: &PlanAttestationSource,
    ) -> Result<u64> {
        let source_block_slot = source.slot;
        let Some(block) = self.get_block_at_slot(source_block_slot).await? else {
            bail!(
                "attestation plan referenced missing source block at slot {}",
                source_block_slot
            );
        };

        let attestation_count = block.message().body().attestations_len();
        if attestation_count == 0 {
            return Ok(0);
        }

        let head_snapshot = self.chain.head_snapshot();
        let head_state = &head_snapshot.beacon_state;
        let mut ctxt = ConsensusContext::new(source_block_slot);

        let mut indexed_attestations = Vec::with_capacity(attestation_count);
        for attestation in block.message().body().attestations() {
            if let Some(max_slot) = source.max_attestation_slot {
                if attestation.data().slot > max_slot {
                    continue;
                }
            }
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

        let inject_slot = sim_slot + 1;
        let mut fc = self.chain.canonical_head.fork_choice_write_lock();
        let mut injected = 0;

        let spec = self.chain.spec.clone();
        for indexed in &indexed_attestations {
            let data = indexed.data();
            let target_root = data.target.root;

            if let Err(e) = fc.on_attestation(
                inject_slot,
                indexed.to_ref(),
                AttestationFromBlock::True,
                &spec,
            ) {
                let target_in_fc = fc.proto_array().contains_block(&target_root);
                let head_root = indexed.to_ref().data().beacon_block_root;
                let head_in_fc = fc.proto_array().contains_block(&head_root);
                let finalized = fc.finalized_checkpoint();
                warn!(
                    error = ?e,
                    %sim_slot,
                    %source_block_slot,
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

    fn build_slot_result(
        &self,
        slot: Slot,
        has_block: bool,
        block_root: Option<Hash256>,
        source_block_slot: Option<Slot>,
        num_attestations_injected: u64,
        fcr_eval_duration_us: u64,
    ) -> SlotResult {
        let head = self.chain.head();
        let head_root = head.head_block_root();
        let finalized_epoch = head.finalized_checkpoint().epoch.as_u64();
        let justified_epoch = head.justified_checkpoint().epoch.as_u64();

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

        let fast_confirmed = confirmed_root != Hash256::ZERO && confirmed_slot == slot.as_u64();
        let eval_slot = slot.as_u64().saturating_add(1);
        let delay = eval_slot.saturating_sub(confirmed_slot);
        let strict_one_slot_confirmed = has_block
            && confirmed_root != Hash256::ZERO
            && block_root == Some(confirmed_root)
            && confirmed_slot == slot.as_u64()
            && delay == 1;

        SlotResult {
            slot: slot.as_u64(),
            epoch: slot.as_u64() / 32,
            has_block,
            block_root: block_root.map(|root| format!("{:?}", root)),
            head_root: format!("{:?}", head_root),
            confirmed_root: format!("{:?}", confirmed_root),
            confirmed_slot,
            confirmation_delay_slots: delay,
            fast_confirmed,
            strict_one_slot_confirmed,
            finalized_epoch,
            justified_epoch,
            source_block_slot: source_block_slot.map(|slot| slot.as_u64()),
            num_attestations_injected,
            is_epoch_boundary: slot.as_u64().is_multiple_of(32),
            is_missed_slot: !has_block,
            fcr_eval_duration_us,
        }
    }
}

fn build_chain(
    checkpoint_state: BeaconState<MainnetEthSpec>,
    checkpoint_block: SignedBeaconBlock<MainnetEthSpec>,
    genesis_state: BeaconState<MainnetEthSpec>,
    spec: &Arc<ChainSpec>,
    config: &Config,
) -> Result<(Arc<BeaconChain<T>>, TestRuntime, tempfile::TempDir)> {
    let runtime = TestRuntime::default();

    let mut spec_mut = (**spec).clone();
    spec_mut.confirmation_byzantine_threshold = config.byzantine_threshold;
    let spec_arc = Arc::new(spec_mut);

    let db_dir = tempfile::tempdir().context("failed to create DB directory")?;
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

    let slot_clock = TestingSlotClock::new(
        checkpoint_slot,
        std::time::Duration::from_secs(0),
        spec.get_slot_duration(),
    );

    let chain = builder
        .slot_clock(slot_clock)
        .build()
        .map_err(|e| anyhow::anyhow!("BeaconChain build failed: {}", e))?;

    info!(%checkpoint_slot, "BeaconChain initialized from checkpoint");

    Ok((Arc::new(chain), runtime, db_dir))
}

fn micros_since(start: Instant) -> u64 {
    start.elapsed().as_micros().min(u128::from(u64::MAX)) as u64
}
