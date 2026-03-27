use std::collections::HashMap;

use anyhow::{Context, Result};
use tracing::debug;
use types::{ChainSpec, MainnetEthSpec, SignedBeaconBlock, Slot};

use super::downloader::EraDownloader;
use super::reader;

/// Iterates over beacon blocks from ERA files in slot order.
/// Maintains a current and next ERA cache for efficient peek-ahead.
pub struct EraBlockIterator {
    downloader: EraDownloader,
    spec: ChainSpec,
    /// Current loaded ERA's blocks
    current_blocks: HashMap<Slot, SignedBeaconBlock<MainnetEthSpec>>,
    current_era: Option<u64>,
    /// Next ERA's blocks (for peek-ahead across ERA boundaries)
    next_blocks: HashMap<Slot, SignedBeaconBlock<MainnetEthSpec>>,
    next_era: Option<u64>,
}

impl EraBlockIterator {
    pub fn new(downloader: EraDownloader, spec: ChainSpec) -> Self {
        Self {
            downloader,
            spec,
            current_blocks: HashMap::new(),
            current_era: None,
            next_blocks: HashMap::new(),
            next_era: None,
        }
    }

    /// Ensure the ERA containing `slot` is loaded into current_blocks
    fn ensure_loaded(&mut self, slot: Slot) -> Result<()> {
        let era = reader::era_number_for_slot(slot.as_u64());

        if self.current_era == Some(era) {
            return Ok(());
        }

        // Promote from next cache if available
        if self.next_era == Some(era) {
            self.current_blocks = std::mem::take(&mut self.next_blocks);
            self.current_era = self.next_era.take();
            return Ok(());
        }

        // Load fresh
        debug!(era, "Loading ERA file");
        let data = self
            .downloader
            .get_era(era)
            .with_context(|| format!("failed to get ERA {}", era))?;
        let blocks = reader::parse_era_blocks(&data, &self.spec)?;
        debug!(era, block_count = blocks.len(), "Parsed ERA file");

        self.current_blocks = blocks;
        self.current_era = Some(era);
        Ok(())
    }

    /// Ensure the ERA containing `slot` is loaded into next_blocks
    fn ensure_next_loaded(&mut self, slot: Slot) -> Result<()> {
        let era = reader::era_number_for_slot(slot.as_u64());

        // Already in current
        if self.current_era == Some(era) {
            return Ok(());
        }

        // Already in next
        if self.next_era == Some(era) {
            return Ok(());
        }

        // Load into next cache
        debug!(era, "Pre-loading next ERA file");
        let data = self
            .downloader
            .get_era(era)
            .with_context(|| format!("failed to get next ERA {}", era))?;
        let blocks = reader::parse_era_blocks(&data, &self.spec)?;
        self.next_blocks = blocks;
        self.next_era = Some(era);
        Ok(())
    }

    /// Get the block at a specific slot, if one exists
    pub fn block_at_slot(
        &mut self,
        slot: Slot,
    ) -> Result<Option<&SignedBeaconBlock<MainnetEthSpec>>> {
        self.ensure_loaded(slot)?;
        Ok(self.current_blocks.get(&slot))
    }

    /// Peek at a block at any slot without advancing the current ERA.
    /// Used for scanning forward to find attestations across missed slots.
    pub fn peek_next_block(
        &mut self,
        slot: Slot,
    ) -> Result<Option<&SignedBeaconBlock<MainnetEthSpec>>> {
        let era = reader::era_number_for_slot(slot.as_u64());

        // In current ERA
        if self.current_era == Some(era) {
            return Ok(self.current_blocks.get(&slot));
        }

        // Need the next ERA
        self.ensure_next_loaded(slot)?;

        if self.next_era == Some(era) {
            return Ok(self.next_blocks.get(&slot));
        }

        Ok(None)
    }
}
