use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;

use anyhow::{Context, Result};

use crate::output::SlotResult;

pub struct OutputWriter {
    writer: BufWriter<File>,
}

impl OutputWriter {
    pub fn new(path: &Path) -> Result<Self> {
        let file = File::create(path)
            .with_context(|| format!("failed to create output file: {}", path.display()))?;

        Ok(Self {
            writer: BufWriter::new(file),
        })
    }

    pub fn write(&mut self, result: &SlotResult) -> Result<()> {
        serde_json::to_writer(&mut self.writer, result).context("failed to write JSONL record")?;
        self.writer
            .write_all(b"\n")
            .context("failed to write JSONL newline")?;
        Ok(())
    }

    /// Flush to disk periodically so the orchestrator can salvage partial output.
    pub fn flush_if_needed(&mut self, records_written: u64) -> Result<()> {
        if records_written.is_multiple_of(100) {
            self.flush()?;
        }
        Ok(())
    }

    pub fn flush(&mut self) -> Result<()> {
        self.writer.flush().context("failed to flush JSONL writer")
    }
}
