use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;

use anyhow::{Context, Result};

use crate::config::OutputFormat;
use crate::output::SlotResult;

pub struct OutputWriter {
    format: OutputFormat,
    csv_writer: Option<csv::Writer<BufWriter<File>>>,
    json_file: Option<BufWriter<File>>,
    first_json_record: bool,
}

impl OutputWriter {
    pub fn new(path: &Path, format: &OutputFormat) -> Result<Self> {
        match format {
            OutputFormat::Csv => {
                let file = File::create(path)
                    .with_context(|| format!("failed to create output file: {}", path.display()))?;
                let writer = csv::Writer::from_writer(BufWriter::new(file));
                Ok(Self {
                    format: format.clone(),
                    csv_writer: Some(writer),
                    json_file: None,
                    first_json_record: true,
                })
            }
            OutputFormat::Json => {
                let file = File::create(path)
                    .with_context(|| format!("failed to create output file: {}", path.display()))?;
                let mut writer = BufWriter::new(file);
                writer.write_all(b"[\n")?;
                Ok(Self {
                    format: format.clone(),
                    csv_writer: None,
                    json_file: Some(writer),
                    first_json_record: true,
                })
            }
        }
    }

    pub fn write(&mut self, result: &SlotResult) -> Result<()> {
        match self.format {
            OutputFormat::Csv => {
                self.csv_writer
                    .as_mut()
                    .expect("csv writer should exist")
                    .serialize(result)
                    .context("failed to write CSV record")?;
            }
            OutputFormat::Json => {
                let writer = self.json_file.as_mut().expect("json writer should exist");
                if !self.first_json_record {
                    writer.write_all(b",\n")?;
                }
                self.first_json_record = false;
                serde_json::to_writer(&mut *writer, result)?;
            }
        }
        Ok(())
    }

    /// Flush to disk periodically so we don't lose progress on crash.
    pub fn flush_if_needed(&mut self, records_written: u64) -> Result<()> {
        if records_written % 100 == 0 {
            self.flush_inner()?;
        }
        Ok(())
    }

    /// Final flush.
    pub fn flush(&mut self) -> Result<()> {
        match self.format {
            OutputFormat::Json => {
                let writer = self.json_file.as_mut().expect("json writer should exist");
                writer.write_all(b"\n]\n")?;
            }
            _ => {}
        }
        self.flush_inner()
    }

    fn flush_inner(&mut self) -> Result<()> {
        match self.format {
            OutputFormat::Csv => {
                self.csv_writer
                    .as_mut()
                    .expect("csv writer should exist")
                    .flush()
                    .context("failed to flush CSV writer")?;
            }
            OutputFormat::Json => {
                self.json_file
                    .as_mut()
                    .expect("json writer should exist")
                    .flush()
                    .context("failed to flush JSON writer")?;
            }
        }
        Ok(())
    }
}
