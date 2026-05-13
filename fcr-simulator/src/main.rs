mod beacon;
mod config;
mod era;
mod output;
mod sim;
mod xatu;

use std::io::{BufRead, BufReader, Write};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Instant;

use anyhow::{Context, Result, bail};
use clap::Parser;
use config::Config;
use serde::Serialize;
use tracing::info;
use types::{EthSpec, MainnetEthSpec};

use crate::output::CSV_SCHEMA_HEADER;

const RUN_MANIFEST_SCHEMA_VERSION: u64 = 1;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
                tracing_subscriber::EnvFilter::new(
                    "info,beacon_chain::canonical_head=off,store=warn",
                )
            }),
        )
        .init();

    let config = Config::parse();
    let ranges = config.split_ranges();
    let n_workers = ranges.len();

    info!(
        start_epoch = config.start_epoch,
        end_epoch = config.end_epoch,
        workers = n_workers,
        warmup_epochs = config.warmup_epochs,
        "Starting FCR simulator"
    );

    // Pre-download all ERA files before any workers start.
    let earliest_slot = ranges
        .first()
        .map(|(s, _)| s.saturating_sub(config.warmup_epochs) * 32)
        .unwrap_or(0);
    let latest_slot = ranges.last().map(|(_, e)| e * 32).unwrap_or(0);

    info!(earliest_slot, latest_slot, "Pre-downloading ERA files");
    let mut downloader = era::EraDownloader::new(&config.era_url, &config.resolved_cache_dir())
        .context("failed to create ERA downloader")?;
    downloader
        .pre_download(earliest_slot, latest_slot)
        .context("failed to pre-download ERA files")?;

    info!("Pre-fetching genesis state");
    let spec = MainnetEthSpec::default_spec();
    beacon::fetch_genesis_state(&config.beacon_node_url, &config.resolved_cache_dir(), &spec)
        .context("failed to pre-fetch genesis state")?;

    if n_workers <= 1 {
        let range = ranges.into_iter().next().unwrap();
        let mut engine =
            sim::Engine::new_for_range(&config, range.0, range.1, config.output.clone(), None)
                .await
                .context("failed to initialize engine")?;
        let result = engine.run().await.context("simulation failed")?;
        write_run_manifest(&config, std::slice::from_ref(&result), &[], &[], false)?;
        log_summary(&[result]);
    } else {
        // Each worker gets a permanent output file and shared progress state
        let output_dir = config
            .output
            .parent()
            .unwrap_or_else(|| std::path::Path::new("."));
        let output_stem = config
            .output
            .file_stem()
            .map(|s| s.to_string_lossy().to_string())
            .unwrap_or_else(|| "results".to_string());
        let output_ext = config
            .output
            .extension()
            .map(|s| s.to_string_lossy().to_string())
            .unwrap_or_else(|| "csv".to_string());

        // Create shared progress state for all workers
        let all_progress: Vec<Arc<sim::WorkerProgress>> = ranges
            .iter()
            .enumerate()
            .map(|(i, (start, end))| {
                Arc::new(sim::WorkerProgress::new(i as u64, start * 32, end * 32))
            })
            .collect();

        // Spawn progress reporter
        let reporter_progress = all_progress.clone();
        let start_time = Instant::now();
        let reporter = tokio::spawn(async move {
            progress_reporter(reporter_progress, start_time).await;
        });

        let mut handles = Vec::with_capacity(n_workers);
        let mut worker_paths = Vec::with_capacity(n_workers);

        for (i, (start, end)) in ranges.into_iter().enumerate() {
            let worker_output =
                output_dir.join(format!("{}.worker-{}.{}", output_stem, i, output_ext));
            worker_paths.push(worker_output.clone());
            let cfg = config.clone();
            let progress = all_progress[i].clone();

            let handle = tokio::task::spawn(async move {
                info!(worker = i, start_epoch = start, end_epoch = end, output = %worker_output.display(), "Spawning worker");
                let mut engine =
                    sim::Engine::new_for_range(&cfg, start, end, worker_output, Some(progress))
                        .await?;
                engine.run().await
            });
            handles.push(handle);
        }

        // Wait for all workers
        let mut results = Vec::new();
        let mut failures = Vec::new();
        for (i, handle) in handles.into_iter().enumerate() {
            match handle.await {
                Ok(Ok(result)) => results.push(result),
                Ok(Err(e)) => {
                    tracing::error!(worker = i, error = %e, "Worker failed");
                    failures.push(i);
                }
                Err(e) => {
                    tracing::error!(worker = i, error = %e, "Worker panicked");
                    failures.push(i);
                }
            }
        }

        // Stop the reporter
        reporter.abort();

        if !failures.is_empty() {
            tracing::warn!(
                failed_workers = ?failures,
                "Some workers failed. Partial results are in individual worker files."
            );
        }

        let partial = !failures.is_empty();
        write_run_manifest(&config, &results, &failures, &worker_paths, partial)?;

        // Merge successful worker outputs
        let successful_paths: Vec<_> = worker_paths
            .iter()
            .enumerate()
            .filter(|(i, _)| !failures.contains(i))
            .map(|(_, p)| p.clone())
            .collect();

        if successful_paths.is_empty() {
            bail!("all workers failed; no output produced");
        }

        if partial && !config.allow_partial {
            bail!(
                "{} worker(s) failed; rerun with --allow-partial to merge successful worker output",
                failures.len()
            );
        }

        merge_csv_outputs(&successful_paths, &config.output)?;
        log_summary(&results);
    }

    Ok(())
}

async fn progress_reporter(workers: Vec<Arc<sim::WorkerProgress>>, start_time: Instant) {
    let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));
    interval.tick().await; // skip first immediate tick

    loop {
        interval.tick().await;

        let elapsed = start_time.elapsed().as_secs_f64();
        let mut total_recorded = 0u64;
        let mut total_target = 0u64;
        let mut worker_summaries = Vec::new();

        for w in &workers {
            let id = w.worker_id.load(Ordering::Relaxed);
            let current = w.current_slot.load(Ordering::Relaxed);
            let start = w.start_slot.load(Ordering::Relaxed);
            let end = w.end_slot.load(Ordering::Relaxed);
            let recorded = w.total_recorded.load(Ordering::Relaxed);

            let target = end.saturating_sub(start);
            let done = current.saturating_sub(start);
            let pct = if target > 0 {
                done as f64 / target as f64 * 100.0
            } else {
                0.0
            };

            total_recorded += recorded;
            total_target += target;

            worker_summaries.push(format!("W{}: {:.0}%", id, pct));
        }

        let slots_per_sec = if elapsed > 0.0 {
            total_recorded as f64 / elapsed
        } else {
            0.0
        };

        let remaining = total_target.saturating_sub(total_recorded);
        let eta_mins = if slots_per_sec > 0.0 {
            remaining as f64 / slots_per_sec / 60.0
        } else {
            f64::INFINITY
        };

        let overall_pct = if total_target > 0 {
            total_recorded as f64 / total_target as f64 * 100.0
        } else {
            0.0
        };

        info!(
            progress = format!("{}/{} ({:.1}%)", total_recorded, total_target, overall_pct),
            slots_per_sec = format!("{:.1}", slots_per_sec),
            eta = format!("{:.0}m", eta_mins),
            workers = worker_summaries.join(", "),
            "Overall progress"
        );
    }
}

fn merge_csv_outputs(worker_paths: &[PathBuf], output: &PathBuf) -> Result<()> {
    info!(
        output = %output.display(),
        workers = worker_paths.len(),
        "Merging worker outputs"
    );
    let mut out = std::fs::File::create(output)
        .with_context(|| format!("failed to create {}", output.display()))?;

    let mut header_written = false;
    let mut expected_csv_header = None;

    for path in worker_paths {
        let file = std::fs::File::open(path)
            .with_context(|| format!("failed to open worker output {}", path.display()))?;
        let mut lines = BufReader::new(file).lines();

        let schema_header = lines
            .next()
            .transpose()?
            .with_context(|| format!("worker output {} is empty", path.display()))?;
        if schema_header != CSV_SCHEMA_HEADER {
            bail!(
                "worker output {} has CSV schema header '{}', expected '{}'",
                path.display(),
                schema_header,
                CSV_SCHEMA_HEADER
            );
        }

        let csv_header = lines
            .next()
            .transpose()?
            .with_context(|| format!("worker output {} missing CSV header", path.display()))?;

        if !header_written {
            writeln!(out, "{}", schema_header)?;
            writeln!(out, "{}", csv_header)?;
            expected_csv_header = Some(csv_header);
            header_written = true;
        } else if expected_csv_header.as_ref() != Some(&csv_header) {
            bail!("worker output {} has mismatched CSV header", path.display());
        }

        for line in lines {
            writeln!(out, "{}", line?)?;
        }
    }

    Ok(())
}

#[derive(Serialize)]
struct RunManifest {
    schema_version: u64,
    requested_start_slot: u64,
    requested_end_slot: u64,
    allow_partial: bool,
    partial: bool,
    failed_workers: Vec<usize>,
    completed_ranges: Vec<CompletedRange>,
}

#[derive(Serialize)]
struct CompletedRange {
    worker_id: u64,
    start_slot: u64,
    end_slot: u64,
    total_slots: u64,
    source_path: String,
}

fn write_run_manifest(
    config: &Config,
    results: &[sim::WorkerResult],
    failed_workers: &[usize],
    worker_paths: &[PathBuf],
    partial: bool,
) -> Result<()> {
    let completed_ranges = results
        .iter()
        .map(|result| {
            let source_path = worker_paths
                .get(result.worker_id as usize)
                .unwrap_or(&config.output);

            CompletedRange {
                worker_id: result.worker_id,
                start_slot: result.start_slot,
                end_slot: result.end_slot,
                total_slots: result.total_slots,
                source_path: source_path.display().to_string(),
            }
        })
        .collect();

    let manifest = RunManifest {
        schema_version: RUN_MANIFEST_SCHEMA_VERSION,
        requested_start_slot: config.start_slot(),
        requested_end_slot: config.end_slot(),
        allow_partial: config.allow_partial,
        partial,
        failed_workers: failed_workers.to_vec(),
        completed_ranges,
    };

    let path = manifest_path(&config.output);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let file = std::fs::File::create(&path)
        .with_context(|| format!("failed to create manifest {}", path.display()))?;
    serde_json::to_writer_pretty(file, &manifest)
        .with_context(|| format!("failed to write manifest {}", path.display()))?;

    info!(manifest = %path.display(), "Wrote run manifest");
    Ok(())
}

fn manifest_path(output: &PathBuf) -> PathBuf {
    let manifest_name = output
        .file_name()
        .map(|name| format!("{}.manifest.json", name.to_string_lossy()))
        .unwrap_or_else(|| "results.csv.manifest.json".to_string());
    output.with_file_name(manifest_name)
}

fn log_summary(results: &[sim::WorkerResult]) {
    let total_slots: u64 = results.iter().map(|r| r.total_slots).sum();
    let max_duration = results
        .iter()
        .map(|r| r.duration_secs)
        .fold(0.0f64, f64::max);

    info!(
        total_slots,
        wall_time_secs = format!("{:.1}", max_duration),
        workers = results.len(),
        "Simulation complete"
    );
}
