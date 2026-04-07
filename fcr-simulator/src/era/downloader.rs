use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use tracing::{debug, info, warn};

use super::reader;

/// Downloads and caches ERA files from a remote endpoint
pub struct EraDownloader {
    base_url: String,
    cache_dir: PathBuf,
    /// Cached mapping of era number -> filename (discovered from index page)
    era_filenames: Option<Vec<(u64, String)>>,
}

impl EraDownloader {
    pub fn new(base_url: &str, cache_dir: &Path) -> Result<Self> {
        let era_dir = cache_dir.join("era");
        fs::create_dir_all(&era_dir).with_context(|| {
            format!(
                "failed to create ERA cache directory: {}",
                era_dir.display()
            )
        })?;

        Ok(Self {
            base_url: base_url.trim_end_matches('/').to_string(),
            cache_dir: era_dir,
            era_filenames: None,
        })
    }

    /// Pre-download all ERA files needed for a slot range.
    /// This ensures all data is available before any simulation starts.
    pub fn pre_download(&mut self, start_slot: u64, end_slot: u64) -> Result<()> {
        let start_era = reader::era_number_for_slot(start_slot);
        // +1 for peek-ahead at the end
        let end_era = reader::era_number_for_slot(end_slot + 32);

        let mut needed = Vec::new();
        for era in start_era..=end_era {
            let pattern = format!("mainnet-{:05}-", era);
            if self.find_cached_file(&pattern)?.is_none() {
                needed.push(era);
            }
        }

        if needed.is_empty() {
            info!(
                start_era,
                end_era,
                "All {} ERA files already cached",
                end_era - start_era + 1
            );
            return Ok(());
        }

        info!(
            total = end_era - start_era + 1,
            cached = (end_era - start_era + 1) as usize - needed.len(),
            to_download = needed.len(),
            "Pre-downloading ERA files"
        );

        for (i, era) in needed.iter().enumerate() {
            info!(
                era,
                progress = format!("{}/{}", i + 1, needed.len()),
                "Downloading ERA file"
            );
            self.download_era_with_retries(*era, 3)?;
        }

        info!("All ERA files downloaded");
        Ok(())
    }

    fn fetch_index(&mut self) -> Result<()> {
        if self.era_filenames.is_some() {
            return Ok(());
        }

        info!(url = %self.base_url, "Fetching ERA file index");
        let body = reqwest::blocking::get(&self.base_url)
            .context("failed to fetch ERA index")?
            .text()
            .context("failed to read ERA index body")?;

        let mut filenames = Vec::new();
        for line in body.lines() {
            if let Some(start) = line.find("mainnet-")
                && let Some(end) = line[start..].find(".era")
            {
                let filename = &line[start..start + end + 4];
                if let Some(num_str) = filename
                    .strip_prefix("mainnet-")
                    .and_then(|s| s.split('-').next())
                    && let Ok(era_num) = num_str.parse::<u64>()
                {
                    filenames.push((era_num, filename.to_string()));
                }
            }
        }

        filenames.sort_by_key(|(n, _)| *n);
        info!(count = filenames.len(), "Discovered ERA files");
        self.era_filenames = Some(filenames);
        Ok(())
    }

    fn filename_for_era(&mut self, era_number: u64) -> Result<String> {
        self.fetch_index()?;

        let filenames = self
            .era_filenames
            .as_ref()
            .context("ERA index not loaded")?;

        filenames
            .iter()
            .find(|(n, _)| *n == era_number)
            .map(|(_, f)| f.clone())
            .with_context(|| format!("ERA file not found for era number {}", era_number))
    }

    /// Get ERA file data from cache. Fails if not cached (use pre_download first).
    pub fn get_era(&mut self, era_number: u64) -> Result<Vec<u8>> {
        let pattern = format!("mainnet-{:05}-", era_number);
        if let Some(cached) = self.find_cached_file(&pattern)? {
            debug!(era = era_number, "Using cached ERA file");
            return fs::read(&cached)
                .with_context(|| format!("failed to read cached ERA file: {}", cached.display()));
        }

        // Fallback: download with retries (shouldn't happen if pre_download was called)
        warn!(
            era = era_number,
            "ERA file not in cache, downloading on-the-fly"
        );
        self.download_era_with_retries(era_number, 3)?;

        let cached = self
            .find_cached_file(&pattern)?
            .with_context(|| format!("ERA {} still not in cache after download", era_number))?;
        fs::read(&cached)
            .with_context(|| format!("failed to read cached ERA file: {}", cached.display()))
    }

    fn download_era_with_retries(&mut self, era_number: u64, max_retries: u32) -> Result<()> {
        let filename = self.filename_for_era(era_number)?;
        let url = format!("{}/{}", self.base_url, filename);

        let client = reqwest::blocking::Client::builder()
            .timeout(std::time::Duration::from_secs(600))
            .build()?;

        for attempt in 1..=max_retries {
            match self.try_download(&client, &url, &filename) {
                Ok(()) => return Ok(()),
                Err(e) => {
                    if attempt < max_retries {
                        let delay = std::time::Duration::from_secs(5 * attempt as u64);
                        warn!(
                            era = era_number,
                            attempt,
                            max_retries,
                            error = %e,
                            retry_secs = delay.as_secs(),
                            "ERA download failed, retrying"
                        );
                        std::thread::sleep(delay);
                    } else {
                        return Err(e).with_context(|| {
                            format!(
                                "failed to download ERA {} after {} attempts",
                                era_number, max_retries
                            )
                        });
                    }
                }
            }
        }
        unreachable!()
    }

    fn try_download(
        &self,
        client: &reqwest::blocking::Client,
        url: &str,
        filename: &str,
    ) -> Result<()> {
        let response = client
            .get(url)
            .send()
            .with_context(|| format!("failed to download ERA file: {}", url))?;

        if !response.status().is_success() {
            bail!("HTTP {} from {}", response.status(), url);
        }

        let data = response
            .bytes()
            .context("failed to read ERA file response body")?
            .to_vec();

        let cache_path = self.cache_dir.join(filename);
        let mut file = fs::File::create(&cache_path)
            .with_context(|| format!("failed to create cache file: {}", cache_path.display()))?;
        file.write_all(&data)?;
        info!(
            path = %cache_path.display(),
            size_mb = data.len() / (1024 * 1024),
            "Cached ERA file"
        );

        Ok(())
    }

    fn find_cached_file(&self, pattern: &str) -> Result<Option<PathBuf>> {
        let Ok(entries) = fs::read_dir(&self.cache_dir) else {
            return Ok(None);
        };

        for entry in entries {
            let entry = entry?;
            let name = entry.file_name().to_string_lossy().to_string();
            if name.starts_with(pattern) && name.ends_with(".era") {
                return Ok(Some(entry.path()));
            }
        }
        Ok(None)
    }
}
