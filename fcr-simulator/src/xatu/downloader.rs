use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use tracing::{debug, info, warn};

/// Downloads and caches daily xatu attestation timing parquet files from R2.
pub struct XatuDownloader {
    cache_dir: PathBuf,
}

const BASE_URL: &str =
    "https://data.ethpandaops.io/fast_confirmation_simulation/attestation_timing/mainnet/v1";

impl XatuDownloader {
    pub fn new(cache_dir: &Path) -> Result<Self> {
        let xatu_dir = cache_dir.join("xatu");
        fs::create_dir_all(&xatu_dir).with_context(|| {
            format!(
                "failed to create xatu cache directory: {}",
                xatu_dir.display()
            )
        })?;

        Ok(Self {
            cache_dir: xatu_dir,
        })
    }

    /// Get the path to the cached parquet file for a given date string (YYYY-MM-DD).
    /// Downloads on-demand if not already cached.
    pub fn get_parquet(&self, date: &str) -> Result<PathBuf> {
        let filename = format!("{}.parquet", date);
        let cached_path = self.cache_dir.join(&filename);

        if cached_path.exists() {
            debug!(date, "Using cached xatu parquet file");
            return Ok(cached_path);
        }

        info!(date, "Downloading xatu attestation timing data");
        self.download_with_retries(date, &cached_path, 3)?;
        Ok(cached_path)
    }

    fn download_with_retries(&self, date: &str, dest: &Path, max_retries: u32) -> Result<()> {
        let url = format!("{}/{}.parquet", BASE_URL, date);
        let client = reqwest::blocking::Client::builder()
            .timeout(std::time::Duration::from_secs(120))
            .build()?;

        for attempt in 1..=max_retries {
            match self.try_download(&client, &url, dest) {
                Ok(()) => return Ok(()),
                Err(e) => {
                    if attempt < max_retries {
                        let delay = std::time::Duration::from_secs(5 * attempt as u64);
                        warn!(
                            date,
                            attempt,
                            max_retries,
                            error = %e,
                            retry_secs = delay.as_secs(),
                            "Xatu download failed, retrying"
                        );
                        std::thread::sleep(delay);
                    } else {
                        return Err(e).with_context(|| {
                            format!(
                                "failed to download xatu data for {} after {} attempts",
                                date, max_retries
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
        dest: &Path,
    ) -> Result<()> {
        let response = client
            .get(url)
            .send()
            .with_context(|| format!("failed to download xatu file: {}", url))?;

        if !response.status().is_success() {
            bail!("HTTP {} from {}", response.status(), url);
        }

        let data = response
            .bytes()
            .context("failed to read xatu file response body")?
            .to_vec();

        let mut file = fs::File::create(dest)
            .with_context(|| format!("failed to create cache file: {}", dest.display()))?;
        file.write_all(&data)?;
        info!(
            path = %dest.display(),
            size_kb = data.len() / 1024,
            "Cached xatu parquet file"
        );

        Ok(())
    }
}
