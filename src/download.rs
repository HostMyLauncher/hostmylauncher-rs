//! File download with size + SHA verification (feature `download`).
//!
//! This covers per-file download/verify. The full differential sync and
//! `overrides.zip` extraction are launcher-specific (disk layout, manifests,
//! progress events) and intentionally left to the consumer.

use std::path::Path;

use futures::StreamExt;
use sha1::Sha1;
use sha2::{Digest, Sha512};
use tokio::io::AsyncWriteExt;

use crate::client::Client;
use crate::error::{Error, Result};
use crate::models::RemoteFile;

impl Client {
    /// Downloads a file to `dest`, verifying its declared `size` and hash
    /// (preferring sha512, then sha1). Retries up to 3 times. `progress` is
    /// called with `(downloaded, total)` byte counts.
    pub async fn download_file<F>(&self, file: &RemoteFile, dest: &Path, progress: F) -> Result<()>
    where
        F: Fn(u64, u64),
    {
        let url = file
            .downloads
            .first()
            .ok_or_else(|| Error::Other("file has no download url".into()))?;

        if let Some(parent) = dest.parent() {
            tokio::fs::create_dir_all(parent).await.ok();
        }

        let mut attempt = 0u8;
        loop {
            attempt += 1;
            match try_download(&self.http, url, dest, file, &progress).await {
                Ok(()) => return Ok(()),
                Err(e) => {
                    let _ = tokio::fs::remove_file(dest).await;
                    if attempt >= 3 {
                        return Err(e);
                    }
                }
            }
        }
    }
}

async fn try_download<F>(
    http: &reqwest::Client,
    url: &str,
    dest: &Path,
    file: &RemoteFile,
    progress: &F,
) -> Result<()>
where
    F: Fn(u64, u64),
{
    let resp = http.get(url).send().await?.error_for_status()?;
    let total = file.size.or_else(|| resp.content_length()).unwrap_or(0);

    let mut out = tokio::fs::File::create(dest).await?;
    let mut sha1 = Sha1::new();
    let mut sha512 = Sha512::new();
    let mut downloaded = 0u64;

    let mut stream = resp.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        out.write_all(&chunk).await?;
        sha1.update(&chunk);
        sha512.update(&chunk);
        downloaded += chunk.len() as u64;
        progress(downloaded, total);
    }
    out.flush().await?;

    if let Some(expected) = file.size {
        if downloaded != expected {
            return Err(Error::Other(format!(
                "size mismatch: got {downloaded}, expected {expected}"
            )));
        }
    }
    if let Some(h) = &file.hashes {
        if let Some(expected) = &h.sha512 {
            let got = hex::encode(sha512.finalize());
            if &got != expected {
                return Err(Error::Other("sha512 mismatch".into()));
            }
        } else if let Some(expected) = &h.sha1 {
            let got = hex::encode(sha1.finalize());
            if &got != expected {
                return Err(Error::Other("sha1 mismatch".into()));
            }
        }
    }
    Ok(())
}
