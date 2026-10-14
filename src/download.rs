//! File download with size + SHA verification (feature `download`).
//!
//! HostMyLauncher uses `overrides.zip` as a special instance asset. The client owns
//! safe archive extraction, while the launcher owns the destination policy and
//! any differential cleanup based on its own manifest.

use std::{
    collections::BTreeMap,
    collections::HashSet,
    fs::OpenOptions,
    io::{self, Read},
    path::{Component, Path, PathBuf},
};

use futures::StreamExt;
use serde::{Deserialize, Serialize};
use sha1::Sha1;
use sha2::{Digest, Sha512};
use tempfile::NamedTempFile;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use zip::ZipArchive;

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

    /// Downloads and safely extracts HostMyLauncher's special `overrides.zip`
    /// asset into `destination`.
    ///
    /// The archive is verified before extraction. Paths outside the
    /// destination, symlink entries, duplicate paths, and excessive archives
    /// are rejected. Existing regular files may be replaced; files missing
    /// from a newer archive are intentionally left alone for the caller's
    /// manifest policy.
    pub async fn download_overrides<F>(
        &self,
        file: &RemoteFile,
        destination: &Path,
        progress: F,
    ) -> Result<()>
    where
        F: Fn(u64, u64) + Send + Sync + 'static,
    {
        if file.path.as_deref() != Some("overrides.zip") {
            return Err(Error::Other("file is not overrides.zip".into()));
        }
        tokio::fs::create_dir_all(destination).await?;
        let temp = NamedTempFile::new_in(destination)?;
        let temp_path = temp.path().to_path_buf();
        self.download_file(file, &temp_path, progress).await?;
        let destination = destination.to_path_buf();
        tokio::task::spawn_blocking(move || extract_overrides(&temp_path, &destination))
            .await
            .map_err(|error| Error::Other(format!("override extraction task failed: {error}")))??;
        Ok(())
    }

    /// Synchronizes an instance manifest into `destination`.
    ///
    /// Only paths recorded by the previous manifest are eligible for removal.
    /// If a user changed one of those files, it is preserved rather than
    /// silently deleted. Override updates are compared with the new content.
    /// Destination parents must be real directories, not symlinks. Changes are
    /// backed up and rolled back if installation, removal, or manifest saving
    /// fails. If rollback itself fails, the error identifies retained backups.
    /// This is error recovery, not crash atomicity: callers must prevent other
    /// processes from modifying the destination during synchronization.
    pub async fn sync_instance_files<F>(
        &self,
        files: &[RemoteFile],
        destination: &Path,
        manifest_path: &Path,
        progress: F,
    ) -> Result<SyncReport>
    where
        F: Fn(SyncProgress) + Send + Sync + 'static,
    {
        tokio::fs::create_dir_all(destination).await?;
        let root = std::fs::canonicalize(destination)?;
        let previous = load_manifest(manifest_path)?;
        // The manifest is control state, never an installable instance asset.
        let manifest_target = manifest_output_target(manifest_path)?;
        let mut regular = BTreeMap::new();
        let mut overrides = None;
        for file in files {
            let path = file
                .path
                .as_deref()
                .or(file.name.as_deref())
                .ok_or_else(|| Error::Other("HostMyLauncher file has no path".into()))?;
            if path == "overrides.zip" {
                if overrides.replace(file).is_some() {
                    return Err(Error::Other("duplicate overrides.zip entry".into()));
                }
                continue;
            }
            let safe = safe_entry_path(path)?;
            if regular.insert(safe, file).is_some() {
                return Err(Error::Other(format!("duplicate remote file: {path}")));
            }
        }

        let staging = tempfile::tempdir_in(&root)?;
        let regular_stage = staging.path().join("regular");
        let override_stage = staging.path().join("overrides");
        let mut desired = BTreeMap::new();
        let mut staged_files = Vec::new();
        let total = regular.len() + usize::from(overrides.is_some());
        let mut downloaded = 0;
        for (index, (path, file)) in regular.iter().enumerate() {
            let target = root.join(path);
            reject_manifest_conflict(&target, &manifest_target)?;
            validate_file_target(&target, &root)?;
            let unchanged = previous
                .files
                .get(path.to_string_lossy().as_ref())
                .is_some_and(|old| current_matches(&target, old, file));
            let manifest_source;
            if !unchanged {
                downloaded += 1;
                let staged = regular_stage.join(path);
                if let Some(parent) = staged.parent() {
                    std::fs::create_dir_all(parent)?;
                }
                let progress_ref = &progress;
                self.download_file(file, &staged, move |downloaded, bytes| {
                    progress_ref(SyncProgress::download(
                        path.to_string_lossy().into_owned(),
                        downloaded,
                        bytes,
                    ));
                })
                .await?;
                manifest_source = staged.clone();
                staged_files.push((path.clone(), staged));
            } else {
                manifest_source = target.clone();
            }
            let entry = manifest_file(&manifest_source, file).await?;
            desired.insert(path.to_string_lossy().into_owned(), entry);
            progress(SyncProgress::stage(
                SyncStage::Install,
                index + 1,
                total,
                path.to_string_lossy().into_owned(),
            ));
        }

        if let Some(file) = overrides {
            downloaded += 1;
            let archive = staging.path().join("overrides.zip");
            self.download_file(file, &archive, {
                let progress_ref = &progress;
                move |downloaded, bytes| {
                    progress_ref(SyncProgress::download(
                        "overrides.zip".into(),
                        downloaded,
                        bytes,
                    ));
                }
            })
            .await?;
            extract_overrides(&archive, &override_stage)?;
            for path in collect_files(&override_stage)? {
                let relative = path
                    .strip_prefix(&override_stage)
                    .map_err(|error| Error::Other(error.to_string()))?
                    .to_path_buf();
                if regular.contains_key(&relative) {
                    return Err(Error::Other(format!(
                        "override conflicts with remote file: {}",
                        relative.display()
                    )));
                }
                let target = root.join(&relative);
                let staged = override_stage.join(&relative);
                let entry = manifest_file(
                    &staged,
                    &RemoteFile {
                        path: Some(relative.to_string_lossy().into_owned()),
                        name: None,
                        size: Some(std::fs::metadata(&staged)?.len()),
                        hashes: None,
                        downloads: Vec::new(),
                    },
                )
                .await?;
                reject_manifest_conflict(&target, &manifest_target)?;
                validate_file_target(&target, &root)?;
                let unchanged = current_manifest_matches(&target, &entry)?;
                desired.insert(relative.to_string_lossy().into_owned(), entry);
                if !unchanged {
                    staged_files.push((relative, staged));
                }
            }
            progress(SyncProgress::stage(
                SyncStage::Extract,
                1,
                1,
                "overrides.zip".into(),
            ));
        }

        let mut obsolete = Vec::new();
        for (path, old) in &previous.files {
            if desired.contains_key(path) {
                continue;
            }
            let target = root.join(safe_entry_path(path)?);
            reject_manifest_conflict(&target, &manifest_target)?;
            validate_real_directory(target.parent().expect("rooted file has a parent"), &root)?;
            // Inspect the final component before hashing: never follow a stale
            // file symlink, even if it points to content matching the manifest.
            if std::fs::symlink_metadata(&target)
                .map(|m| m.file_type().is_file())
                .unwrap_or(false)
                && current_manifest_matches(&target, old)?
            {
                obsolete.push(target);
            }
        }
        let removed = obsolete.len();
        apply_sync_changes(
            &root,
            staged_files,
            obsolete,
            manifest_path,
            &SyncManifest {
                version: 1,
                files: desired,
            },
        )?;
        Ok(SyncReport {
            downloaded,
            removed,
        })
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum SyncStage {
    Download,
    Extract,
    Install,
    Remove,
}

#[derive(Debug, Clone)]
pub struct SyncProgress {
    pub stage: SyncStage,
    pub path: String,
    pub current: u64,
    pub total: u64,
}

impl SyncProgress {
    fn download(path: String, current: u64, total: u64) -> Self {
        Self {
            stage: SyncStage::Download,
            path,
            current,
            total,
        }
    }

    fn stage(stage: SyncStage, current: usize, total: usize, path: String) -> Self {
        Self {
            stage,
            path,
            current: current as u64,
            total: total as u64,
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct SyncReport {
    pub downloaded: usize,
    pub removed: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct SyncManifest {
    version: u32,
    files: BTreeMap<String, ManifestFile>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ManifestFile {
    size: u64,
    sha1: String,
    sha512: String,
}

async fn manifest_file(path: &Path, remote: &RemoteFile) -> Result<ManifestFile> {
    let hashes = hash_file(path).await?;
    if let Some(expected) = remote.hashes.as_ref().and_then(|h| h.sha512.as_ref()) {
        if expected != &hashes.1 {
            return Err(Error::Other(format!(
                "sha512 mismatch for {}",
                path.display()
            )));
        }
    }
    if let Some(expected) = remote.hashes.as_ref().and_then(|h| h.sha1.as_ref()) {
        if expected != &hashes.0 {
            return Err(Error::Other(format!(
                "sha1 mismatch for {}",
                path.display()
            )));
        }
    }
    Ok(ManifestFile {
        size: hashes.2,
        sha1: hashes.0,
        sha512: hashes.1,
    })
}

async fn hash_file(path: &Path) -> Result<(String, String, u64)> {
    let mut file = tokio::fs::File::open(path).await?;
    let mut sha1 = Sha1::new();
    let mut sha512 = Sha512::new();
    let mut size = 0;
    let mut buffer = [0u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer).await?;
        if read == 0 {
            break;
        }
        sha1.update(&buffer[..read]);
        sha512.update(&buffer[..read]);
        size += read as u64;
    }
    Ok((
        hex::encode(sha1.finalize()),
        hex::encode(sha512.finalize()),
        size,
    ))
}

fn current_matches(path: &Path, old: &ManifestFile, remote: &RemoteFile) -> bool {
    if remote.size != Some(old.size) {
        return false;
    }
    if let Some(hash) = remote.hashes.as_ref().and_then(|h| h.sha512.as_ref()) {
        return hash == &old.sha512;
    }
    if let Some(hash) = remote.hashes.as_ref().and_then(|h| h.sha1.as_ref()) {
        return hash == &old.sha1;
    }
    current_manifest_matches(path, old).unwrap_or(false)
}

fn current_manifest_matches(path: &Path, old: &ManifestFile) -> Result<bool> {
    if !path.is_file() {
        return Ok(false);
    }
    let metadata = std::fs::metadata(path)?;
    if metadata.len() != old.size {
        return Ok(false);
    }
    let mut file = std::fs::File::open(path)?;
    let mut sha512 = Sha512::new();
    let mut buffer = [0u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        sha512.update(&buffer[..read]);
    }
    Ok(hex::encode(sha512.finalize()) == old.sha512)
}

fn load_manifest(path: &Path) -> Result<SyncManifest> {
    if !path.exists() {
        return Ok(SyncManifest::default());
    }
    Ok(serde_json::from_slice(&std::fs::read(path)?)?)
}

fn save_manifest(path: &Path, manifest: &SyncManifest) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let parent = path
        .parent()
        .ok_or_else(|| Error::Other("manifest has no parent".into()))?;
    let temp = tempfile::NamedTempFile::new_in(parent)?;
    serde_json::to_writer(&temp, manifest)?;
    temp.persist(path).map_err(|error| Error::Io(error.error))?;
    Ok(())
}

fn collect_files(root: &Path) -> Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    collect_files_inner(root, root, &mut files)?;
    Ok(files)
}

fn collect_files_inner(root: &Path, current: &Path, files: &mut Vec<PathBuf>) -> Result<()> {
    for entry in std::fs::read_dir(current)? {
        let entry = entry?;
        let path = entry.path();
        let metadata = std::fs::symlink_metadata(&path)?;
        if metadata.file_type().is_symlink() {
            return Err(Error::Other(format!(
                "symlink in staged overrides: {}",
                path.display()
            )));
        }
        if metadata.is_dir() {
            collect_files_inner(root, &path, files)?;
        } else if metadata.is_file() {
            files.push(path);
        } else {
            return Err(Error::Other(format!(
                "special file in staged overrides: {}",
                path.display()
            )));
        }
    }
    let _ = root;
    Ok(())
}

struct PreparedChange {
    target: PathBuf,
    staged: Option<PathBuf>,
    backup: Option<PathBuf>,
}

/// Applies a prepared batch, keeping originals until the manifest is committed.
/// Backup copies leave installed files in place until their atomic replacement.
fn apply_sync_changes(
    root: &Path,
    installs: Vec<(PathBuf, PathBuf)>,
    obsolete: Vec<PathBuf>,
    manifest_path: &Path,
    manifest: &SyncManifest,
) -> Result<()> {
    let backups = tempfile::tempdir_in(root)?;
    let mut changes = Vec::new();
    let operations = installs
        .into_iter()
        .map(|(path, staged)| (root.join(path), Some(staged)))
        .chain(obsolete.into_iter().map(|path| (path, None)));
    for (index, (target, staged)) in operations.enumerate() {
        validate_file_target(&target, root)?;
        let backup = match std::fs::symlink_metadata(&target) {
            Ok(_) => {
                let backup = backups.path().join(index.to_string());
                std::fs::copy(&target, &backup)?;
                Some(backup)
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => None,
            Err(error) => return Err(error.into()),
        };
        changes.push(PreparedChange {
            target,
            staged,
            backup,
        });
    }

    let mut applied = Vec::new();
    let mut created = Vec::new();
    let result = (|| -> Result<()> {
        for (index, change) in changes.iter().enumerate() {
            let parent = change.target.parent().expect("rooted file has a parent");
            create_real_directory(parent, root, &mut created)?;
            validate_file_target(&change.target, root)?;
            if let Some(staged) = &change.staged {
                replace_regular_file(staged, &change.target)?;
            } else {
                std::fs::remove_file(&change.target)?;
            }
            applied.push(index);
        }
        save_manifest(manifest_path, manifest)
    })();

    if let Err(error) = result {
        return Err(rollback_sync_changes(
            error, root, &changes, &applied, &created, backups,
        ));
    }
    Ok(())
}

fn rollback_sync_changes(
    error: Error,
    root: &Path,
    changes: &[PreparedChange],
    applied: &[usize],
    created: &[PathBuf],
    backups: tempfile::TempDir,
) -> Error {
    let mut failures = Vec::new();
    for &index in applied.iter().rev() {
        let change = &changes[index];
        let restored = (|| -> Result<()> {
            validate_file_target(&change.target, root)?;
            if let Some(backup) = &change.backup {
                replace_regular_file(backup, &change.target)?;
            } else {
                std::fs::remove_file(&change.target)?;
            }
            Ok(())
        })();
        if let Err(rollback_error) = restored {
            failures.push(format!("{}: {rollback_error}", change.target.display()));
        }
    }
    for directory in created.iter().rev() {
        let removed = validate_real_directory(directory, root)
            .and_then(|_| std::fs::remove_dir(directory).map_err(Error::from));
        if let Err(rollback_error) = removed {
            failures.push(format!("{}: {rollback_error}", directory.display()));
        }
    }
    if !failures.is_empty() {
        // Never discard the only remaining originals after failed recovery.
        let retained = backups.keep();
        return Error::Other(format!(
            "sync failed: {error}; rollback failed: {}; backups retained at {}",
            failures.join("; "),
            retained.display()
        ));
    }
    error
}

fn replace_regular_file(staged: &Path, target: &Path) -> Result<()> {
    if let Ok(metadata) = std::fs::symlink_metadata(target) {
        if metadata.file_type().is_symlink() || !metadata.file_type().is_file() {
            return Err(Error::Other(format!(
                "unsafe existing target: {}",
                target.display()
            )));
        }
    }
    // Rename replaces an existing regular target without deleting it first.
    // If it fails, the previous file is still present.
    std::fs::rename(staged, target)?;
    Ok(())
}

const MAX_ENTRIES: usize = 10_000;
const MAX_UNCOMPRESSED_BYTES: u64 = 512 * 1024 * 1024;
const MAX_FILE_BYTES: u64 = 128 * 1024 * 1024;

/// Extracts a previously downloaded `overrides.zip` safely into `destination`.
pub fn extract_overrides(archive_path: &Path, destination: &Path) -> Result<()> {
    std::fs::create_dir_all(destination)?;
    let root = std::fs::canonicalize(destination)?;
    let archive_file = std::fs::File::open(archive_path)?;
    let mut archive = ZipArchive::new(archive_file)
        .map_err(|error| Error::Other(format!("invalid overrides.zip: {error}")))?;
    if archive.len() > MAX_ENTRIES {
        return Err(Error::Other("overrides.zip has too many entries".into()));
    }

    let mut paths = HashSet::new();
    let mut total_size = 0u64;
    for index in 0..archive.len() {
        let entry = archive
            .by_index(index)
            .map_err(|error| Error::Other(format!("invalid overrides.zip entry: {error}")))?;
        let path = safe_entry_path(entry.name())?;
        if !paths.insert(path.clone()) {
            return Err(Error::Other(format!(
                "duplicate overrides.zip entry: {}",
                entry.name()
            )));
        }
        if entry
            .unix_mode()
            .is_some_and(|mode| mode & 0o170000 == 0o120000)
        {
            return Err(Error::Other(format!(
                "symlink in overrides.zip: {}",
                entry.name()
            )));
        }
        let size = entry.size();
        if size > MAX_FILE_BYTES
            || total_size
                .checked_add(size)
                .is_none_or(|size| size > MAX_UNCOMPRESSED_BYTES)
        {
            return Err(Error::Other(
                "overrides.zip exceeds extraction limits".into(),
            ));
        }
        total_size += size;
        let output = root.join(path);
        if !output.starts_with(&root) {
            return Err(Error::Other(format!(
                "unsafe overrides.zip path: {}",
                entry.name()
            )));
        }
    }

    let staging = tempfile::tempdir_in(&root)?;
    for index in 0..archive.len() {
        let entry = archive
            .by_index(index)
            .map_err(|error| Error::Other(format!("invalid overrides.zip entry: {error}")))?;
        let path = safe_entry_path(entry.name())?;
        let staged = staging.path().join(&path);
        if entry.is_dir() {
            std::fs::create_dir_all(staged)?;
            continue;
        }
        if let Some(parent) = staged.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let mut output = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&staged)?;
        let copied = io::copy(&mut entry.take(MAX_FILE_BYTES + 1), &mut output)?;
        if copied > MAX_FILE_BYTES {
            return Err(Error::Other(
                "overrides.zip entry exceeds extraction limits".into(),
            ));
        }
    }

    for index in 0..archive.len() {
        let entry = archive
            .by_index(index)
            .map_err(|error| Error::Other(format!("invalid overrides.zip entry: {error}")))?;
        let path = safe_entry_path(entry.name())?;
        let staged = staging.path().join(&path);
        let output = root.join(&path);
        if entry.is_dir() {
            ensure_real_directory(&output, &root)?;
            continue;
        }
        if let Some(parent) = output.parent() {
            ensure_real_directory(parent, &root)?;
        }
        if let Ok(metadata) = std::fs::symlink_metadata(&output) {
            if !metadata.file_type().is_file() {
                return Err(Error::Other(format!(
                    "unsafe existing override path: {}",
                    entry.name()
                )));
            }
        }
        replace_regular_file(&staged, &output)?;
    }
    Ok(())
}

fn safe_entry_path(name: &str) -> Result<PathBuf> {
    if name.is_empty() || name.contains('\\') || name.starts_with('/') {
        return Err(Error::Other(format!("unsafe overrides.zip path: {name}")));
    }
    let path = Path::new(name);
    let mut safe = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Normal(component) => safe.push(component),
            _ => return Err(Error::Other(format!("unsafe overrides.zip path: {name}"))),
        }
    }
    if safe.as_os_str().is_empty() {
        return Err(Error::Other(format!("unsafe overrides.zip path: {name}")));
    }
    Ok(safe)
}

/// Validates every existing parent without creating anything or following links.
/// Missing suffixes are allowed; creation happens only after this whole check.
fn validate_real_directory(path: &Path, root: &Path) -> Result<()> {
    let relative = path
        .strip_prefix(root)
        .map_err(|_| Error::Other("override destination escapes root".into()))?;
    if relative
        .components()
        .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(Error::Other("override destination escapes root".into()));
    }
    let mut current = root.to_path_buf();
    check_directory_component(&current, root)?;
    for component in relative.components() {
        if !matches!(component, Component::Normal(_)) {
            return Err(Error::Other("override destination escapes root".into()));
        }
        current.push(component);
        match std::fs::symlink_metadata(&current) {
            Ok(_) => check_directory_component(&current, root)?,
            Err(error) if error.kind() == io::ErrorKind::NotFound => break,
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

fn check_directory_component(path: &Path, root: &Path) -> Result<()> {
    let metadata = std::fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() || !metadata.file_type().is_dir() {
        return Err(Error::Other(
            "override destination contains a symlink or non-directory".into(),
        ));
    }
    if !std::fs::canonicalize(path)?.starts_with(root) {
        return Err(Error::Other("override destination escapes root".into()));
    }
    Ok(())
}

/// Resolve existing parent aliases and normalize missing suffixes, including
/// `..`. Do not resolve the final component: persist replaces that entry itself.
fn manifest_output_target(path: &Path) -> Result<PathBuf> {
    let absolute = std::path::absolute(path)?;
    let name = absolute
        .file_name()
        .ok_or_else(|| Error::Other("manifest has no filename".into()))?;
    let mut parent = PathBuf::new();
    for component in absolute
        .parent()
        .expect("absolute file has a parent")
        .components()
    {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                parent.pop();
            }
            _ => {
                parent.push(component);
                if let Ok(canonical) = std::fs::canonicalize(&parent) {
                    parent = canonical;
                }
            }
        }
    }
    Ok(parent.join(name))
}

fn reject_manifest_conflict(target: &Path, manifest_target: &Path) -> Result<()> {
    if target == manifest_target {
        return Err(Error::Other(
            "instance file conflicts with manifest path".into(),
        ));
    }
    Ok(())
}

fn validate_file_target(target: &Path, root: &Path) -> Result<()> {
    validate_real_directory(target.parent().expect("rooted file has a parent"), root)?;
    match std::fs::symlink_metadata(target) {
        Ok(metadata) if !metadata.file_type().is_file() => Err(Error::Other(format!(
            "unsafe existing target: {}",
            target.display()
        ))),
        Ok(_) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

fn create_real_directory(path: &Path, root: &Path, created: &mut Vec<PathBuf>) -> Result<()> {
    validate_real_directory(path, root)?;
    let mut current = root.to_path_buf();
    for component in path
        .strip_prefix(root)
        .expect("validated path")
        .components()
    {
        current.push(component);
        match std::fs::symlink_metadata(&current) {
            Ok(_) => check_directory_component(&current, root)?,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                // The parent was checked before each individual creation.
                std::fs::create_dir(&current)?;
                created.push(current.clone());
                check_directory_component(&current, root)?;
            }
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

fn ensure_real_directory(path: &Path, root: &Path) -> Result<()> {
    create_real_directory(path, root, &mut Vec::new())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use zip::{write::SimpleFileOptions, ZipWriter};

    fn archive(entries: &[(&str, &[u8])]) -> tempfile::NamedTempFile {
        let file = tempfile::NamedTempFile::new().unwrap();
        let mut writer = ZipWriter::new(file.reopen().unwrap());
        for (name, contents) in entries {
            writer
                .start_file(*name, SimpleFileOptions::default())
                .unwrap();
            writer.write_all(contents).unwrap();
        }
        writer.finish().unwrap();
        file
    }

    #[test]
    fn extracts_regular_entries() {
        let zip = archive(&[("config/example.txt", b"hello")]);
        let destination = tempfile::tempdir().unwrap();
        extract_overrides(zip.path(), destination.path()).unwrap();
        assert_eq!(
            std::fs::read_to_string(destination.path().join("config/example.txt")).unwrap(),
            "hello"
        );
    }

    #[test]
    fn rejects_traversal() {
        let zip = archive(&[("../outside.txt", b"no")]);
        let destination = tempfile::tempdir().unwrap();
        assert!(extract_overrides(zip.path(), destination.path()).is_err());
        assert!(!destination.path().join("../outside.txt").exists());
    }

    #[test]
    fn rejects_backslash_paths() {
        let zip = archive(&[("config\\example.txt", b"no")]);
        let destination = tempfile::tempdir().unwrap();
        assert!(extract_overrides(zip.path(), destination.path()).is_err());
    }

    #[test]
    fn does_not_claim_modified_file_is_owned_by_previous_manifest() {
        let destination = tempfile::tempdir().unwrap();
        let path = destination.path().join("user.txt");
        std::fs::write(&path, b"user content").unwrap();
        let old = ManifestFile {
            size: 9,
            sha1: String::new(),
            sha512: hex::encode(Sha512::digest(b"managed content")),
        };
        assert!(!current_manifest_matches(&path, &old).unwrap());
    }

    // A missing staged source deterministically forces rename to fail.
    #[test]
    fn failed_replacement_preserves_previous_file() {
        let sandbox = tempfile::tempdir().unwrap();
        let target = sandbox.path().join("installed.txt");
        let staged = sandbox.path().join("missing-staged.txt");
        std::fs::write(&target, b"previous content").unwrap();
        assert_eq!(std::fs::read(&target).unwrap(), b"previous content");
        assert!(!staged.exists());

        let error = replace_regular_file(&staged, &target).unwrap_err();
        match error {
            Error::Io(error) => assert_eq!(error.kind(), io::ErrorKind::NotFound),
            error => panic!("expected rename I/O failure, got {error}"),
        }
        assert_eq!(std::fs::read(&target).unwrap(), b"previous content");
        assert!(!staged.exists());
        assert_eq!(std::fs::read_dir(sandbox.path()).unwrap().count(), 1);
    }

    #[test]
    fn rollback_failure_reports_error_and_retains_original_backup() {
        let sandbox = tempfile::tempdir().unwrap();
        let root = std::fs::canonicalize(sandbox.path()).unwrap();
        let target = root.join("installed.txt");
        // Simulate a filesystem state that cannot safely be restored.
        std::fs::create_dir(&target).unwrap();
        let backups = tempfile::tempdir_in(&root).unwrap();
        let retained = backups.path().to_path_buf();
        let original = retained.join("0");
        std::fs::write(&original, b"previous content").unwrap();
        let changes = [PreparedChange {
            target: target.clone(),
            staged: None,
            backup: Some(original.clone()),
        }];

        let error = rollback_sync_changes(
            Error::Other("installation failed".into()),
            &root,
            &changes,
            &[0],
            &[],
            backups,
        );
        assert_eq!(error.to_string(), format!(
            "sync failed: installation failed; rollback failed: {}: unsafe existing target: {}; backups retained at {}",
            target.display(), target.display(), retained.display()
        ));
        assert!(target.is_dir());
        assert_eq!(std::fs::read_dir(&target).unwrap().count(), 0);
        assert_eq!(std::fs::read(&original).unwrap(), b"previous content");
        assert_eq!(std::fs::read_dir(&retained).unwrap().count(), 1);
    }

    #[test]
    fn reads_missing_manifest_as_empty() {
        let path = tempfile::tempdir().unwrap().path().join("missing.json");
        assert!(load_manifest(&path).unwrap().files.is_empty());
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
