//! Disk inspection and repair, deliberately separate from provider-change sync.
use super::*;

/// A difference between an installed file and the current provider content.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FileIssueKind {
    Missing,
    Different,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileIssue {
    /// Relative provider path, including entries extracted from overrides.
    pub path: String,
    pub kind: FileIssueKind,
}

/// Only expected files are checked; unrelated files are not reported or deleted.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct VerificationReport {
    pub checked: usize,
    /// Sorted by relative path. Unsafe paths and IO errors return `Err` instead.
    pub issues: Vec<FileIssue>,
}

impl VerificationReport {
    pub fn is_valid(&self) -> bool {
        self.issues.is_empty()
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RepairReport {
    /// Disk inspection results before the repair was applied.
    pub verification: VerificationReport,
    pub repaired: usize,
    /// Includes reference downloads for unhashed files and overrides.
    pub downloaded: usize,
}

struct Inspection {
    report: VerificationReport,
    expected: BTreeMap<String, ManifestFile>,
    installs: Vec<(PathBuf, PathBuf)>,
    downloaded: usize,
}

impl Inspection {
    fn record(&mut self, path: &Path, actual: Option<&ManifestFile>, matches: bool) {
        self.report.checked += 1;
        if !matches {
            self.report.issues.push(FileIssue {
                path: path.to_string_lossy().into_owned(),
                kind: if actual.is_none() {
                    FileIssueKind::Missing
                } else {
                    FileIssueKind::Different
                },
            });
        }
    }
}

impl Client {
    /// Compares actual installed files with the supplied current provider list.
    /// Does not create or modify the destination or read/write its manifest.
    /// Remote hashes permit local-only inspection; unhashed files and overrides
    /// require verified reference downloads into temporary storage.
    /// Unsafe paths/symlinks, inaccessible files, and download failures return
    /// `Err`, not a successful or partial integrity report.
    /// The caller must prevent concurrent changes throughout inspection.
    ///
    /// ```no_run
    /// # async fn example() -> hostmylauncher::Result<()> {
    /// let client = hostmylauncher::Client::new();
    /// let files = client.get_instance_files("launcher", "instance", None).await?;
    /// let root = std::path::Path::new("instances/instance");
    /// let report = client.verify_instance_files(&files, root).await?;
    /// if !report.is_valid() {
    ///     let repair = client.repair_instance_files(
    ///         &files, root, &root.join("manifest.json"), |_| {},
    ///     ).await?;
    ///     println!("Repaired {} files", repair.repaired);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn verify_instance_files(
        &self,
        files: &[RemoteFile],
        destination: &Path,
    ) -> Result<VerificationReport> {
        let root = inspection_root(destination)?;
        let staging = tempfile::tempdir()?;
        let inspection = self
            .inspect_files(files, &root, staging.path(), None, &|_| {})
            .await?;
        Ok(inspection.report)
    }

    /// Restores missing/different expected files, including user modifications.
    /// Unrelated and obsolete files are not removed; their existing manifest
    /// entries are retained so a later sync can still manage them.
    /// New content is verified in staging before applying a transactional batch
    /// and saving the manifest last. Returned errors trigger the same rollback
    /// as sync; neither operation is crash-atomic or safe for concurrent use.
    /// The destination root may remain created after a failed first repair.
    pub async fn repair_instance_files<F>(
        &self,
        files: &[RemoteFile],
        destination: &Path,
        manifest_path: &Path,
        progress: F,
    ) -> Result<RepairReport>
    where
        F: Fn(SyncProgress) + Send + Sync + 'static,
    {
        // Reject an existing unsafe root before creating any directories.
        let root = inspection_root(destination)?;
        std::fs::create_dir_all(&root)?;
        let root = std::fs::canonicalize(root)?;
        let manifest_target = manifest_output_target(manifest_path)?;
        let mut manifest = load_manifest(manifest_path)?;
        let staging = tempfile::tempdir_in(&root)?;
        let inspection = self
            .inspect_files(
                files,
                &root,
                staging.path(),
                Some(&manifest_target),
                &progress,
            )
            .await?;
        manifest.version = 1;
        manifest.files.extend(inspection.expected);
        let repaired = inspection.installs.len();
        apply_sync_changes(
            &root,
            inspection.installs,
            Vec::new(),
            manifest_path,
            &manifest,
        )?;
        Ok(RepairReport {
            verification: inspection.report,
            repaired,
            downloaded: inspection.downloaded,
        })
    }

    // `manifest_target` selects repair: only repair prepares installs and checks
    // conflicts with its control file. Both routes inspect actual disk content.
    async fn inspect_files<F>(
        &self,
        files: &[RemoteFile],
        root: &Path,
        staging: &Path,
        manifest_target: Option<&Path>,
        progress: &F,
    ) -> Result<Inspection>
    where
        F: Fn(SyncProgress) + Send + Sync,
    {
        let repair = manifest_target.is_some();
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
            } else {
                let relative = safe_entry_path(path)?;
                if regular.insert(relative, file).is_some() {
                    return Err(Error::Other(format!("duplicate remote file: {path}")));
                }
            }
        }
        let mut inspection = Inspection {
            report: VerificationReport::default(),
            expected: BTreeMap::new(),
            installs: Vec::new(),
            downloaded: 0,
        };
        for (path, file) in &regular {
            check_inspection_target(root, path, manifest_target)?;
            let actual = installed_entry(root, path).await?;
            let has_hash = file
                .hashes
                .as_ref()
                .is_some_and(|hashes| hashes.sha512.is_some() || hashes.sha1.is_some());
            if has_hash {
                let matches = actual
                    .as_ref()
                    .is_some_and(|actual| disk_matches_remote(actual, file));
                inspection.record(path, actual.as_ref(), matches);
                if matches {
                    inspection
                        .expected
                        .insert(path.to_string_lossy().into_owned(), actual.unwrap());
                    continue;
                }
                if !repair {
                    continue;
                }
            }
            let staged = staging.join("regular").join(path);
            download_reference(self, file, path, &staged, progress).await?;
            inspection.downloaded += 1;
            let expected = manifest_file(&staged, file).await?;
            let matches = actual.as_ref() == Some(&expected);
            if !has_hash {
                inspection.record(path, actual.as_ref(), matches);
            }
            inspection
                .expected
                .insert(path.to_string_lossy().into_owned(), expected);
            if repair && !matches {
                inspection.installs.push((path.clone(), staged));
            }
        }
        if let Some(file) = overrides {
            let archive = staging.join("overrides.zip");
            download_reference(self, file, Path::new("overrides.zip"), &archive, progress).await?;
            // download_file prefers SHA512; check every supplied hash too.
            manifest_file(&archive, file).await?;
            inspection.downloaded += 1;
            let extracted = staging.join("overrides");
            extract_overrides(&archive, &extracted)?;
            for staged in collect_files(&extracted)? {
                let path = staged
                    .strip_prefix(&extracted)
                    .map_err(|error| Error::Other(error.to_string()))?
                    .to_path_buf();
                if regular
                    .keys()
                    .any(|regular| path.starts_with(regular) || regular.starts_with(&path))
                {
                    return Err(Error::Other(format!(
                        "override conflicts with remote file: {}",
                        path.display()
                    )));
                }
                check_inspection_target(root, &path, manifest_target)?;
                let expected = entry_from_hashes(hash_file(&staged).await?);
                let actual = installed_entry(root, &path).await?;
                let matches = actual.as_ref() == Some(&expected);
                inspection.record(&path, actual.as_ref(), matches);
                inspection
                    .expected
                    .insert(path.to_string_lossy().into_owned(), expected);
                if repair && !matches {
                    inspection.installs.push((path, staged));
                }
            }
        }
        inspection.report.issues.sort_by(|a, b| a.path.cmp(&b.path));
        Ok(inspection)
    }
}

fn inspection_root(destination: &Path) -> Result<PathBuf> {
    match std::fs::symlink_metadata(destination) {
        Ok(metadata) if metadata.file_type().is_dir() => Ok(std::fs::canonicalize(destination)?),
        Ok(_) => Err(Error::Other(format!(
            "unsafe instance root: {}",
            destination.display()
        ))),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            manifest_output_target(destination)
        }
        Err(error) => Err(error.into()),
    }
}

fn check_inspection_target(root: &Path, relative: &Path, manifest: Option<&Path>) -> Result<()> {
    let target = root.join(relative);
    if let Some(manifest) = manifest {
        reject_manifest_conflict(&target, manifest)?;
        if manifest.starts_with(&target) || target.starts_with(manifest) {
            return Err(Error::Other(
                "instance file conflicts with manifest path".into(),
            ));
        }
    }
    if root.exists() {
        validate_file_target(&target, root)?;
    }
    Ok(())
}

async fn installed_entry(root: &Path, relative: &Path) -> Result<Option<ManifestFile>> {
    if !root.exists() {
        return Ok(None);
    }
    let target = root.join(relative);
    validate_file_target(&target, root)?;
    match std::fs::symlink_metadata(&target) {
        Ok(_) => Ok(Some(entry_from_hashes(hash_file(&target).await?))),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.into()),
    }
}

fn entry_from_hashes((sha1, sha512, size): (String, String, u64)) -> ManifestFile {
    ManifestFile { sha1, sha512, size }
}

fn disk_matches_remote(actual: &ManifestFile, remote: &RemoteFile) -> bool {
    remote.size.is_none_or(|size| size == actual.size)
        && remote.hashes.as_ref().is_some_and(|hashes| {
            hashes.sha1.as_ref().is_none_or(|hash| hash == &actual.sha1)
                && hashes
                    .sha512
                    .as_ref()
                    .is_none_or(|hash| hash == &actual.sha512)
        })
}

async fn download_reference<F>(
    client: &Client,
    remote: &RemoteFile,
    path: &Path,
    staged: &Path,
    progress: &F,
) -> Result<()>
where
    F: Fn(SyncProgress) + Send + Sync,
{
    if let Some(parent) = staged.parent() {
        std::fs::create_dir_all(parent)?;
    }
    client
        .download_file(remote, staged, |current, total| {
            progress(SyncProgress::download(
                path.to_string_lossy().into_owned(),
                current,
                total,
            ));
        })
        .await
}
