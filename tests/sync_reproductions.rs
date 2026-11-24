//! Regression tests for transactional sync, path confinement, and overrides.
#![cfg(feature = "download")]

use hostmylauncher::{models::RemoteFile, Client};
use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    path::Path,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread,
    time::Duration,
};
use zip::{write::SimpleFileOptions, ZipWriter};

struct FileServer {
    url: String,
    stop: Arc<AtomicBool>,
    worker: Option<thread::JoinHandle<()>>,
}

impl FileServer {
    fn new(body: Vec<u8>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}/file", listener.local_addr().unwrap());
        listener.set_nonblocking(true).unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        let worker_stop = stop.clone();
        let worker = thread::spawn(move || {
            while !worker_stop.load(Ordering::SeqCst) {
                match listener.accept() {
                    Ok((mut stream, _)) => {
                        stream
                            .set_read_timeout(Some(Duration::from_secs(2)))
                            .unwrap();
                        stream
                            .set_write_timeout(Some(Duration::from_secs(2)))
                            .unwrap();
                        serve(&mut stream, &body);
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(5));
                    }
                    Err(error) => panic!("local HTTP server failed: {error}"),
                }
            }
        });
        Self {
            url,
            stop,
            worker: Some(worker),
        }
    }

    fn remote(&self, path: &str, size: usize) -> RemoteFile {
        RemoteFile {
            path: Some(path.into()),
            name: None,
            size: Some(size as u64),
            hashes: None,
            downloads: vec![self.url.clone()],
        }
    }
}

impl Drop for FileServer {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        self.worker.take().unwrap().join().unwrap();
    }
}

fn serve(stream: &mut TcpStream, body: &[u8]) {
    let mut request = Vec::new();
    let mut buffer = [0; 1024];
    while !request.windows(4).any(|bytes| bytes == b"\r\n\r\n") {
        match stream.read(&mut buffer) {
            Ok(0) | Err(_) => return,
            Ok(count) => request.extend_from_slice(&buffer[..count]),
        }
        if request.len() > 16 * 1024 {
            return;
        }
    }
    let header = format!(
        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    if stream.write_all(header.as_bytes()).is_ok() {
        let _ = stream.write_all(body);
    }
}

fn archive(path: &str, contents: &[u8]) -> Vec<u8> {
    let mut writer = ZipWriter::new(std::io::Cursor::new(Vec::new()));
    writer
        .start_file(path, SimpleFileOptions::default())
        .unwrap();
    writer.write_all(contents).unwrap();
    writer.finish().unwrap().into_inner()
}

fn client(server: &FileServer) -> Client {
    Client::with_base_url(&server.url).unwrap()
}

fn read(path: impl AsRef<Path>) -> Vec<u8> {
    std::fs::read(path).unwrap()
}

#[tokio::test]
async fn failed_install_preserves_previous_files_and_manifest() {
    let sandbox = tempfile::tempdir().unwrap();
    let root = sandbox.path().join("instance");
    let manifest = sandbox.path().join("manifest.json");
    let old_server = FileServer::new(b"old".to_vec());
    let client = client(&old_server);
    client
        .sync_instance_files(&[old_server.remote("a.txt", 3)], &root, &manifest, |_| {})
        .await
        .unwrap();
    let previous_manifest = read(&manifest);

    // An unsafe target is rejected during preparation, before any installation.
    std::fs::create_dir(root.join("z.txt")).unwrap();
    let new_server = FileServer::new(b"updated".to_vec());
    let error = client
        .sync_instance_files(
            &[new_server.remote("a.txt", 7), new_server.remote("z.txt", 7)],
            &root,
            &manifest,
            |_| {},
        )
        .await
        .unwrap_err();

    assert_eq!(
        error.to_string(),
        format!("unsafe existing target: {}", root.join("z.txt").display())
    );
    assert_eq!(read(root.join("a.txt")), b"old");
    assert!(root.join("z.txt").is_dir());
    assert_eq!(read(&manifest), previous_manifest);
}

#[cfg(unix)]
#[tokio::test]
async fn rejects_obsolete_file_parent_symlink_without_deleting_outside_file() {
    use std::os::unix::fs::symlink;
    let sandbox = tempfile::tempdir().unwrap();
    let root = sandbox.path().join("instance");
    let outside = sandbox.path().join("outside");
    let manifest = sandbox.path().join("manifest.json");
    let server = FileServer::new(b"managed".to_vec());
    let client = client(&server);
    client
        .sync_instance_files(
            &[server.remote("mods/obsolete.jar", 7)],
            &root,
            &manifest,
            |_| {},
        )
        .await
        .unwrap();
    // Move the legitimate managed directory outside the configured root.
    std::fs::rename(root.join("mods"), &outside).unwrap();
    symlink(&outside, root.join("mods")).unwrap();
    let outside_file = outside.join("obsolete.jar");
    assert_eq!(read(&outside_file), b"managed");

    let previous_manifest = read(&manifest);
    let error = client
        .sync_instance_files(&[], &root, &manifest, |_| {})
        .await
        .unwrap_err();
    assert_eq!(
        error.to_string(),
        "override destination contains a symlink or non-directory"
    );
    assert_eq!(read(&outside_file), b"managed");
    assert_eq!(read(&manifest), previous_manifest);
    assert!(std::fs::symlink_metadata(root.join("mods"))
        .unwrap()
        .file_type()
        .is_symlink());
    assert_eq!(std::fs::read_dir(&outside).unwrap().count(), 1);
}

#[cfg(unix)]
#[tokio::test]
async fn rejects_parent_symlink_before_creating_outside_directory() {
    use std::os::unix::fs::symlink;
    let sandbox = tempfile::tempdir().unwrap();
    let root = sandbox.path().join("instance");
    let outside = sandbox.path().join("outside");
    let manifest = sandbox.path().join("manifest.json");
    std::fs::create_dir(&root).unwrap();
    std::fs::create_dir(&outside).unwrap();
    symlink(&outside, root.join("config")).unwrap();
    let body = archive("config/new/file.txt", b"content");
    let server = FileServer::new(body.clone());
    assert!(!outside.join("new").exists());

    let error = client(&server)
        .sync_instance_files(
            &[server.remote("overrides.zip", body.len())],
            &root,
            &manifest,
            |_| {},
        )
        .await
        .unwrap_err();
    assert_eq!(
        error.to_string(),
        "override destination contains a symlink or non-directory"
    );
    assert!(!outside.join("new").exists());
    assert_eq!(std::fs::read_dir(&outside).unwrap().count(), 0);
    assert!(!outside.join("new/file.txt").exists());
    assert!(!manifest.exists());
    assert!(std::fs::symlink_metadata(root.join("config"))
        .unwrap()
        .file_type()
        .is_symlink());
}

#[tokio::test]
async fn changed_override_is_installed_on_first_sync() {
    let sandbox = tempfile::tempdir().unwrap();
    let root = sandbox.path().join("instance");
    let manifest = sandbox.path().join("manifest.json");
    let old_body = archive("config/example.txt", b"old");
    let old_server = FileServer::new(old_body.clone());
    let client = client(&old_server);
    client
        .sync_instance_files(
            &[old_server.remote("overrides.zip", old_body.len())],
            &root,
            &manifest,
            |_| {},
        )
        .await
        .unwrap();
    let old_manifest = read(&manifest);
    assert_eq!(read(root.join("config/example.txt")), b"old");

    let new_body = archive("config/example.txt", b"updated");
    let new_server = FileServer::new(new_body.clone());
    let files = [new_server.remote("overrides.zip", new_body.len())];
    let first = client
        .sync_instance_files(&files, &root, &manifest, |_| {})
        .await
        .unwrap();
    let first_manifest = read(&manifest);
    let saved: serde_json::Value = serde_json::from_slice(&first_manifest).unwrap();
    assert_eq!(first.downloaded, 1);
    assert_eq!(first.removed, 0);
    assert_eq!(read(root.join("config/example.txt")), b"updated");
    assert_ne!(first_manifest, old_manifest);
    assert_eq!(saved["files"]["config/example.txt"]["size"], 7);
    use sha2::{Digest, Sha512};
    assert_eq!(
        saved["files"]["config/example.txt"]["sha512"],
        hex::encode(Sha512::digest(b"updated"))
    );

    let second = client
        .sync_instance_files(&files, &root, &manifest, |_| {})
        .await
        .unwrap();
    assert_eq!(second.downloaded, 1);
    assert_eq!(second.removed, 0);
    assert_eq!(read(root.join("config/example.txt")), b"updated");
    assert_eq!(read(&manifest), first_manifest);
}

#[tokio::test]
async fn manifest_save_failure_rolls_back_files_and_new_directories() {
    let sandbox = tempfile::tempdir().unwrap();
    let root = sandbox.path().join("instance");
    std::fs::create_dir(&root).unwrap();
    std::fs::write(root.join("a.txt"), b"old").unwrap();
    // Loading a missing manifest succeeds; saving below a file cannot succeed.
    let blocked_parent = sandbox.path().join("blocked");
    std::fs::write(&blocked_parent, b"not a directory").unwrap();
    let manifest = blocked_parent.join("manifest.json");
    let server = FileServer::new(b"updated".to_vec());
    let error = client(&server)
        .sync_instance_files(
            &[
                server.remote("a.txt", 7),
                server.remote("new/nested/file.txt", 7),
            ],
            &root,
            &manifest,
            |_| {},
        )
        .await
        .unwrap_err();
    assert!(matches!(error, hostmylauncher::Error::Io(_)), "{error}");
    assert_eq!(read(root.join("a.txt")), b"old");
    assert!(!root.join("new").exists());
    assert_eq!(std::fs::read_dir(&root).unwrap().count(), 1);
    assert_eq!(read(blocked_parent), b"not a directory");
    assert!(!manifest.exists());
}

#[tokio::test]
async fn unchanged_regular_file_is_skipped_and_modified_stale_file_is_preserved() {
    let sandbox = tempfile::tempdir().unwrap();
    let root = sandbox.path().join("instance");
    let manifest = sandbox.path().join("manifest.json");
    let server = FileServer::new(b"managed".to_vec());
    let client = client(&server);
    let files = [
        server.remote("keep.txt", 7),
        server.remote("obsolete.txt", 7),
        server.remote("modified.txt", 7),
    ];
    let initial = client
        .sync_instance_files(&files, &root, &manifest, |_| {})
        .await
        .unwrap();
    assert_eq!(initial.downloaded, 3);
    assert_eq!(initial.removed, 0);
    std::fs::write(root.join("modified.txt"), b"user content").unwrap();
    let report = client
        .sync_instance_files(&files[..1], &root, &manifest, |_| {})
        .await
        .unwrap();
    assert_eq!(report.downloaded, 1);
    assert_eq!(report.removed, 1);
    assert_eq!(read(root.join("keep.txt")), b"managed");
    assert_eq!(read(root.join("modified.txt")), b"user content");
    assert!(!root.join("obsolete.txt").exists());
    let saved: serde_json::Value = serde_json::from_slice(&read(&manifest)).unwrap();
    assert_eq!(saved["files"].as_object().unwrap().len(), 1);
    assert!(saved["files"].get("keep.txt").is_some());
}

#[tokio::test]
async fn extracting_overrides_replaces_existing_regular_file() {
    let sandbox = tempfile::tempdir().unwrap();
    let root = sandbox.path().join("instance");
    std::fs::create_dir(&root).unwrap();
    std::fs::write(root.join("file.txt"), b"old").unwrap();
    let body = archive("file.txt", b"updated");
    let server = FileServer::new(body.clone());
    client(&server)
        .download_overrides(
            &server.remote("overrides.zip", body.len()),
            &root,
            |_, _| {},
        )
        .await
        .unwrap();
    assert_eq!(read(root.join("file.txt")), b"updated");
    assert_eq!(std::fs::read_dir(&root).unwrap().count(), 1);
}

#[cfg(unix)]
#[tokio::test]
async fn extraction_rejects_parent_symlink_before_creating_directories() {
    use std::os::unix::fs::symlink;
    let sandbox = tempfile::tempdir().unwrap();
    let root = sandbox.path().join("instance");
    let outside = sandbox.path().join("outside");
    std::fs::create_dir(&root).unwrap();
    std::fs::create_dir(&outside).unwrap();
    symlink(&outside, root.join("config")).unwrap();
    let body = archive("config/new/file.txt", b"content");
    let server = FileServer::new(body.clone());
    let error = client(&server)
        .download_overrides(
            &server.remote("overrides.zip", body.len()),
            &root,
            |_, _| {},
        )
        .await
        .unwrap_err();
    assert_eq!(
        error.to_string(),
        "override destination contains a symlink or non-directory"
    );
    assert_eq!(std::fs::read_dir(&outside).unwrap().count(), 0);
    assert_eq!(std::fs::read_dir(&root).unwrap().count(), 1);
}

#[tokio::test]
async fn manifest_save_failure_restores_deleted_managed_files() {
    use hostmylauncher::SyncStage;
    let sandbox = tempfile::tempdir().unwrap();
    let root = sandbox.path().join("instance");
    let state = sandbox.path().join("state");
    let retained_state = sandbox.path().join("retained-state");
    let manifest = state.join("manifest.json");
    let server = FileServer::new(b"old".to_vec());
    let client = client(&server);
    client
        .sync_instance_files(
            &[server.remote("a.txt", 3), server.remote("obsolete.txt", 3)],
            &root,
            &manifest,
            |_| {},
        )
        .await
        .unwrap();
    let previous_manifest = read(&manifest);

    let new_server = FileServer::new(b"updated".to_vec());
    let callback_state = state.clone();
    let callback_retained = retained_state.clone();
    // Fail only the final save, after the previous manifest has been read.
    // Move the fixture's control state aside rather than destroying its bytes.
    let error = client
        .sync_instance_files(
            &[new_server.remote("a.txt", 7)],
            &root,
            &manifest,
            move |progress| {
                if matches!(progress.stage, SyncStage::Install) {
                    std::fs::rename(&callback_state, &callback_retained).unwrap();
                    std::fs::write(&callback_state, b"blocked").unwrap();
                }
            },
        )
        .await
        .unwrap_err();
    assert!(matches!(error, hostmylauncher::Error::Io(_)), "{error}");
    assert_eq!(read(root.join("a.txt")), b"old");
    assert_eq!(read(root.join("obsolete.txt")), b"old");
    assert_eq!(std::fs::read_dir(&root).unwrap().count(), 2);
    assert_eq!(read(&state), b"blocked");
    assert_eq!(
        read(retained_state.join("manifest.json")),
        previous_manifest
    );
    // Restore the injected fixture fault and verify the manifest is unchanged.
    std::fs::remove_file(&state).unwrap();
    std::fs::rename(&retained_state, &state).unwrap();
    assert_eq!(read(&manifest), previous_manifest);
}

#[tokio::test]
async fn remote_assets_cannot_replace_the_instance_manifest() {
    let sandbox = tempfile::tempdir().unwrap();
    let root = sandbox.path().join("instance");
    let manifest = root.join(".manifest.json");
    let old_server = FileServer::new(b"old".to_vec());
    let client = client(&old_server);
    client
        .sync_instance_files(&[old_server.remote("a.txt", 3)], &root, &manifest, |_| {})
        .await
        .unwrap();
    let previous_manifest = read(&manifest);
    let server = FileServer::new(b"updated".to_vec());
    let error = client
        .sync_instance_files(
            &[server.remote(".manifest.json", 7)],
            &root,
            &manifest,
            |_| {},
        )
        .await
        .unwrap_err();
    assert_eq!(
        error.to_string(),
        "instance file conflicts with manifest path"
    );
    assert_eq!(read(root.join("a.txt")), b"old");
    assert_eq!(read(&manifest), previous_manifest);
    assert_eq!(std::fs::read_dir(&root).unwrap().count(), 2);
}

#[cfg(unix)]
#[tokio::test]
async fn obsolete_final_symlink_is_preserved_without_touching_its_target() {
    use std::os::unix::fs::symlink;
    let sandbox = tempfile::tempdir().unwrap();
    let root = sandbox.path().join("instance");
    let manifest = sandbox.path().join("manifest.json");
    let server = FileServer::new(b"managed".to_vec());
    let client = client(&server);
    client
        .sync_instance_files(
            &[server.remote("obsolete.txt", 7)],
            &root,
            &manifest,
            |_| {},
        )
        .await
        .unwrap();
    let outside = sandbox.path().join("outside.txt");
    std::fs::rename(root.join("obsolete.txt"), &outside).unwrap();
    symlink(&outside, root.join("obsolete.txt")).unwrap();
    let report = client
        .sync_instance_files(&[], &root, &manifest, |_| {})
        .await
        .unwrap();
    assert_eq!(report.downloaded, 0);
    assert_eq!(report.removed, 0);
    assert_eq!(read(&outside), b"managed");
    assert!(std::fs::symlink_metadata(root.join("obsolete.txt"))
        .unwrap()
        .file_type()
        .is_symlink());
    let saved: serde_json::Value = serde_json::from_slice(&read(&manifest)).unwrap();
    assert_eq!(saved["files"], serde_json::json!({}));
}

#[tokio::test]
async fn late_install_failure_removes_previously_applied_new_file() {
    let sandbox = tempfile::tempdir().unwrap();
    let root = sandbox.path().join("instance");
    let manifest = sandbox.path().join("manifest.json");
    let server = FileServer::new(b"new".to_vec());
    let client = client(&server);
    client
        .sync_instance_files(&[], &root, &manifest, |_| {})
        .await
        .unwrap();
    let previous_manifest = read(&manifest);
    // Use separate regular/override staging trees so both paths reach apply.
    // Installing "a" then makes it an invalid parent for the next operation.
    let body = archive("a/child.txt", b"new");
    let override_server = FileServer::new(body.clone());
    let error = client
        .sync_instance_files(
            &[
                server.remote("a", 3),
                override_server.remote("overrides.zip", body.len()),
            ],
            &root,
            &manifest,
            |_| {},
        )
        .await
        .unwrap_err();
    assert_eq!(
        error.to_string(),
        "override destination contains a symlink or non-directory"
    );
    assert!(!root.join("a").exists());
    assert_eq!(std::fs::read_dir(&root).unwrap().count(), 0);
    assert_eq!(read(&manifest), previous_manifest);
}

#[tokio::test]
async fn manifest_alias_cannot_overwrite_an_installed_asset() {
    let sandbox = tempfile::tempdir().unwrap();
    let root = sandbox.path().join("instance");
    std::fs::create_dir(&root).unwrap();
    std::fs::write(root.join("a.txt"), b"old").unwrap();
    let manifest = root.join("missing").join("..").join("a.txt");
    let server = FileServer::new(b"updated".to_vec());
    let error = client(&server)
        .sync_instance_files(&[server.remote("a.txt", 7)], &root, &manifest, |_| {})
        .await
        .unwrap_err();
    assert_eq!(
        error.to_string(),
        "instance file conflicts with manifest path"
    );
    assert_eq!(read(root.join("a.txt")), b"old");
    assert!(!root.join("missing").exists());
    assert_eq!(std::fs::read_dir(&root).unwrap().count(), 1);
}

fn hashed_remote(server: &FileServer, path: &str, body: &[u8]) -> RemoteFile {
    use sha2::{Digest, Sha512};
    let mut remote = server.remote(path, body.len());
    remote.hashes = Some(hostmylauncher::models::Hashes {
        sha512: Some(hex::encode(Sha512::digest(body))),
        sha1: None,
    });
    remote
}

#[tokio::test]
async fn provider_regular_policy_preserves_modified_and_missing_files() {
    let sandbox = tempfile::tempdir().unwrap();
    let root = sandbox.path().join("instance");
    let manifest = sandbox.path().join("manifest.json");
    let server = FileServer::new(b"managed".to_vec());
    let client = client(&server);
    let files = [
        hashed_remote(&server, "modified", b"managed"),
        hashed_remote(&server, "missing", b"managed"),
    ];
    client
        .sync_instance_files(&files, &root, &manifest, |_| {})
        .await
        .unwrap();
    let saved = read(&manifest);
    std::fs::write(root.join("modified"), b"user content").unwrap();
    std::fs::remove_file(root.join("missing")).unwrap();
    let report = client
        .sync_instance_files(&files, &root, &manifest, |_| {})
        .await
        .unwrap();
    assert_eq!(report.downloaded, 0);
    assert_eq!(report.removed, 0);
    assert_eq!(read(root.join("modified")), b"user content");
    assert!(!root.join("missing").exists());
    assert_eq!(read(&manifest), saved);
    let updated = FileServer::new(b"updated".to_vec());
    let report = client
        .sync_instance_files(
            &[
                hashed_remote(&updated, "modified", b"updated"),
                hashed_remote(&updated, "missing", b"updated"),
            ],
            &root,
            &manifest,
            |_| {},
        )
        .await
        .unwrap();
    assert_eq!(report.downloaded, 2);
    assert_eq!(read(root.join("modified")), b"updated");
    assert_eq!(read(root.join("missing")), b"updated");
}

#[tokio::test]
async fn provider_override_policy_preserves_local_changes_until_provider_changes() {
    let sandbox = tempfile::tempdir().unwrap();
    let root = sandbox.path().join("instance");
    let manifest = sandbox.path().join("manifest.json");
    let body = archive("config.txt", b"managed");
    let server = FileServer::new(body.clone());
    let client = client(&server);
    let files = [server.remote("overrides.zip", body.len())];
    client
        .sync_instance_files(&files, &root, &manifest, |_| {})
        .await
        .unwrap();
    let saved = read(&manifest);
    std::fs::write(root.join("config.txt"), b"user content").unwrap();
    client
        .sync_instance_files(&files, &root, &manifest, |_| {})
        .await
        .unwrap();
    assert_eq!(read(root.join("config.txt")), b"user content");
    assert_eq!(read(&manifest), saved);
    std::fs::remove_file(root.join("config.txt")).unwrap();
    client
        .sync_instance_files(&files, &root, &manifest, |_| {})
        .await
        .unwrap();
    assert!(!root.join("config.txt").exists());
    assert_eq!(read(&manifest), saved);
    let body = archive("config.txt", b"updated");
    let updated = FileServer::new(body.clone());
    client
        .sync_instance_files(
            &[updated.remote("overrides.zip", body.len())],
            &root,
            &manifest,
            |_| {},
        )
        .await
        .unwrap();
    assert_eq!(read(root.join("config.txt")), b"updated");
}

#[tokio::test]
async fn provider_without_hashes_compares_staging_and_rejects_corrupt_updates() {
    let sandbox = tempfile::tempdir().unwrap();
    let root = sandbox.path().join("instance");
    let manifest = sandbox.path().join("manifest.json");
    let server = FileServer::new(b"managed".to_vec());
    let client = client(&server);
    let files = [server.remote("file.txt", 7)];
    client
        .sync_instance_files(&files, &root, &manifest, |_| {})
        .await
        .unwrap();
    let saved = read(&manifest);
    std::fs::write(root.join("file.txt"), b"user content").unwrap();
    let report = client
        .sync_instance_files(&files, &root, &manifest, |_| {})
        .await
        .unwrap();
    assert_eq!(report.downloaded, 1);
    assert_eq!(report.removed, 0);
    assert_eq!(read(root.join("file.txt")), b"user content");
    assert_eq!(read(&manifest), saved);
    let updated = FileServer::new(b"updated".to_vec());
    // Same size is not enough to infer unchanged provider content.
    client
        .sync_instance_files(&[updated.remote("file.txt", 7)], &root, &manifest, |_| {})
        .await
        .unwrap();
    assert_eq!(read(root.join("file.txt")), b"updated");
    let saved = read(&manifest);
    let bad = hashed_remote(&updated, "file.txt", b"invalid");
    let error = client
        .sync_instance_files(&[bad], &root, &manifest, |_| {})
        .await
        .unwrap_err();
    assert_eq!(error.to_string(), "sha512 mismatch");
    assert_eq!(read(root.join("file.txt")), b"updated");
    assert_eq!(read(&manifest), saved);
}

#[tokio::test]
async fn provider_fast_path_requires_all_supplied_hashes_to_match() {
    let sandbox = tempfile::tempdir().unwrap();
    let root = sandbox.path().join("instance");
    let manifest = sandbox.path().join("manifest.json");
    let server = FileServer::new(b"managed".to_vec());
    let client = client(&server);
    let mut remote = hashed_remote(&server, "file.txt", b"managed");
    client
        .sync_instance_files(&[remote.clone()], &root, &manifest, |_| {})
        .await
        .unwrap();
    let saved = read(&manifest);
    remote.hashes.as_mut().unwrap().sha1 = Some("0".repeat(40));
    let error = client
        .sync_instance_files(&[remote], &root, &manifest, |_| {})
        .await
        .unwrap_err();
    assert!(error.to_string().starts_with("sha1 mismatch"), "{error}");
    assert_eq!(read(root.join("file.txt")), b"managed");
    assert_eq!(read(&manifest), saved);
}

#[tokio::test]
async fn verification_reports_disk_differences_without_modifying_installation() {
    use hostmylauncher::FileIssueKind;
    let sandbox = tempfile::tempdir().unwrap();
    let root = sandbox.path().join("instance");
    let manifest = sandbox.path().join("manifest.json");
    let server = FileServer::new(b"managed".to_vec());
    let client = client(&server);
    let mut files = [
        hashed_remote(&server, "good", b"managed"),
        hashed_remote(&server, "modified", b"managed"),
        hashed_remote(&server, "missing", b"managed"),
    ];
    client
        .sync_instance_files(&files, &root, &manifest, |_| {})
        .await
        .unwrap();
    let saved = read(&manifest);
    std::fs::write(root.join("modified"), b"edited!").unwrap();
    std::fs::remove_file(root.join("missing")).unwrap();
    std::fs::write(root.join("unmanaged"), b"user").unwrap();
    // Hash-bearing verification must not need download URLs or previous history.
    for file in &mut files {
        file.downloads.clear();
    }
    let report = client.verify_instance_files(&files, &root).await.unwrap();
    assert_eq!(report.checked, 3);
    assert!(!report.is_valid());
    assert_eq!(report.issues.len(), 2);
    assert_eq!(report.issues[0].path, "missing");
    assert_eq!(report.issues[0].kind, FileIssueKind::Missing);
    assert_eq!(report.issues[1].path, "modified");
    assert_eq!(report.issues[1].kind, FileIssueKind::Different);
    assert_eq!(read(root.join("modified")), b"edited!");
    assert!(!root.join("missing").exists());
    assert_eq!(read(root.join("good")), b"managed");
    assert_eq!(read(root.join("unmanaged")), b"user");
    assert_eq!(read(&manifest), saved);
    assert_eq!(std::fs::read_dir(&root).unwrap().count(), 3);
}

#[tokio::test]
async fn verification_of_missing_root_does_not_create_it() {
    let sandbox = tempfile::tempdir().unwrap();
    let root = sandbox.path().join("missing/instance");
    let server = FileServer::new(b"managed".to_vec());
    let mut file = hashed_remote(&server, "mods/a", b"managed");
    file.downloads.clear();
    let report = client(&server)
        .verify_instance_files(&[file], &root)
        .await
        .unwrap();
    assert_eq!(report.checked, 1);
    assert_eq!(report.issues.len(), 1);
    assert_eq!(
        report.issues[0].kind,
        hostmylauncher::FileIssueKind::Missing
    );
    assert!(!sandbox.path().join("missing").exists());
}

#[tokio::test]
async fn repair_restores_user_edits_and_missing_files_without_removing_extras() {
    let sandbox = tempfile::tempdir().unwrap();
    let root = sandbox.path().join("instance");
    let manifest = sandbox.path().join("manifest.json");
    let server = FileServer::new(b"managed".to_vec());
    let client = client(&server);
    let files = [
        hashed_remote(&server, "good", b"managed"),
        hashed_remote(&server, "modified", b"managed"),
        hashed_remote(&server, "missing", b"managed"),
    ];
    let mut initial = files.to_vec();
    initial.push(hashed_remote(&server, "obsolete", b"managed"));
    client
        .sync_instance_files(&initial, &root, &manifest, |_| {})
        .await
        .unwrap();
    let original_manifest = read(&manifest);
    std::fs::write(root.join("modified"), b"edited!").unwrap();
    std::fs::remove_file(root.join("missing")).unwrap();
    std::fs::write(root.join("unmanaged"), b"user").unwrap();
    let report = client
        .repair_instance_files(&files, &root, &manifest, |_| {})
        .await
        .unwrap();
    assert_eq!(report.repaired, 2);
    assert_eq!(report.downloaded, 2);
    assert_eq!(report.verification.checked, 3);
    assert_eq!(report.verification.issues.len(), 2);
    for path in ["good", "modified", "missing", "obsolete"] {
        assert_eq!(read(root.join(path)), b"managed");
    }
    assert_eq!(read(root.join("unmanaged")), b"user");
    assert_eq!(read(&manifest), original_manifest);
    assert!(client
        .verify_instance_files(&files, &root)
        .await
        .unwrap()
        .is_valid());
    let report = client
        .repair_instance_files(&files, &root, &manifest, |_| {})
        .await
        .unwrap();
    assert_eq!(report.repaired, 0);
    assert_eq!(report.downloaded, 0);
    assert!(report.verification.is_valid());
}

#[tokio::test]
async fn verification_and_repair_handle_unhashed_files_and_overrides() {
    let sandbox = tempfile::tempdir().unwrap();
    let root = sandbox.path().join("instance");
    let manifest = sandbox.path().join("manifest.json");
    let server = FileServer::new(b"managed".to_vec());
    let body = archive("config.txt", b"expected");
    let overrides = FileServer::new(body.clone());
    let client = client(&server);
    let files = [
        server.remote("regular", 7),
        overrides.remote("overrides.zip", body.len()),
    ];
    client
        .sync_instance_files(&files, &root, &manifest, |_| {})
        .await
        .unwrap();
    let saved = read(&manifest);
    std::fs::write(root.join("regular"), b"edited!").unwrap();
    std::fs::remove_file(root.join("config.txt")).unwrap();
    let report = client.verify_instance_files(&files, &root).await.unwrap();
    assert_eq!(report.checked, 2);
    assert_eq!(report.issues.len(), 2);
    assert_eq!(read(root.join("regular")), b"edited!");
    assert!(!root.join("config.txt").exists());
    assert_eq!(read(&manifest), saved);
    let report = client
        .repair_instance_files(&files, &root, &manifest, |_| {})
        .await
        .unwrap();
    assert_eq!(report.repaired, 2);
    assert_eq!(report.downloaded, 2);
    assert_eq!(read(root.join("regular")), b"managed");
    assert_eq!(read(root.join("config.txt")), b"expected");
    assert!(client
        .verify_instance_files(&files, &root)
        .await
        .unwrap()
        .is_valid());
}

#[tokio::test]
async fn repair_rejects_bad_downloads_without_changing_files_or_manifest() {
    let sandbox = tempfile::tempdir().unwrap();
    let root = sandbox.path().join("instance");
    let manifest = sandbox.path().join("manifest.json");
    let server = FileServer::new(b"managed".to_vec());
    let client = client(&server);
    client
        .sync_instance_files(
            &[hashed_remote(&server, "file", b"managed")],
            &root,
            &manifest,
            |_| {},
        )
        .await
        .unwrap();
    let saved = read(&manifest);
    let bad = hashed_remote(&server, "file", b"invalid");
    let error = client
        .repair_instance_files(&[bad], &root, &manifest, |_| {})
        .await
        .unwrap_err();
    assert_eq!(error.to_string(), "sha512 mismatch");
    assert_eq!(read(root.join("file")), b"managed");
    assert_eq!(read(&manifest), saved);
    assert_eq!(std::fs::read_dir(&root).unwrap().count(), 1);
}

#[tokio::test]
async fn repair_rolls_back_if_manifest_cannot_be_saved() {
    let sandbox = tempfile::tempdir().unwrap();
    let root = sandbox.path().join("instance");
    std::fs::create_dir(&root).unwrap();
    std::fs::write(root.join("existing"), b"user").unwrap();
    let blocker = sandbox.path().join("blocker");
    std::fs::write(&blocker, b"keep").unwrap();
    let server = FileServer::new(b"managed".to_vec());
    let files = [
        hashed_remote(&server, "existing", b"managed"),
        hashed_remote(&server, "nested/new", b"managed"),
    ];
    assert!(client(&server)
        .repair_instance_files(&files, &root, &blocker.join("manifest.json"), |_| {})
        .await
        .is_err());
    assert_eq!(read(root.join("existing")), b"user");
    assert_eq!(read(&blocker), b"keep");
    assert!(!root.join("nested").exists());
    assert_eq!(std::fs::read_dir(&root).unwrap().count(), 1);
}

#[cfg(unix)]
#[tokio::test]
async fn verification_and_repair_reject_symlinks_without_touching_outside_files() {
    use std::os::unix::fs::symlink;
    let sandbox = tempfile::tempdir().unwrap();
    let root = sandbox.path().join("instance");
    std::fs::create_dir(&root).unwrap();
    let outside = sandbox.path().join("outside");
    std::fs::write(&outside, b"outside").unwrap();
    symlink(&outside, root.join("file")).unwrap();
    let server = FileServer::new(b"managed".to_vec());
    let files = [hashed_remote(&server, "file", b"managed")];
    let client = client(&server);
    assert!(client.verify_instance_files(&files, &root).await.is_err());
    let manifest = sandbox.path().join("manifest.json");
    assert!(client
        .repair_instance_files(&files, &root, &manifest, |_| {})
        .await
        .is_err());
    assert_eq!(read(&outside), b"outside");
    assert!(!manifest.exists());
    assert!(std::fs::symlink_metadata(root.join("file"))
        .unwrap()
        .file_type()
        .is_symlink());
}

#[tokio::test]
async fn verification_supports_sha1_without_size_and_checks_every_supplied_hash() {
    use sha1::{Digest, Sha1};
    let sandbox = tempfile::tempdir().unwrap();
    let root = sandbox.path().join("instance");
    std::fs::create_dir(&root).unwrap();
    std::fs::write(root.join("file"), b"managed").unwrap();
    let server = FileServer::new(b"managed".to_vec());
    let mut file = server.remote("file", 7);
    file.size = None;
    file.downloads.clear();
    file.hashes = Some(hostmylauncher::Hashes {
        sha1: Some(hex::encode(Sha1::digest(b"managed"))),
        sha512: None,
    });
    assert!(client(&server)
        .verify_instance_files(&[file], &root)
        .await
        .unwrap()
        .is_valid());
    let mut file = hashed_remote(&server, "file", b"managed");
    file.hashes.as_mut().unwrap().sha1 = Some("0".repeat(40));
    let report = client(&server)
        .verify_instance_files(&[file.clone()], &root)
        .await
        .unwrap();
    assert_eq!(report.issues.len(), 1);
    assert_eq!(
        report.issues[0].kind,
        hostmylauncher::FileIssueKind::Different
    );
    assert!(client(&server)
        .repair_instance_files(&[file], &root, &root.join("manifest.json"), |_| {})
        .await
        .is_err());
    assert_eq!(read(root.join("file")), b"managed");
    assert!(!root.join("manifest.json").exists());
}

#[tokio::test]
async fn inspection_rejects_unsafe_duplicate_and_manifest_conflicting_paths() {
    let sandbox = tempfile::tempdir().unwrap();
    let root = sandbox.path().join("instance");
    let manifest = root.join("manifest.json");
    let server = FileServer::new(b"managed".to_vec());
    let client = client(&server);
    client
        .sync_instance_files(
            &[hashed_remote(&server, "good", b"managed")],
            &root,
            &manifest,
            |_| {},
        )
        .await
        .unwrap();
    let saved = read(&manifest);
    for files in [
        vec![server.remote("../escape", 7)],
        vec![server.remote("good", 7), server.remote("good", 7)],
    ] {
        assert!(client.verify_instance_files(&files, &root).await.is_err());
        assert!(client
            .repair_instance_files(&files, &root, &manifest, |_| {})
            .await
            .is_err());
        assert_eq!(read(root.join("good")), b"managed");
        assert_eq!(read(&manifest), saved);
        assert!(!sandbox.path().join("escape").exists());
        assert_eq!(std::fs::read_dir(&root).unwrap().count(), 2);
    }
    for path in ["manifest.json", "manifest.json/asset"] {
        let error = client
            .repair_instance_files(&[server.remote(path, 7)], &root, &manifest, |_| {})
            .await
            .unwrap_err();
        assert_eq!(
            error.to_string(),
            "instance file conflicts with manifest path"
        );
        assert_eq!(read(&manifest), saved);
        assert_eq!(read(root.join("good")), b"managed");
    }
}

#[cfg(unix)]
#[tokio::test]
async fn inspection_rejects_symlink_parents_for_regular_and_override_entries() {
    use std::os::unix::fs::symlink;
    let sandbox = tempfile::tempdir().unwrap();
    let root = sandbox.path().join("instance");
    let outside = sandbox.path().join("outside");
    std::fs::create_dir(&root).unwrap();
    std::fs::create_dir(&outside).unwrap();
    std::fs::write(outside.join("file"), b"outside").unwrap();
    symlink(&outside, root.join("mods")).unwrap();
    let server = FileServer::new(b"managed".to_vec());
    let body = archive("mods/file", b"managed");
    let overrides = FileServer::new(body.clone());
    let manifest = sandbox.path().join("manifest.json");
    for files in [
        vec![hashed_remote(&server, "mods/file", b"managed")],
        vec![overrides.remote("overrides.zip", body.len())],
    ] {
        assert!(client(&server)
            .verify_instance_files(&files, &root)
            .await
            .is_err());
        assert!(client(&server)
            .repair_instance_files(&files, &root, &manifest, |_| {})
            .await
            .is_err());
        assert_eq!(read(outside.join("file")), b"outside");
        assert_eq!(std::fs::read_dir(&outside).unwrap().count(), 1);
        assert_eq!(std::fs::read_dir(&root).unwrap().count(), 1);
        assert!(!manifest.exists());
    }
}

/// Serves every connection on its own thread, holding each response briefly so
/// overlapping downloads are observable. Paths containing "missing" answer 404.
struct ParallelServer {
    url: String,
    peak: Arc<std::sync::atomic::AtomicUsize>,
}

impl ParallelServer {
    fn new(body: &'static [u8]) -> Self {
        use std::sync::atomic::AtomicUsize;
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let active = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let server_peak = peak.clone();
        // The listener thread ends with the test process.
        thread::spawn(move || {
            for mut stream in listener.incoming().flatten() {
                let (active, peak) = (active.clone(), server_peak.clone());
                thread::spawn(move || {
                    let mut request = [0; 1024];
                    let read = stream.read(&mut request).unwrap_or(0);
                    let now = active.fetch_add(1, Ordering::SeqCst) + 1;
                    peak.fetch_max(now, Ordering::SeqCst);
                    thread::sleep(Duration::from_millis(100));
                    active.fetch_sub(1, Ordering::SeqCst);
                    let response = if String::from_utf8_lossy(&request[..read]).contains("missing")
                    {
                        b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                            .to_vec()
                    } else {
                        let mut ok = format!(
                            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                            body.len()
                        )
                        .into_bytes();
                        ok.extend_from_slice(body);
                        ok
                    };
                    let _ = stream.write_all(&response);
                });
            }
        });
        Self { url, peak }
    }

    fn remote(&self, path: &str, body: &[u8]) -> RemoteFile {
        use sha2::{Digest, Sha512};
        RemoteFile {
            path: Some(path.into()),
            name: None,
            size: Some(body.len() as u64),
            hashes: Some(hostmylauncher::models::Hashes {
                sha512: Some(hex::encode(Sha512::digest(body))),
                sha1: None,
            }),
            downloads: vec![format!("{}/{path}", self.url)],
        }
    }
}

#[tokio::test]
async fn sync_and_repair_download_in_parallel_within_the_limit() {
    let server = ParallelServer::new(b"mod");
    let files: Vec<_> = (0..6)
        .map(|index| server.remote(&format!("mods/{index}.jar"), b"mod"))
        .collect();
    let client = Client::with_base_url(&server.url)
        .unwrap()
        .with_download_concurrency(3);
    let sandbox = tempfile::tempdir().unwrap();
    let root = sandbox.path().join("instance");
    let manifest = sandbox.path().join("manifest.json");

    let report = client
        .sync_instance_files(&files, &root, &manifest, |_| {})
        .await
        .unwrap();
    assert_eq!(report.downloaded, 6);
    assert_eq!(server.peak.swap(0, Ordering::SeqCst), 3);
    for index in 0..6 {
        assert_eq!(read(root.join(format!("mods/{index}.jar"))), b"mod");
    }

    for index in 0..6 {
        std::fs::remove_file(root.join(format!("mods/{index}.jar"))).unwrap();
    }
    let repair = client
        .repair_instance_files(&files, &root, &manifest, |_| {})
        .await
        .unwrap();
    assert_eq!(repair.repaired, 6);
    assert_eq!(server.peak.load(Ordering::SeqCst), 3);
    let missing: Vec<_> = repair.verification.issues.iter().map(|i| &i.path).collect();
    let expected: Vec<_> = (0..6).map(|index| format!("mods/{index}.jar")).collect();
    assert_eq!(missing, expected.iter().collect::<Vec<_>>());
}

#[tokio::test]
async fn one_failed_parallel_download_installs_nothing() {
    let server = ParallelServer::new(b"mod");
    let mut files: Vec<_> = (0..5)
        .map(|index| server.remote(&format!("mods/{index}.jar"), b"mod"))
        .collect();
    files.push(server.remote("mods/missing.jar", b"mod"));
    let client = Client::with_base_url(&server.url)
        .unwrap()
        .with_download_concurrency(3);
    let sandbox = tempfile::tempdir().unwrap();
    let root = sandbox.path().join("instance");
    let manifest = sandbox.path().join("manifest.json");

    assert!(client
        .sync_instance_files(&files, &root, &manifest, |_| {})
        .await
        .is_err());
    assert!(!manifest.exists());
    // Only the empty root remains; no staged or installed file leaks.
    assert_eq!(std::fs::read_dir(&root).unwrap().count(), 0);
}

/// Hosts run these operations on spawned tasks, which require `Send` futures.
#[allow(dead_code)]
fn file_operations_can_run_on_spawned_tasks(client: Client, files: Vec<RemoteFile>) {
    let path = Path::new("instance");
    let manifest = Path::new("manifest.json");
    let (a, b, c) = (client.clone(), client.clone(), client);
    let (fa, fb, fc) = (files.clone(), files.clone(), files);
    tokio::spawn(async move { a.sync_instance_files(&fa, path, manifest, |_| {}).await });
    tokio::spawn(async move { b.verify_instance_files(&fb, path).await });
    tokio::spawn(async move { c.repair_instance_files(&fc, path, manifest, |_| {}).await });
}

/// Host tasks often run on 2 MiB stacks in debug builds. Large on-stack buffers
/// inside these futures once overflowed a spawned bridge operation.
#[test]
fn file_operation_futures_stay_small() {
    let client = Client::with_base_url("http://127.0.0.1:9").unwrap();
    let (path, manifest) = (Path::new("instance"), Path::new("manifest.json"));
    let sizes = [
        std::mem::size_of_val(&client.sync_instance_files(&[], path, manifest, |_| {})),
        std::mem::size_of_val(&client.verify_instance_files(&[], path)),
        std::mem::size_of_val(&client.repair_instance_files(&[], path, manifest, |_| {})),
    ];
    eprintln!("future sizes: {sizes:?}");
    assert!(sizes.iter().all(|&size| size < 16 * 1024), "{sizes:?}");
}
