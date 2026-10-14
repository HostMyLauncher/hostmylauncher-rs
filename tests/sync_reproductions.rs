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
