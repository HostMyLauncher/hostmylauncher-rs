# hostmylauncher

Async Rust client (REST + WebSocket) for the **HostMyLauncher** launcher API.
Built for Tauri launchers.

- Typed REST: list/get/resolve-by-code instances, file lists, news, download counter.
- Real-time: a self-reconnecting [`Realtime`] event stream over the launcher WebSocket
  (Durable Object), with `since` replay and keepalive. Dropping it (or `close()`) closes
  the socket right away.
- `download` feature: per-file download with size + SHA-1/SHA-512 verification.
- `watch` feature: `InstanceWatcher` keeps a live, reconciled instance list (re-fetches on
  events; a now-hidden instance is dropped = access revocation). Receivers wake only when
  the list changes; dropping the watcher closes its connection.

Requires a Tokio runtime.

## Usage

`Client::new()` connects to `https://api.hostmylauncher.com` by default.
`Client::default()` is equivalent. To override the server URL, use
`Client::with_base_url("http://localhost:8787")?`; an invalid URL returns an error.

```rust
use hostmylauncher::{Client, ConnectOpts, ListOpts};
use futures::StreamExt;

#[tokio::main]
async fn main() -> hostmylauncher::Result<()> {
    let client = Client::new();

    // Catalog
    let instances = client.list_instances("lns", &ListOpts::default()).await?;

    // "Enter code" flow → public instance, then files (hidden/locked too)
    let inst = client.resolve_by_code(&["lns"], "SECRET").await?;
    let files = client.get_instance_files("lns", &inst.id, Some("SECRET")).await?;
    for f in &files {
        if f.path.as_deref() == Some("overrides.zip") {
            continue;
        }
        // f.downloads[0] is a direct URL; verifies size + sha if present
        client.download_file(f, std::path::Path::new("/tmp/out"), |d, t| {
            let _ = (d, t);
        }).await?;
    }

    // Use the manifest-backed instance synchronizer. It handles regular
    // files and overrides.zip together and preserves user-modified stale files.
    let report = client.sync_instance_files(
        &files,
        std::path::Path::new("/tmp/instance"),
        std::path::Path::new("/tmp/instance/.hostmylauncher-manifest.json"),
        |progress| println!("{progress:?}"),
    ).await?;
    println!("synced: {report:?}");

    // Real-time: keep state fresh without polling
    let mut rt = client.connect("lns", ConnectOpts::default());
    while let Some(event) = rt.next().await {
        println!("{event:?}");
    }
    Ok(())
}
```

## Resolve an access code

```rust,no_run
# async fn example() -> hostmylauncher::Result<()> {
let client = hostmylauncher::Client::new();
let instance = client.resolve_by_code(&["launcher-a", "launcher-b"], "SECRET").await?;
let launcher_id = instance.launcher_id.as_deref().expect("resolver returns launcher_id");
let files = client.get_instance_files(launcher_id, &instance.id, Some("SECRET")).await?;
# Ok(())
# }
```

`resolve_by_code` sends `POST /instances/resolve` with JSON `code` and `launcher_ids`;
it no longer uses the launcher-scoped GET resolver. Supply 1–20 known launcher IDs.
The response always contains public fields only, even with a bearer token or for a hidden
instance. Fetch files separately with the returned launcher ID and original code.

The API rejects ambiguous matches with `409`, and persists attempt quotas in D1 across
requests and restarts: 30 attempts/IP and 120 attempts/launcher per 60-second window.
Temporary blocks return `429 Too Many Requests` and an HTTP `Retry-After` header.
The SDK propagates `409`, `429`, and protection failures (`503`) as `Error::Api` and does
not retry automatically; the current error type does not expose response headers.
Launcher IDs restrict the search, but are not themselves an authorization credential.

## Instance synchronization safety

`sync_instance_files` compares provider content with the saved manifest, not
with the current installed contents. Regular files with unchanged remote size
and hashes reuse their previous manifest entries without reading local contents;
local edits and missing files are left as-is. If remote hashes are absent, the
file is downloaded and verified in staging, then compared with the previous
manifest before deciding whether to install it. Size alone cannot prove that
provider content is unchanged. `SyncReport.downloaded` counts these downloads,
even when no replacement is necessary.

`overrides.zip` is downloaded and verified on each sync. Extracted entries are
compared with the previous manifest, not the installed files: unchanged provider
entries preserve local edits and missing files, while changed or new entries
are installed. The manifest continues to describe provider content. Sync is not
an integrity audit or repair operation.

All new downloads are verified in staging. The synchronizer installs overrides
on the first sync and removes only unchanged files owned by the previous
manifest. User-modified obsolete files and final-component symlinks are preserved.
Existing symlinks in destination parent directories are rejected before hashing,
creating directories, or deleting files. A remote asset cannot use the manifest's
output path (`instance file conflicts with manifest path`).

Before applying a batch, the synchronizer backs up existing files inside the
instance directory. Replacements use rename without first deleting the installed
file. An installation, removal, or manifest-save error triggers rollback of
applied files and newly created instance subdirectories. The manifest is written
last using a temporary file and rename. Allow enough free space for staging and
backup copies of replaced or removed files.

This provides recovery from returned filesystem errors, **not crash-atomic or
concurrent synchronization**. The caller must ensure exclusive access to the
instance and manifest while syncing. A process crash or power loss can interrupt
application. If rollback itself fails, the returned error includes
`rollback failed` and the retained backup directory; preserve it for manual
recovery. The destination root may remain created after a failed first sync, and
parent directories for a caller-selected external manifest are not rolled back.
`download_overrides` alone does not provide batch rollback.

### Parallel downloads

Sync, verify and repair download up to 8 instance files at the same time.
Change the limit with `Client::with_download_concurrency(n)`. Every file is
still staged and verified before anything is installed, and the batch is applied
in path order once all downloads succeed: one failed download installs nothing.
`overrides.zip` is downloaded after the regular files.

`Download` progress covers the whole operation: `current` is the bytes downloaded
so far across all its files (`overrides.zip` included) and `total` their declared
size, or 0 if any file has no size. `path` names the file that advanced. A retried
file starts over, so `current` can go back. `Install` events count prepared
entries (`current` of `total`) in completion order.

Run the regression tests with `cargo test --offline --lib --test sync_reproductions`.
Symlink tests run on Unix only.

## Verify and repair an installation

These operations require the `download` feature and a current file list from
`get_instance_files`. Neither operation uses the old manifest to decide whether
an installed file is correct: they compare the actual disk with provider content.

```rust,no_run
# async fn example() -> hostmylauncher::Result<()> {
use hostmylauncher::{Client, FileIssueKind};
use std::path::Path;

let client = Client::new();
let files = client.get_instance_files("launcher-a", "instance-a", None).await?;
let destination = Path::new("instances/instance-a");
let manifest = destination.join(".hostmylauncher-manifest.json");

let verification = client.verify_instance_files(&files, destination).await?;
for issue in &verification.issues {
    match issue.kind {
        FileIssueKind::Missing => println!("Missing: {}", issue.path),
        FileIssueKind::Different => println!("Different: {}", issue.path),
    }
}

// Repair is explicit: it replaces user edits as well as corrupt/missing files.
if !verification.is_valid() {
    let repair = client.repair_instance_files(
        &files, destination, &manifest,
        |progress| println!("{progress:?}"),
    ).await?;
    println!("Repaired {} files", repair.repaired);
}
# Ok(())
# }
```

| Operation | Compares | Effect |
|-----------|----------|--------|
| `sync_instance_files` | Current provider content vs saved provider manifest | Installs provider changes; preserves local divergence for unchanged provider files. |
| `verify_instance_files` | Actual installed files vs current provider content | Returns missing/different paths; does not modify or create the installation or its manifest. |
| `repair_instance_files` | Actual installed files vs current provider content | Restores missing/different expected files, including user modifications. |

`VerificationReport.checked` counts expected files, including extracted override
entries, not the ZIP itself. `issues` is sorted by relative path and distinguishes
`Missing` from `Different`. An empty expected list produces a valid empty report;
this is not proof that a complete instance is installed. Unrelated files are not
inspected or reported.

Verification hashes actual files. If remote hashes are supplied, it checks every
supplied hash and the size when present, without downloading those regular files.
Without hashes, it downloads and verifies reference content in temporary storage.
`overrides.zip` is always downloaded, verified, and extracted to temporary storage
so its entries can be checked. These reference downloads may need network access
and free space; verification's temporary storage uses the system temporary directory.

Repair performs its own inspection rather than trusting an earlier report. It
stages verified content inside the destination and uses the same backup/rollback
and manifest-last application as synchronization. It does **not** remove obsolete
or unrelated files; previous manifest entries outside the supplied list are retained
for future synchronization. `RepairReport.verification` describes differences
**before** repair, `repaired` counts installed files, and `downloaded` counts
downloads of regular files and override archives, including reference downloads
that need no replacement. The progress callback reports download progress.

Unsafe paths, symlinks at/below the instance root, non-file targets, unreadable
files, and download/integrity failures return `Err`, not a partial success report.
The manifest may not collide with an expected asset or its parent/child paths.
Both operations require exclusive access while running. Repair has the same
crash/concurrency and failed-rollback limitations as sync. A new destination root
may remain after a failed first repair. These operations only manage listed assets;
they do not install Java, Minecraft loaders, or other runtime prerequisites.

## Features

| Feature    | Default | Adds                                                |
|------------|---------|-----------------------------------------------------|
| `download` | yes     | `Client::download_file`, `download_overrides`, `sync_instance_files`, `verify_instance_files`, and `repair_instance_files` (size + SHA verification) |
| `watch`    | no      | `InstanceWatcher` live reconciled instance list     |

## Example

The `launcher` example lists instances, optionally resolves an instance by access
code and fetches its file list, then listens for real-time events for 30 seconds.
It reads the following environment variables; the library itself does not read them.

| Variable | Purpose | When omitted |
|----------|---------|--------------|
| `HML_BASE_URL` | Override the API base URL, including the scheme (`https://` or `http://`). | Uses production: `https://api.hostmylauncher.com`. |
| `HML_LAUNCHER_ID` | Launcher identifier used to query instances and subscribe to events. | Uses the example identifier `lns`; set your own launcher ID. |
| `HML_ACCESS_CODE` | Instance access code used to resolve an instance and fetch its files; not an API authentication token. | Skips the access-code flow. |

Against production:

```bash
HML_LAUNCHER_ID=your-launcher-id cargo run --example launcher
```

Against a local API, including the access-code flow:

```bash
HML_BASE_URL=http://localhost:8787 HML_LAUNCHER_ID=lns HML_ACCESS_CODE=SECRET cargo run --example launcher
```

Applications using the library pass these values directly to its methods. For
API authentication, use `Client::with_token(...)` rather than `HML_ACCESS_CODE`.

## License

MIT
