# hostmylauncher

Async Rust client (REST + WebSocket) for the **HostMyLauncher** launcher API.
Built for Tauri launchers.

- Typed REST: list/get/resolve-by-code instances, file lists, news, download counter.
- Real-time: a self-reconnecting [`Realtime`] event stream over the launcher WebSocket
  (Durable Object), with `since` replay and keepalive.
- `download` feature: per-file download with size + SHA-1/SHA-512 verification.
- `watch` feature: `InstanceWatcher` keeps a live, reconciled instance list (re-fetches on
  events; a now-hidden instance is dropped = access revocation).

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

    // "Enter code" flow → instance + files (works for hidden/locked too)
    let inst = client.resolve_by_code("lns", "SECRET").await?;
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

## Features

| Feature    | Default | Adds                                                |
|------------|---------|-----------------------------------------------------|
| `download` | yes     | `Client::download_file`, `download_overrides`, and `sync_instance_files` (size + SHA verification) |
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
