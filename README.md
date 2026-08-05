# stackedhost-sdk

Async Rust client (REST + WebSocket) for the **StackedHost** launcher API
([`stackedhost-d1`](https://github.com/fitzxel/stackedhost-d1)). Built for Tauri launchers.

- Typed REST: list/get/resolve-by-code instances, file lists, news, download counter.
- Real-time: a self-reconnecting [`Realtime`] event stream over the launcher WebSocket
  (Durable Object), with `since` replay and keepalive.
- `download` feature: per-file download with size + SHA-1/SHA-512 verification.
- `watch` feature: `InstanceWatcher` keeps a live, reconciled instance list (re-fetches on
  events; a now-hidden instance is dropped = access revocation).

Requires a Tokio runtime.

## Usage

```rust
use stackedhost_sdk::{Client, ConnectOpts, ListOpts};
use futures::StreamExt;

#[tokio::main]
async fn main() -> stackedhost_sdk::Result<()> {
    let client = Client::new("https://stackedhost-d1.example.workers.dev")?;

    // Catalog
    let instances = client.list_instances("lns", &ListOpts::default()).await?;

    // "Enter code" flow → instance + files (works for hidden/locked too)
    let inst = client.resolve_by_code("lns", "SECRET").await?;
    let files = client.get_instance_files("lns", &inst.id, Some("SECRET")).await?;
    for f in &files {
        // f.downloads[0] is a direct URL; verifies size + sha if present
        client.download_file(f, std::path::Path::new("/tmp/out"), |d, t| {
            let _ = (d, t);
        }).await?;
    }

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
| `download` | yes     | `Client::download_file` (size + SHA verification)   |
| `watch`    | no      | `InstanceWatcher` live reconciled instance list     |

## Example

```bash
SH_BASE=http://localhost:8787 SH_LAUNCHER=lns SH_CODE=SECRET cargo run --example launcher
```

## License

MIT
