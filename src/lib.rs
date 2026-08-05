//! Async Rust client for the **StackedHost** launcher API (`stackedhost-d1`).
//!
//! Provides typed REST access (instances, files, news), a self-reconnecting
//! real-time [`Realtime`] event stream over the launcher WebSocket, optional
//! file download with SHA verification (feature `download`), and an optional
//! live [`InstanceWatcher`] (feature `watch`).
//!
//! Requires a Tokio runtime (HTTP via `reqwest`, WebSocket via
//! `tokio-tungstenite`).
//!
//! ```no_run
//! # async fn run() -> stackedhost_sdk::Result<()> {
//! use stackedhost_sdk::{Client, ListOpts};
//! let client = Client::new("https://stackedhost-d1.example.workers.dev")?;
//! let instances = client.list_instances("lns", &ListOpts::default()).await?;
//! let instance = client.resolve_by_code("lns", "SECRET").await?;
//! let files = client.get_instance_files("lns", &instance.id, Some("SECRET")).await?;
//! # let _ = (instances, files);
//! # Ok(())
//! # }
//! ```

mod client;
pub mod error;
pub mod models;
mod realtime;

#[cfg(feature = "download")]
mod download;
#[cfg(feature = "watch")]
pub mod watch;

pub use client::Client;
pub use error::{Error, Result};
pub use models::{
    Event, Hashes, Instance, ListOpts, Loader, News, RawEvent, RemoteFile, SyncMode, Users,
};
pub use realtime::{ConnectOpts, Realtime};

#[cfg(feature = "watch")]
pub use watch::InstanceWatcher;
