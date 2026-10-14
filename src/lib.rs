//! Async Rust client for the **HostMyLauncher** launcher API.
//!
//! Provides typed REST access (instances, files, news), a self-reconnecting
//! real-time [`Realtime`] event stream over the launcher WebSocket, optional
//! file download with SHA verification, safe `overrides.zip` extraction, and
//! manifest-backed instance synchronization (feature `download`), and an optional
//! live [`InstanceWatcher`] (feature `watch`).
//!
//! Requires a Tokio runtime (HTTP via `reqwest`, WebSocket via
//! `tokio-tungstenite`).
//!
//! ```no_run
//! # async fn run() -> hostmylauncher::Result<()> {
//! use hostmylauncher::{Client, ListOpts};
//! let client = Client::new();
//! let instances = client.list_instances("lns", &ListOpts::default()).await?;
//! let instance = client.resolve_by_code(&["lns"], "SECRET").await?;
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

#[cfg(feature = "download")]
pub use download::extract_overrides;
#[cfg(feature = "download")]
pub use download::{SyncProgress, SyncReport, SyncStage};

#[cfg(feature = "watch")]
pub use watch::InstanceWatcher;
