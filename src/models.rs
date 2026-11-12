//! Typed models for the HostMyLauncher API. Fields are tolerant (`Option`,
//! `#[serde(default)]`) because `files`/`data` are flexible JSON.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

/// A launcher instance (modpack / server config).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Instance {
    pub id: String,
    #[serde(default)]
    pub launcher_id: Option<String>,
    #[serde(default)]
    pub slug: Option<String>,
    #[serde(default)]
    pub title: Option<String>,
    #[serde(default)]
    pub minecraft_version: Option<String>,
    #[serde(default)]
    pub server: Option<String>,
    #[serde(default)]
    pub loader: Option<Loader>,
    #[serde(default)]
    pub locked: bool,
    /// Only present for full authenticated reads, never code resolution.
    #[serde(default)]
    pub hide: Option<bool>,
    #[serde(default)]
    pub users: Option<Users>,
    #[serde(default, deserialize_with = "de::opt_u32")]
    pub java_version: Option<u32>,
    /// Suggested RAM in MB.
    #[serde(default, deserialize_with = "de::opt_u32")]
    pub suggested_ram: Option<u32>,
    #[serde(default)]
    pub icon: Option<String>,
    #[serde(default)]
    pub landscape: Option<String>,
    #[serde(default)]
    pub poster: Option<String>,
    #[serde(default)]
    pub animation: Option<String>,
    #[serde(default)]
    pub downloads: u64,
    /// Present on full authenticated reads; code resolution omits files.
    /// Fetch downloads separately through the `/files` endpoint.
    #[serde(default)]
    pub files: Option<Vec<RemoteFile>>,
    /// Any additional fields (e.g. `code`, `created_at`, `updated_at`).
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

/// Mod loader configuration.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Loader {
    #[serde(rename = "type", default)]
    pub kind: Option<String>,
    #[serde(default)]
    pub build: Option<String>,
    #[serde(default, alias = "enable")]
    pub enabled: bool,
}

/// Player access settings.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Users {
    #[serde(default)]
    pub whitelist: Vec<String>,
    #[serde(rename = "noPremium", default, alias = "no_premium")]
    pub no_premium: bool,
}

/// A downloadable file group. `downloads` holds direct URLs (often an external
/// CDN). `size`/`hashes` allow integrity verification. A special entry with
/// `path == "overrides.zip"` carries a zipped overrides bundle.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemoteFile {
    #[serde(default)]
    pub path: Option<String>,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub size: Option<u64>,
    #[serde(default)]
    pub hashes: Option<Hashes>,
    #[serde(default)]
    pub downloads: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Hashes {
    #[serde(default)]
    pub sha1: Option<String>,
    #[serde(default)]
    pub sha512: Option<String>,
}

/// A launcher news item.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct News {
    pub id: String,
    #[serde(default)]
    pub launcher_id: Option<String>,
    #[serde(default)]
    pub title: Option<String>,
    #[serde(default)]
    pub body: Option<String>,
    #[serde(default)]
    pub published: bool,
    #[serde(default)]
    pub created_at: Option<i64>,
    #[serde(default)]
    pub data: Option<Value>,
}

/// Options for listing instances.
#[derive(Debug, Clone, Default)]
pub struct ListOpts {
    pub limit: Option<u32>,
    pub offset: Option<u32>,
    /// One of `title`, `slug`, `downloads`.
    pub sort_by: Option<String>,
    /// `asc` or `desc`.
    pub order: Option<String>,
}

/// Sync mode sent by the server when a real-time connection opens.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncMode {
    /// Buffer could not cover the client's `since`; re-fetch current state via REST.
    Hydrate,
    /// Missed events are included and were replayed.
    Replay,
}

/// A raw real-time event frame, used for unrecognized event types.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RawEvent {
    #[serde(rename = "type")]
    pub kind: String,
    #[serde(default)]
    pub payload: Value,
    #[serde(default)]
    pub revision: u64,
    #[serde(default)]
    pub ts: u64,
}

/// A parsed real-time event from the launcher WebSocket.
#[derive(Debug, Clone)]
pub enum Event {
    /// First frame on connect.
    Sync {
        mode: SyncMode,
        revision: u64,
        events: Vec<Event>,
    },
    InstanceCreated {
        id: String,
        slug: Option<String>,
        revision: u64,
    },
    InstanceUpdated {
        id: String,
        revision: u64,
    },
    InstanceDeleted {
        id: String,
        revision: u64,
    },
    NewsCreated {
        id: String,
        revision: u64,
    },
    NewsUpdated {
        id: String,
        revision: u64,
    },
    NewsDeleted {
        id: String,
        revision: u64,
    },
    StorageChanged {
        revision: u64,
    },
    LauncherDeleted {
        revision: u64,
    },
    /// Any event type the client does not specifically model.
    Other(RawEvent),
}

impl Event {
    /// The event's revision (0 for sync `hydrate`/unknown).
    pub fn revision(&self) -> u64 {
        match self {
            Event::Sync { revision, .. }
            | Event::InstanceCreated { revision, .. }
            | Event::InstanceUpdated { revision, .. }
            | Event::InstanceDeleted { revision, .. }
            | Event::NewsCreated { revision, .. }
            | Event::NewsUpdated { revision, .. }
            | Event::NewsDeleted { revision, .. }
            | Event::StorageChanged { revision }
            | Event::LauncherDeleted { revision } => *revision,
            Event::Other(e) => e.revision,
        }
    }

    /// Parses a single JSON frame (or a replayed event) into an [`Event`].
    pub fn parse(v: Value) -> Event {
        let kind = v
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        let revision = v.get("revision").and_then(Value::as_u64).unwrap_or(0);
        let payload_id = || {
            v.get("payload")
                .and_then(|p| p.get("id"))
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string()
        };

        match kind.as_str() {
            "sync" => {
                let mode = match v.get("mode").and_then(Value::as_str) {
                    Some("replay") => SyncMode::Replay,
                    _ => SyncMode::Hydrate,
                };
                let events = v
                    .get("events")
                    .and_then(Value::as_array)
                    .map(|arr| arr.iter().cloned().map(Event::parse).collect())
                    .unwrap_or_default();
                Event::Sync {
                    mode,
                    revision,
                    events,
                }
            }
            "instance.created" => Event::InstanceCreated {
                id: payload_id(),
                slug: v
                    .get("payload")
                    .and_then(|p| p.get("slug"))
                    .and_then(Value::as_str)
                    .map(String::from),
                revision,
            },
            "instance.updated" => Event::InstanceUpdated {
                id: payload_id(),
                revision,
            },
            "instance.deleted" => Event::InstanceDeleted {
                id: payload_id(),
                revision,
            },
            "news.created" => Event::NewsCreated {
                id: payload_id(),
                revision,
            },
            "news.updated" => Event::NewsUpdated {
                id: payload_id(),
                revision,
            },
            "news.deleted" => Event::NewsDeleted {
                id: payload_id(),
                revision,
            },
            "storage.changed" => Event::StorageChanged { revision },
            "launcher.deleted" => Event::LauncherDeleted { revision },
            _ => Event::Other(serde_json::from_value(v).unwrap_or(RawEvent {
                kind,
                payload: Value::Null,
                revision,
                ts: 0,
            })),
        }
    }
}

mod de {
    use serde::{Deserialize, Deserializer};
    use serde_json::Value;

    /// Accepts a number, numeric string, or null for an optional `u32`.
    pub fn opt_u32<'de, D: Deserializer<'de>>(d: D) -> Result<Option<u32>, D::Error> {
        Ok(match Option::<Value>::deserialize(d)? {
            None | Some(Value::Null) => None,
            Some(Value::Number(n)) => n.as_u64().map(|x| x as u32),
            Some(Value::String(s)) => s.parse().ok(),
            _ => None,
        })
    }
}
