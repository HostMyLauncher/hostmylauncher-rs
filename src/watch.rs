//! High-level live instance list (feature `watch`).
//!
//! [`InstanceWatcher`] keeps an up-to-date list of a launcher's instances by
//! seeding from REST and then reconciling real-time events: an `instance.*`
//! event triggers a re-fetch of that instance (a 404 — e.g. it became hidden —
//! removes it, i.e. access revocation), and `sync(hydrate)` re-lists everything.

use std::collections::HashMap;

use futures::StreamExt;
use tokio::sync::watch;
use tokio::task::AbortHandle;

use crate::client::Client;
use crate::error::{Error, Result};
use crate::models::{Event, Instance, ListOpts, SyncMode};
use crate::realtime::ConnectOpts;

/// A self-updating view of a launcher's instances.
///
/// Dropping it stops the background task and closes the real-time connection
/// right away; receivers from [`InstanceWatcher::subscribe`] then stop changing.
pub struct InstanceWatcher {
    instances: watch::Receiver<Vec<Instance>>,
    task: AbortHandle,
}

impl Drop for InstanceWatcher {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl InstanceWatcher {
    /// Starts watching a launcher: fetches the current instances, then keeps them
    /// updated from the real-time connection.
    pub async fn start(client: Client, launcher_id: &str) -> Result<Self> {
        let initial = client
            .list_instances(launcher_id, &ListOpts::default())
            .await?;
        let mut map: HashMap<String, Instance> =
            initial.into_iter().map(|i| (i.id.clone(), i)).collect();

        let (tx, rx) = watch::channel(sorted(&map));
        let lid = launcher_id.to_string();

        let task = tokio::spawn(async move {
            let mut rt = client.connect(&lid, ConnectOpts::default());
            while let Some(ev) = rt.next().await {
                apply(&client, &lid, &mut map, ev).await;
                let next = sorted(&map);
                // Events that change nothing (news, storage) wake no receiver.
                tx.send_if_modified(|current| {
                    let changed = *current != next;
                    if changed {
                        *current = next;
                    }
                    changed
                });
                if tx.is_closed() {
                    break; // all receivers dropped
                }
            }
        })
        .abort_handle();

        Ok(Self {
            instances: rx,
            task,
        })
    }

    /// A snapshot of the current instances.
    pub fn current(&self) -> Vec<Instance> {
        self.instances.borrow().clone()
    }

    /// A receiver to observe updates (`changed().await` then `borrow()`).
    pub fn subscribe(&self) -> watch::Receiver<Vec<Instance>> {
        self.instances.clone()
    }

    /// Awaits the next change to the instance list. Returns `false` once the
    /// watcher has stopped.
    pub async fn changed(&mut self) -> bool {
        self.instances.changed().await.is_ok()
    }
}

fn sorted(map: &HashMap<String, Instance>) -> Vec<Instance> {
    let mut v: Vec<Instance> = map.values().cloned().collect();
    v.sort_by(|a, b| a.title.cmp(&b.title).then_with(|| a.id.cmp(&b.id)));
    v
}

async fn apply(client: &Client, lid: &str, map: &mut HashMap<String, Instance>, ev: Event) {
    match ev {
        Event::Sync {
            mode: SyncMode::Hydrate,
            ..
        } => {
            if let Ok(list) = client.list_instances(lid, &ListOpts::default()).await {
                map.clear();
                for i in list {
                    map.insert(i.id.clone(), i);
                }
            }
        }
        Event::Sync { events, .. } => {
            for e in events {
                apply_one(client, lid, map, e).await;
            }
        }
        other => apply_one(client, lid, map, other).await,
    }
}

async fn apply_one(client: &Client, lid: &str, map: &mut HashMap<String, Instance>, ev: Event) {
    match ev {
        Event::InstanceCreated { id, .. } | Event::InstanceUpdated { id, .. } => {
            match client.get_instance(lid, &id).await {
                Ok(inst) => {
                    map.insert(inst.id.clone(), inst);
                }
                // 404 → no longer visible to us (e.g. became hidden): drop it.
                Err(Error::NotFound) => {
                    map.remove(&id);
                }
                Err(_) => {}
            }
        }
        Event::InstanceDeleted { id, .. } => {
            map.remove(&id);
        }
        _ => {}
    }
}
