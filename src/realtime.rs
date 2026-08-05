//! Real-time launcher updates over the Durable Object WebSocket.

use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use futures::{SinkExt, Stream, StreamExt};
use serde_json::Value;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::Message;
use url::Url;

use crate::client::Client;
use crate::models::Event;

/// Options for opening a real-time connection.
#[derive(Debug, Clone, Default)]
pub struct ConnectOpts {
    /// Last revision the client has already applied. The server replays missed
    /// events when possible, otherwise asks the client to re-hydrate via REST.
    pub since: Option<u64>,
}

/// A live, self-reconnecting stream of launcher [`Event`]s.
///
/// Implements [`futures::Stream`]. Reconnection (with `since` replay), keepalive
/// pings and backoff are handled by a background task; dropping the `Realtime`
/// (or calling [`Realtime::close`]) stops it.
pub struct Realtime {
    rx: ReceiverStream<Event>,
    revision: Arc<AtomicU64>,
    stop: Arc<AtomicBool>,
}

impl Realtime {
    /// The highest revision seen so far.
    pub fn revision(&self) -> u64 {
        self.revision.load(Ordering::Relaxed)
    }

    /// Signals the background task to stop and close the connection.
    pub fn close(&self) {
        self.stop.store(true, Ordering::Relaxed);
    }

    /// Convenience: await the next event (`None` once the stream ends).
    pub async fn recv(&mut self) -> Option<Event> {
        self.next().await
    }
}

impl Drop for Realtime {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
    }
}

impl Stream for Realtime {
    type Item = Event;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Event>> {
        Pin::new(&mut self.get_mut().rx).poll_next(cx)
    }
}

impl Client {
    /// Opens a real-time connection for a launcher.
    pub fn connect(&self, launcher_id: &str, opts: ConnectOpts) -> Realtime {
        let (tx, rx) = mpsc::channel::<Event>(256);
        let revision = Arc::new(AtomicU64::new(opts.since.unwrap_or(0)));
        let stop = Arc::new(AtomicBool::new(false));

        let client = self.clone();
        let lid = launcher_id.to_string();
        let rev = revision.clone();
        let stop_task = stop.clone();
        tokio::spawn(async move { run_loop(client, lid, tx, rev, stop_task).await });

        Realtime {
            rx: ReceiverStream::new(rx),
            revision,
            stop,
        }
    }
}

fn ws_url(base: &Url, launcher_id: &str, since: u64) -> Url {
    let mut u = base.clone();
    let scheme = if u.scheme() == "https" { "wss" } else { "ws" };
    let _ = u.set_scheme(scheme);
    u.set_path(&format!("/launchers/{launcher_id}/ws"));
    u.set_query(Some(&format!("since={since}")));
    u
}

async fn run_loop(
    client: Client,
    launcher_id: String,
    tx: mpsc::Sender<Event>,
    rev: Arc<AtomicU64>,
    stop: Arc<AtomicBool>,
) {
    let mut backoff = 1u64;
    while !stop.load(Ordering::Relaxed) {
        let since = rev.load(Ordering::Relaxed);
        let url = ws_url(&client.base, &launcher_id, since);

        let req = match url.as_str().into_client_request() {
            Ok(mut req) => {
                if let Some(t) = &client.token {
                    if let Ok(val) = format!("Bearer {t}").parse() {
                        req.headers_mut().insert("Authorization", val);
                    }
                }
                req
            }
            Err(_) => break,
        };

        match tokio_tungstenite::connect_async(req).await {
            Ok((ws, _resp)) => {
                backoff = 1;
                let should_stop = handle_conn(ws, &tx, &rev, &stop).await;
                if should_stop {
                    break;
                }
            }
            Err(_) => { /* connection failed — fall through to backoff */ }
        }

        if stop.load(Ordering::Relaxed) {
            break;
        }
        tokio::time::sleep(Duration::from_secs(backoff)).await;
        backoff = (backoff * 2).min(30);
    }
}

/// Drives one connection. Returns `true` if the whole loop should stop (the
/// consumer dropped the stream or `close()` was called), `false` to reconnect.
async fn handle_conn<S>(
    ws: S,
    tx: &mpsc::Sender<Event>,
    rev: &Arc<AtomicU64>,
    stop: &Arc<AtomicBool>,
) -> bool
where
    S: Stream<Item = Result<Message, tokio_tungstenite::tungstenite::Error>>
        + SinkExt<Message>
        + Unpin,
{
    let (mut sink, mut stream) = ws.split();
    let mut ping = tokio::time::interval(Duration::from_secs(30));
    ping.tick().await; // consume the immediate first tick

    loop {
        if stop.load(Ordering::Relaxed) {
            return true;
        }
        tokio::select! {
            _ = ping.tick() => {
                // Literal "ping" frame: the DO replies "pong" without waking.
                if sink.send(Message::Text("ping".to_string().into())).await.is_err() {
                    return false; // reconnect
                }
            }
            msg = stream.next() => {
                match msg {
                    Some(Ok(Message::Text(txt))) => {
                        let s = txt.as_str();
                        if s == "pong" {
                            continue;
                        }
                        if let Ok(v) = serde_json::from_str::<Value>(s) {
                            let ev = Event::parse(v);
                            bump_revision(rev, ev.revision());
                            if tx.send(ev).await.is_err() {
                                return true; // consumer gone
                            }
                        }
                    }
                    Some(Ok(Message::Close(_))) | None => return false,
                    Some(Ok(_)) => {} // ignore binary/ping/pong control frames
                    Some(Err(_)) => return false,
                }
            }
        }
    }
}

fn bump_revision(rev: &Arc<AtomicU64>, value: u64) {
    let mut cur = rev.load(Ordering::Relaxed);
    while value > cur {
        match rev.compare_exchange_weak(cur, value, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => break,
            Err(actual) => cur = actual,
        }
    }
}
