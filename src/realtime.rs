//! Real-time launcher updates over the Durable Object WebSocket.

use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use futures::{SinkExt, Stream, StreamExt};
use serde_json::Value;
use tokio::sync::{mpsc, watch};
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
    /// Dropping the sender also stops the task, without waiting for traffic.
    stop: watch::Sender<bool>,
}

impl Realtime {
    /// The highest revision seen so far.
    pub fn revision(&self) -> u64 {
        self.revision.load(Ordering::Relaxed)
    }

    /// Stops the background task and closes the connection right away.
    pub fn close(&self) {
        self.stop.send_replace(true);
    }

    /// Convenience: await the next event (`None` once the stream ends).
    pub async fn recv(&mut self) -> Option<Event> {
        self.next().await
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
        let (stop, stop_task) = watch::channel(false);

        let client = self.clone();
        let lid = launcher_id.to_string();
        let rev = revision.clone();
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

/// Whether the owner closed or dropped the [`Realtime`].
fn stopped(stop: &watch::Receiver<bool>) -> bool {
    *stop.borrow() || stop.has_changed().is_err()
}

/// Resolves once the owner closes or drops the [`Realtime`].
async fn until_stopped(stop: &mut watch::Receiver<bool>) {
    let _ = stop.wait_for(|stopped| *stopped).await;
}

async fn run_loop(
    client: Client,
    launcher_id: String,
    tx: mpsc::Sender<Event>,
    rev: Arc<AtomicU64>,
    mut stop: watch::Receiver<bool>,
) {
    let mut backoff = 1u64;
    while !stopped(&stop) {
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

        let connected = tokio::select! {
            _ = until_stopped(&mut stop) => break,
            connected = tokio_tungstenite::connect_async(req) => connected,
        };
        match connected {
            Ok((ws, _resp)) => {
                backoff = 1;
                let should_stop = handle_conn(ws, &tx, &rev, &mut stop).await;
                if should_stop {
                    break;
                }
            }
            Err(_) => { /* connection failed — fall through to backoff */ }
        }

        tokio::select! {
            _ = until_stopped(&mut stop) => break,
            _ = tokio::time::sleep(Duration::from_secs(backoff)) => {}
        }
        backoff = (backoff * 2).min(30);
    }
}

/// Drives one connection. Returns `true` if the whole loop should stop (the
/// consumer dropped the stream or `close()` was called), `false` to reconnect.
async fn handle_conn<S>(
    ws: S,
    tx: &mpsc::Sender<Event>,
    rev: &Arc<AtomicU64>,
    stop: &mut watch::Receiver<bool>,
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
        tokio::select! {
            _ = until_stopped(stop) => {
                let _ = sink.send(Message::Close(None)).await;
                return true;
            }
            _ = ping.tick() => {
                // Literal "ping" frame: the DO replies "pong" without waking.
                if sink.send(Message::Text("ping".to_string())).await.is_err() {
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
