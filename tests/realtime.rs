//! Real-time connection lifecycle against a local WebSocket server.
use futures::{SinkExt, StreamExt};
use hostmylauncher::{Client, ConnectOpts};
use std::time::Duration;
use tokio::net::TcpListener;
use tokio_tungstenite::tungstenite::Message;

/// Regression: closing used to wait for the next 30 s ping or server message,
/// so switching launchers left the previous socket open meanwhile.
#[tokio::test]
async fn dropping_the_stream_closes_the_socket_immediately() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let client =
        Client::with_base_url(&format!("http://{}", listener.local_addr().unwrap())).unwrap();
    let mut realtime = client.connect("launcher-a", ConnectOpts::default());

    let (stream, _) = listener.accept().await.unwrap();
    let mut socket = tokio_tungstenite::accept_async(stream).await.unwrap();
    // Receiving an event proves the client is idle on the open connection.
    socket
        .send(Message::Text(
            r#"{"type":"news.created","revision":1}"#.into(),
        ))
        .await
        .unwrap();
    realtime.next().await.unwrap();
    drop(realtime);

    let ended = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            match socket.next().await {
                None | Some(Err(_)) | Some(Ok(Message::Close(_))) => break,
                Some(Ok(_)) => {}
            }
        }
    })
    .await;
    assert!(
        ended.is_ok(),
        "socket still open after the stream was dropped"
    );
}
