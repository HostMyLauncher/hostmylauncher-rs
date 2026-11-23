use hostmylauncher::Client;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::thread;
use std::time::Duration;

fn fixture(status: u16, body: &str) -> (Client, thread::JoinHandle<String>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let client =
        Client::with_base_url(&format!("http://{}", listener.local_addr().unwrap())).unwrap();
    let body = body.to_owned();
    let worker = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        stream
            .set_write_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let mut bytes = Vec::new();
        let mut buffer = [0; 1024];
        loop {
            let count = stream.read(&mut buffer).unwrap();
            assert!(count > 0, "request ended prematurely");
            bytes.extend_from_slice(&buffer[..count]);
            assert!(bytes.len() < 16384);
            if let Some(end) = bytes.windows(4).position(|part| part == b"\r\n\r\n") {
                let headers = String::from_utf8_lossy(&bytes[..end]);
                let length = headers
                    .lines()
                    .find_map(|line| {
                        let (key, value) = line.split_once(':')?;
                        key.eq_ignore_ascii_case("content-length")
                            .then(|| value.trim().parse::<usize>().unwrap())
                    })
                    .unwrap_or(0);
                if bytes.len() >= end + 4 + length {
                    break;
                }
            }
        }
        write!(stream, "HTTP/1.1 {status} Test\r\nContent-Type: application/json\r\nContent-Length: {}\r\nRetry-After: 60\r\nConnection: close\r\n\r\n{body}", body.len()).unwrap();
        String::from_utf8(bytes).unwrap()
    });
    (client, worker)
}

#[tokio::test]
async fn resolve_posts_code_and_launcher_list_and_reads_public_fields() {
    let (client, worker) = fixture(
        200,
        r#"{"success":true,"data":{"id":"hidden","launcher_id":"b","title":"Pack","locked":true}}"#,
    );
    let instance = client
        .resolve_by_code(&["a", "b"], "secret+/?")
        .await
        .unwrap();
    assert_eq!(instance.id, "hidden");
    assert_eq!(instance.launcher_id.as_deref(), Some("b"));
    assert_eq!(instance.title.as_deref(), Some("Pack"));
    assert!(instance.locked);
    assert!(instance.hide.is_none() && instance.users.is_none() && instance.files.is_none());
    let request = worker.join().unwrap();
    assert!(
        request.starts_with("POST /instances/resolve HTTP/1.1\r\n"),
        "{request}"
    );
    assert!(request
        .to_lowercase()
        .contains("content-type: application/json\r\n"));
    let (headers, body) = request.split_once("\r\n\r\n").unwrap();
    assert!(!headers.contains("secret"));
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(body).unwrap(),
        serde_json::json!({"code":"secret+/?", "launcher_ids":["a", "b"]})
    );
}

#[tokio::test]
async fn resolve_propagates_collision_throttle_and_storage_failure_without_retry() {
    for (status, message) in [
        (409, "Ambiguous code"),
        (429, "Too many requests"),
        (503, "Resolver protection unavailable"),
    ] {
        let body = serde_json::json!({"success":false,"error":message}).to_string();
        let (client, worker) = fixture(status, &body);
        let error = client.resolve_by_code(&["a"], "secret").await.unwrap_err();
        assert!(
            matches!(error, hostmylauncher::Error::Api { status: actual, message: actual_message } if actual == status && actual_message == message)
        );
        assert!(worker
            .join()
            .unwrap()
            .starts_with("POST /instances/resolve HTTP/1.1"));
    }
}

#[tokio::test]
async fn existing_global_id_and_file_reads_keep_get_contracts_and_send_code_in_header() {
    let (client, worker) = fixture(200, r#"{"success":true,"data":{"id":"visible"}}"#);
    assert_eq!(
        client.get_instance_global("visible").await.unwrap().id,
        "visible"
    );
    assert!(worker
        .join()
        .unwrap()
        .starts_with("GET /instances/visible HTTP/1.1\r\n"));
    let (client, worker) = fixture(
        200,
        r#"{"success":true,"results":[{"path":"mods/a.jar","downloads":["https://cdn.example/a.jar"]}]}"#,
    );
    let files = client
        .get_instance_files("a", "hidden", Some("secret"))
        .await
        .unwrap();
    assert_eq!(files.len(), 1);
    assert_eq!(files[0].path.as_deref(), Some("mods/a.jar"));
    assert_eq!(files[0].downloads, vec!["https://cdn.example/a.jar"]);
    let request = worker.join().unwrap();
    assert!(request.starts_with("GET /launchers/a/instances/hidden/files HTTP/1.1\r\n"));
    assert!(request
        .to_ascii_lowercase()
        .contains("\r\nx-instance-code: secret\r\n"));

    // A code that cannot be a header value still reaches the API in the query.
    let (client, worker) = fixture(200, r#"{"success":true,"results":[]}"#);
    client
        .get_instance_files("a", "hidden", Some("ñandú"))
        .await
        .unwrap();
    let request = worker.join().unwrap();
    assert!(request
        .starts_with("GET /launchers/a/instances/hidden/files?code=%C3%B1and%C3%BA HTTP/1.1\r\n"));
    assert!(!request.to_ascii_lowercase().contains("x-instance-code"));
}

#[test]
fn new_uses_production_url() {
    assert_eq!(
        Client::new().base_url().as_str(),
        "https://api.hostmylauncher.com/"
    );
}

#[test]
fn default_uses_production_url() {
    assert_eq!(
        Client::default().base_url().as_str(),
        "https://api.hostmylauncher.com/"
    );
}

#[test]
fn custom_base_url_is_preserved() {
    let client = Client::with_base_url("http://localhost:8787/custom?key=value").unwrap();
    assert_eq!(
        client.base_url().as_str(),
        "http://localhost:8787/custom?key=value"
    );
}

#[test]
fn invalid_custom_base_url_is_rejected() {
    assert!(matches!(
        Client::with_base_url("not a URL"),
        Err(hostmylauncher::Error::Url(_))
    ));
}
