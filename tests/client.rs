use hostmylauncher::Client;

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
