use thiserror::Error;

/// Errors returned by the SDK.
#[derive(Debug, Error)]
pub enum Error {
    #[error("http error: {0}")]
    Http(#[from] reqwest::Error),

    #[error("websocket error: {0}")]
    Ws(#[from] tokio_tungstenite::tungstenite::Error),

    #[error("decode error: {0}")]
    Decode(#[from] serde_json::Error),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("invalid url: {0}")]
    Url(#[from] url::ParseError),

    /// The API responded with `{ "success": false, "error": ... }` or a non-2xx status.
    #[error("api error {status}: {message}")]
    Api { status: u16, message: String },

    #[error("resource not found")]
    NotFound,

    #[error("unauthorized")]
    Unauthorized,

    #[error("{0}")]
    Other(String),
}

pub type Result<T> = std::result::Result<T, Error>;
