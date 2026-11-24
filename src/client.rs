use reqwest::header::HeaderValue;
use serde::de::DeserializeOwned;
use serde_json::Value;
use url::Url;

use crate::error::{Error, Result};
use crate::models::{Instance, ListOpts, News, RemoteFile};

/// Client for the HostMyLauncher REST API. Cheap to clone (shares the inner
/// `reqwest::Client` connection pool).
#[derive(Clone)]
pub struct Client {
    pub(crate) http: reqwest::Client,
    pub(crate) base: Url,
    pub(crate) token: Option<String>,
    #[cfg_attr(not(feature = "download"), allow(dead_code))]
    pub(crate) download_concurrency: usize,
}

/// Instance files downloaded at the same time by default.
pub const DEFAULT_DOWNLOAD_CONCURRENCY: usize = 8;

impl Client {
    /// Creates a client for the production API at `https://api.hostmylauncher.com`.
    /// Use [`Self::with_base_url`] to connect to another server.
    pub fn new() -> Self {
        Self::with_base_url("https://api.hostmylauncher.com")
            .expect("the production API URL must be valid")
    }

    /// Creates a client for a custom base URL (e.g. `http://localhost:8787`).
    /// Returns an error if the URL cannot be parsed.
    pub fn with_base_url(base_url: &str) -> Result<Self> {
        Ok(Self {
            http: reqwest::Client::new(),
            base: Url::parse(base_url)?,
            token: None,
            download_concurrency: DEFAULT_DOWNLOAD_CONCURRENCY,
        })
    }

    /// Sets a bearer token (Better Auth session token or master secret). Optional —
    /// public endpoints work without it.
    pub fn with_token(mut self, token: impl Into<String>) -> Self {
        self.token = Some(token.into());
        self
    }

    /// How many instance files sync, verify and repair download at the same
    /// time (default [`DEFAULT_DOWNLOAD_CONCURRENCY`]; values below 1 become 1).
    /// Each file is still staged and verified before anything is installed.
    pub fn with_download_concurrency(mut self, files: usize) -> Self {
        self.download_concurrency = files.max(1);
        self
    }

    /// The configured base URL.
    pub fn base_url(&self) -> &Url {
        &self.base
    }

    pub(crate) fn build_url(&self, path: &str, query: &[(&str, String)]) -> Url {
        let mut u = self.base.clone();
        u.set_path(path);
        u.set_query(None);
        if !query.is_empty() {
            let mut qp = u.query_pairs_mut();
            for (k, v) in query {
                qp.append_pair(k, v);
            }
        }
        u
    }

    async fn send(&self, mut rb: reqwest::RequestBuilder) -> Result<Value> {
        if let Some(t) = &self.token {
            rb = rb.bearer_auth(t);
        }
        let resp = rb.send().await?;
        let status = resp.status();
        let body: Value = resp.json().await.unwrap_or(Value::Null);

        let api_ok = body.get("success") != Some(&Value::Bool(false));
        if !status.is_success() || !api_ok {
            let message = body
                .get("error")
                .and_then(Value::as_str)
                .unwrap_or("request failed")
                .to_string();
            return match status.as_u16() {
                404 => Err(Error::NotFound),
                401 => Err(Error::Unauthorized),
                s => Err(Error::Api { status: s, message }),
            };
        }
        Ok(body)
    }

    async fn get(&self, path: &str, query: &[(&str, String)]) -> Result<Value> {
        self.send(self.http.get(self.build_url(path, query))).await
    }

    // ---- Instances --------------------------------------------------------

    /// Lists the visible instances of a launcher (public projection unless the
    /// client is authenticated as a member/admin/owner).
    pub async fn list_instances(
        &self,
        launcher_id: &str,
        opts: &ListOpts,
    ) -> Result<Vec<Instance>> {
        let mut q: Vec<(&str, String)> = Vec::new();
        if let Some(v) = opts.limit {
            q.push(("limit", v.to_string()));
        }
        if let Some(v) = opts.offset {
            q.push(("offset", v.to_string()));
        }
        if let Some(v) = &opts.sort_by {
            q.push(("sort_by", v.clone()));
        }
        if let Some(v) = &opts.order {
            q.push(("order", v.clone()));
        }
        let body = self
            .get(&format!("/launchers/{launcher_id}/instances"), &q)
            .await?;
        take_results(body)
    }

    /// Fetches a single instance of a launcher by id or slug.
    pub async fn get_instance(&self, launcher_id: &str, id_or_slug: &str) -> Result<Instance> {
        let body = self
            .get(
                &format!("/launchers/{launcher_id}/instances/{id_or_slug}"),
                &[],
            )
            .await?;
        take_data(body)
    }

    /// Fetches a single instance by id, without knowing its launcher.
    pub async fn get_instance_global(&self, id: &str) -> Result<Instance> {
        let body = self.get(&format!("/instances/{id}"), &[]).await?;
        take_data(body)
    }

    /// Resolves an access code only among the supplied launcher IDs (1–20).
    /// Returns public fields even for hidden instances and authenticated callers.
    /// Fetch files separately with the returned launcher ID and the original code.
    /// Ambiguity (409), throttling (429), and protection failure (503) are
    /// returned as [`Error::Api`]; this method never retries automatically.
    pub async fn resolve_by_code(&self, launcher_ids: &[&str], code: &str) -> Result<Instance> {
        let body = self
            .send(
                self.http
                    .post(self.build_url("/instances/resolve", &[]))
                    .json(&serde_json::json!({ "code": code, "launcher_ids": launcher_ids })),
            )
            .await?;
        take_data(body)
    }

    /// Returns an instance's downloadable file list. Hidden or locked instances
    /// require a valid `code` for public callers.
    ///
    /// The code travels in the `X-Instance-Code` header so it stays out of URLs
    /// and access logs. A non-ASCII code, which headers cannot carry intact, falls
    /// back to the `?code=` query, which the API still accepts.
    pub async fn get_instance_files(
        &self,
        launcher_id: &str,
        id: &str,
        code: Option<&str>,
    ) -> Result<Vec<RemoteFile>> {
        let mut query: Vec<(&str, String)> = Vec::new();
        let mut header = None;
        if let Some(code) = code {
            // Header values reach the Worker as Latin-1, so only ASCII survives intact.
            match HeaderValue::from_str(code).ok().filter(|_| code.is_ascii()) {
                Some(mut value) => {
                    value.set_sensitive(true);
                    header = Some(value);
                }
                None => query.push(("code", code.to_string())),
            }
        }
        let path = format!("/launchers/{launcher_id}/instances/{id}/files");
        let mut request = self.http.get(self.build_url(&path, &query));
        if let Some(value) = header {
            request = request.header("X-Instance-Code", value);
        }
        take_results(self.send(request).await?)
    }

    /// Registers a download, returning the new total count.
    pub async fn increment_downloads(&self, id: &str) -> Result<u64> {
        let body = self
            .send(
                self.http
                    .post(self.build_url(&format!("/instances/{id}/downloads"), &[])),
            )
            .await?;
        Ok(body
            .get("data")
            .and_then(|d| d.get("downloads"))
            .and_then(Value::as_u64)
            .unwrap_or(0))
    }

    // ---- News -------------------------------------------------------------

    /// Lists a launcher's news (published only for public callers).
    pub async fn list_news(&self, launcher_id: &str) -> Result<Vec<News>> {
        let body = self
            .get(&format!("/launchers/{launcher_id}/news"), &[])
            .await?;
        take_results(body)
    }
}

impl Default for Client {
    fn default() -> Self {
        Self::new()
    }
}

fn take_data<T: DeserializeOwned>(body: Value) -> Result<T> {
    let data = body
        .get("data")
        .cloned()
        .ok_or_else(|| Error::Other("response missing `data`".into()))?;
    Ok(serde_json::from_value(data)?)
}

fn take_results<T: DeserializeOwned>(body: Value) -> Result<T> {
    let results = body
        .get("results")
        .cloned()
        .ok_or_else(|| Error::Other("response missing `results`".into()))?;
    Ok(serde_json::from_value(results)?)
}
