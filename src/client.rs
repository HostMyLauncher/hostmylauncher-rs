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
}

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
        })
    }

    /// Sets a bearer token (Better Auth session token or master secret). Optional —
    /// public endpoints work without it.
    pub fn with_token(mut self, token: impl Into<String>) -> Self {
        self.token = Some(token.into());
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

    /// Resolves an instance by its access code (the code grants access, so this
    /// returns the full instance even when hidden). Powers the "enter code" flow.
    pub async fn resolve_by_code(&self, launcher_id: &str, code: &str) -> Result<Instance> {
        let body = self
            .get(
                &format!("/launchers/{launcher_id}/instances/resolve"),
                &[("code", code.to_string())],
            )
            .await?;
        take_data(body)
    }

    /// Returns an instance's downloadable file list. Locked instances require a
    /// valid `code` for public callers.
    pub async fn get_instance_files(
        &self,
        launcher_id: &str,
        id: &str,
        code: Option<&str>,
    ) -> Result<Vec<RemoteFile>> {
        let q: Vec<(&str, String)> = match code {
            Some(c) => vec![("code", c.to_string())],
            None => vec![],
        };
        let body = self
            .get(
                &format!("/launchers/{launcher_id}/instances/{id}/files"),
                &q,
            )
            .await?;
        take_results(body)
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
