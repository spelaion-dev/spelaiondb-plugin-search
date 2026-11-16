//! Search engine HTTP transports. The core parses, classifies and guards the request;
//! this plugin sends it once to the connection's endpoint and returns Arrow rows.
mod http;
mod rows;
mod tls;

use async_trait::async_trait;
use serde_json::{Value, json};
use spelaion_driver_api::{
    CancelHandle, Capabilities, ConnectParams, Connection, Driver, DriverError, EventStream,
    ExecOptions, ObjectInfo, ObjectKind, Result, SchemaInfo, TableInfo,
};
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Debug, Clone, Copy)]
pub struct SearchDriver(pub &'static str);

#[async_trait]
impl Driver for SearchDriver {
    fn engine(&self) -> &str {
        self.0
    }
    async fn connect(&self, params: &ConnectParams) -> Result<Box<dyn Connection>> {
        if matches!(self.0, "solr" | "opensearch")
            && params.user.is_empty()
            && params.password.is_some()
        {
            return Err(DriverError::Connection("this engine needs a user for Basic authentication; token authentication is not implemented".into()));
        }
        if !params.options.is_empty() {
            return Err(DriverError::Connection(
                "search connections have no options".into(),
            ));
        }
        let identity = match (&params.client_certificate, &params.client_key) {
            (Some(cert), Some(key)) => Some(tls::ClientIdentity { cert, key }),
            _ => None,
        };
        let tls = tls::configure(params.tls, params.ca_certificate.as_deref(), identity)?;
        let endpoint = Arc::new(http::Endpoint::new(
            params,
            tls,
            if self.0 == "meilisearch" {
                "Bearer"
            } else {
                "ApiKey"
            },
        ));
        let conn = SearchConnection {
            engine: self.0,
            client: Arc::new(Mutex::new(http::Client::new(endpoint))),
            capabilities: Capabilities {
                schemas: true,
                ..Default::default()
            },
        };
        tokio::time::timeout(params.connect_timeout(), conn.server_version())
            .await
            .map_err(|_| {
                DriverError::Connection(
                    "the server did not answer within the connection timeout".into(),
                )
            })??;
        Ok(Box::new(conn))
    }
}

struct SearchConnection {
    engine: &'static str,
    client: Arc<Mutex<http::Client>>,
    capabilities: Capabilities,
}
fn invalid(message: impl Into<String>) -> DriverError {
    DriverError::Other(message.into())
}

fn query_value(value: &str) -> String {
    let mut out = String::new();
    for b in value.bytes() {
        if b.is_ascii_alphanumeric() || b"-_.~".contains(&b) {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

async fn request(client: &mut http::Client, value: &Value) -> Result<Value> {
    let method = value["method"]
        .as_str()
        .ok_or_else(|| invalid("request has no method"))?;
    if !matches!(method, "GET" | "HEAD" | "POST" | "PUT" | "PATCH" | "DELETE") {
        return Err(invalid("invalid HTTP method"));
    }
    let mut path = value["path"]
        .as_str()
        .ok_or_else(|| invalid("request has no path"))?
        .to_string();
    if !path.starts_with('/')
        || path.starts_with("//")
        || path.contains(['%', '\\', '?', '#'])
        || path.split('/').any(|p| matches!(p, "." | ".."))
    {
        return Err(invalid("request must name a plain path on this server"));
    }
    if let Some(query) = value["query"].as_object() {
        for (i, (key, val)) in query.iter().enumerate() {
            path.push(if i == 0 { '?' } else { '&' });
            path.push_str(&query_value(key));
            path.push('=');
            path.push_str(&query_value(
                val.as_str()
                    .ok_or_else(|| invalid("query values must be strings"))?,
            ));
        }
    }
    let body = if value["body"].is_null() {
        String::new()
    } else {
        value["body"].to_string()
    };
    let bytes = client.request(method, &path, body).await?;
    if bytes.is_empty() {
        return Ok(json!({"ok":true}));
    }
    serde_json::from_slice(&bytes)
        .map_err(|e| invalid(format!("server returned invalid JSON: {e}")))
}

impl SearchConnection {
    async fn get(&self, path: &str, query: Value) -> Result<Value> {
        request(
            &mut *self.client.lock().await,
            &json!({"method":"GET","path":path,"query":query}),
        )
        .await
    }
}

#[async_trait]
impl Connection for SearchConnection {
    fn capabilities(&self) -> &Capabilities {
        &self.capabilities
    }
    async fn execute(&self, _sql: &str, _opts: &ExecOptions) -> Result<EventStream> {
        Err(invalid(
            "search engines run the core's HTTP requests, not SQL",
        ))
    }
    async fn execute_request(&self, value: Value, opts: &ExecOptions) -> Result<EventStream> {
        let client = self.client.clone();
        let engine = self.engine;
        let batch_size = opts.batch_size.clamp(1, 8192);
        let stream = futures_util::stream::once(async move {
            let answer = request(&mut *client.lock().await, &value).await?;
            rows::events(engine, answer, batch_size)
        });
        use futures_util::{StreamExt, TryStreamExt};
        Ok(Box::pin(
            stream
                .map_ok(|events| futures_util::stream::iter(events.into_iter().map(Ok)))
                .try_flatten()
                .boxed(),
        ))
    }
    fn cancel_handle(&self) -> Arc<dyn CancelHandle> {
        Arc::new(NoCancel)
    }
    async fn server_version(&self) -> Result<String> {
        let (path, query) = match self.engine {
            "solr" => ("/solr/admin/info/system", json!({"wt":"json"})),
            "meilisearch" => ("/version", json!({})),
            _ => ("/", json!({})),
        };
        let value = self.get(path, query).await?;
        let version = match self.engine {
            "solr" => value["lucene"]["solr-spec-version"].as_str(),
            "meilisearch" => value["pkgVersion"].as_str(),
            _ => value["version"]["number"].as_str(),
        }
        .ok_or_else(|| invalid("the endpoint did not identify the expected search engine"))?;
        Ok(format!("{} {version}", self.engine))
    }
    async fn list_schemas(&self) -> Result<Vec<SchemaInfo>> {
        Ok(vec![SchemaInfo {
            name: "indexes".into(),
            system: false,
        }])
    }
    async fn list_objects(&self, schema: &str) -> Result<Vec<ObjectInfo>> {
        if schema != "indexes" {
            return Ok(Vec::new());
        }
        let names: Vec<String> = match self.engine {
            "meilisearch" => {
                let mut names = Vec::new();
                let mut offset: usize = 0;
                loop {
                    let value = self
                        .get(
                            "/indexes",
                            json!({"offset":offset.to_string(),"limit":"200"}),
                        )
                        .await?;
                    let page = value["results"]
                        .as_array()
                        .ok_or_else(|| invalid("index list has no results"))?;
                    names.extend(
                        page.iter()
                            .filter_map(|v| v["uid"].as_str().map(str::to_string)),
                    );
                    offset += page.len();
                    if offset >= value["total"].as_u64().unwrap_or(offset as u64) as usize
                        || page.is_empty()
                    {
                        break;
                    }
                    if offset > 100000 {
                        return Err(invalid("too many indexes to list"));
                    }
                }
                names
            }
            "solr" => {
                let value = self
                    .get(
                        "/solr/admin/cores",
                        json!({"action":"STATUS","wt":"json","indexInfo":"false"}),
                    )
                    .await?;
                value["status"]
                    .as_object()
                    .ok_or_else(|| invalid("Solr returned no core list"))?
                    .keys()
                    .cloned()
                    .collect()
            }
            _ => {
                let value = self
                    .get("/_cat/indices", json!({"format":"json","h":"index"}))
                    .await?;
                value
                    .as_array()
                    .ok_or_else(|| invalid("server returned no index list"))?
                    .iter()
                    .filter_map(|v| v["index"].as_str().map(str::to_string))
                    .collect()
            }
        };
        Ok(names
            .into_iter()
            .map(|name| ObjectInfo {
                schema: schema.into(),
                name,
                kind: ObjectKind::Collection,
                estimated_rows: None,
                comment: None,
            })
            .collect())
    }
    async fn describe_table(&self, _schema: &str, _name: &str) -> Result<TableInfo> {
        Err(invalid("a search index is a collection, not a SQL table"))
    }
}

struct NoCancel;
#[async_trait]
impl CancelHandle for NoCancel {
    async fn cancel(&self) -> Result<()> {
        Err(invalid(
            "the HTTP engine has no statement cancellation endpoint",
        ))
    }
}
