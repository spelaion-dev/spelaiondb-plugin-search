//! The HTTP connection to the server: one at a time per session, opened by this plugin itself
//! to `connect_addr` when it is given (the local end of SpelaionDB's tunnel or proxy), never
//! through a proxy of the system's, with TLS verified against `host` (`tls.rs`, libpq's modes).
//! The user and password travel in the `Authorization` header (HTTP Basic), never in the
//! address, and are taken out of every error.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::client::conn::http1::SendRequest;
use hyper::{Request, StatusCode};
use hyper_util::rt::TokioIo;
use rustls::pki_types::ServerName;
use spelaion_driver_api::{DriverError, Result, Secret};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;

use crate::tls::Tls;

/// A connection kept open longer than this between two requests is not used again: a server
/// or a proxy may be closing it, and a request sent then would be lost on the way.
const REUSE_WITHIN: Duration = Duration::from_secs(5);

/// The largest answer read: a result bigger than this fails instead of filling memory.
pub const MAX_BODY: usize = 64 * 1024 * 1024;

trait Io: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> Io for T {}

/// Where the server is and how to reach it.
pub struct Endpoint {
    /// The name TLS verifies and `Host` carries.
    host: String,
    port: u16,
    /// Where to connect, if not `host:port`.
    connect_addr: Option<SocketAddr>,
    tls: Tls,
    /// `prefer` once decided: 0 not yet, 1 TLS, 2 plain.
    preferred: AtomicU8,
    connect_timeout: Duration,
    user: String,
    password: Option<Secret>,
    token_scheme: &'static str,
}

impl Endpoint {
    pub fn new(
        params: &spelaion_driver_api::ConnectParams,
        tls: Tls,
        token_scheme: &'static str,
    ) -> Self {
        Self {
            host: params.host.clone(),
            port: params.port,
            connect_addr: params.connect_addr,
            tls,
            preferred: AtomicU8::new(0),
            connect_timeout: params.connect_timeout(),
            user: params.user.clone(),
            password: params.password.clone(),
            token_scheme,
        }
    }

    /// The password, and the header it travels in, taken out of anything shown (the server
    /// repeats neither, but an error from a proxy on the way might).
    pub fn scrub(&self, text: &str) -> String {
        match &self.password {
            Some(password) if !password.expose().is_empty() => text
                .replace(&self.basic(), "***")
                .replace(password.expose(), "***"),
            _ => text.to_string(),
        }
    }

    /// `user:password` in base64, as HTTP Basic sends it.
    fn basic(&self) -> String {
        let password = self.password.as_ref().map_or("", |p| p.expose());
        base64_encode(format!("{}:{password}", self.user).as_bytes())
    }

    async fn tcp(&self) -> Result<TcpStream> {
        let stream = match self.connect_addr {
            Some(addr) => TcpStream::connect(addr).await,
            None => TcpStream::connect((self.host.as_str(), self.port)).await,
        };
        let stream = stream.map_err(|e| {
            DriverError::Connection(format!("could not reach {}:{}: {e}", self.host, self.port))
        })?;
        let _ = stream.set_nodelay(true);
        Ok(stream)
    }

    async fn tls_stream(
        &self,
        config: &Arc<rustls::ClientConfig>,
        tcp: TcpStream,
    ) -> std::io::Result<Box<dyn Io>> {
        let name = ServerName::try_from(self.host.clone())
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;
        let stream = TlsConnector::from(config.clone())
            .connect(name, tcp)
            .await?;
        Ok(Box::new(stream))
    }

    /// A new connection, with the TLS its mode asks for.
    async fn open(&self) -> Result<Box<dyn Io>> {
        let tcp = self.tcp().await?;
        match &self.tls {
            Tls::Off => Ok(Box::new(tcp)),
            Tls::Required(config) => self
                .tls_stream(config, tcp)
                .await
                .map_err(|e| DriverError::Connection(format!("TLS: {e}"))),
            Tls::Preferred(config) => match self.preferred.load(Ordering::SeqCst) {
                1 => self
                    .tls_stream(config, tcp)
                    .await
                    .map_err(|e| DriverError::Connection(format!("TLS: {e}"))),
                2 => Ok(Box::new(tcp)),
                _ => match self.tls_stream(config, tcp).await {
                    Ok(stream) => {
                        self.preferred.store(1, Ordering::SeqCst);
                        Ok(stream)
                    }
                    // The server does not speak TLS on this port: plain HTTP, on a new
                    // connection (the first one heard a handshake).
                    Err(_) => {
                        self.preferred.store(2, Ordering::SeqCst);
                        Ok(Box::new(self.tcp().await?))
                    }
                },
            },
        }
    }

    /// A new HTTP/1.1 connection, within the connect timeout.
    async fn handshake(&self) -> Result<SendRequest<Full<Bytes>>> {
        let open = async {
            let io = self.open().await?;
            let (sender, connection) = hyper::client::conn::http1::handshake(TokioIo::new(io))
                .await
                .map_err(|e| DriverError::Connection(format!("HTTP: {e}")))?;
            tokio::spawn(async move {
                let _ = connection.await;
            });
            Ok(sender)
        };
        match tokio::time::timeout(self.connect_timeout, open).await {
            Ok(result) => result,
            Err(_) => Err(DriverError::Connection(format!(
                "could not connect to {}:{} within {} s",
                self.host,
                self.port,
                self.connect_timeout.as_secs()
            ))),
        }
    }

    fn request(&self, method: &str, path: &str, body: String) -> Result<Request<Full<Bytes>>> {
        let host = if self.host.contains(':') {
            format!("[{}]:{}", self.host, self.port)
        } else {
            format!("{}:{}", self.host, self.port)
        };
        let mut builder = Request::builder()
            .method(method)
            .uri(path)
            .header(hyper::header::HOST, host)
            .header(hyper::header::CONTENT_TYPE, "application/json")
            // Neo4j's typed JSON: every value says its Cypher type.
            .header(hyper::header::ACCEPT, "application/json");
        if !self.user.is_empty() {
            builder = builder.header(
                hyper::header::AUTHORIZATION,
                format!("Basic {}", self.basic()),
            );
        }
        if self.user.is_empty()
            && let Some(secret) = &self.password
        {
            builder = builder.header(
                hyper::header::AUTHORIZATION,
                format!("{} {}", self.token_scheme, secret.expose()),
            );
        }
        builder
            .body(Full::new(Bytes::from(body)))
            .map_err(|e| DriverError::Other(format!("HTTP request: {e}")))
    }
}

/// One connection to the server, opened again when the last one is too old or closed. Never
/// shared: a session's requests go one at a time.
pub struct Client {
    endpoint: Arc<Endpoint>,
    open: Option<(SendRequest<Full<Bytes>>, Instant)>,
}

impl Client {
    pub fn new(endpoint: Arc<Endpoint>) -> Self {
        Self {
            endpoint,
            open: None,
        }
    }

    /// Sends one request and returns its whole answer: `2xx`, or a `4xx` whose body is the
    /// server's JSON errors, which the caller reads. A refused login or another answer is an
    /// error here. A request is sent once: if it is lost on the way, it is not sent again.
    pub async fn request(&mut self, method: &str, path: &str, body: String) -> Result<Vec<u8>> {
        let request = self.endpoint.request(method, path, body)?;
        let reusable = match &mut self.open {
            Some((sender, used)) if used.elapsed() < REUSE_WITHIN && !sender.is_closed() => {
                sender.ready().await.is_ok()
            }
            _ => false,
        };
        if !reusable {
            self.open = Some((self.endpoint.handshake().await?, Instant::now()));
        }
        let (sender, used) = self.open.as_mut().expect("just opened");
        let response = sender.send_request(request).await;
        *used = Instant::now();
        let response = match response {
            Ok(response) => response,
            Err(e) => {
                self.open = None;
                return Err(DriverError::Connection(
                    self.endpoint.scrub(&format!("the request was lost: {e}")),
                ));
            }
        };
        let status = response.status();
        let body = read_bounded(response.into_body(), MAX_BODY).await?;
        if status.is_success() {
            return Ok(body);
        }
        let text = self.endpoint.scrub(String::from_utf8_lossy(&body).trim());
        Err(match status {
            StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => DriverError::Connection(format!(
                "the server refused the user or password ({status}){}",
                if text.is_empty() {
                    String::new()
                } else {
                    format!(": {text}")
                }
            )),
            // The statement's errors come as JSON: the caller reads them.
            _ if text.is_empty() => {
                DriverError::Connection(format!("the server answered {status}"))
            }
            _ => DriverError::Connection(format!("the server answered {status}: {text}")),
        })
    }
}

/// A whole body, or an error when it is bigger than `limit`.
async fn read_bounded(mut body: Incoming, limit: usize) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    while let Some(frame) = body.frame().await {
        let frame =
            frame.map_err(|e| DriverError::Connection(format!("reading the answer: {e}")))?;
        if let Ok(data) = frame.into_data() {
            if out.len() + data.len() > limit {
                return Err(DriverError::Other(format!(
                    "the server's answer is bigger than {} MiB: add a LIMIT",
                    limit / (1024 * 1024)
                )));
            }
            out.extend_from_slice(&data);
        }
    }
    Ok(out)
}

/// Standard base64 with padding, as HTTP Basic writes it.
pub(crate) fn base64_encode(bytes: &[u8]) -> String {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let n = (u32::from(chunk[0]) << 16)
            | (u32::from(*chunk.get(1).unwrap_or(&0)) << 8)
            | u32::from(*chunk.get(2).unwrap_or(&0));
        for i in 0..4 {
            if i <= chunk.len() {
                out.push(TABLE[((n >> (18 - 6 * i)) & 63) as usize] as char);
            } else {
                out.push('=');
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::base64_encode;

    #[test]
    fn base64_as_http_basic_writes_it() {
        assert_eq!(base64_encode(b"neo4j:secret"), "bmVvNGo6c2VjcmV0");
        assert_eq!(base64_encode(b"a"), "YQ==");
        assert_eq!(base64_encode(b"ab"), "YWI=");
        assert_eq!(base64_encode(b""), "");
    }
}
