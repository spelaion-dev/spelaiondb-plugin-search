//! Credentials, routes and redirects are checked against a stand-in endpoint, independently
//! of the engines' server tests. The stand-in never logs a credential.
use futures_util::StreamExt;
use serde_json::json;
use spelaion_driver_api::{ConnectParams, Driver, ExecOptions, Secret, TlsMode};
use spelaion_driver_search::SearchDriver;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

async fn header(socket: &mut tokio::net::TcpStream) -> String {
    let mut bytes = Vec::new();
    while !bytes.ends_with(b"\r\n\r\n") {
        let mut byte = [0];
        socket.read_exact(&mut byte).await.unwrap();
        bytes.push(byte[0]);
        assert!(bytes.len() < 16384);
    }
    String::from_utf8(bytes).unwrap()
}

#[tokio::test]
async fn tokens_and_basic_passwords_are_headers_and_are_scrubbed_from_server_errors() {
    for (engine, user, expected, version) in [
        (
            "meilisearch",
            "",
            "Bearer private-search-token",
            json!({"pkgVersion":"test"}),
        ),
        (
            "elasticsearch",
            "",
            "ApiKey private-search-token",
            json!({"version":{"number":"test"}}),
        ),
        (
            "solr",
            "reader",
            "Basic cmVhZGVyOnByaXZhdGUtc2VhcmNoLXRva2Vu",
            json!({"lucene":{"solr-spec-version":"test"}}),
        ),
    ] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            for i in 0..2 {
                let (mut socket, _) = listener.accept().await.unwrap();
                let request = header(&mut socket).await;
                assert!(
                    request
                        .to_ascii_lowercase()
                        .contains(&format!("authorization: {}", expected.to_ascii_lowercase()))
                );
                assert!(
                    !request
                        .lines()
                        .next()
                        .unwrap()
                        .contains("private-search-token")
                );
                let (status, body) = if i == 0 {
                    ("200 OK", version.to_string())
                } else {
                    ("500 Error", format!("echo {expected} private-search-token"))
                };
                socket.write_all(format!("HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len()).as_bytes()).await.unwrap();
            }
        });
        let connection = SearchDriver(engine)
            .connect(&ConnectParams {
                host: "search.internal.invalid".into(),
                port: 9200,
                user: user.into(),
                password: Some(Secret::new("private-search-token")),
                connect_addr: Some(address),
                tls: TlsMode::Disable,
                ..Default::default()
            })
            .await
            .unwrap();
        let mut events = connection
            .execute_request(
                json!({"method":"GET","path":"/failure"}),
                &ExecOptions::default(),
            )
            .await
            .unwrap();
        let error = match events.next().await.unwrap() {
            Err(e) => e.to_string(),
            Ok(_) => panic!("expected refusal"),
        };
        assert!(!error.contains("private-search-token"));
        assert!(!error.contains(expected));
        assert!(error.contains("***"));
        server.await.unwrap();
    }
}

#[tokio::test]
async fn an_http_redirect_cannot_send_a_request_to_another_endpoint() {
    let target = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let destination = target.local_addr().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        for i in 0..2 {
            let (mut socket, _) = listener.accept().await.unwrap();
            let _ = header(&mut socket).await;
            let reply = if i == 0 {
                "HTTP/1.1 200 OK\r\nContent-Length: 29\r\nConnection: close\r\n\r\n{\"version\":{\"number\":\"test\"}}".into()
            } else {
                format!(
                    "HTTP/1.1 302 Found\r\nLocation: http://{destination}/stolen\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                )
            };
            socket.write_all(reply.as_bytes()).await.unwrap();
        }
    });
    let connection = SearchDriver("elasticsearch")
        .connect(&ConnectParams {
            host: "localhost".into(),
            port: address.port(),
            connect_addr: Some(address),
            tls: TlsMode::Disable,
            ..Default::default()
        })
        .await
        .unwrap();
    let mut events = connection
        .execute_request(
            json!({"method":"GET","path":"/redirect"}),
            &ExecOptions::default(),
        )
        .await
        .unwrap();
    assert!(events.next().await.unwrap().is_err());
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(200), target.accept())
            .await
            .is_err()
    );
    server.await.unwrap();
}
