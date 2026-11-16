use futures_util::StreamExt;
use serde_json::{Value, json};
use spelaion_driver_api::{ConnectParams, Connection, Driver, ExecEvent, ExecOptions, TlsMode};
use spelaion_driver_search::SearchDriver;
use spelaion_plugin_host::conformance::{RequestTarget, assert_conforms_requests};

async fn run(
    connection: &dyn Connection,
    method: &str,
    path: &str,
    query: Value,
    body: Value,
) -> Vec<ExecEvent> {
    connection
        .execute_request(
            json!({"method":method,"path":path,"query":query,"body":body}),
            &ExecOptions::default(),
        )
        .await
        .unwrap()
        .map(|e| e.unwrap())
        .collect()
        .await
}

#[tokio::test]
async fn every_engine_connects_lists_indexes_and_runs_a_search_through_its_plugin() {
    for (engine, variable, version_path, version_query) in [
        (
            "elasticsearch",
            "SPELAION_TEST_ELASTICSEARCH",
            "/",
            json!({}),
        ),
        ("opensearch", "SPELAION_TEST_OPENSEARCH", "/", json!({})),
        (
            "solr",
            "SPELAION_TEST_SOLR",
            "/solr/admin/info/system",
            json!({"wt":"json"}),
        ),
        (
            "meilisearch",
            "SPELAION_TEST_MEILISEARCH",
            "/version",
            json!({}),
        ),
    ] {
        let Ok(addr) = std::env::var(variable) else {
            eprintln!("{variable} not set: skipped");
            continue;
        };
        let (host, port) = addr.rsplit_once(':').unwrap();
        let params = ConnectParams {
            host: host.into(),
            port: port.parse().unwrap(),
            tls: TlsMode::Disable,
            ..Default::default()
        };
        let plugin =
            spelaion_plugin_host::Plugin::new(env!("CARGO_BIN_EXE_spelaiondb-plugin-search"));
        let connection = plugin.driver(engine).connect(&params).await.unwrap();
        assert!(connection.server_version().await.unwrap().contains(engine));
        match engine {
            "solr" => {
                run(
                    connection.as_ref(),
                    "POST",
                    "/solr/orders/update",
                    json!({"commit":"true"}),
                    json!([{"id":"one","name":"Spelaion"}]),
                )
                .await;
            }
            "meilisearch" => {
                run(
                    connection.as_ref(),
                    "POST",
                    "/indexes/orders/documents",
                    json!({"primaryKey":"id"}),
                    json!([{"id":"one","name":"Spelaion"}]),
                )
                .await;
            }
            _ => {
                run(
                    connection.as_ref(),
                    "PUT",
                    "/orders/_doc/one",
                    json!({"refresh":"true"}),
                    json!({"name":"Spelaion","amount":42,"active":true,"nested":{"x":1}}),
                )
                .await;
            }
        }
        let (path, query, body) = match engine {
            "solr" => (
                "/solr/orders/select",
                json!({"q":"id:one","rows":"200","wt":"json"}),
                Value::Null,
            ),
            "meilisearch" => (
                "/indexes/orders/search",
                json!({}),
                json!({"q":"Spelaion","limit":200}),
            ),
            _ => (
                "/orders/_search",
                json!({}),
                json!({"query":{"match":{"name":"Spelaion"}},"size":200}),
            ),
        };
        let mut found = false;
        for _ in 0..100 {
            if engine == "meilisearch"
                && !connection
                    .list_objects("indexes")
                    .await
                    .unwrap()
                    .iter()
                    .any(|o| o.name == "orders")
            {
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                continue;
            }
            let events = run(
                connection.as_ref(),
                if engine == "meilisearch" {
                    "POST"
                } else {
                    "GET"
                },
                path,
                query.clone(),
                body.clone(),
            )
            .await;
            let count: usize = events
                .iter()
                .filter_map(|e| {
                    if let ExecEvent::Batch(b) = e {
                        Some(b.num_rows())
                    } else {
                        None
                    }
                })
                .sum();
            if count == 1 {
                found = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        assert!(found, "{engine}: seeded hit not returned");
        let objects = connection.list_objects("indexes").await.unwrap();
        assert!(
            objects.iter().any(|o| o.name == "orders"),
            "{engine}: {objects:?}"
        );
        assert!(
            objects
                .iter()
                .all(|o| o.kind == spelaion_driver_api::ObjectKind::Collection)
        );
        assert_conforms_requests(&RequestTarget {
            program: env!("CARGO_BIN_EXE_spelaiondb-plugin-search").into(),
            engine: engine.into(),
            params,
            one_row: json!({"method":"GET","path":version_path,"query":version_query}),
            refused: json!({"method":"GET","path":"/spelaion_missing_route"}),
            slow: None,
        })
        .await;
    }
}

#[tokio::test]
async fn a_tunnel_address_is_used_instead_of_the_server_name() {
    let Ok(addr) = std::env::var("SPELAION_TEST_ELASTICSEARCH") else {
        return;
    };
    let params = ConnectParams {
        host: "does-not-resolve.invalid".into(),
        port: 9200,
        connect_addr: Some(addr.parse().unwrap()),
        tls: TlsMode::Disable,
        ..Default::default()
    };
    assert!(SearchDriver("elasticsearch").connect(&params).await.is_ok());
}
