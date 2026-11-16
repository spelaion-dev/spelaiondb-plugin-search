use spelaion_driver_search::SearchDriver;
use spelaion_plugin_sdk::{Plugin, run};
use std::sync::Arc;
#[tokio::main]
async fn main() {
    run(Plugin::new(
        "spelaiondb-plugin-search",
        env!("CARGO_PKG_VERSION"),
        vec![
            Arc::new(SearchDriver("elasticsearch")),
            Arc::new(SearchDriver("opensearch")),
            Arc::new(SearchDriver("solr")),
            Arc::new(SearchDriver("meilisearch")),
        ],
    ))
    .await
}
