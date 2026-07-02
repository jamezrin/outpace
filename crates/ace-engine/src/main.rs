//! outpace daemon entry point.

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = ace_engine::runtime::config_from_env()?;
    let peers = ace_engine::runtime::bootstrap_peers_from_env();
    let runtime = ace_engine::runtime::build_runtime(config, peers).await?;
    ace_engine::runtime::serve_http(runtime).await
}
