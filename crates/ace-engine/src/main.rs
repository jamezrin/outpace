//! outpace daemon entry point.

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    ace_engine::cli::run().await
}
