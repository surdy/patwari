use patwari_server::{config::Config, serve};
use tracing_subscriber::{EnvFilter, fmt};

#[tokio::main]
async fn main() {
    fmt()
        .json()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();

    let config = match Config::from_env() {
        Ok(config) => config,
        Err(error) => {
            tracing::error!(%error, "archive configuration is invalid");
            std::process::exit(1);
        }
    };
    if let Err(error) = serve(config).await {
        tracing::error!(%error, "archive service failed");
        std::process::exit(1);
    }
}
