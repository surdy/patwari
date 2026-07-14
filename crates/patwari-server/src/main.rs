use patwari_server::{Service, config::Config, serve};
use tracing_subscriber::{EnvFilter, fmt};

#[tokio::main]
async fn main() {
    fmt()
        .json()
        .with_writer(std::io::stderr)
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();

    let arguments = std::env::args().skip(1).collect::<Vec<_>>();
    match arguments.as_slice() {
        [] => {}
        [command] if command == "serve" => {}
        [command] if command == "verify" => {
            verify().await;
            return;
        }
        _ => {
            tracing::error!("unknown archive command");
            print_machine_error("invalid_command");
            std::process::exit(2);
        }
    }

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

async fn verify() {
    let config = match Config::from_env() {
        Ok(config) => config,
        Err(error) => {
            tracing::error!(%error, "archive configuration is invalid");
            print_machine_error("configuration_error");
            std::process::exit(2);
        }
    };
    let (service, _) = match Service::bootstrap_for_integrity(&config).await {
        Ok(service) => service,
        Err(error) => {
            tracing::error!(%error, "archive integrity scanner could not bootstrap");
            print_machine_error("bootstrap_error");
            std::process::exit(2);
        }
    };
    match service.verify_integrity().await {
        Ok(report) => {
            match serde_json::to_string(&report) {
                Ok(document) => println!("{document}"),
                Err(error) => {
                    tracing::error!(%error, "archive integrity report could not serialize");
                    print_machine_error("report_serialization_error");
                    std::process::exit(2);
                }
            }
            if report.status != patwari_server::contract::IntegrityRunStatus::Healthy {
                std::process::exit(1);
            }
        }
        Err(error) => {
            tracing::error!(%error, "archive integrity scan failed");
            print_machine_error("scan_error");
            std::process::exit(2);
        }
    }
}

fn print_machine_error(code: &str) {
    println!(
        "{}",
        serde_json::json!({
            "status": "scanner_failed",
            "error_code": code,
        })
    );
}
