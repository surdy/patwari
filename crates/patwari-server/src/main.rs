use std::path::PathBuf;

use patwari_server::{
    Service,
    backup::{self, BackupError},
    config::Config,
    contract::IntegrityRunStatus,
    serve,
};
use tracing_subscriber::{EnvFilter, fmt};

/// The usage block printed for `--help` and after the machine-readable error on an unrecognized
/// command.
///
/// It is deliberately plain text rather than a generated clap surface: the accepted argument forms
/// are matched positionally and exactly (below), so the usage text and the matcher are one small
/// pair that a reader can check against each other in a single screen.
const USAGE: &str = "\
patwari-server — a self-hosted archive for complete coding-agent sessions.

Usage: patwari-server [COMMAND]

Commands:
  serve                                     Run the HTTP archive service (the default with no arguments)
  verify                                    Run the full integrity scan without listening; write one JSON report to stdout
  backup create --output <dir>              Take a consistent online backup into a new directory outside the data dir
  backup verify <dir>                       Check a backup directory's manifest, checksums, and a staged integrity scan
  backup restore <dir> --data-dir <dir>     Restore a verified backup into an empty data directory

Options:
  -h, --help                                Print this usage and exit
      --version                             Print the version and exit

Every command is configured entirely through PATWARI_* environment variables; see docs/api.md
for the full table and docs/self-hosting.md for the operator runbooks.";

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
        [command] if command == "--help" || command == "-h" || command == "help" => {
            println!("{USAGE}");
            return;
        }
        [command] if command == "--version" => {
            println!("patwari-server {}", env!("CARGO_PKG_VERSION"));
            return;
        }
        [command] if command == "verify" => {
            verify().await;
            return;
        }
        [backup, command, flag, output]
            if backup == "backup" && command == "create" && flag == "--output" =>
        {
            backup_create(output).await;
            return;
        }
        [backup, command, directory] if backup == "backup" && command == "verify" => {
            backup_verify(directory).await;
            return;
        }
        [backup, command, directory, flag, data_dir]
            if backup == "backup" && command == "restore" && flag == "--data-dir" =>
        {
            backup_restore(directory, data_dir).await;
            return;
        }
        _ => {
            tracing::error!("unknown archive command");
            // The JSON line stays first on stdout and the exit code stays 2, so a script that
            // parses `error_code` is unaffected; the usage goes to stderr, after it, for a person.
            print_machine_error("invalid_command");
            eprintln!("{USAGE}");
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

async fn backup_create(output: &str) {
    let config = backup_config();
    match backup::create(&config, output).await {
        Ok(result) => print_json(&result, "backup_report_serialization_error"),
        Err(error) => backup_failure(&error, "backup_create_failed"),
    }
}

async fn backup_verify(directory: &str) {
    let config = backup_config();
    match backup::verify(directory, &config).await {
        Ok(result) => {
            let healthy = result.integrity.status == IntegrityRunStatus::Healthy;
            print_json(&result, "backup_report_serialization_error");
            if !healthy {
                std::process::exit(1);
            }
        }
        Err(error) => backup_failure(&error, "backup_verify_failed"),
    }
}

async fn backup_restore(directory: &str, data_dir: &str) {
    let mut config = backup_config();
    config.data_dir = PathBuf::from(data_dir);
    if let Err(error) = config.validate() {
        tracing::error!(%error, "archive configuration is invalid");
        print_machine_error("configuration_error");
        std::process::exit(2);
    }
    match backup::restore(directory, data_dir, &config).await {
        Ok(result) => print_json(&result, "backup_report_serialization_error"),
        Err(error) => backup_failure(&error, "backup_restore_failed"),
    }
}

fn backup_config() -> Config {
    match Config::from_env() {
        Ok(config) => config,
        Err(error) => {
            tracing::error!(%error, "archive configuration is invalid");
            print_machine_error("configuration_error");
            std::process::exit(2);
        }
    }
}

fn backup_failure(error: &BackupError, code: &str) -> ! {
    tracing::error!(%error, "archive backup operation failed");
    println!(
        "{}",
        serde_json::json!({
            "status": "backup_failed",
            "error_code": code,
        })
    );
    std::process::exit(2);
}

fn print_json(value: &impl serde::Serialize, error_code: &str) {
    match serde_json::to_string(value) {
        Ok(document) => println!("{document}"),
        Err(error) => {
            tracing::error!(%error, "archive report could not serialize");
            print_machine_error(error_code);
            std::process::exit(2);
        }
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
