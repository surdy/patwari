use std::{fs, path::PathBuf, process::Command};

use uuid::Uuid;

struct TestDataDir(PathBuf);

impl TestDataDir {
    fn new() -> Self {
        let path = std::env::current_dir()
            .expect("current directory exists")
            .join("target")
            .join(format!("patwari-verify-cli-{}", Uuid::now_v7()));
        Self(path)
    }
}

impl Drop for TestDataDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

#[test]
fn verify_emits_json_and_distinguishes_findings_from_scanner_errors() {
    let data_dir = TestDataDir::new();
    let binary = env!("CARGO_BIN_EXE_patwari-server");

    let healthy = Command::new(binary)
        .arg("verify")
        .env("PATWARI_DATA_DIR", &data_dir.0)
        .env("RUST_LOG", "error")
        .output()
        .expect("verify command starts");
    assert!(healthy.status.success());
    let healthy_json: serde_json::Value =
        serde_json::from_slice(&healthy.stdout).expect("healthy stdout is JSON");
    assert_eq!(healthy_json["status"], "healthy");
    assert!(healthy_json["run_id"].as_str().is_some());

    let unexpected = data_dir
        .0
        .join("blobs")
        .join("sha256")
        .join("aa")
        .join(format!("aa{}", "b".repeat(62)));
    fs::create_dir_all(unexpected.parent().expect("blob path has parent"))
        .expect("unexpected shard can be created");
    fs::write(&unexpected, b"unexpected").expect("unexpected blob can be created");
    let findings = Command::new(binary)
        .arg("verify")
        .env("PATWARI_DATA_DIR", &data_dir.0)
        .env("RUST_LOG", "error")
        .output()
        .expect("verify command starts");
    assert_eq!(findings.status.code(), Some(1));
    let findings_json: serde_json::Value =
        serde_json::from_slice(&findings.stdout).expect("finding stdout is JSON");
    assert_eq!(findings_json["status"], "action_required");
    assert!(
        findings_json["findings"]
            .as_array()
            .expect("findings are an array")
            .iter()
            .any(|finding| finding["kind"] == "unexpected_blob_file")
    );

    let failed = Command::new(binary)
        .arg("verify")
        .env("PATWARI_DATA_DIR", &data_dir.0)
        .env("PATWARI_INTEGRITY_SCAN_CONCURRENCY", "0")
        .env("RUST_LOG", "error")
        .output()
        .expect("verify command starts");
    assert_eq!(failed.status.code(), Some(2));
    let failed_json: serde_json::Value =
        serde_json::from_slice(&failed.stdout).expect("failure stdout is JSON");
    assert_eq!(failed_json["status"], "scanner_failed");
    assert_eq!(failed_json["error_code"], "configuration_error");
    assert!(!failed.stderr.is_empty());
}
