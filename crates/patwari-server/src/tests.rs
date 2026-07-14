use std::{fs as stdfs, path::PathBuf, time::Duration};

use axum::{
    Router,
    body::Body,
    http::{HeaderMap, Request, StatusCode, header},
};
use http_body_util::BodyExt;
use sha2::{Digest, Sha256};
use tower::ServiceExt;
use uuid::Uuid;

use crate::{
    Service,
    config::Config,
    contract::{
        Compression, Receipt, SnapshotResponse, UploadResponse, UploadStatus, UploadStatusResponse,
    },
};

struct TestDataDir(PathBuf);

impl TestDataDir {
    fn new() -> Self {
        let path = std::env::current_dir()
            .expect("current directory exists")
            .join("target")
            .join(format!("patwari-test-{}", Uuid::now_v7()));
        Self(path)
    }
}

impl Drop for TestDataDir {
    fn drop(&mut self) {
        let _ = stdfs::remove_dir_all(&self.0);
    }
}

fn test_config(data_dir: &TestDataDir) -> Config {
    Config {
        data_dir: data_dir.0.clone(),
        chunk_size_bytes: 1024,
        max_artifact_stored_bytes: 64 * 1024 * 1024,
        ..Config::default()
    }
}

async fn call(app: Router, request: Request<Body>) -> (StatusCode, HeaderMap, Vec<u8>) {
    let response = app.oneshot(request).await.expect("router response");
    let status = response.status();
    let headers = response.headers().clone();
    let bytes = response
        .into_body()
        .collect()
        .await
        .expect("body can be read")
        .to_bytes()
        .to_vec();
    (status, headers, bytes)
}

fn json_request<T: serde::Serialize + ?Sized>(method: &str, uri: &str, body: &T) -> Request<Body> {
    Request::builder()
        .method(method)
        .uri(uri)
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(
            serde_json::to_string(body).expect("test JSON serializes"),
        ))
        .expect("request is valid")
}

fn digest(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut output = String::from("sha256:");
    for byte in digest {
        use std::fmt::Write;
        write!(output, "{byte:02x}").expect("writing to string succeeds");
    }
    output
}

fn manifest(original: &[u8], stored: &[u8], compression: Compression) -> serde_json::Value {
    manifest_with_session(original, stored, compression, "source-session-1")
}

fn manifest_with_session(
    original: &[u8],
    stored: &[u8],
    compression: Compression,
    source_session_id: &str,
) -> serde_json::Value {
    serde_json::json!({
        "schema_version": 1,
        "session": {
            "source_agent": "copilot-cli",
            "source_session_id": source_session_id
        },
        "capture": {
            "captured_at": "2026-07-13T20:00:00Z",
            "source_cursor": "1",
            "project": "patwari",
            "repository": "surdy/patwari",
            "branch": "main",
            "source_agent_version": "1.0",
            "munshi_version": "1.0"
        },
        "artifact": {
            "logical_path": "events.jsonl",
            "media_type": "application/x-ndjson",
            "original_size_bytes": original.len(),
            "original_sha256": digest(original),
            "stored_size_bytes": stored.len(),
            "stored_sha256": digest(stored),
            "compression": compression
        }
    })
}

async fn register(app: Router, client_id: Uuid) {
    let (status, _, _) = call(
        app,
        json_request(
            "PUT",
            &format!("/api/v1/clients/{client_id}"),
            &serde_json::json!({
                "hostname": "developer-host",
                "display_name": "Developer",
                "metadata": {"munshi": "1.0"}
            }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
}

async fn create_upload(
    service: &Service,
    config: &Config,
    client_id: Uuid,
    key: &str,
    document: serde_json::Value,
) -> UploadResponse {
    let (status, _, body) = call(
        service.router(config),
        json_request(
            "POST",
            "/api/v1/uploads",
            &serde_json::json!({
                "client_id": client_id.to_string(),
                "idempotency_key": key,
                "manifest": document
            }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    serde_json::from_slice(&body).expect("upload response parses")
}

fn chunk_url(upload_id: &str, index: u64) -> String {
    format!("/api/v1/uploads/{upload_id}/artifacts/0/chunks/{index}")
}

fn chunk_request(url: &str, bytes: &[u8]) -> Request<Body> {
    Request::builder()
        .method("PUT")
        .uri(url)
        .header(header::CONTENT_TYPE, "application/octet-stream")
        .header("x-patwari-chunk-length", bytes.len().to_string())
        .header("x-patwari-chunk-sha256", digest(bytes))
        .body(Body::from(bytes.to_vec()))
        .expect("request is valid")
}

async fn upload_chunk(
    service: &Service,
    config: &Config,
    upload_id: &str,
    index: u64,
    bytes: &[u8],
) -> StatusCode {
    let (status, _, _) = call(
        service.router(config),
        chunk_request(&chunk_url(upload_id, index), bytes),
    )
    .await;
    status
}

async fn snapshot_count(service: &Service) -> i64 {
    let row: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM snapshots")
        .fetch_one(&service.state.database)
        .await
        .expect("snapshots query succeeds");
    row.0
}

#[tokio::test]
async fn bootstrap_from_empty_volume_creates_layout_and_identity_once() {
    let data_dir = TestDataDir::new();
    let config = test_config(&data_dir);

    let (service, identity) = Service::bootstrap(&config)
        .await
        .expect("empty volume bootstraps");

    assert_eq!(identity.owner_namespace, crate::database::OWNER_NAMESPACE);
    assert!(Uuid::parse_str(&identity.archive_instance_id).is_ok());
    assert!(data_dir.0.join("patwari.db").is_file());
    assert!(service.state.storage.blobs.is_dir());
    assert!(service.state.storage.uploads.is_dir());
}

#[tokio::test]
async fn chunk_negotiation_reports_bitmap_and_resume_survives_restart() {
    let data_dir = TestDataDir::new();
    let config = test_config(&data_dir);
    let (service, _) = Service::bootstrap(&config).await.expect("bootstraps");
    let client_id = Uuid::new_v4();
    register(service.router(&config), client_id).await;

    let stored = vec![b'a'; 2 * 1024 + 9];
    let upload = create_upload(
        &service,
        &config,
        client_id,
        "resume-bitmap",
        manifest(&stored, &stored, Compression::Identity),
    )
    .await;
    assert_eq!(upload.chunk_size_bytes, 1024);
    assert_eq!(upload.artifacts[0].chunk_count, 3);
    assert_eq!(upload.artifacts[0].accepted_chunk_bitmap, "00");
    assert_eq!(upload.artifacts[0].missing_chunk_indexes, vec![0, 1, 2]);

    assert_eq!(
        upload_chunk(&service, &config, &upload.upload_id, 0, &stored[..1024]).await,
        StatusCode::NO_CONTENT
    );
    let (status, _, body) = call(
        service.router(&config),
        Request::builder()
            .uri(&upload.status_url)
            .body(Body::empty())
            .expect("status request is valid"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let progress: UploadStatusResponse =
        serde_json::from_slice(&body).expect("status response parses");
    assert_eq!(progress.status, UploadStatus::Created);
    assert_eq!(progress.artifacts[0].accepted_chunk_bitmap, "01");
    assert_eq!(progress.artifacts[0].missing_chunk_indexes, vec![1, 2]);
    drop(service);

    let (restarted, _) = Service::bootstrap(&config).await.expect("restarts");
    let (status, _, body) = call(
        restarted.router(&config),
        Request::builder()
            .uri(&upload.status_url)
            .body(Body::empty())
            .expect("status request is valid"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let resumed: UploadStatusResponse =
        serde_json::from_slice(&body).expect("resumed status parses");
    assert_eq!(resumed.artifacts[0].accepted_chunk_bitmap, "01");

    assert_eq!(
        upload_chunk(
            &restarted,
            &config,
            &upload.upload_id,
            1,
            &stored[1024..2048]
        )
        .await,
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        upload_chunk(&restarted, &config, &upload.upload_id, 2, &stored[2048..]).await,
        StatusCode::NO_CONTENT
    );
    let (status, _, receipt) = call(
        restarted.router(&config),
        Request::builder()
            .method("POST")
            .uri(&upload.completion_url)
            .body(Body::empty())
            .expect("completion request is valid"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let receipt: Receipt = serde_json::from_slice(&receipt).expect("receipt parses");
    assert_eq!(receipt.total_stored_bytes, stored.len() as u64);
}

#[tokio::test]
async fn restart_migrates_previous_one_chunk_temporary_upload() {
    let data_dir = TestDataDir::new();
    let config = test_config(&data_dir);
    let (service, _) = Service::bootstrap(&config).await.expect("bootstraps");
    let client_id = Uuid::new_v4();
    register(service.router(&config), client_id).await;
    let bytes = vec![b'l'; 1024];
    let upload = create_upload(
        &service,
        &config,
        client_id,
        "legacy-one-chunk",
        manifest(&bytes, &bytes, Compression::Identity),
    )
    .await;
    let legacy_path = service
        .state
        .storage
        .upload_dir(&upload.upload_id)
        .join("artifact");
    stdfs::create_dir_all(legacy_path.parent().expect("legacy path has parent"))
        .expect("create legacy directory");
    stdfs::write(&legacy_path, &bytes).expect("write legacy artifact");
    sqlx::query(
        "UPDATE uploads SET chunk_size_bytes = 4194304, chunk_count = 0,
                declared_stored_size_bytes = 0, declared_original_size_bytes = 0,
                expires_at = '1970-01-01T00:00:00Z', status = 'artifact_uploaded'
         WHERE id = ?1",
    )
    .bind(&upload.upload_id)
    .execute(&service.state.database)
    .await
    .expect("simulate pre-chunk row");
    drop(service);

    let (restarted, _) = Service::bootstrap(&config)
        .await
        .expect("migrates legacy upload");
    let (status, _, body) = call(
        restarted.router(&config),
        Request::builder()
            .uri(&upload.status_url)
            .body(Body::empty())
            .expect("status request is valid"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let progress: UploadStatusResponse = serde_json::from_slice(&body).expect("status parses");
    assert_eq!(progress.artifacts[0].chunk_count, 1);
    assert_eq!(progress.artifacts[0].accepted_chunk_bitmap, "01");
    assert!(!legacy_path.exists());

    let (status, _, _) = call(
        restarted.router(&config),
        Request::builder()
            .method("POST")
            .uri(&upload.completion_url)
            .body(Body::empty())
            .expect("completion request is valid"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
}

#[tokio::test]
async fn identical_chunk_is_idempotent_but_conflicting_retry_is_rejected() {
    let data_dir = TestDataDir::new();
    let config = test_config(&data_dir);
    let (service, _) = Service::bootstrap(&config).await.expect("bootstraps");
    let client_id = Uuid::new_v4();
    register(service.router(&config), client_id).await;
    let bytes = vec![b'x'; 1024];
    let upload = create_upload(
        &service,
        &config,
        client_id,
        "chunk-retry",
        manifest(&bytes, &bytes, Compression::Identity),
    )
    .await;

    assert_eq!(
        upload_chunk(&service, &config, &upload.upload_id, 0, &bytes).await,
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        upload_chunk(&service, &config, &upload.upload_id, 0, &bytes).await,
        StatusCode::NO_CONTENT
    );
    let conflicting = vec![b'y'; 1024];
    let (status, _, body) = call(
        service.router(&config),
        chunk_request(&chunk_url(&upload.upload_id, 0), &conflicting),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert!(
        String::from_utf8(body)
            .expect("error is utf-8")
            .contains("chunk_conflict")
    );
    let (status, _, body) = call(
        service.router(&config),
        chunk_request(&chunk_url(&upload.upload_id, 0), b"x"),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert!(
        String::from_utf8(body)
            .expect("error is utf-8")
            .contains("chunk_conflict")
    );
    let rows: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM upload_chunks")
        .fetch_one(&service.state.database)
        .await
        .expect("chunks query succeeds");
    assert_eq!(rows.0, 1);
}

#[tokio::test]
async fn headerless_previous_single_chunk_client_remains_compatible() {
    let data_dir = TestDataDir::new();
    let config = test_config(&data_dir);
    let (service, _) = Service::bootstrap(&config).await.expect("bootstraps");
    let client_id = Uuid::new_v4();
    register(service.router(&config), client_id).await;
    let bytes = b"previous one-chunk client";
    let upload = create_upload(
        &service,
        &config,
        client_id,
        "legacy-client",
        manifest(bytes, bytes, Compression::Identity),
    )
    .await;
    let (status, _, _) = call(
        service.router(&config),
        Request::builder()
            .method("PUT")
            .uri(&upload.artifact_upload_url)
            .header(header::CONTENT_TYPE, "application/octet-stream")
            .body(Body::from(bytes.as_slice()))
            .expect("legacy request is valid"),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let (status, _, _) = call(
        service.router(&config),
        Request::builder()
            .method("POST")
            .uri(&upload.completion_url)
            .body(Body::empty())
            .expect("completion request is valid"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_identical_chunks_are_idempotent() {
    let data_dir = TestDataDir::new();
    let config = test_config(&data_dir);
    let (service, _) = Service::bootstrap(&config).await.expect("bootstraps");
    let client_id = Uuid::new_v4();
    register(service.router(&config), client_id).await;
    let bytes = vec![b'z'; 1024];
    let upload = create_upload(
        &service,
        &config,
        client_id,
        "concurrent-chunk",
        manifest(&bytes, &bytes, Compression::Identity),
    )
    .await;

    let mut tasks = Vec::new();
    for _ in 0..8 {
        let app = service.router(&config);
        let url = chunk_url(&upload.upload_id, 0);
        let body = bytes.clone();
        tasks.push(tokio::spawn(async move {
            call(app, chunk_request(&url, &body)).await.0
        }));
    }
    for task in tasks {
        assert_eq!(task.await.expect("task completes"), StatusCode::NO_CONTENT);
    }
    let rows: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM upload_chunks")
        .fetch_one(&service.state.database)
        .await
        .expect("chunks query succeeds");
    assert_eq!(rows.0, 1);
}

#[tokio::test]
async fn completion_rejects_gaps_lengths_and_checksum_mismatches_without_snapshot() {
    let data_dir = TestDataDir::new();
    let config = test_config(&data_dir);
    let (service, _) = Service::bootstrap(&config).await.expect("bootstraps");
    let client_id = Uuid::new_v4();
    register(service.router(&config), client_id).await;
    let bytes = [vec![b'a'; 1024], vec![b'b'; 1]].concat();
    let upload = create_upload(
        &service,
        &config,
        client_id,
        "completion-errors",
        manifest(&bytes, &bytes, Compression::Identity),
    )
    .await;

    let bad_final_chunk = Request::builder()
        .method("PUT")
        .uri(chunk_url(&upload.upload_id, 1))
        .header(header::CONTENT_TYPE, "application/octet-stream")
        .header("x-patwari-chunk-length", "2")
        .header("x-patwari-chunk-sha256", digest(b"bb"))
        .body(Body::from(b"bb".to_vec()))
        .expect("request is valid");
    assert_eq!(
        call(service.router(&config), bad_final_chunk).await.0,
        StatusCode::UNPROCESSABLE_ENTITY
    );
    assert_eq!(
        upload_chunk(&service, &config, &upload.upload_id, 1, &bytes[1024..]).await,
        StatusCode::NO_CONTENT
    );
    let (status, _, _) = call(
        service.router(&config),
        Request::builder()
            .method("POST")
            .uri(&upload.completion_url)
            .body(Body::empty())
            .expect("completion request is valid"),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(snapshot_count(&service).await, 0);

    let bad_length_request = Request::builder()
        .method("PUT")
        .uri(chunk_url(&upload.upload_id, 0))
        .header(header::CONTENT_TYPE, "application/octet-stream")
        .header("x-patwari-chunk-length", "1023")
        .header("x-patwari-chunk-sha256", digest(&bytes[..1024]))
        .body(Body::from(bytes[..1024].to_vec()))
        .expect("request is valid");
    assert_eq!(
        call(service.router(&config), bad_length_request).await.0,
        StatusCode::UNPROCESSABLE_ENTITY
    );

    let bad_hash_request = Request::builder()
        .method("PUT")
        .uri(chunk_url(&upload.upload_id, 0))
        .header(header::CONTENT_TYPE, "application/octet-stream")
        .header("x-patwari-chunk-length", "1024")
        .header(
            "x-patwari-chunk-sha256",
            "sha256:0000000000000000000000000000000000000000000000000000000000000000",
        )
        .body(Body::from(bytes[..1024].to_vec()))
        .expect("request is valid");
    assert_eq!(
        call(service.router(&config), bad_hash_request).await.0,
        StatusCode::UNPROCESSABLE_ENTITY
    );
    assert_eq!(snapshot_count(&service).await, 0);

    assert_eq!(
        upload_chunk(&service, &config, &upload.upload_id, 0, &bytes[..1024]).await,
        StatusCode::NO_CONTENT
    );
    let chunk_path = service.state.storage.chunk_path(&upload.upload_id, 0);
    stdfs::write(&chunk_path, vec![b'c'; 1024]).expect("test can corrupt accepted chunk");
    let (status, _, _) = call(
        service.router(&config),
        Request::builder()
            .method("POST")
            .uri(&upload.completion_url)
            .body(Body::empty())
            .expect("completion request is valid"),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(snapshot_count(&service).await, 0);
}

#[tokio::test]
async fn completes_and_retries_receipt_for_identity_and_zstd_artifacts() {
    let data_dir = TestDataDir::new();
    let config = test_config(&data_dir);
    let (service, identity) = Service::bootstrap(&config).await.expect("bootstraps");
    let client_id = Uuid::new_v4();
    register(service.router(&config), client_id).await;
    let original = b"repeated source event\n".repeat(200);
    let stored = zstd::stream::encode_all(&original[..], 1).expect("compresses");
    let upload = create_upload(
        &service,
        &config,
        client_id,
        "complete-zstd",
        manifest(&original, &stored, Compression::Zstd),
    )
    .await;
    for (index, bytes) in stored.chunks(1024).enumerate() {
        assert_eq!(
            upload_chunk(&service, &config, &upload.upload_id, index as u64, bytes).await,
            StatusCode::NO_CONTENT
        );
    }
    let (status, _, receipt_bytes) = call(
        service.router(&config),
        Request::builder()
            .method("POST")
            .uri(&upload.completion_url)
            .body(Body::empty())
            .expect("completion request is valid"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let receipt: Receipt = serde_json::from_slice(&receipt_bytes).expect("receipt parses");
    assert_eq!(receipt.archive_instance_id, identity.archive_instance_id);
    let (retry_status, _, retry_bytes) = call(
        service.router(&config),
        Request::builder()
            .method("POST")
            .uri(&upload.completion_url)
            .body(Body::empty())
            .expect("retry request is valid"),
    )
    .await;
    assert_eq!(retry_status, StatusCode::OK);
    assert_eq!(retry_bytes, receipt_bytes);

    let (snapshot_status, _, snapshot_bytes) = call(
        service.router(&config),
        Request::builder()
            .uri(format!("/api/v1/snapshots/{}", receipt.snapshot_id))
            .body(Body::empty())
            .expect("snapshot request is valid"),
    )
    .await;
    assert_eq!(snapshot_status, StatusCode::OK);
    let snapshot: SnapshotResponse =
        serde_json::from_slice(&snapshot_bytes).expect("snapshot parses");
    let (download_status, headers, downloaded) = call(
        service.router(&config),
        Request::builder()
            .uri(&snapshot.artifacts[0].content_url)
            .body(Body::empty())
            .expect("download request is valid"),
    )
    .await;
    assert_eq!(download_status, StatusCode::OK);
    assert_eq!(downloaded, stored);
    assert_eq!(headers["x-patwari-stored-sha256"], digest(&stored));
}

#[tokio::test]
async fn abandon_redacts_chunk_detail_and_removes_temporary_bytes() {
    let data_dir = TestDataDir::new();
    let config = test_config(&data_dir);
    let (service, _) = Service::bootstrap(&config).await.expect("bootstraps");
    let client_id = Uuid::new_v4();
    register(service.router(&config), client_id).await;
    let bytes = vec![b'a'; 1024];
    let upload = create_upload(
        &service,
        &config,
        client_id,
        "abandon",
        manifest(&bytes, &bytes, Compression::Identity),
    )
    .await;
    assert_eq!(
        upload_chunk(&service, &config, &upload.upload_id, 0, &bytes).await,
        StatusCode::NO_CONTENT
    );
    let (status, _, body) = call(
        service.router(&config),
        Request::builder()
            .method("POST")
            .uri(&upload.abandon_url)
            .body(Body::empty())
            .expect("abandon request is valid"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let terminal: UploadStatusResponse =
        serde_json::from_slice(&body).expect("terminal status parses");
    assert_eq!(terminal.status, UploadStatus::Abandoned);
    assert!(terminal.artifacts.is_empty());
    assert!(!service.state.storage.upload_dir(&upload.upload_id).exists());

    let chunks: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM upload_chunks")
        .fetch_one(&service.state.database)
        .await
        .expect("chunk query succeeds");
    let manifests: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM manifests")
        .fetch_one(&service.state.database)
        .await
        .expect("manifest query succeeds");
    let audit: (String, i64, i64, i64, String) = sqlx::query_as(
        "SELECT terminal_reason, declared_original_size_bytes, declared_stored_size_bytes,
                chunk_count, error_code
         FROM upload_audits WHERE upload_id = ?1",
    )
    .bind(&upload.upload_id)
    .fetch_one(&service.state.database)
    .await
    .expect("audit query succeeds");
    assert_eq!(chunks.0, 0);
    assert_eq!(manifests.0, 0);
    assert_eq!(audit.0, "abandoned");
    assert_eq!(audit.1, 1024);
    assert_eq!(audit.2, 1024);
    assert_eq!(audit.3, 1);
    assert_eq!(audit.4, "upload_abandoned");
}

#[tokio::test]
async fn expiry_maintenance_removes_partial_upload_and_retains_audit() {
    let data_dir = TestDataDir::new();
    let mut config = test_config(&data_dir);
    config.upload_expiry = Duration::from_mins(1);
    let (service, _) = Service::bootstrap(&config).await.expect("bootstraps");
    let client_id = Uuid::new_v4();
    register(service.router(&config), client_id).await;
    let bytes = vec![b'a'; 1024];
    let upload = create_upload(
        &service,
        &config,
        client_id,
        "expiry",
        manifest(&bytes, &bytes, Compression::Identity),
    )
    .await;
    assert_eq!(
        upload_chunk(&service, &config, &upload.upload_id, 0, &bytes).await,
        StatusCode::NO_CONTENT
    );
    let expired = service
        .expire_uploads_at(time::OffsetDateTime::now_utc() + time::Duration::seconds(61))
        .await
        .expect("expiry maintenance succeeds");
    assert_eq!(expired, 1);
    assert!(!service.state.storage.upload_dir(&upload.upload_id).exists());
    let (status, _, body) = call(
        service.router(&config),
        Request::builder()
            .uri(&upload.status_url)
            .body(Body::empty())
            .expect("status request is valid"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let terminal: UploadStatusResponse =
        serde_json::from_slice(&body).expect("terminal status parses");
    assert_eq!(terminal.status, UploadStatus::Expired);
}

#[tokio::test]
async fn restart_removes_orphaned_blob_from_interrupted_promotion() {
    let data_dir = TestDataDir::new();
    let config = test_config(&data_dir);
    let (service, _) = Service::bootstrap(&config).await.expect("bootstraps");
    let digest = "ab00000000000000000000000000000000000000000000000000000000000000";
    let orphan_path = service.state.storage.blob_path(digest);
    stdfs::create_dir_all(orphan_path.parent().expect("blob has parent"))
        .expect("create orphan shard");
    stdfs::write(&orphan_path, b"orphaned promoted bytes").expect("write orphan");
    drop(service);

    let (_restarted, _) = Service::bootstrap(&config).await.expect("restarts");
    assert!(
        !orphan_path.exists(),
        "recovery removes an unreferenced file left by a crash before metadata commit"
    );
}

async fn complete_upload_for_receipt(
    service: &Service,
    config: &Config,
    completion_url: &str,
) -> Receipt {
    let (status, _, body) = call(
        service.router(config),
        Request::builder()
            .method("POST")
            .uri(completion_url)
            .body(Body::empty())
            .expect("completion request is valid"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    serde_json::from_slice(&body).expect("receipt parses")
}

#[tokio::test]
async fn duplicate_snapshot_with_different_stored_representation_keeps_only_first_blob() {
    let data_dir = TestDataDir::new();
    let config = test_config(&data_dir);
    let (service, _) = Service::bootstrap(&config).await.expect("bootstraps");
    let client_id = Uuid::new_v4();
    register(service.router(&config), client_id).await;
    let original = b"repeated dedup source event\n".repeat(200);
    let zstd_stored = zstd::stream::encode_all(&original[..], 1).expect("compresses");

    // Same session/stable capture context and original bytes, but stored as
    // identity first.
    let identity_upload = create_upload(
        &service,
        &config,
        client_id,
        "dedup-identity",
        manifest(&original, &original, Compression::Identity),
    )
    .await;
    for (index, bytes) in original.chunks(1024).enumerate() {
        assert_eq!(
            upload_chunk(
                &service,
                &config,
                &identity_upload.upload_id,
                index as u64,
                bytes
            )
            .await,
            StatusCode::NO_CONTENT
        );
    }
    let identity_receipt =
        complete_upload_for_receipt(&service, &config, &identity_upload.completion_url).await;

    // Same session/stable capture context and original bytes, but stored as
    // zstd this time: a different, equally valid stored representation.
    let zstd_upload = create_upload(
        &service,
        &config,
        client_id,
        "dedup-zstd",
        manifest(&original, &zstd_stored, Compression::Zstd),
    )
    .await;
    for (index, bytes) in zstd_stored.chunks(1024).enumerate() {
        assert_eq!(
            upload_chunk(
                &service,
                &config,
                &zstd_upload.upload_id,
                index as u64,
                bytes
            )
            .await,
            StatusCode::NO_CONTENT
        );
    }
    let zstd_receipt =
        complete_upload_for_receipt(&service, &config, &zstd_upload.completion_url).await;

    // Both completions resolve to the same snapshot and receipt semantics.
    assert_eq!(zstd_receipt.snapshot_id, identity_receipt.snapshot_id);
    assert_eq!(
        zstd_receipt.snapshot_fingerprint,
        identity_receipt.snapshot_fingerprint
    );
    assert_eq!(snapshot_count(&service).await, 1);

    let artifacts: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM artifacts")
        .fetch_one(&service.state.database)
        .await
        .expect("artifact query succeeds");
    assert_eq!(artifacts.0, 1);

    let blobs: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM blobs")
        .fetch_one(&service.state.database)
        .await
        .expect("blob query succeeds");
    assert_eq!(
        blobs.0, 1,
        "only the blob referenced by the winning artifact remains"
    );

    // No restart is required: the alternate stored representation must never
    // have been promoted to permanent storage in the first place.
    let identity_blob_path = service
        .state
        .storage
        .blob_path(&digest_storage_hex(&original));
    assert!(identity_blob_path.exists());
    let zstd_blob_path = service
        .state
        .storage
        .blob_path(&digest_storage_hex(&zstd_stored));
    assert!(
        !zstd_blob_path.exists(),
        "unreferenced alternate stored representation must not persist on disk"
    );

    assert!(
        !service
            .state
            .storage
            .upload_dir(&zstd_upload.upload_id)
            .exists()
    );
}

fn digest_storage_hex(bytes: &[u8]) -> String {
    digest(bytes)
        .strip_prefix("sha256:")
        .expect("digest carries sha256 prefix")
        .to_owned()
}

async fn fetch_snapshot(service: &Service, config: &Config, snapshot_id: &str) -> SnapshotResponse {
    let (status, _, body) = call(
        service.router(config),
        Request::builder()
            .uri(format!("/api/v1/snapshots/{snapshot_id}"))
            .body(Body::empty())
            .expect("snapshot request is valid"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    serde_json::from_slice(&body).expect("snapshot response parses")
}

async fn fetch_content(
    service: &Service,
    config: &Config,
    content_url: &str,
) -> (StatusCode, Vec<u8>) {
    let (status, _, body) = call(
        service.router(config),
        Request::builder()
            .uri(content_url)
            .body(Body::empty())
            .expect("content request is valid"),
    )
    .await;
    (status, body)
}

/// Lists every file name present under the blob store's sha256 shards,
/// mirroring the walk `recover_unreferenced_blobs` performs, so a test can
/// assert the exact set of canonical files left on disk.
fn blob_files_on_disk(service: &Service) -> std::collections::HashSet<String> {
    let mut found = std::collections::HashSet::new();
    let sha256_root = service.state.storage.blobs.join("sha256");
    let Ok(shard_entries) = stdfs::read_dir(&sha256_root) else {
        return found;
    };
    for shard in shard_entries.flatten() {
        if !shard.file_type().is_ok_and(|kind| kind.is_dir()) {
            continue;
        }
        let Ok(blob_entries) = stdfs::read_dir(shard.path()) else {
            continue;
        };
        for blob in blob_entries.flatten() {
            if let Ok(name) = blob.file_name().into_string() {
                found.insert(name);
            }
        }
    }
    found
}

async fn upload_full(
    service: &Service,
    config: &Config,
    client_id: Uuid,
    idempotency_key: &str,
    manifest_document: serde_json::Value,
    stored_bytes: &[u8],
) -> UploadResponse {
    let upload = create_upload(
        service,
        config,
        client_id,
        idempotency_key,
        manifest_document,
    )
    .await;
    for (index, bytes) in stored_bytes.chunks(1024).enumerate() {
        assert_eq!(
            upload_chunk(service, config, &upload.upload_id, index as u64, bytes).await,
            StatusCode::NO_CONTENT
        );
    }
    upload
}

/// Asserts the invariants a fixed three-party data-loss race must preserve:
/// D's committed artifact remains downloadable and checksum-valid, exactly
/// the correct blob rows/files remain, and no dangling reference or
/// orphaned file exists.
#[allow(clippy::too_many_arguments)]
async fn assert_three_party_race_invariants(
    service: &Service,
    config: &Config,
    a_receipt: &Receipt,
    b_receipt: &Receipt,
    d_receipt: &Receipt,
    original: &[u8],
    zstd_stored: &[u8],
    upload_ids: [&str; 3],
) {
    // B lost the race and reused A's snapshot; D's own session always gets
    // its own snapshot regardless of the A/B outcome.
    assert_eq!(b_receipt.snapshot_id, a_receipt.snapshot_id);
    assert_ne!(d_receipt.snapshot_id, a_receipt.snapshot_id);
    assert_eq!(snapshot_count(service).await, 2);

    let artifacts: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM artifacts")
        .fetch_one(&service.state.database)
        .await
        .expect("artifact query succeeds");
    assert_eq!(
        artifacts.0, 2,
        "the winning A/B artifact and D's own artifact are the only artifacts"
    );

    let blobs: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM blobs")
        .fetch_one(&service.state.database)
        .await
        .expect("blob query succeeds");
    assert_eq!(
        blobs.0, 2,
        "exactly one blob row for the winning identity digest and one for the zstd digest \
         D reused remain"
    );

    // D's committed artifact remains downloadable and checksum-valid: the
    // loser's cleanup must never have deleted the file D's own commit now
    // depends on.
    let d_snapshot = fetch_snapshot(service, config, &d_receipt.snapshot_id).await;
    assert_eq!(d_snapshot.artifacts.len(), 1);
    let d_artifact = &d_snapshot.artifacts[0];
    assert_eq!(d_artifact.stored_sha256, digest(zstd_stored));
    let (content_status, content_body) =
        fetch_content(service, config, &d_artifact.content_url).await;
    assert_eq!(content_status, StatusCode::OK);
    assert_eq!(
        content_body, zstd_stored,
        "downloaded bytes match the stored representation D committed"
    );
    assert_eq!(
        digest(&content_body),
        digest(zstd_stored),
        "downloaded content is checksum-valid"
    );

    // Exactly the correct blob rows/files remain: no dangling reference and
    // no orphaned leftover file.
    let identity_digest = digest_storage_hex(original);
    let zstd_digest = digest_storage_hex(zstd_stored);
    let identity_blob_path = service.state.storage.blob_path(&identity_digest);
    let zstd_blob_path = service.state.storage.blob_path(&zstd_digest);
    assert!(identity_blob_path.exists());
    assert!(zstd_blob_path.exists());
    assert_eq!(
        stdfs::read(&zstd_blob_path).expect("zstd blob file reads"),
        zstd_stored,
        "the canonical zstd file on disk matches the reused stored bytes"
    );

    let files_on_disk = blob_files_on_disk(service);
    assert_eq!(
        files_on_disk,
        std::collections::HashSet::from([identity_digest, zstd_digest]),
        "exactly the two referenced blob files remain on disk, with no dangling leftovers"
    );

    // No live-leak: every upload-scoped temporary directory is gone.
    for upload_id in upload_ids {
        assert!(!service.state.storage.upload_dir(upload_id).exists());
    }
}

/// Spawns B's completion, pauses it just before it opens its own commit
/// transaction (after its fast-path duplicate-snapshot check has already
/// found nothing, since A has not committed yet), then lets A run to
/// completion uncontested so A deterministically wins the session +
/// fingerprint race. Resumes B, which then loses against A's now-committed
/// snapshot and rolls back into a second checkpoint -- exactly the
/// historical vulnerable window between releasing the database lock and
/// conditionally deleting the file B had promoted. Returns once B is
/// paused there, along with its still-running completion task.
async fn race_b_as_loser_against_a(
    service: &Service,
    config: &Config,
    a_upload: &UploadResponse,
    b_upload: &UploadResponse,
) -> (
    Receipt,
    tokio::task::JoinHandle<(StatusCode, HeaderMap, Vec<u8>)>,
    std::sync::Arc<crate::service::Checkpoint>,
) {
    use crate::service::Checkpoint;

    let race_checkpoint = Checkpoint::new();
    service
        .state
        .test_hooks
        .set_before_snapshot_commit(race_checkpoint.clone());

    let b_app = service.router(config);
    let b_url = b_upload.completion_url.clone();
    let b_task = tokio::spawn(async move {
        call(
            b_app,
            Request::builder()
                .method("POST")
                .uri(&b_url)
                .body(Body::empty())
                .expect("completion request is valid"),
        )
        .await
    });

    race_checkpoint.wait_for_arrival().await;
    service.state.test_hooks.clear_before_snapshot_commit();
    let a_receipt = complete_upload_for_receipt(service, config, &a_upload.completion_url).await;

    let cleanup_checkpoint = Checkpoint::new();
    service
        .state
        .test_hooks
        .set_after_losing_rollback(cleanup_checkpoint.clone());
    race_checkpoint.resume();
    cleanup_checkpoint.wait_for_arrival().await;

    (a_receipt, b_task, cleanup_checkpoint)
}

/// Spawns D's completion targeting the same digest B is paused on, waits
/// for D to reach its own attempt to acquire the shared per-digest lock,
/// then gives D ample real wall-clock time to finish its remaining
/// lightweight steps before asserting it is still blocked. This keeps the
/// assertion meaningful (able to fail if the lock regresses) rather than
/// merely restating that D has not been scheduled yet.
async fn spawn_third_party_and_assert_blocked(
    service: &Service,
    config: &Config,
    d_upload: &UploadResponse,
) -> tokio::task::JoinHandle<(StatusCode, HeaderMap, Vec<u8>)> {
    let lock_attempt_checkpoint = crate::service::Checkpoint::new();
    service
        .state
        .test_hooks
        .set_before_blob_lock_attempt(lock_attempt_checkpoint.clone());

    let d_app = service.router(config);
    let d_url = d_upload.completion_url.clone();
    let d_task = tokio::spawn(async move {
        call(
            d_app,
            Request::builder()
                .method("POST")
                .uri(&d_url)
                .body(Body::empty())
                .expect("completion request is valid"),
        )
        .await
    });

    lock_attempt_checkpoint.wait_for_arrival().await;
    std::thread::sleep(Duration::from_millis(200));
    for _ in 0..50 {
        tokio::task::yield_now().await;
    }
    assert!(
        !d_task.is_finished(),
        "a third party targeting the same digest as a paused losing completion must block \
         behind the same per-digest lock instead of slipping in before cleanup runs"
    );
    d_task
}

/// Reproduces the historical three-party data-loss race: A and B race to
/// complete the same session+fingerprint snapshot with different stored
/// representations, and D (a different session entirely) happens to encode
/// its own capture to the exact same stored bytes as B, the loser.
///
/// Without per-digest serialization, the loser rolls back its losing
/// transaction (discarding its freshly-inserted blob row), releases the
/// `SQLite` write lock, and then deletes the canonical file it had promoted
/// because that row was never visible to anyone else. If D's completion
/// commits a brand-new blob/artifact reference to that same digest in the
/// gap before the delete runs, the loser's cleanup deletes the file D's
/// commit now depends on, leaving D with a dangling reference to a missing
/// file.
///
/// This test uses deterministic checkpoints (rather than real timing) to
/// pause the loser exactly in that historical gap, spawns D concurrently,
/// and asserts D is blocked (not merely lucky) before resuming the loser.
/// It then asserts D's committed artifact remains downloadable and
/// checksum-valid, that exactly the correct blob rows/files remain, and
/// that no dangling reference or orphaned file exists.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn third_party_reusing_losers_digest_survives_snapshot_race_cleanup() {
    let data_dir = TestDataDir::new();
    let config = test_config(&data_dir);
    let (service, _) = Service::bootstrap(&config).await.expect("bootstraps");
    let client_id = Uuid::new_v4();
    register(service.router(&config), client_id).await;

    let original = b"three-party dedup race source event\n".repeat(200);
    let zstd_stored = zstd::stream::encode_all(&original[..], 1).expect("compresses");

    // A and B: same session and original bytes (so the same session +
    // fingerprint), but different stored representations, racing to
    // complete the same snapshot. D: a different session whose own capture
    // happens to encode to the exact same stored bytes (and therefore the
    // same stored digest) as B.
    let a_upload = upload_full(
        &service,
        &config,
        client_id,
        "three-party-a",
        manifest(&original, &original, Compression::Identity),
        &original,
    )
    .await;
    let b_upload = upload_full(
        &service,
        &config,
        client_id,
        "three-party-b",
        manifest(&original, &zstd_stored, Compression::Zstd),
        &zstd_stored,
    )
    .await;
    let d_upload = upload_full(
        &service,
        &config,
        client_id,
        "three-party-d",
        manifest_with_session(
            &original,
            &zstd_stored,
            Compression::Zstd,
            "source-session-2",
        ),
        &zstd_stored,
    )
    .await;

    let (a_receipt, b_task, cleanup_checkpoint) =
        race_b_as_loser_against_a(&service, &config, &a_upload, &b_upload).await;

    // While B is paused mid-cleanup (holding the per-digest blob lock under
    // the fix), spawn D targeting the exact same digest B just promoted.
    let d_task = spawn_third_party_and_assert_blocked(&service, &config, &d_upload).await;

    cleanup_checkpoint.resume();

    let (b_status, _, b_body) = b_task.await.expect("b completion task completes");
    let (d_status, _, d_body) = d_task.await.expect("d completion task completes");
    assert_eq!(b_status, StatusCode::OK);
    assert_eq!(d_status, StatusCode::OK);
    let b_receipt: Receipt = serde_json::from_slice(&b_body).expect("b receipt parses");
    let d_receipt: Receipt = serde_json::from_slice(&d_body).expect("d receipt parses");

    assert_three_party_race_invariants(
        &service,
        &config,
        &a_receipt,
        &b_receipt,
        &d_receipt,
        &original,
        &zstd_stored,
        [
            &a_upload.upload_id,
            &b_upload.upload_id,
            &d_upload.upload_id,
        ],
    )
    .await;
}

#[tokio::test]
async fn upload_lock_storage_stays_bounded_across_many_upload_ids() {
    use crate::service::UPLOAD_LOCK_STRIPES;

    let data_dir = TestDataDir::new();
    let config = test_config(&data_dir);
    let (service, _) = Service::bootstrap(&config).await.expect("bootstraps");

    let mut distinct_locks = std::collections::HashSet::new();
    for _ in 0..(UPLOAD_LOCK_STRIPES * 20) {
        let upload_id = Uuid::new_v4().to_string();
        let lock = service.state.upload_lock(&upload_id);
        distinct_locks.insert(std::sync::Arc::as_ptr(&lock) as usize);
    }
    assert!(
        distinct_locks.len() <= UPLOAD_LOCK_STRIPES,
        "lock storage must stay bounded by the fixed stripe count regardless of how many \
         distinct upload IDs are used, got {} distinct locks",
        distinct_locks.len()
    );
}

#[tokio::test]
async fn declared_artifact_limits_are_enforced_before_upload() {
    let data_dir = TestDataDir::new();
    let mut config = test_config(&data_dir);
    config.max_artifact_stored_bytes = 1024;
    config.max_artifact_original_bytes = 1024;
    let (service, _) = Service::bootstrap(&config).await.expect("bootstraps");
    let client_id = Uuid::new_v4();
    register(service.router(&config), client_id).await;
    let bytes = vec![b'a'; 1025];
    let (status, _, _) = call(
        service.router(&config),
        json_request(
            "POST",
            "/api/v1/uploads",
            &serde_json::json!({
                "client_id": client_id.to_string(),
                "idempotency_key": "too-large",
                "manifest": manifest(&bytes, &bytes, Compression::Identity)
            }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
}

#[tokio::test]
async fn health_remains_live_when_storage_is_not_ready() {
    let data_dir = TestDataDir::new();
    let config = test_config(&data_dir);
    let (service, _) = Service::bootstrap(&config).await.expect("bootstraps");
    stdfs::remove_dir(&service.state.storage.uploads).expect("uploads is empty");
    stdfs::write(&service.state.storage.uploads, "not a directory").expect("create fault");

    let (live, _, _) = call(
        service.router(&config),
        Request::builder()
            .uri("/healthz")
            .body(Body::empty())
            .expect("request is valid"),
    )
    .await;
    let (ready, _, _) = call(
        service.router(&config),
        Request::builder()
            .uri("/readyz")
            .body(Body::empty())
            .expect("request is valid"),
    )
    .await;
    assert_eq!(live, StatusCode::OK);
    assert_eq!(ready, StatusCode::SERVICE_UNAVAILABLE);
}
