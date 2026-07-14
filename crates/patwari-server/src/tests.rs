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
    ReconciliationError, Service,
    config::Config,
    contract::{
        Artifact, ArtifactMetadataResponse, CanonicalManifestResponse, CanonicalManifestSummary,
        CaptureProvenance, CompletionResponse, Compression, Manifest, ManifestInput,
        PaginatedResponse, Receipt, SessionInput, SessionResponse, SnapshotCapturesResponse,
        SnapshotResponse, SnapshotSummary, UploadResponse, UploadStatus, UploadStatusResponse,
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

fn normalized_manifest(document: serde_json::Value) -> Manifest {
    let input: ManifestInput = serde_json::from_value(document).expect("manifest input parses");
    crate::validation::normalize_manifest(
        input,
        crate::validation::ManifestLimits {
            artifact_count: 128,
            artifact_stored_bytes: 64 * 1024 * 1024,
            artifact_original_bytes: 64 * 1024 * 1024,
            snapshot_stored_bytes: 64 * 1024 * 1024,
            snapshot_original_bytes: 64 * 1024 * 1024,
        },
    )
    .expect("manifest normalizes")
}

fn fingerprint(document: serde_json::Value) -> String {
    crate::ingestion::snapshot_fingerprint(&normalized_manifest(document))
        .expect("fingerprint computes")
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

async fn get_json<T: serde::de::DeserializeOwned>(
    service: &Service,
    config: &Config,
    uri: impl AsRef<str>,
) -> T {
    let (status, _, body) = call(
        service.router(config),
        Request::builder()
            .uri(uri.as_ref())
            .body(Body::empty())
            .expect("GET request is valid"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    serde_json::from_slice(&body).expect("GET response parses")
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
            "artifact_set_version": 1,
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

fn manifest_with_context(
    original: &[u8],
    stored: &[u8],
    source_session_id: &str,
    project: &str,
    repository: &str,
    branch: &str,
    source_metadata: serde_json::Value,
) -> serde_json::Value {
    let mut document =
        manifest_with_session(original, stored, Compression::Identity, source_session_id);
    let capture = document["capture"]
        .as_object_mut()
        .expect("manifest capture is an object");
    capture.insert("project".into(), serde_json::json!(project));
    capture.insert("repository".into(), serde_json::json!(repository));
    capture.insert("branch".into(), serde_json::json!(branch));
    capture.insert("source_metadata".into(), source_metadata);
    document
}

fn multi_manifest(
    artifacts: Vec<(&str, &[u8], &[u8], Compression)>,
    source_session_id: &str,
) -> serde_json::Value {
    let artifacts = artifacts
        .into_iter()
        .map(|(logical_path, original, stored, compression)| {
            serde_json::json!({
                "logical_path": logical_path,
                "media_type": "application/octet-stream",
                "original_size_bytes": original.len(),
                "original_sha256": digest(original),
                "stored_size_bytes": stored.len(),
                "stored_sha256": digest(stored),
                "compression": compression
            })
        })
        .collect::<Vec<_>>();
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
            "artifact_set_version": 1,
            "munshi_version": "1.0"
        },
        "artifacts": artifacts
    })
}

#[allow(clippy::too_many_lines)]
#[test]
fn snapshot_fingerprint_includes_only_stable_semantic_dimensions() {
    let original = b"semantic artifact";
    let base = manifest(original, original, Compression::Identity);
    let expected = fingerprint(base.clone());

    for (name, pointer, value) in [
        (
            "project",
            "/capture/project",
            serde_json::json!("other-project"),
        ),
        (
            "repository",
            "/capture/repository",
            serde_json::json!("other/repository"),
        ),
        ("branch", "/capture/branch", serde_json::json!("release")),
        (
            "source agent version",
            "/capture/source_agent_version",
            serde_json::json!("2.0"),
        ),
        (
            "artifact set version",
            "/capture/artifact_set_version",
            serde_json::json!(2),
        ),
        (
            "logical path",
            "/artifact/logical_path",
            serde_json::json!("other-events.jsonl"),
        ),
        (
            "source agent",
            "/session/source_agent",
            serde_json::json!("other-agent"),
        ),
        (
            "source session",
            "/session/source_session_id",
            serde_json::json!("other-session"),
        ),
    ] {
        let mut changed = base.clone();
        *changed
            .pointer_mut(pointer)
            .expect("stable fingerprint field exists") = value;
        assert_ne!(expected, fingerprint(changed), "{name} changes identity");
    }

    assert_ne!(
        expected,
        fingerprint(manifest(
            b"different semantic artifact",
            b"different semantic artifact",
            Compression::Identity,
        )),
        "verified original artifact identity changes the fingerprint"
    );
    let mut changed_original_hash = base.clone();
    changed_original_hash["artifact"]["original_sha256"] =
        serde_json::json!(digest(b"semantic artifact!"));
    assert_ne!(
        expected,
        fingerprint(changed_original_hash),
        "original content hash changes identity"
    );
    let mut changed_original_size = base.clone();
    changed_original_size["artifact"]["original_size_bytes"] =
        serde_json::json!(original.len() + 1);
    assert_ne!(
        expected,
        fingerprint(changed_original_size),
        "original content size changes identity"
    );

    let first = b"first artifact";
    let second = b"second artifact";
    let ordered = multi_manifest(
        vec![
            ("a.bin", first, first, Compression::Identity),
            ("b.bin", second, second, Compression::Identity),
        ],
        "ordered-fingerprint",
    );
    let mut reordered = ordered.clone();
    reordered["artifacts"]
        .as_array_mut()
        .expect("artifacts is an array")
        .reverse();
    assert_eq!(
        fingerprint(ordered),
        fingerprint(reordered),
        "canonical artifact ordering does not change identity"
    );
    assert_ne!(
        fingerprint(multi_manifest(
            vec![("a.bin", first, first, Compression::Identity)],
            "ordered-fingerprint",
        )),
        fingerprint(multi_manifest(
            vec![
                ("a.bin", first, first, Compression::Identity),
                ("b.bin", second, second, Compression::Identity),
            ],
            "ordered-fingerprint",
        )),
        "the complete canonical artifact set changes identity"
    );

    let mut excluded = base.clone();
    excluded["capture"]["captured_at"] = serde_json::json!("2027-01-01T00:00:00Z");
    excluded["capture"]["source_cursor"] = serde_json::json!("later");
    excluded["capture"]["source_state_hash"] = serde_json::json!("source-state-hash");
    excluded["capture"]["source_metadata"] = serde_json::json!({"opaque": "source fact"});
    excluded["capture"]["munshi_version"] = serde_json::json!("99.0");
    excluded["artifact"]["stored_size_bytes"] = serde_json::json!(42);
    excluded["artifact"]["stored_sha256"] = serde_json::json!(digest(b"alternate stored bytes"));
    excluded["artifact"]["compression"] = serde_json::json!("zstd");
    excluded["artifact"]["media_type"] = serde_json::json!("application/octet-stream");
    assert_eq!(
        expected,
        fingerprint(excluded),
        "provenance, transfer, and stored representation do not change identity"
    );
}

#[allow(clippy::too_many_lines)]
#[tokio::test]
async fn capture_id_is_explicit_idempotent_and_unambiguous() {
    let data_dir = TestDataDir::new();
    let config = test_config(&data_dir);
    let (service, _) = Service::bootstrap(&config).await.expect("bootstraps");
    let client_id = Uuid::new_v4();
    register(service.router(&config), client_id).await;
    let bytes = b"capture identity";
    let document = manifest(bytes, bytes, Compression::Identity);

    let create_body = serde_json::json!({
        "client_id": client_id.to_string(),
        "capture_id": "capture-explicit",
        "manifest": document
    });
    let (created, _, body) = call(
        service.router(&config),
        json_request("POST", "/api/v1/uploads", &create_body),
    )
    .await;
    assert_eq!(created, StatusCode::CREATED);
    let created_upload: UploadResponse = serde_json::from_slice(&body).expect("upload parses");
    assert_eq!(created_upload.capture_id, "capture-explicit");

    let (repeated, _, repeated_body) = call(
        service.router(&config),
        json_request("POST", "/api/v1/uploads", &create_body),
    )
    .await;
    assert_eq!(repeated, StatusCode::OK);
    let repeated_upload: UploadResponse =
        serde_json::from_slice(&repeated_body).expect("repeated upload parses");
    assert_eq!(repeated_upload.upload_id, created_upload.upload_id);

    let mut changed_document = manifest(bytes, bytes, Compression::Identity);
    changed_document["capture"]["source_cursor"] = serde_json::json!("changed");
    let (conflict, _, conflict_body) = call(
        service.router(&config),
        json_request(
            "POST",
            "/api/v1/uploads",
            &serde_json::json!({
                "client_id": client_id.to_string(),
                "capture_id": "capture-explicit",
                "manifest": changed_document
            }),
        ),
    )
    .await;
    assert_eq!(conflict, StatusCode::CONFLICT);
    assert!(
        String::from_utf8(conflict_body)
            .expect("error is text")
            .contains("capture_id_conflict")
    );

    let (legacy_status, _, legacy_body) = call(
        service.router(&config),
        json_request(
            "POST",
            "/api/v1/uploads",
            &serde_json::json!({
                "client_id": client_id.to_string(),
                "idempotency_key": "legacy-capture",
                "manifest": manifest(bytes, bytes, Compression::Identity)
            }),
        ),
    )
    .await;
    assert_eq!(legacy_status, StatusCode::CREATED);
    let legacy_upload: UploadResponse =
        serde_json::from_slice(&legacy_body).expect("legacy upload parses");
    assert_eq!(legacy_upload.capture_id, "legacy-capture");

    let (ambiguous, _, _) = call(
        service.router(&config),
        json_request(
            "POST",
            "/api/v1/uploads",
            &serde_json::json!({
                "client_id": client_id.to_string(),
                "capture_id": "one",
                "idempotency_key": "two",
                "manifest": manifest(bytes, bytes, Compression::Identity)
            }),
        ),
    )
    .await;
    assert_eq!(ambiguous, StatusCode::UNPROCESSABLE_ENTITY);

    let mut missing_artifact_set_version = manifest(bytes, bytes, Compression::Identity);
    missing_artifact_set_version["capture"]
        .as_object_mut()
        .expect("capture is an object")
        .remove("artifact_set_version");
    let (missing_version, _, _) = call(
        service.router(&config),
        json_request(
            "POST",
            "/api/v1/uploads",
            &serde_json::json!({
                "client_id": client_id.to_string(),
                "capture_id": "missing-artifact-set-version",
                "manifest": missing_artifact_set_version
            }),
        ),
    )
    .await;
    assert_eq!(missing_version, StatusCode::UNPROCESSABLE_ENTITY);
}

#[allow(clippy::too_many_lines)]
#[tokio::test]
async fn successful_recaptures_coalesce_receipts_and_retain_provenance() {
    let data_dir = TestDataDir::new();
    let config = test_config(&data_dir);
    let (service, _) = Service::bootstrap(&config).await.expect("bootstraps");
    let first_client = Uuid::new_v4();
    let second_client = Uuid::new_v4();
    register(service.router(&config), first_client).await;
    register(service.router(&config), second_client).await;
    let original = b"recaptured semantic state\n".repeat(200);
    let compressed = zstd::stream::encode_all(&original[..], 1).expect("compresses");

    let mut first_document = manifest(&original, &original, Compression::Identity);
    first_document["capture"]["source_state_hash"] = serde_json::json!("state-a");
    first_document["capture"]["source_metadata"] = serde_json::json!({"adapter_cursor": "a"});
    let first_upload = upload_full(
        &service,
        &config,
        first_client,
        "capture-a",
        first_document,
        &original,
    )
    .await;
    let (before_completion, _, _) = call(
        service.router(&config),
        Request::builder()
            .uri(&first_upload.capture_url)
            .body(Body::empty())
            .expect("capture request is valid"),
    )
    .await;
    assert_eq!(before_completion, StatusCode::NOT_FOUND);
    let first =
        complete_upload_for_completion(&service, &config, &first_upload.completion_url).await;

    let mut second_document = manifest(&original, &compressed, Compression::Zstd);
    second_document["capture"]["captured_at"] = serde_json::json!("2026-07-13T21:00:00Z");
    second_document["capture"]["source_cursor"] = serde_json::json!("2");
    second_document["capture"]["source_state_hash"] = serde_json::json!("state-b");
    second_document["capture"]["source_metadata"] = serde_json::json!({"adapter_cursor": "b"});
    second_document["capture"]["munshi_version"] = serde_json::json!("2.0");
    let second_upload = upload_full(
        &service,
        &config,
        second_client,
        "capture-b",
        second_document,
        &compressed,
    )
    .await;
    let second =
        complete_upload_for_completion(&service, &config, &second_upload.completion_url).await;

    assert_eq!(first.receipt.snapshot_id, second.receipt.snapshot_id);
    assert_eq!(
        serde_json::to_vec(&first.receipt).expect("receipt serializes"),
        serde_json::to_vec(&second.receipt).expect("receipt serializes"),
        "all captures resolving to one snapshot get the same receipt"
    );
    let receipt_json = serde_json::to_value(&first.receipt).expect("receipt serializes");
    assert!(receipt_json.get("upload_transfer_bytes").is_none());
    assert!(receipt_json.get("newly_persisted_physical_bytes").is_none());
    assert_eq!(first.transfer.upload_transfer_bytes, original.len() as u64);
    assert_eq!(
        second.transfer.upload_transfer_bytes,
        compressed.len() as u64
    );
    assert_ne!(first.capture.capture_id, first.receipt.snapshot_fingerprint);
    assert_eq!(snapshot_count(&service).await, 1);

    let captures: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM captures")
        .fetch_one(&service.state.database)
        .await
        .expect("captures query succeeds");
    assert_eq!(captures.0, 2);
    assert_eq!(first.capture.client_id, first_client.to_string());
    assert_eq!(first.capture.source_cursor.as_deref(), Some("1"));
    assert_eq!(first.capture.source_state_hash.as_deref(), Some("state-a"));
    assert_eq!(
        first.capture.source_metadata.get("adapter_cursor"),
        Some(&"a".to_owned())
    );
    assert_eq!(second.capture.client_id, second_client.to_string());
    assert_eq!(second.capture.source_captured_at, "2026-07-13T21:00:00Z");
    assert_eq!(second.capture.source_cursor.as_deref(), Some("2"));
    assert_eq!(second.capture.source_state_hash.as_deref(), Some("state-b"));
    assert_eq!(second.capture.source_agent_version.as_deref(), Some("1.0"));
    assert_eq!(second.capture.artifact_set_version, 1);
    assert_eq!(second.capture.munshi_version.as_deref(), Some("2.0"));
    assert!(!second.capture.server_received_at.is_empty());
    assert!(!second.capture.server_completed_at.is_empty());

    let (capture_status, _, capture_body) = call(
        service.router(&config),
        Request::builder()
            .uri(&second.capture.capture_url)
            .body(Body::empty())
            .expect("capture request is valid"),
    )
    .await;
    assert_eq!(capture_status, StatusCode::OK);
    let fetched: CaptureProvenance =
        serde_json::from_slice(&capture_body).expect("capture provenance parses");
    assert_eq!(fetched, second.capture);

    let (lookup_status, _, lookup_body) = call(
        service.router(&config),
        Request::builder()
            .uri(format!(
                "/api/v1/captures?client_id={second_client}&capture_id=capture-b"
            ))
            .body(Body::empty())
            .expect("capture lookup request is valid"),
    )
    .await;
    assert_eq!(lookup_status, StatusCode::OK);
    let looked_up: CaptureProvenance =
        serde_json::from_slice(&lookup_body).expect("capture lookup parses");
    assert_eq!(looked_up, second.capture);

    let (relation_status, _, relation_body) = call(
        service.router(&config),
        Request::builder()
            .uri(format!(
                "/api/v1/snapshots/{}/captures",
                first.receipt.snapshot_id
            ))
            .body(Body::empty())
            .expect("snapshot captures request is valid"),
    )
    .await;
    assert_eq!(relation_status, StatusCode::OK);
    let relation: SnapshotCapturesResponse =
        serde_json::from_slice(&relation_body).expect("snapshot captures parse");
    assert_eq!(relation.captures.len(), 2);
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
                "capture_id": key,
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

fn artifact_chunk_url(upload_id: &str, artifact_index: u32, chunk_index: u64) -> String {
    format!("/api/v1/uploads/{upload_id}/artifacts/{artifact_index}/chunks/{chunk_index}")
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

async fn upload_artifact_chunk(
    service: &Service,
    config: &Config,
    upload_id: &str,
    artifact_index: u32,
    chunk_index: u64,
    bytes: &[u8],
) -> StatusCode {
    let (status, _, _) = call(
        service.router(config),
        chunk_request(
            &artifact_chunk_url(upload_id, artifact_index, chunk_index),
            bytes,
        ),
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
    let completion: CompletionResponse =
        serde_json::from_slice(&receipt).expect("completion response parses");
    let receipt = completion.receipt;
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
    let completion: CompletionResponse =
        serde_json::from_slice(&receipt_bytes).expect("completion response parses");
    let receipt = completion.receipt;
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
    let audit: (String, String, i64, i64, i64, String) = sqlx::query_as(
        "SELECT terminal_reason, capture_id, declared_original_size_bytes, declared_stored_size_bytes,
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
    assert_eq!(audit.1, "abandon");
    assert_eq!(audit.2, 1024);
    assert_eq!(audit.3, 1024);
    assert_eq!(audit.4, 1);
    assert_eq!(audit.5, "upload_abandoned");
    let captures: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM captures")
        .fetch_one(&service.state.database)
        .await
        .expect("captures query succeeds");
    assert_eq!(captures.0, 0, "terminal uploads never become captures");

    let mut changed_manifest = manifest(&bytes, &bytes, Compression::Identity);
    changed_manifest["capture"]["source_cursor"] = serde_json::json!("replacement");
    let (reuse_conflict, _, reuse_body) = call(
        service.router(&config),
        json_request(
            "POST",
            "/api/v1/uploads",
            &serde_json::json!({
                "client_id": client_id.to_string(),
                "capture_id": "abandon",
                "manifest": changed_manifest
            }),
        ),
    )
    .await;
    assert_eq!(reuse_conflict, StatusCode::CONFLICT);
    assert!(
        String::from_utf8(reuse_body)
            .expect("error is text")
            .contains("capture_id_conflict")
    );
    let retry = create_upload(
        &service,
        &config,
        client_id,
        "abandon",
        manifest(&bytes, &bytes, Compression::Identity),
    )
    .await;
    assert_ne!(retry.upload_id, upload.upload_id);
}

/// The exact pre-provenance `Capture` shape: before `source_state_hash`,
/// `source_metadata`, and `artifact_set_version` were added. Mirrors the
/// server's own `LegacyCaptureV1`/`LegacyManifestV1` bridge shape so this
/// test can independently reproduce the digest a pre-upgrade server would
/// have stored, rather than relying on the server's bridge to grade itself.
#[derive(serde::Serialize)]
struct HistoricalCaptureShape<'a> {
    captured_at: &'a str,
    source_cursor: &'a Option<String>,
    project: &'a Option<String>,
    repository: &'a Option<String>,
    branch: &'a Option<String>,
    source_agent_version: &'a Option<String>,
    munshi_version: &'a Option<String>,
}

#[derive(serde::Serialize)]
struct HistoricalManifestShape<'a> {
    schema_version: u16,
    session: &'a SessionInput,
    capture: HistoricalCaptureShape<'a>,
    artifacts: &'a Vec<Artifact>,
}

fn historical_pre_provenance_manifest_sha256(document: &serde_json::Value) -> String {
    let typed: Manifest =
        serde_json::from_value(document.clone()).expect("manifest parses into canonical shape");
    let historical = HistoricalManifestShape {
        schema_version: typed.schema_version,
        session: &typed.session,
        capture: HistoricalCaptureShape {
            captured_at: &typed.capture.captured_at,
            source_cursor: &typed.capture.source_cursor,
            project: &typed.capture.project,
            repository: &typed.capture.repository,
            branch: &typed.capture.branch,
            source_agent_version: &typed.capture.source_agent_version,
            munshi_version: &typed.capture.munshi_version,
        },
        artifacts: &typed.artifacts,
    };
    let historical_json =
        serde_json::to_vec(&historical).expect("historical manifest shape serializes");
    digest(&historical_json)
        .strip_prefix("sha256:")
        .expect("digest has prefix")
        .to_owned()
}

#[allow(clippy::too_many_lines)]
#[tokio::test]
async fn terminal_audit_reuse_bridges_pre_provenance_legacy_manifest_digest() {
    let data_dir = TestDataDir::new();
    let config = test_config(&data_dir);
    let (service, _) = Service::bootstrap(&config).await.expect("bootstraps");
    let client_id = Uuid::new_v4();
    register(service.router(&config), client_id).await;
    let bytes = b"legacy terminal audit retry";
    let capture_id = "legacy-terminal-retry";
    let document = manifest(bytes, bytes, Compression::Identity);
    let upload = create_upload(&service, &config, client_id, capture_id, document.clone()).await;

    // Simulate an active upload created by a pre-provenance server: its
    // stored `manifest_sha256` is the digest of the `Capture` shape before
    // `source_state_hash`, `source_metadata`, and `artifact_set_version`
    // existed, not the digest the current schema would compute.
    let historical_hash = historical_pre_provenance_manifest_sha256(&document);
    assert_ne!(
        historical_hash, upload.manifest_sha256,
        "the simulated legacy digest must differ from the current canonical digest"
    );
    sqlx::query("UPDATE uploads SET manifest_sha256 = ?1 WHERE id = ?2")
        .bind(&historical_hash)
        .bind(&upload.upload_id)
        .execute(&service.state.database)
        .await
        .expect("simulate pre-provenance active upload manifest digest");

    // Terminalize the simulated legacy-format active upload.
    let (abandon_status, _, _) = call(
        service.router(&config),
        Request::builder()
            .method("POST")
            .uri(&upload.abandon_url)
            .body(Body::empty())
            .expect("abandon request is valid"),
    )
    .await;
    assert_eq!(abandon_status, StatusCode::OK);

    let audited_digest: (Option<String>,) =
        sqlx::query_as("SELECT manifest_sha256 FROM upload_audits WHERE upload_id = ?1")
            .bind(&upload.upload_id)
            .fetch_one(&service.state.database)
            .await
            .expect("audit query succeeds");
    assert_eq!(
        audited_digest.0.as_deref(),
        Some(historical_hash.as_str()),
        "terminal audit retains the pre-upgrade digest shape"
    );

    // A retry with the exact same semantic manifest succeeds despite the
    // schema-shape mismatch, because it bridges to the recognized legacy
    // digest rather than comparing raw digests only.
    let retried = create_upload(
        &service,
        &config,
        client_id,
        capture_id,
        manifest(bytes, bytes, Compression::Identity),
    )
    .await;
    assert_ne!(retried.upload_id, upload.upload_id);

    // Changing a stable field still conflicts: the legacy bridge cannot be
    // used to smuggle a semantically different capture past the same
    // capture ID.
    let mut changed_field = manifest(bytes, bytes, Compression::Identity);
    changed_field["capture"]["branch"] = serde_json::json!("release");
    let (field_conflict, _, field_conflict_body) = call(
        service.router(&config),
        json_request(
            "POST",
            "/api/v1/uploads",
            &serde_json::json!({
                "client_id": client_id.to_string(),
                "capture_id": capture_id,
                "manifest": changed_field
            }),
        ),
    )
    .await;
    assert_eq!(field_conflict, StatusCode::CONFLICT);
    assert!(
        String::from_utf8(field_conflict_body)
            .expect("error is text")
            .contains("capture_id_conflict")
    );

    // Changing artifact content still conflicts too.
    let other_bytes = b"different legacy terminal audit retry content";
    let (content_conflict, _, content_conflict_body) = call(
        service.router(&config),
        json_request(
            "POST",
            "/api/v1/uploads",
            &serde_json::json!({
                "client_id": client_id.to_string(),
                "capture_id": capture_id,
                "manifest": manifest(other_bytes, other_bytes, Compression::Identity)
            }),
        ),
    )
    .await;
    assert_eq!(content_conflict, StatusCode::CONFLICT);
    assert!(
        String::from_utf8(content_conflict_body)
            .expect("error is text")
            .contains("capture_id_conflict")
    );

    // The audit row remains compact and redacted: no manifest, path, or
    // chunk detail persists alongside the digest for either terminalized
    // upload.
    let redacted: (i64, i64) = sqlx::query_as(
        "SELECT
            (SELECT COUNT(*) FROM manifests WHERE upload_id = ?1),
            (SELECT COUNT(*) FROM upload_chunks WHERE upload_id = ?1)",
    )
    .bind(&upload.upload_id)
    .fetch_one(&service.state.database)
    .await
    .expect("audit redaction query succeeds");
    assert_eq!(redacted.0, 0, "terminal audits retain no manifest rows");
    assert_eq!(redacted.1, 0, "terminal audits retain no chunk rows");
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
    complete_upload_for_completion(service, config, completion_url)
        .await
        .receipt
}

async fn complete_upload_for_completion(
    service: &Service,
    config: &Config,
    completion_url: &str,
) -> CompletionResponse {
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
    serde_json::from_slice(&body).expect("completion response parses")
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
        multi_manifest(
            vec![("events.jsonl", &original, &zstd_stored, Compression::Zstd)],
            "source-session-1",
        ),
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

#[tokio::test]
async fn conflicting_same_digest_manifest_metadata_is_an_integrity_error() {
    let data_dir = TestDataDir::new();
    let config = test_config(&data_dir);
    let (service, _) = Service::bootstrap(&config).await.expect("bootstraps");
    let client_id = Uuid::new_v4();
    register(service.router(&config), client_id).await;
    let bytes = b"same digest";
    let mut document = multi_manifest(
        vec![
            ("one.bin", bytes, bytes, Compression::Identity),
            ("two.bin", bytes, bytes, Compression::Identity),
        ],
        "same-digest-conflict",
    );
    document["artifacts"][1]["compression"] = serde_json::json!("zstd");
    let (status, _, body) = call(
        service.router(&config),
        json_request(
            "POST",
            "/api/v1/uploads",
            &serde_json::json!({
                "client_id": client_id.to_string(),
                "capture_id": "same-digest-conflict",
                "manifest": document
            }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert!(
        String::from_utf8(body)
            .expect("error is text")
            .contains("blob_integrity_conflict")
    );
}

#[tokio::test]
async fn conflicting_existing_blob_metadata_is_an_integrity_error_not_a_dedup_hit() {
    let data_dir = TestDataDir::new();
    let config = test_config(&data_dir);
    let (service, _) = Service::bootstrap(&config).await.expect("bootstraps");
    let client_id = Uuid::new_v4();
    register(service.router(&config), client_id).await;
    let bytes = b"blob integrity fixture";

    let first_upload = upload_full(
        &service,
        &config,
        client_id,
        "blob-integrity-first",
        manifest(bytes, bytes, Compression::Identity),
        bytes,
    )
    .await;
    let _first =
        complete_upload_for_completion(&service, &config, &first_upload.completion_url).await;

    sqlx::query("UPDATE blobs SET compression = 'zstd'")
        .execute(&service.state.database)
        .await
        .expect("test can induce metadata conflict");
    let compression_upload = upload_full(
        &service,
        &config,
        client_id,
        "blob-integrity-compression",
        manifest(bytes, bytes, Compression::Identity),
        bytes,
    )
    .await;
    let (compression_status, _, compression_body) = call(
        service.router(&config),
        Request::builder()
            .method("POST")
            .uri(&compression_upload.completion_url)
            .body(Body::empty())
            .expect("completion request is valid"),
    )
    .await;
    assert_eq!(compression_status, StatusCode::CONFLICT);
    assert!(
        String::from_utf8(compression_body)
            .expect("error is text")
            .contains("blob_integrity_conflict")
    );

    sqlx::query("UPDATE blobs SET compression = 'identity', stored_size_bytes = 0")
        .execute(&service.state.database)
        .await
        .expect("test can induce size conflict");
    let size_upload = upload_full(
        &service,
        &config,
        client_id,
        "blob-integrity-size",
        manifest(bytes, bytes, Compression::Identity),
        bytes,
    )
    .await;
    let (size_status, _, size_body) = call(
        service.router(&config),
        Request::builder()
            .method("POST")
            .uri(&size_upload.completion_url)
            .body(Body::empty())
            .expect("completion request is valid"),
    )
    .await;
    assert_eq!(size_status, StatusCode::CONFLICT);
    assert!(
        String::from_utf8(size_body)
            .expect("error is text")
            .contains("blob_integrity_conflict")
    );
    assert_eq!(
        snapshot_count(&service).await,
        1,
        "a metadata conflict cannot be treated as an existing snapshot"
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
    capture_id: &str,
    manifest_document: serde_json::Value,
    stored_bytes: &[u8],
) -> UploadResponse {
    let upload = create_upload(service, config, client_id, capture_id, manifest_document).await;
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
    let b_completion: CompletionResponse =
        serde_json::from_slice(&b_body).expect("b completion parses");
    let d_completion: CompletionResponse =
        serde_json::from_slice(&d_body).expect("d completion parses");
    let b_receipt = b_completion.receipt;
    let d_receipt = d_completion.receipt;

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
                "capture_id": "too-large",
                "manifest": manifest(&bytes, &bytes, Compression::Identity)
            }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
}

#[tokio::test]
async fn declared_snapshot_count_and_aggregate_limits_are_enforced_before_upload() {
    let data_dir = TestDataDir::new();
    let mut config = test_config(&data_dir);
    config.max_artifact_count = 2;
    config.max_snapshot_stored_bytes = 1_500;
    config.max_snapshot_original_bytes = 1_500;
    let (service, _) = Service::bootstrap(&config).await.expect("bootstraps");
    let client_id = Uuid::new_v4();
    register(service.router(&config), client_id).await;
    let bytes = vec![b'a'; 1_000];
    let count_document = multi_manifest(
        vec![
            ("count-a.bin", &bytes, &bytes, Compression::Identity),
            ("count-b.bin", &bytes, &bytes, Compression::Identity),
            ("count-c.bin", &bytes, &bytes, Compression::Identity),
        ],
        "aggregate-limits",
    );
    let (count_status, _, _) = call(
        service.router(&config),
        json_request(
            "POST",
            "/api/v1/uploads",
            &serde_json::json!({
                "client_id": client_id.to_string(),
                "capture_id": "count-too-large",
                "manifest": count_document
            }),
        ),
    )
    .await;
    assert_eq!(count_status, StatusCode::UNPROCESSABLE_ENTITY);

    let aggregate_document = multi_manifest(
        vec![
            ("one.bin", &bytes, &bytes, Compression::Identity),
            ("two.bin", &bytes, &bytes, Compression::Identity),
        ],
        "aggregate-limits",
    );
    let (status, _, _) = call(
        service.router(&config),
        json_request(
            "POST",
            "/api/v1/uploads",
            &serde_json::json!({
                "client_id": client_id.to_string(),
                "capture_id": "aggregate-too-large",
                "manifest": aggregate_document
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

#[tokio::test]
async fn multi_artifact_upload_canonicalizes_paths_and_verifies_mixed_encodings() {
    let data_dir = TestDataDir::new();
    let config = test_config(&data_dir);
    let (service, _) = Service::bootstrap(&config).await.expect("bootstraps");
    let client_id = Uuid::new_v4();
    register(service.router(&config), client_id).await;

    let identity = vec![b'i'; 1_500];
    let original_zstd = (0_u32..2_700)
        .map(|value| {
            let mixed = value.wrapping_mul(1_103_515_245).wrapping_add(12_345);
            mixed.to_le_bytes()[2]
        })
        .collect::<Vec<_>>();
    let zstd = zstd::stream::encode_all(&original_zstd[..], 1).expect("compresses");
    assert!(zstd.len() > 1_024, "fixture spans more than one zstd chunk");
    let upload = create_upload(
        &service,
        &config,
        client_id,
        "multi-mixed",
        multi_manifest(
            vec![
                ("z-events.bin", &original_zstd, &zstd, Compression::Zstd),
                ("a-index.bin", &identity, &identity, Compression::Identity),
            ],
            "multi-mixed",
        ),
    )
    .await;

    assert_eq!(upload.artifacts.len(), 2);
    assert_eq!(upload.artifacts[0].artifact_index, 0);
    assert_eq!(upload.artifacts[0].logical_path, "a-index.bin");
    assert_eq!(
        upload.artifacts[0].original_sha256,
        digest(&identity),
        "per-artifact status exposes the declared original identity"
    );
    assert_eq!(upload.artifacts[0].compression, Compression::Identity);
    assert_eq!(upload.artifacts[0].chunk_count, 2);
    assert_eq!(upload.artifacts[1].artifact_index, 1);
    assert_eq!(upload.artifacts[1].logical_path, "z-events.bin");
    assert_eq!(upload.artifacts[1].stored_sha256, digest(&zstd));
    assert_eq!(upload.artifacts[1].compression, Compression::Zstd);
    assert!(upload.artifacts[1].chunk_count > 1);
    assert!(
        upload.artifacts[1]
            .chunk_upload_url
            .contains("/artifacts/1/chunks/{chunk_index}")
    );

    for (index, bytes) in identity.chunks(1_024).enumerate() {
        assert_eq!(
            upload_artifact_chunk(&service, &config, &upload.upload_id, 0, index as u64, bytes)
                .await,
            StatusCode::NO_CONTENT
        );
    }
    for (index, bytes) in zstd.chunks(1_024).enumerate() {
        assert_eq!(
            upload_artifact_chunk(&service, &config, &upload.upload_id, 1, index as u64, bytes)
                .await,
            StatusCode::NO_CONTENT
        );
    }
    let completion =
        complete_upload_for_completion(&service, &config, &upload.completion_url).await;
    let receipt = completion.receipt;
    assert_eq!(receipt.artifact_count, 2);
    assert_eq!(
        receipt.total_original_bytes,
        (identity.len() + original_zstd.len()) as u64
    );
    assert_eq!(
        receipt.total_stored_bytes,
        (identity.len() + zstd.len()) as u64
    );
    assert_eq!(
        completion.transfer.upload_transfer_bytes,
        receipt.total_stored_bytes
    );
    assert_eq!(
        completion.transfer.newly_persisted_physical_bytes,
        receipt.total_stored_bytes
    );

    let snapshot = fetch_snapshot(&service, &config, &receipt.snapshot_id).await;
    assert_eq!(snapshot.artifact_count, 2);
    assert_eq!(
        snapshot
            .manifest
            .artifacts
            .iter()
            .map(|artifact| artifact.logical_path.as_str())
            .collect::<Vec<_>>(),
        vec!["a-index.bin", "z-events.bin"]
    );
    assert_eq!(snapshot.artifacts.len(), 2);
    assert_eq!(snapshot.artifacts[0].artifact_index, 0);
    assert_eq!(snapshot.artifacts[1].artifact_index, 1);
}

#[tokio::test]
async fn incomplete_or_corrupt_member_never_creates_a_partial_snapshot() {
    let data_dir = TestDataDir::new();
    let config = test_config(&data_dir);
    let (service, _) = Service::bootstrap(&config).await.expect("bootstraps");
    let client_id = Uuid::new_v4();
    register(service.router(&config), client_id).await;
    let first = vec![b'a'; 1_024];
    let second = b"second artifact bytes".to_vec();
    let upload = create_upload(
        &service,
        &config,
        client_id,
        "no-partial-snapshot",
        multi_manifest(
            vec![
                ("first.bin", &first, &first, Compression::Identity),
                ("second.bin", &second, &second, Compression::Identity),
            ],
            "no-partial-snapshot",
        ),
    )
    .await;

    assert_eq!(
        upload_artifact_chunk(&service, &config, &upload.upload_id, 0, 0, &first).await,
        StatusCode::NO_CONTENT
    );
    let (missing_status, _, _) = call(
        service.router(&config),
        Request::builder()
            .method("POST")
            .uri(&upload.completion_url)
            .body(Body::empty())
            .expect("completion request is valid"),
    )
    .await;
    assert_eq!(missing_status, StatusCode::CONFLICT);
    assert_eq!(snapshot_count(&service).await, 0);

    assert_eq!(
        upload_artifact_chunk(&service, &config, &upload.upload_id, 1, 0, &second).await,
        StatusCode::NO_CONTENT
    );
    let second_chunk = service
        .state
        .storage
        .artifact_chunk_path(&upload.upload_id, 1, 0);
    stdfs::write(&second_chunk, vec![b'x'; second.len()]).expect("test can corrupt chunk");
    let (corrupt_status, _, _) = call(
        service.router(&config),
        Request::builder()
            .method("POST")
            .uri(&upload.completion_url)
            .body(Body::empty())
            .expect("completion request is valid"),
    )
    .await;
    assert_eq!(corrupt_status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(snapshot_count(&service).await, 0);
}

#[tokio::test]
async fn rejects_duplicate_and_unsafe_multi_artifact_paths_and_object_kinds() {
    let data_dir = TestDataDir::new();
    let config = test_config(&data_dir);
    let (service, _) = Service::bootstrap(&config).await.expect("bootstraps");
    let client_id = Uuid::new_v4();
    register(service.router(&config), client_id).await;
    let bytes = b"path fixture";

    let mut duplicate = multi_manifest(
        vec![
            ("Alpha.txt", bytes, bytes, Compression::Identity),
            ("alpha.txt", bytes, bytes, Compression::Identity),
        ],
        "unsafe-paths",
    );
    let mut file_tree_conflict = multi_manifest(
        vec![
            ("node", bytes, bytes, Compression::Identity),
            ("node/child", bytes, bytes, Compression::Identity),
        ],
        "unsafe-paths",
    );
    let mut traversal = multi_manifest(
        vec![("safe.txt", bytes, bytes, Compression::Identity)],
        "unsafe-paths",
    );
    traversal["artifacts"][0]["logical_path"] = serde_json::json!("../escape");
    let mut drive = multi_manifest(
        vec![("safe.txt", bytes, bytes, Compression::Identity)],
        "unsafe-paths",
    );
    drive["artifacts"][0]["logical_path"] = serde_json::json!("C:drive.txt");
    let mut reserved_device = multi_manifest(
        vec![("safe.txt", bytes, bytes, Compression::Identity)],
        "unsafe-paths",
    );
    reserved_device["artifacts"][0]["logical_path"] = serde_json::json!("CON.txt");
    let mut symlink_kind = multi_manifest(
        vec![("safe.txt", bytes, bytes, Compression::Identity)],
        "unsafe-paths",
    );
    symlink_kind["artifacts"][0]["kind"] = serde_json::json!("symlink");

    for (key, document) in [
        ("duplicate-portable-path", &mut duplicate),
        ("file-tree-conflict", &mut file_tree_conflict),
        ("traversal-path", &mut traversal),
        ("drive-path", &mut drive),
        ("reserved-device-path", &mut reserved_device),
        ("symlink-kind", &mut symlink_kind),
    ] {
        let (status, _, _) = call(
            service.router(&config),
            json_request(
                "POST",
                "/api/v1/uploads",
                &serde_json::json!({
                    "client_id": client_id.to_string(),
                    "capture_id": key,
                    "manifest": document
                }),
            ),
        )
        .await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    }
}

#[tokio::test]
async fn restart_resumes_each_artifact_independently() {
    let data_dir = TestDataDir::new();
    let config = test_config(&data_dir);
    let (service, _) = Service::bootstrap(&config).await.expect("bootstraps");
    let client_id = Uuid::new_v4();
    register(service.router(&config), client_id).await;
    let first = vec![b'a'; 1_500];
    let second = vec![b'b'; 1_500];
    let upload = create_upload(
        &service,
        &config,
        client_id,
        "restart-per-artifact",
        multi_manifest(
            vec![
                ("first.bin", &first, &first, Compression::Identity),
                ("second.bin", &second, &second, Compression::Identity),
            ],
            "restart-per-artifact",
        ),
    )
    .await;
    assert_eq!(
        upload_artifact_chunk(&service, &config, &upload.upload_id, 0, 0, &first[..1024]).await,
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        upload_artifact_chunk(&service, &config, &upload.upload_id, 1, 0, &second[..1024]).await,
        StatusCode::NO_CONTENT
    );
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
    assert_eq!(resumed.artifacts.len(), 2);
    assert_eq!(resumed.artifacts[0].accepted_chunk_bitmap, "01");
    assert_eq!(resumed.artifacts[1].accepted_chunk_bitmap, "01");

    assert_eq!(
        upload_artifact_chunk(&restarted, &config, &upload.upload_id, 0, 1, &first[1024..]).await,
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        upload_artifact_chunk(
            &restarted,
            &config,
            &upload.upload_id,
            1,
            1,
            &second[1024..]
        )
        .await,
        StatusCode::NO_CONTENT
    );
    let receipt = complete_upload_for_receipt(&restarted, &config, &upload.completion_url).await;
    assert_eq!(receipt.artifact_count, 2);
}

#[allow(clippy::too_many_lines)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_multi_artifact_completion_creates_one_complete_snapshot() {
    let data_dir = TestDataDir::new();
    let config = test_config(&data_dir);
    let (service, _) = Service::bootstrap(&config).await.expect("bootstraps");
    let first_client_id = Uuid::new_v4();
    let second_client_id = Uuid::new_v4();
    register(service.router(&config), first_client_id).await;
    register(service.router(&config), second_client_id).await;
    let first = vec![b'a'; 1_100];
    let second = vec![b'b'; 1_200];
    let first_manifest = multi_manifest(
        vec![
            ("b.bin", &second, &second, Compression::Identity),
            ("a.bin", &first, &first, Compression::Identity),
        ],
        "concurrent-multi",
    );
    let second_manifest = multi_manifest(
        vec![
            ("a.bin", &first, &first, Compression::Identity),
            ("b.bin", &second, &second, Compression::Identity),
        ],
        "concurrent-multi",
    );
    let first_upload = create_upload(
        &service,
        &config,
        first_client_id,
        "concurrent-multi-a",
        first_manifest,
    )
    .await;
    let second_upload = create_upload(
        &service,
        &config,
        second_client_id,
        "concurrent-multi-b",
        second_manifest,
    )
    .await;

    for upload in [&first_upload, &second_upload] {
        for (artifact_index, bytes) in [(0_u32, &first), (1_u32, &second)] {
            for (chunk_index, chunk) in bytes.chunks(1024).enumerate() {
                assert_eq!(
                    upload_artifact_chunk(
                        &service,
                        &config,
                        &upload.upload_id,
                        artifact_index,
                        chunk_index as u64,
                        chunk
                    )
                    .await,
                    StatusCode::NO_CONTENT
                );
            }
        }
    }

    let first_app = service.router(&config);
    let first_url = first_upload.completion_url.clone();
    let first_task = tokio::spawn(async move {
        call(
            first_app,
            Request::builder()
                .method("POST")
                .uri(first_url)
                .body(Body::empty())
                .expect("completion request is valid"),
        )
        .await
    });
    let second_app = service.router(&config);
    let second_url = second_upload.completion_url.clone();
    let second_task = tokio::spawn(async move {
        call(
            second_app,
            Request::builder()
                .method("POST")
                .uri(second_url)
                .body(Body::empty())
                .expect("completion request is valid"),
        )
        .await
    });
    let (first_status, _, first_body) = first_task.await.expect("first task completes");
    let (second_status, _, second_body) = second_task.await.expect("second task completes");
    assert_eq!(first_status, StatusCode::OK);
    assert_eq!(second_status, StatusCode::OK);
    let first_completion: CompletionResponse =
        serde_json::from_slice(&first_body).expect("first completion parses");
    let second_completion: CompletionResponse =
        serde_json::from_slice(&second_body).expect("second completion parses");
    let first_receipt = first_completion.receipt;
    let second_receipt = second_completion.receipt;
    assert_eq!(first_receipt.snapshot_id, second_receipt.snapshot_id);
    assert_eq!(
        serde_json::to_vec(&first_receipt).expect("receipt serializes"),
        serde_json::to_vec(&second_receipt).expect("receipt serializes"),
        "concurrent duplicate captures return the same archival receipt"
    );
    assert_eq!(
        first_completion.capture.client_id,
        first_client_id.to_string()
    );
    assert_eq!(
        second_completion.capture.client_id,
        second_client_id.to_string()
    );
    assert_eq!(snapshot_count(&service).await, 1);
    let snapshot = fetch_snapshot(&service, &config, &first_receipt.snapshot_id).await;
    assert_eq!(snapshot.artifacts.len(), 2);
    assert_eq!(
        first_completion.transfer.newly_persisted_physical_bytes
            + second_completion.transfer.newly_persisted_physical_bytes,
        (first.len() + second.len()) as u64
    );
    let captures: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM captures WHERE snapshot_id = ?1")
        .bind(&first_receipt.snapshot_id)
        .fetch_one(&service.state.database)
        .await
        .expect("captures query succeeds");
    assert_eq!(captures.0, 2);
}

#[allow(clippy::too_many_lines)]
#[tokio::test]
async fn shared_blobs_and_alternate_representations_report_distinct_metrics() {
    let data_dir = TestDataDir::new();
    let config = test_config(&data_dir);
    let (service, _) = Service::bootstrap(&config).await.expect("bootstraps");
    let client_id = Uuid::new_v4();
    register(service.router(&config), client_id).await;
    let original = b"shared original artifact\n".repeat(300);
    let first_upload = create_upload(
        &service,
        &config,
        client_id,
        "shared-identity",
        multi_manifest(
            vec![
                ("a.txt", &original, &original, Compression::Identity),
                ("b.txt", &original, &original, Compression::Identity),
            ],
            "shared-and-dedup",
        ),
    )
    .await;
    for artifact_index in 0..2 {
        for (chunk_index, bytes) in original.chunks(1024).enumerate() {
            assert_eq!(
                upload_artifact_chunk(
                    &service,
                    &config,
                    &first_upload.upload_id,
                    artifact_index,
                    chunk_index as u64,
                    bytes
                )
                .await,
                StatusCode::NO_CONTENT
            );
        }
    }
    let first_completion =
        complete_upload_for_completion(&service, &config, &first_upload.completion_url).await;
    let first_receipt = first_completion.receipt;
    assert_eq!(first_receipt.artifact_count, 2);
    assert_eq!(
        first_receipt.total_stored_bytes,
        (original.len() * 2) as u64
    );
    assert_eq!(
        first_completion.transfer.upload_transfer_bytes,
        (original.len() * 2) as u64
    );
    assert_eq!(
        first_completion.transfer.newly_persisted_physical_bytes,
        original.len() as u64
    );
    let blob_count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM blobs")
        .fetch_one(&service.state.database)
        .await
        .expect("blob query succeeds");
    assert_eq!(blob_count.0, 1);

    let zstd = zstd::stream::encode_all(&original[..], 1).expect("compresses");
    let alternate_upload = create_upload(
        &service,
        &config,
        client_id,
        "shared-zstd",
        multi_manifest(
            vec![
                ("a.txt", &original, &zstd, Compression::Zstd),
                ("b.txt", &original, &zstd, Compression::Zstd),
            ],
            "shared-and-dedup",
        ),
    )
    .await;
    for artifact_index in 0..2 {
        for (chunk_index, bytes) in zstd.chunks(1024).enumerate() {
            assert_eq!(
                upload_artifact_chunk(
                    &service,
                    &config,
                    &alternate_upload.upload_id,
                    artifact_index,
                    chunk_index as u64,
                    bytes
                )
                .await,
                StatusCode::NO_CONTENT
            );
        }
    }
    let alternate_completion =
        complete_upload_for_completion(&service, &config, &alternate_upload.completion_url).await;
    let alternate_receipt = alternate_completion.receipt;
    assert_eq!(alternate_receipt.snapshot_id, first_receipt.snapshot_id);
    assert_eq!(
        alternate_completion.transfer.upload_transfer_bytes,
        (zstd.len() * 2) as u64
    );
    assert_eq!(
        alternate_completion.transfer.newly_persisted_physical_bytes,
        0
    );
    let blobs_after: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM blobs")
        .fetch_one(&service.state.database)
        .await
        .expect("blob query succeeds");
    assert_eq!(blobs_after.0, 1);
}

#[tokio::test]
async fn reconciliation_accepts_canonical_rows_and_detects_induced_drift() {
    let data_dir = TestDataDir::new();
    let config = test_config(&data_dir);
    let (service, _) = Service::bootstrap(&config).await.expect("bootstraps");
    let client_id = Uuid::new_v4();
    register(service.router(&config), client_id).await;
    let first = b"first";
    let second = b"second";
    let upload = create_upload(
        &service,
        &config,
        client_id,
        "reconcile",
        multi_manifest(
            vec![
                ("first.txt", first, first, Compression::Identity),
                ("second.txt", second, second, Compression::Identity),
            ],
            "reconcile",
        ),
    )
    .await;
    assert_eq!(
        upload_artifact_chunk(&service, &config, &upload.upload_id, 0, 0, first).await,
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        upload_artifact_chunk(&service, &config, &upload.upload_id, 1, 0, second).await,
        StatusCode::NO_CONTENT
    );
    let receipt = complete_upload_for_receipt(&service, &config, &upload.completion_url).await;
    assert!(
        service
            .reconcile_snapshot(&receipt.snapshot_id)
            .await
            .is_ok()
    );
    sqlx::query(
        "UPDATE artifacts SET logical_path = 'drift.txt'
         WHERE snapshot_id = ?1 AND artifact_index = 0",
    )
    .bind(&receipt.snapshot_id)
    .execute(&service.state.database)
    .await
    .expect("induce normalized-row drift");
    assert!(matches!(
        service.reconcile_snapshot(&receipt.snapshot_id).await,
        Err(ReconciliationError::Drift)
    ));
}

#[tokio::test]
async fn legacy_singleton_manifest_rows_remain_readable_but_new_rows_are_canonical_arrays() {
    let data_dir = TestDataDir::new();
    let config = test_config(&data_dir);
    let (service, _) = Service::bootstrap(&config).await.expect("bootstraps");
    let client_id = Uuid::new_v4();
    register(service.router(&config), client_id).await;
    let bytes = b"legacy manifest compatibility";
    let upload = create_upload(
        &service,
        &config,
        client_id,
        "legacy-manifest-row",
        manifest(bytes, bytes, Compression::Identity),
    )
    .await;
    let canonical_json: (String,) =
        sqlx::query_as("SELECT canonical_json FROM manifests WHERE upload_id = ?1")
            .bind(&upload.upload_id)
            .fetch_one(&service.state.database)
            .await
            .expect("canonical manifest query succeeds");
    let canonical: serde_json::Value =
        serde_json::from_str(&canonical_json.0).expect("canonical manifest parses");
    assert!(canonical.get("artifacts").is_some());
    assert!(canonical.get("artifact").is_none());

    assert_eq!(
        upload_artifact_chunk(&service, &config, &upload.upload_id, 0, 0, bytes).await,
        StatusCode::NO_CONTENT
    );
    let receipt = complete_upload_for_receipt(&service, &config, &upload.completion_url).await;

    // Simulate the raw singleton JSON and digest retained by a pre-issue-4
    // completed database. It must deserialize and reconcile without turning
    // its normalized artifact projection into an unreadable snapshot.
    let historical_json =
        serde_json::to_string(&manifest(bytes, bytes, Compression::Identity)).expect("serializes");
    let historical_hash = digest(historical_json.as_bytes())
        .strip_prefix("sha256:")
        .expect("digest has prefix")
        .to_owned();
    sqlx::query(
        "UPDATE manifests SET canonical_json = ?1, sha256 = ?2
         WHERE id = (SELECT manifest_id FROM snapshots WHERE id = ?3)",
    )
    .bind(historical_json)
    .bind(historical_hash)
    .bind(&receipt.snapshot_id)
    .execute(&service.state.database)
    .await
    .expect("simulate historical manifest row");
    sqlx::query("DELETE FROM upload_artifacts WHERE upload_id = ?1")
        .bind(&upload.upload_id)
        .execute(&service.state.database)
        .await
        .expect("simulate a pre-multi-artifact upload projection");
    drop(service);

    let (restarted, _) = Service::bootstrap(&config)
        .await
        .expect("migrates legacy projection");
    let projected: (i64,) =
        sqlx::query_as("SELECT COUNT(*) FROM upload_artifacts WHERE upload_id = ?1")
            .bind(&upload.upload_id)
            .fetch_one(&restarted.state.database)
            .await
            .expect("projection query succeeds");
    assert_eq!(projected.0, 1);
    let snapshot = fetch_snapshot(&restarted, &config, &receipt.snapshot_id).await;
    assert_eq!(snapshot.manifest.artifacts.len(), 1);
    assert!(
        restarted
            .reconcile_snapshot(&receipt.snapshot_id)
            .await
            .is_ok()
    );
}

#[tokio::test]
async fn restart_backfills_completed_capture_provenance_and_reissues_receipt() {
    let data_dir = TestDataDir::new();
    let config = test_config(&data_dir);
    let (service, _) = Service::bootstrap(&config).await.expect("bootstraps");
    let client_id = Uuid::new_v4();
    register(service.router(&config), client_id).await;
    let bytes = b"historical capture provenance";
    let upload = upload_full(
        &service,
        &config,
        client_id,
        "historical-capture",
        manifest(bytes, bytes, Compression::Identity),
        bytes,
    )
    .await;
    let original = complete_upload_for_completion(&service, &config, &upload.completion_url).await;

    let mut historical_manifest = manifest(bytes, bytes, Compression::Identity);
    historical_manifest["capture"]
        .as_object_mut()
        .expect("capture is an object")
        .remove("artifact_set_version");
    let historical_json =
        serde_json::to_string(&historical_manifest).expect("historical manifest serializes");
    let historical_hash = digest(historical_json.as_bytes())
        .strip_prefix("sha256:")
        .expect("digest has prefix")
        .to_owned();
    sqlx::query(
        "UPDATE manifests SET canonical_json = ?1, sha256 = ?2
         WHERE upload_id = ?3",
    )
    .bind(historical_json)
    .bind(historical_hash)
    .bind(&upload.upload_id)
    .execute(&service.state.database)
    .await
    .expect("simulate historical manifest");
    sqlx::query("DELETE FROM captures WHERE upload_id = ?1")
        .bind(&upload.upload_id)
        .execute(&service.state.database)
        .await
        .expect("simulate pre-provenance archive");
    sqlx::query("UPDATE snapshots SET fingerprint_version = 0 WHERE id = ?1")
        .bind(&original.receipt.snapshot_id)
        .execute(&service.state.database)
        .await
        .expect("simulate pre-versioned fingerprint");
    drop(service);

    let (restarted, _) = Service::bootstrap(&config)
        .await
        .expect("backfills historical capture");
    let (capture_status, _, capture_body) = call(
        restarted.router(&config),
        Request::builder()
            .uri(&upload.capture_url)
            .body(Body::empty())
            .expect("capture request is valid"),
    )
    .await;
    assert_eq!(capture_status, StatusCode::OK);
    let capture: CaptureProvenance =
        serde_json::from_slice(&capture_body).expect("capture provenance parses");
    assert_eq!(capture.capture_id, "historical-capture");
    assert_eq!(capture.artifact_set_version, 1);
    assert_eq!(capture.snapshot_id, original.receipt.snapshot_id);
    let fingerprint_version: (i64,) =
        sqlx::query_as("SELECT fingerprint_version FROM snapshots WHERE id = ?1")
            .bind(&original.receipt.snapshot_id)
            .fetch_one(&restarted.state.database)
            .await
            .expect("fingerprint version query succeeds");
    assert_eq!(fingerprint_version.0, 1);

    let reissued =
        complete_upload_for_completion(&restarted, &config, &upload.completion_url).await;
    assert_eq!(reissued.receipt.snapshot_id, original.receipt.snapshot_id);
    assert_eq!(reissued.receipt.manifest_sha256, capture.manifest_sha256);
}

#[allow(clippy::too_many_lines)]
#[tokio::test]
async fn archive_browsing_projects_latest_context_without_rewriting_history() {
    let data_dir = TestDataDir::new();
    let config = test_config(&data_dir);
    let (service, _) = Service::bootstrap(&config).await.expect("bootstraps");
    let first_client = Uuid::new_v4();
    let second_client = Uuid::new_v4();
    register(service.router(&config), first_client).await;
    register(service.router(&config), second_client).await;

    let old_bytes = b"old branch capture";
    let old_upload = upload_full(
        &service,
        &config,
        first_client,
        "moved-branch-old",
        manifest_with_context(
            old_bytes,
            old_bytes,
            "moved-branch-session",
            "archive-project",
            "example/archive",
            "main",
            serde_json::json!({"opaque": "old-source-value"}),
        ),
        old_bytes,
    )
    .await;
    let old_completion =
        complete_upload_for_completion(&service, &config, &old_upload.completion_url).await;

    let new_bytes = b"release branch capture";
    let latest_upload = upload_full(
        &service,
        &config,
        first_client,
        "moved-branch-latest",
        manifest_with_context(
            new_bytes,
            new_bytes,
            "moved-branch-session",
            "archive-project",
            "example/archive",
            "release",
            serde_json::json!({"opaque": "first-release-observation"}),
        ),
        new_bytes,
    )
    .await;
    let latest_completion =
        complete_upload_for_completion(&service, &config, &latest_upload.completion_url).await;

    let coalesced_upload = upload_full(
        &service,
        &config,
        second_client,
        "moved-branch-coalesced",
        manifest_with_context(
            new_bytes,
            new_bytes,
            "moved-branch-session",
            "archive-project",
            "example/archive",
            "release",
            serde_json::json!({"opaque": "second-client-private-context"}),
        ),
        new_bytes,
    )
    .await;
    let coalesced_completion =
        complete_upload_for_completion(&service, &config, &coalesced_upload.completion_url).await;
    assert_eq!(
        coalesced_completion.receipt.snapshot_id, latest_completion.receipt.snapshot_id,
        "distinct capture provenance can coalesce to one immutable snapshot"
    );
    assert_ne!(
        old_completion.receipt.snapshot_id,
        latest_completion.receipt.snapshot_id
    );

    let release_sessions: PaginatedResponse<SessionResponse> = get_json(
        &service,
        &config,
        "/api/v1/sessions?branch=release&project=archive-project&repository=example/archive",
    )
    .await;
    assert_eq!(release_sessions.items.len(), 1);
    let session = &release_sessions.items[0];
    assert_eq!(session.session_id, latest_completion.receipt.session_id);
    assert_eq!(session.latest_snapshot.branch.as_deref(), Some("release"));
    assert_eq!(
        session.latest_snapshot.snapshot_id,
        latest_completion.receipt.snapshot_id
    );

    let stale_branch: PaginatedResponse<SessionResponse> =
        get_json(&service, &config, "/api/v1/sessions?branch=main").await;
    assert!(stale_branch.items.is_empty());
    let second_client_sessions: PaginatedResponse<SessionResponse> = get_json(
        &service,
        &config,
        format!("/api/v1/sessions?client_id={second_client}"),
    )
    .await;
    assert_eq!(
        second_client_sessions.items.len(),
        1,
        "a client matches when it contributed any capture to the projected latest snapshot"
    );
    let activity_from: PaginatedResponse<SessionResponse> = get_json(
        &service,
        &config,
        "/api/v1/sessions?activity_from=2000-01-01T00:00:00Z",
    )
    .await;
    assert_eq!(activity_from.items.len(), 1);
    let activity_to: PaginatedResponse<SessionResponse> = get_json(
        &service,
        &config,
        "/api/v1/sessions?activity_to=9999-12-31T23:59:59Z",
    )
    .await;
    assert_eq!(activity_to.items.len(), 1);
    let (invalid_activity_status, _, _) = call(
        service.router(&config),
        Request::builder()
            .uri(
                "/api/v1/sessions?activity_from=2027-01-01T00:00:00Z&activity_to=2026-01-01T00:00:00Z",
            )
            .body(Body::empty())
            .expect("request is valid"),
    )
    .await;
    assert_eq!(invalid_activity_status, StatusCode::UNPROCESSABLE_ENTITY);

    let inspected_session: SessionResponse = get_json(
        &service,
        &config,
        format!("/api/v1/sessions/{}", session.session_id),
    )
    .await;
    assert_eq!(inspected_session, *session);
    let session_snapshots: PaginatedResponse<SnapshotSummary> = get_json(
        &service,
        &config,
        format!(
            "/api/v1/sessions/{}/snapshots?limit=100",
            session.session_id
        ),
    )
    .await;
    assert_eq!(session_snapshots.items.len(), 2);
    assert_eq!(
        session_snapshots.items[0].snapshot_id,
        latest_completion.receipt.snapshot_id
    );
    let historical: SnapshotResponse = get_json(
        &service,
        &config,
        format!("/api/v1/snapshots/{}", old_completion.receipt.snapshot_id),
    )
    .await;
    assert_eq!(historical.manifest.capture.branch.as_deref(), Some("main"));
    assert_eq!(
        historical.manifest.capture.source_metadata.get("opaque"),
        Some(&"old-source-value".to_owned())
    );

    let release_snapshots: PaginatedResponse<SnapshotSummary> = get_json(
        &service,
        &config,
        "/api/v1/snapshots?branch=release&artifact_set_version=1",
    )
    .await;
    assert_eq!(release_snapshots.items.len(), 1);
    assert_eq!(
        release_snapshots.items[0].snapshot_id,
        latest_completion.receipt.snapshot_id
    );
    let main_snapshots: PaginatedResponse<SnapshotSummary> =
        get_json(&service, &config, "/api/v1/snapshots?branch=main").await;
    assert_eq!(
        main_snapshots.items[0].snapshot_id,
        old_completion.receipt.snapshot_id
    );

    let session_captures: PaginatedResponse<CaptureProvenance> = get_json(
        &service,
        &config,
        format!("/api/v1/sessions/{}/captures?limit=100", session.session_id),
    )
    .await;
    assert_eq!(session_captures.items.len(), 3);
    let archive_captures: PaginatedResponse<CaptureProvenance> =
        get_json(&service, &config, "/api/v1/captures?limit=100").await;
    assert_eq!(archive_captures.items.len(), 3);
    let snapshot_capture_page: SnapshotCapturesResponse = get_json(
        &service,
        &config,
        format!(
            "/api/v1/snapshots/{}/captures?limit=1",
            latest_completion.receipt.snapshot_id
        ),
    )
    .await;
    assert_eq!(snapshot_capture_page.captures.len(), 1);
    assert!(snapshot_capture_page.high_watermark.is_some());
    let snapshot_capture_next: SnapshotCapturesResponse = get_json(
        &service,
        &config,
        format!(
            "/api/v1/snapshots/{}/captures?limit=1&cursor={}",
            latest_completion.receipt.snapshot_id,
            snapshot_capture_page
                .next_cursor
                .as_deref()
                .expect("coalesced snapshot has a second capture")
        ),
    )
    .await;
    assert_eq!(snapshot_capture_next.captures.len(), 1);
    let coalesced_capture: CaptureProvenance =
        get_json(&service, &config, &coalesced_completion.capture.capture_url).await;
    assert_eq!(
        coalesced_capture.source_metadata.get("opaque"),
        Some(&"second-client-private-context".to_owned())
    );
    assert_eq!(coalesced_capture.artifact_set_version, 1);
    let exact_lookup: CaptureProvenance = get_json(
        &service,
        &config,
        format!("/api/v1/captures?client_id={second_client}&capture_id=moved-branch-coalesced"),
    )
    .await;
    assert_eq!(exact_lookup, coalesced_capture);

    let manifests: PaginatedResponse<CanonicalManifestSummary> = get_json(
        &service,
        &config,
        format!("/api/v1/manifests?session_id={}", session.session_id),
    )
    .await;
    assert_eq!(manifests.items.len(), 3);
    let selected_manifest = manifests
        .items
        .iter()
        .find(|item| item.manifest_id == release_snapshots.items[0].manifest_id)
        .expect("the selected snapshot manifest is listed");
    let canonical_manifest: CanonicalManifestResponse =
        get_json(&service, &config, &selected_manifest.manifest_url).await;
    assert_eq!(
        canonical_manifest.snapshot_id,
        latest_completion.receipt.snapshot_id
    );
    let snapshot_manifest: CanonicalManifestResponse = get_json(
        &service,
        &config,
        format!(
            "/api/v1/snapshots/{}/manifest",
            latest_completion.receipt.snapshot_id
        ),
    )
    .await;
    assert_eq!(
        snapshot_manifest.manifest_id,
        canonical_manifest.manifest_id
    );
    let coalesced_manifest: CanonicalManifestResponse =
        get_json(&service, &config, &coalesced_capture.manifest_url).await;
    assert_eq!(
        coalesced_manifest.capture_record_id,
        coalesced_capture.capture_record_id
    );
    assert_eq!(
        coalesced_manifest
            .manifest
            .capture
            .source_metadata
            .get("opaque"),
        Some(&"second-client-private-context".to_owned())
    );

    let artifacts: PaginatedResponse<ArtifactMetadataResponse> = get_json(
        &service,
        &config,
        format!(
            "/api/v1/artifacts?snapshot_id={}",
            latest_completion.receipt.snapshot_id
        ),
    )
    .await;
    assert_eq!(artifacts.items.len(), 1);
    let artifact: ArtifactMetadataResponse =
        get_json(&service, &config, &artifacts.items[0].metadata_url).await;
    assert_eq!(artifact, artifacts.items[0]);
}

#[allow(clippy::too_many_lines)]
#[tokio::test]
async fn archive_browsing_keyset_pages_are_stable_across_newer_records_and_ties() {
    let data_dir = TestDataDir::new();
    let config = test_config(&data_dir);
    let (service, _) = Service::bootstrap(&config).await.expect("bootstraps");
    let client_id = Uuid::new_v4();
    register(service.router(&config), client_id).await;

    let mut snapshot_ids = Vec::new();
    for name in ["tie-one", "tie-two", "tie-three"] {
        let bytes = format!("pagination {name}").into_bytes();
        let upload = upload_full(
            &service,
            &config,
            client_id,
            name,
            manifest_with_context(
                &bytes,
                &bytes,
                name,
                "pagination-project",
                "example/pagination",
                "main",
                serde_json::json!({}),
            ),
            &bytes,
        )
        .await;
        let completion =
            complete_upload_for_completion(&service, &config, &upload.completion_url).await;
        snapshot_ids.push(completion.receipt.snapshot_id);
    }
    let tied_time = "2000-01-01T00:00:00Z";
    let tied_sort_key =
        crate::database::sort_key_from_rfc3339(tied_time).expect("tied_time parses");
    for snapshot_id in &snapshot_ids {
        sqlx::query("UPDATE snapshots SET completed_at = ?1, completed_at_seq = ?2 WHERE id = ?3")
            .bind(tied_time)
            .bind(tied_sort_key)
            .bind(snapshot_id)
            .execute(&service.state.database)
            .await
            .expect("test can establish a deterministic timestamp tie");
        sqlx::query(
            "UPDATE session_latest_context
             SET completed_at = ?1, completed_at_seq = ?2 WHERE snapshot_id = ?3",
        )
        .bind(tied_time)
        .bind(tied_sort_key)
        .bind(snapshot_id)
        .execute(&service.state.database)
        .await
        .expect("projection timestamp tie updates");
    }
    let mut expected = snapshot_ids.clone();
    expected.sort_unstable_by(|left, right| right.cmp(left));

    let first_page: PaginatedResponse<SnapshotSummary> =
        get_json(&service, &config, "/api/v1/snapshots?limit=1").await;
    assert_eq!(first_page.items.len(), 1);
    assert_eq!(first_page.items[0].snapshot_id, expected[0]);
    assert!(first_page.high_watermark.is_some());
    let first_cursor = first_page
        .next_cursor
        .clone()
        .expect("more tied snapshots require a cursor");
    assert!(
        !first_cursor.contains(&expected[0]),
        "the cursor is opaque rather than a raw resource identifier"
    );

    let newer_bytes = b"newer pagination record";
    let newer_upload = upload_full(
        &service,
        &config,
        client_id,
        "pagination-newer",
        manifest_with_context(
            newer_bytes,
            newer_bytes,
            "pagination-newer",
            "pagination-project",
            "example/pagination",
            "main",
            serde_json::json!({}),
        ),
        newer_bytes,
    )
    .await;
    let newer =
        complete_upload_for_completion(&service, &config, &newer_upload.completion_url).await;

    let mut seen = vec![first_page.items[0].snapshot_id.clone()];
    let mut cursor = Some(first_cursor);
    while let Some(next_cursor) = cursor {
        let page: PaginatedResponse<SnapshotSummary> = get_json(
            &service,
            &config,
            format!("/api/v1/snapshots?limit=1&cursor={next_cursor}"),
        )
        .await;
        seen.extend(page.items.iter().map(|item| item.snapshot_id.clone()));
        cursor = page.next_cursor;
    }
    assert_eq!(seen, expected);
    assert!(!seen.contains(&newer.receipt.snapshot_id));
    assert_eq!(
        seen.iter().collect::<std::collections::HashSet<_>>().len(),
        seen.len(),
        "keyset traversal neither duplicates nor skips tied records"
    );

    let (invalid_status, _, _) = call(
        service.router(&config),
        Request::builder()
            .uri("/api/v1/snapshots?cursor=not-a-cursor")
            .body(Body::empty())
            .expect("request is valid"),
    )
    .await;
    assert_eq!(invalid_status, StatusCode::UNPROCESSABLE_ENTITY);
    let (mismatched_status, _, _) = call(
        service.router(&config),
        Request::builder()
            .uri(format!(
                "/api/v1/snapshots?branch=main&cursor={}",
                first_page
                    .next_cursor
                    .as_deref()
                    .expect("first cursor remains available")
            ))
            .body(Body::empty())
            .expect("request is valid"),
    )
    .await;
    assert_eq!(mismatched_status, StatusCode::UNPROCESSABLE_ENTITY);
    for limit in [0, 101] {
        let (status, _, _) = call(
            service.router(&config),
            Request::builder()
                .uri(format!("/api/v1/snapshots?limit={limit}"))
                .body(Body::empty())
                .expect("request is valid"),
        )
        .await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    }
}

/// RFC 3339's fractional-second component has variable width, so `.12Z`
/// (120 milliseconds) is TEXT-greater than `.123Z` (123 milliseconds) even
/// though it is chronologically earlier. Every keyset-ordered resource must
/// sort by the numeric microsecond key instead of the RFC 3339 column, or
/// this exact pair sorts backwards.
#[allow(clippy::too_many_lines)]
#[tokio::test]
async fn numeric_sort_key_orders_variable_precision_fractional_seconds_correctly() {
    let data_dir = TestDataDir::new();
    let config = test_config(&data_dir);
    let (service, _) = Service::bootstrap(&config).await.expect("bootstraps");
    let client_id = Uuid::new_v4();
    register(service.router(&config), client_id).await;

    let earlier_bytes = b"variable precision earlier";
    let earlier_upload = upload_full(
        &service,
        &config,
        client_id,
        "precision-earlier",
        manifest_with_context(
            earlier_bytes,
            earlier_bytes,
            "precision-session",
            "precision-project",
            "example/precision",
            "main",
            serde_json::json!({}),
        ),
        earlier_bytes,
    )
    .await;
    let earlier =
        complete_upload_for_completion(&service, &config, &earlier_upload.completion_url).await;

    let later_bytes = b"variable precision later";
    let later_upload = upload_full(
        &service,
        &config,
        client_id,
        "precision-later",
        manifest_with_context(
            later_bytes,
            later_bytes,
            "precision-session",
            "precision-project",
            "example/precision",
            "main",
            serde_json::json!({}),
        ),
        later_bytes,
    )
    .await;
    let later =
        complete_upload_for_completion(&service, &config, &later_upload.completion_url).await;

    let earlier_text = "2024-06-01T00:00:00.12Z";
    let later_text = "2024-06-01T00:00:00.123Z";
    assert!(
        later_text < earlier_text,
        "the fixture must reproduce the RFC 3339 TEXT-ordering inversion this test guards against"
    );
    let earlier_key = crate::database::sort_key_from_rfc3339(earlier_text).expect("parses");
    let later_key = crate::database::sort_key_from_rfc3339(later_text).expect("parses");
    assert!(
        later_key > earlier_key,
        "the numeric sort key must still treat .123Z as chronologically later"
    );

    for (snapshot_id, text, key) in [
        (&earlier.receipt.snapshot_id, earlier_text, earlier_key),
        (&later.receipt.snapshot_id, later_text, later_key),
    ] {
        sqlx::query("UPDATE snapshots SET completed_at = ?1, completed_at_seq = ?2 WHERE id = ?3")
            .bind(text)
            .bind(key)
            .bind(snapshot_id)
            .execute(&service.state.database)
            .await
            .expect("test can set a variable-precision fractional-second timestamp");
        sqlx::query(
            "UPDATE captures SET server_completed_at = ?1, server_completed_at_seq = ?2
             WHERE snapshot_id = ?3",
        )
        .bind(text)
        .bind(key)
        .bind(snapshot_id)
        .execute(&service.state.database)
        .await
        .expect("capture provenance timestamp updates");
        sqlx::query(
            "UPDATE session_latest_context SET completed_at = ?1, completed_at_seq = ?2
             WHERE snapshot_id = ?3",
        )
        .bind(text)
        .bind(key)
        .bind(snapshot_id)
        .execute(&service.state.database)
        .await
        .expect("projection timestamp updates");
    }

    let snapshots: PaginatedResponse<SnapshotSummary> =
        get_json(&service, &config, "/api/v1/snapshots?limit=100").await;
    assert_eq!(
        snapshots.items[0].snapshot_id, later.receipt.snapshot_id,
        "the chronologically later .123Z snapshot must sort first even though it is TEXT-smaller"
    );
    assert_eq!(snapshots.items[1].snapshot_id, earlier.receipt.snapshot_id);
    assert_eq!(snapshots.items[0].completed_at, later_text);
    assert_eq!(snapshots.items[1].completed_at, earlier_text);

    let captures: PaginatedResponse<CaptureProvenance> =
        get_json(&service, &config, "/api/v1/captures?limit=100").await;
    assert_eq!(
        captures.items[0].snapshot_id, later.receipt.snapshot_id,
        "captures must also sort by the numeric server-completion key, not RFC 3339 text"
    );
    assert_eq!(captures.items[1].snapshot_id, earlier.receipt.snapshot_id);

    let sessions: PaginatedResponse<SessionResponse> = get_json(
        &service,
        &config,
        "/api/v1/sessions?project=precision-project",
    )
    .await;
    assert_eq!(sessions.items.len(), 1);
    assert_eq!(
        sessions.items[0].latest_snapshot.snapshot_id, later.receipt.snapshot_id,
        "the session's projected latest snapshot must be the chronologically later one"
    );
}

/// The numeric ordering key is backfilled once for rows written before it
/// existed (see `ingestion::backfill_sort_keys`); this never touches the
/// RFC 3339 receipt/API text those rows already carry.
#[tokio::test]
async fn restart_backfills_numeric_sort_keys_without_mutating_receipt_text() {
    let data_dir = TestDataDir::new();
    let config = test_config(&data_dir);
    let (service, _) = Service::bootstrap(&config).await.expect("bootstraps");
    let client_id = Uuid::new_v4();
    register(service.router(&config), client_id).await;
    let bytes = b"pre-migration sort key";
    let upload = upload_full(
        &service,
        &config,
        client_id,
        "pre-migration-sort-key",
        manifest(bytes, bytes, Compression::Identity),
        bytes,
    )
    .await;
    let original = complete_upload_for_completion(&service, &config, &upload.completion_url).await;

    for statement in [
        "UPDATE snapshots SET completed_at_seq = 0 WHERE id = ?1",
        "UPDATE captures SET server_completed_at_seq = 0 WHERE snapshot_id = ?1",
        "UPDATE artifacts SET created_at_seq = 0 WHERE snapshot_id = ?1",
    ] {
        sqlx::query(statement)
            .bind(&original.receipt.snapshot_id)
            .execute(&service.state.database)
            .await
            .expect("simulate a pre-migration row with an unbackfilled sort key");
    }
    let before: (String, i64) =
        sqlx::query_as("SELECT completed_at, completed_at_seq FROM snapshots WHERE id = ?1")
            .bind(&original.receipt.snapshot_id)
            .fetch_one(&service.state.database)
            .await
            .expect("snapshot row is readable before restart");
    assert_eq!(before.1, 0);
    drop(service);

    let (restarted, _) = Service::bootstrap(&config)
        .await
        .expect("backfills the numeric sort key on restart");

    let snapshot_after: (String, i64) =
        sqlx::query_as("SELECT completed_at, completed_at_seq FROM snapshots WHERE id = ?1")
            .bind(&original.receipt.snapshot_id)
            .fetch_one(&restarted.state.database)
            .await
            .expect("snapshot row is readable after restart");
    assert_eq!(
        snapshot_after.0, before.0,
        "backfill must never rewrite the RFC 3339 receipt/API text"
    );
    assert_eq!(
        snapshot_after.1,
        crate::database::sort_key_from_rfc3339(&snapshot_after.0).expect("parses")
    );
    assert_ne!(snapshot_after.1, 0);

    let capture_after: (String, i64) = sqlx::query_as(
        "SELECT server_completed_at, server_completed_at_seq FROM captures WHERE snapshot_id = ?1",
    )
    .bind(&original.receipt.snapshot_id)
    .fetch_one(&restarted.state.database)
    .await
    .expect("capture row is readable after restart");
    assert_eq!(
        capture_after.1,
        crate::database::sort_key_from_rfc3339(&capture_after.0).expect("parses")
    );
    assert_ne!(capture_after.1, 0);

    let artifact_after: (String, i64) =
        sqlx::query_as("SELECT created_at, created_at_seq FROM artifacts WHERE snapshot_id = ?1")
            .bind(&original.receipt.snapshot_id)
            .fetch_one(&restarted.state.database)
            .await
            .expect("artifact row is readable after restart");
    assert_eq!(
        artifact_after.1,
        crate::database::sort_key_from_rfc3339(&artifact_after.0).expect("parses")
    );
    assert_ne!(artifact_after.1, 0);

    let reissued =
        complete_upload_for_completion(&restarted, &config, &upload.completion_url).await;
    assert_eq!(reissued.receipt.snapshot_id, original.receipt.snapshot_id);
    assert_eq!(reissued.receipt.completed_at, original.receipt.completed_at);
    assert_eq!(
        reissued.receipt.manifest_sha256,
        original.receipt.manifest_sha256
    );
    assert_eq!(
        reissued.receipt.snapshot_fingerprint,
        original.receipt.snapshot_fingerprint
    );

    let snapshots: PaginatedResponse<SnapshotSummary> =
        get_json(&restarted, &config, "/api/v1/snapshots").await;
    assert_eq!(snapshots.items[0].snapshot_id, original.receipt.snapshot_id);
}

#[allow(clippy::too_many_lines)]
#[tokio::test]
async fn archive_browsing_excludes_incomplete_and_tombstoned_snapshots_after_restart() {
    let data_dir = TestDataDir::new();
    let config = test_config(&data_dir);
    let (service, _) = Service::bootstrap(&config).await.expect("bootstraps");
    let client_id = Uuid::new_v4();
    register(service.router(&config), client_id).await;

    for endpoint in [
        "/api/v1/sessions",
        "/api/v1/captures",
        "/api/v1/snapshots",
        "/api/v1/manifests",
        "/api/v1/artifacts",
    ] {
        let empty: serde_json::Value = get_json(&service, &config, endpoint).await;
        assert_eq!(empty["items"], serde_json::json!([]));
        assert!(empty["next_cursor"].is_null());
        assert!(empty["high_watermark"].is_null());
    }

    let pending = create_upload(
        &service,
        &config,
        client_id,
        "pending-only",
        manifest_with_context(
            b"pending",
            b"pending",
            "pending-only",
            "project",
            "example/pending",
            "main",
            serde_json::json!({}),
        ),
    )
    .await;
    let no_completed_sessions: PaginatedResponse<SessionResponse> =
        get_json(&service, &config, "/api/v1/sessions").await;
    assert!(no_completed_sessions.items.is_empty());
    assert!(!pending.upload_id.is_empty());

    let bytes = b"visible archive record";
    let upload = upload_full(
        &service,
        &config,
        client_id,
        "visible",
        manifest_with_context(
            bytes,
            bytes,
            "visible-session",
            "project",
            "example/visible",
            "main",
            serde_json::json!({"opaque": "preserve-me"}),
        ),
        bytes,
    )
    .await;
    let completion =
        complete_upload_for_completion(&service, &config, &upload.completion_url).await;
    let before_restart: SessionResponse = get_json(
        &service,
        &config,
        format!("/api/v1/sessions/{}", completion.receipt.session_id),
    )
    .await;
    assert_eq!(
        before_restart.latest_snapshot.snapshot_id,
        completion.receipt.snapshot_id
    );
    drop(service);

    let (restarted, _) = Service::bootstrap(&config).await.expect("restarts");
    let after_restart: SessionResponse = get_json(
        &restarted,
        &config,
        format!("/api/v1/sessions/{}", completion.receipt.session_id),
    )
    .await;
    assert_eq!(after_restart, before_restart);
    let snapshot: SnapshotResponse = get_json(
        &restarted,
        &config,
        format!("/api/v1/snapshots/{}", completion.receipt.snapshot_id),
    )
    .await;
    let artifact_id = snapshot.artifacts[0].artifact_id.clone();

    sqlx::query("UPDATE snapshots SET deleted_at = ?1 WHERE id = ?2")
        .bind("2026-07-13T21:00:00Z")
        .bind(&completion.receipt.snapshot_id)
        .execute(&restarted.state.database)
        .await
        .expect("future tombstone hook is writable for this test");
    for endpoint in [
        "/api/v1/sessions",
        "/api/v1/captures",
        "/api/v1/snapshots",
        "/api/v1/manifests",
        "/api/v1/artifacts",
    ] {
        let list: serde_json::Value = get_json(&restarted, &config, endpoint).await;
        assert_eq!(list["items"], serde_json::json!([]), "{endpoint}");
    }
    for endpoint in [
        format!("/api/v1/snapshots/{}", completion.receipt.snapshot_id),
        format!("/api/v1/captures/{}", completion.capture.capture_record_id),
        format!("/api/v1/artifacts/{artifact_id}"),
    ] {
        let (status, _, _) = call(
            restarted.router(&config),
            Request::builder()
                .uri(endpoint)
                .body(Body::empty())
                .expect("request is valid"),
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }
}
