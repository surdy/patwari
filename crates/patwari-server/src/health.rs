use std::sync::Arc;

use axum::{BoxError, Json, extract::State, http::StatusCode, response::IntoResponse};
use serde::Serialize;

use crate::{error::ApiError, service::AppState};

#[derive(Serialize)]
pub(crate) struct HealthResponse {
    status: &'static str,
}

pub(crate) async fn liveness() -> Json<HealthResponse> {
    Json(HealthResponse { status: "live" })
}

pub(crate) async fn readiness(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let database_ready = sqlx::query("SELECT 1")
        .execute(&state.database)
        .await
        .is_ok();
    let storage_ready = state.storage.is_usable().await;

    if database_ready && storage_ready {
        (StatusCode::OK, Json(HealthResponse { status: "ready" }))
    } else {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(HealthResponse {
                status: "not_ready",
            }),
        )
    }
}

pub(crate) async fn api_not_found() -> ApiError {
    ApiError::not_found("endpoint_not_found", "API endpoint was not found")
}

pub(crate) async fn handle_timeout(error: BoxError) -> impl IntoResponse {
    if error.is::<tower::timeout::error::Elapsed>() {
        ApiError::new(
            StatusCode::REQUEST_TIMEOUT,
            "request_timeout",
            "request exceeded the configured time limit",
        )
    } else {
        ApiError::internal()
    }
}
