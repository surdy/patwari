use axum::{
    Json,
    extract::rejection::JsonRejection,
    http::StatusCode,
    response::{IntoResponse, Response},
};

use crate::contract::{ErrorDetail, ErrorResponse};

#[derive(Debug)]
pub(crate) struct ApiError {
    status: StatusCode,
    code: &'static str,
    message: &'static str,
}

impl ApiError {
    pub(crate) const fn new(status: StatusCode, code: &'static str, message: &'static str) -> Self {
        Self {
            status,
            code,
            message,
        }
    }

    pub(crate) const fn invalid(message: &'static str) -> Self {
        Self::new(
            StatusCode::UNPROCESSABLE_ENTITY,
            "validation_error",
            message,
        )
    }

    pub(crate) const fn conflict(code: &'static str, message: &'static str) -> Self {
        Self::new(StatusCode::CONFLICT, code, message)
    }

    pub(crate) const fn not_found(code: &'static str, message: &'static str) -> Self {
        Self::new(StatusCode::NOT_FOUND, code, message)
    }

    pub(crate) const fn internal() -> Self {
        Self::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal_error",
            "archive operation could not be completed",
        )
    }

    pub(crate) const fn storage() -> Self {
        Self::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "storage_error",
            "archive storage could not be used",
        )
    }

    pub(crate) const fn database() -> Self {
        Self::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "metadata_error",
            "archive metadata could not be used",
        )
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (
            self.status,
            Json(ErrorResponse {
                error: ErrorDetail {
                    code: self.code,
                    message: self.message,
                },
            }),
        )
            .into_response()
    }
}

pub(crate) fn parse_json<T>(payload: Result<Json<T>, JsonRejection>) -> Result<T, ApiError> {
    payload.map(|Json(value)| value).map_err(|_| {
        ApiError::invalid("request body must be valid JSON with application/json content type")
    })
}

pub(crate) fn classify_database_error(error: &sqlx::Error) -> ApiError {
    let is_busy = matches!(
        error,
        sqlx::Error::Database(db_error)
            if matches!(db_error.code().as_deref(), Some("5" | "6"))
    );
    if is_busy {
        ApiError::conflict(
            "upload_completion_contended",
            "archive metadata was busy completing a concurrent request; retry the completion",
        )
    } else {
        ApiError::database()
    }
}
