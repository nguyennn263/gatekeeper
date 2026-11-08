use axum::{
    Json,
    http::{HeaderName, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
};
use serde_json::json;

#[derive(Debug, thiserror::Error)]
pub enum AppError {
    #[error("unauthorized")]
    Unauthorized,
    #[error("forbidden")]
    Forbidden,
    #[error("not found")]
    NotFound,
    #[error("invalid request: {0}")]
    Invalid(String),
    #[error("request Content-Type must be application/json")]
    UnsupportedMediaType,
    #[error("request body is too large")]
    PayloadTooLarge,
    #[error("request body was truncated or could not be read completely")]
    RequestBodyIncomplete,
    #[error("conflict: {0}")]
    Conflict(String),
    #[error("upstream request failed: {0}")]
    Upstream(String),
    #[error("upstream timed out")]
    Timeout,
    #[error("upstream timed out: {0}")]
    UpstreamTimeout(String),
    #[error("upstream did not return response headers within {0} seconds")]
    FirstByteTimeout(u64),
    #[error("service is busy")]
    Busy,
    #[error("storage operation failed")]
    Storage,
    #[error("internal error")]
    Internal,
}

impl AppError {
    pub fn source(&self) -> &'static str {
        match self {
            Self::Upstream(_)
            | Self::Timeout
            | Self::UpstreamTimeout(_)
            | Self::FirstByteTimeout(_) => "PROVIDER",
            _ => "NORTHGATE",
        }
    }

    pub fn is_provider_failure(&self) -> bool {
        self.source() == "PROVIDER"
    }

    pub fn code(&self) -> &'static str {
        match self {
            Self::Unauthorized => "invalid_api_key",
            Self::Forbidden => "forbidden",
            Self::NotFound => "not_found",
            Self::Invalid(_) | Self::UnsupportedMediaType | Self::RequestBodyIncomplete => {
                "invalid_request_error"
            }
            Self::PayloadTooLarge => "request_too_large",
            Self::Conflict(_) => "conflict",
            Self::Upstream(_) => "upstream_error",
            Self::Timeout | Self::UpstreamTimeout(_) | Self::FirstByteTimeout(_) => {
                "upstream_timeout"
            }
            Self::Busy => "gateway_busy",
            Self::Storage => "storage_error",
            Self::Internal => "internal_error",
        }
    }

    pub fn status(&self) -> StatusCode {
        match self {
            Self::Unauthorized => StatusCode::UNAUTHORIZED,
            Self::Forbidden => StatusCode::FORBIDDEN,
            Self::NotFound => StatusCode::NOT_FOUND,
            Self::Invalid(_) => StatusCode::BAD_REQUEST,
            Self::UnsupportedMediaType => StatusCode::UNSUPPORTED_MEDIA_TYPE,
            Self::PayloadTooLarge => StatusCode::PAYLOAD_TOO_LARGE,
            Self::RequestBodyIncomplete => StatusCode::BAD_REQUEST,
            Self::Conflict(_) => StatusCode::CONFLICT,
            Self::Upstream(_) => StatusCode::BAD_GATEWAY,
            Self::Timeout | Self::UpstreamTimeout(_) | Self::FirstByteTimeout(_) => {
                StatusCode::GATEWAY_TIMEOUT
            }
            Self::Busy => StatusCode::SERVICE_UNAVAILABLE,
            Self::Storage | Self::Internal => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let status = self.status();
        let code = self.code();
        let source = self.source();
        let message = format!("{source}: {self}");
        let mut response = (
            status,
            Json(json!({
                "error": {
                    "message": message,
                    "type": code,
                    "code": code,
                    "source": source
                }
            })),
        )
            .into_response();
        response.headers_mut().insert(
            HeaderName::from_static("x-northgate-error-source"),
            HeaderValue::from_static(source),
        );
        response
    }
}

impl From<rusqlite::Error> for AppError {
    fn from(_: rusqlite::Error) -> Self {
        Self::Storage
    }
}
