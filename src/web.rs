use axum::{
    http::{StatusCode, header},
    response::{IntoResponse, Response},
};

pub async fn index() -> Response {
    static_asset(
        "text/html; charset=utf-8",
        include_str!("../web/index.html"),
    )
}

pub async fn dashboard() -> Response {
    static_asset(
        "text/html; charset=utf-8",
        include_str!("../web/dashboard.html"),
    )
}

pub async fn javascript() -> Response {
    static_asset(
        "text/javascript; charset=utf-8",
        include_str!("../web/app.js"),
    )
}

pub async fn dashboard_javascript() -> Response {
    static_asset(
        "text/javascript; charset=utf-8",
        include_str!("../web/dashboard.js"),
    )
}

pub async fn stylesheet() -> Response {
    static_asset("text/css; charset=utf-8", include_str!("../web/style.css"))
}

fn static_asset(content_type: &'static str, body: &'static str) -> Response {
    (
        StatusCode::OK,
        [
            (header::CONTENT_TYPE, content_type),
            (header::CACHE_CONTROL, "no-cache"),
            (header::X_CONTENT_TYPE_OPTIONS, "nosniff"),
        ],
        body,
    )
        .into_response()
}
