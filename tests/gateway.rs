use std::{
    convert::Infallible,
    net::SocketAddr,
    path::Path,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use axum::{
    Json, Router,
    body::Body,
    http::{HeaderMap, Request, StatusCode, header},
    response::IntoResponse,
    routing::{get, post},
};
use bytes::Bytes;
use futures_util::{StreamExt, stream};
use http_body_util::BodyExt;
use northgate::{
    app,
    config::{Config, Secret},
    protocol::UpstreamProtocol,
    store::{ModelCapabilities, ModelMetadata, NewProvider, Store},
};
use serde_json::{Value, json};
use tempfile::TempDir;
use tokio::{net::TcpListener, task::JoinHandle};
use tower::ServiceExt;

const ADMIN_TOKEN: &str = "admin-token-for-integration-tests";
const GATEWAY_KEY: &str = "gateway-key-for-integration-tests";
const PROVIDER_KEY: &str = "provider-key-for-integration-tests";

async fn test_store(directory: &TempDir) -> Store {
    Store::open(directory.path().join("northgate.db"), [7_u8; 32])
        .await
        .unwrap()
}

fn test_config(path: &Path) -> Arc<Config> {
    Arc::new(Config {
        bind: "127.0.0.1:0".parse().unwrap(),
        database_path: path.to_owned(),
        master_key: [7_u8; 32],
        admin_token: Secret::new(ADMIN_TOKEN.to_owned(), "admin").unwrap(),
        gateway_key: Secret::new(GATEWAY_KEY.to_owned(), "gateway").unwrap(),
        max_upstream_concurrency: 4,
        max_inbound_concurrency: 16,
        connect_timeout: Duration::from_secs(2),
        first_byte_timeout: Duration::from_secs(2),
        body_idle_timeout: Duration::from_secs(2),
        stream_idle_timeout: Some(Duration::from_secs(2)),
        request_body_timeout: Duration::from_secs(2),
        allow_private_upstreams: true,
    })
}

async fn spawn(router: Router) -> (SocketAddr, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let handle = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    (address, handle)
}

fn json_request(method: &str, uri: &str, token: &str, body: Value) -> Request<Body> {
    Request::builder()
        .method(method)
        .uri(uri)
        .header(header::AUTHORIZATION, format!("Bearer {token}"))
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(body.to_string()))
        .unwrap()
}

async fn response_json(response: axum::response::Response) -> Value {
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    serde_json::from_slice(&bytes).unwrap()
}

#[tokio::test]
async fn openai_onboarding_catalog_and_passthrough_work_end_to_end() {
    let captured = Arc::new(Mutex::new(None::<Value>));
    let upstream_capture = Arc::clone(&captured);
    let upstream = Router::new()
        .route(
            "/v1/models",
            get(|| async {
                Json(json!({"data": [{
                    "id": "acme/model-a",
                    "display_name": "Model A",
                    "context_length": 131072,
                    "max_output_tokens": 8192,
                    "capabilities": {"streaming": true, "tools": true}
                }, {
                    "id": "acme/model-b",
                    "display_name": "Model B",
                    "context_length": 65536,
                    "max_output_tokens": 4096,
                    "capabilities": {"streaming": true}
                }]}))
            }),
        )
        .route(
            "/v1/chat/completions",
            post(move |Json(body): Json<Value>| {
                let capture = Arc::clone(&upstream_capture);
                async move {
                    *capture.lock().unwrap() = Some(body.clone());
                    Json(json!({
                        "id": "chat_1", "object": "chat.completion", "model": body["model"],
                        "choices": [{"index": 0, "message": {"role": "assistant", "content": "hello"}, "finish_reason": "stop"}]
                    }))
                }
            }),
        );
    let (upstream_address, upstream_task) = spawn(upstream).await;
    let directory = TempDir::new().unwrap();
    let store = test_store(&directory).await;
    let gateway = app::router(test_config(&directory.path().join("northgate.db")), store).unwrap();
    let base_url = format!("http://{upstream_address}/v1");

    let discover = gateway
        .clone()
        .oneshot(json_request(
            "POST",
            "/admin/api/discover",
            ADMIN_TOKEN,
            json!({"base_url": base_url, "api_key": PROVIDER_KEY, "protocol": "openai"}),
        ))
        .await
        .unwrap();
    assert_eq!(discover.status(), StatusCode::OK);
    let discovered = response_json(discover).await;
    assert_eq!(discovered["models"][0]["id"], "acme/model-a");
    assert_eq!(discovered["models"][0]["context_window"], 131072);

    let save = gateway
        .clone()
        .oneshot(json_request(
            "POST",
            "/admin/api/providers",
            ADMIN_TOKEN,
            json!({
                "name": "My Provider", "base_url": base_url,
                "api_key": PROVIDER_KEY, "protocol": "openai",
                "models": [discovered["models"][0].clone()]
            }),
        ))
        .await
        .unwrap();
    assert_eq!(save.status(), StatusCode::CREATED);
    let saved = response_json(save).await;
    assert_eq!(saved["api_key_hint"], "provid…ests");
    assert!(!saved.to_string().contains(PROVIDER_KEY));

    let rediscovered = gateway
        .clone()
        .oneshot(json_request(
            "POST",
            "/admin/api/providers/my-provider/discover",
            ADMIN_TOKEN,
            json!({}),
        ))
        .await
        .unwrap();
    assert_eq!(rediscovered.status(), StatusCode::OK);
    let rediscovered = response_json(rediscovered).await;
    assert_eq!(rediscovered["models"].as_array().unwrap().len(), 2);
    assert!(!rediscovered.to_string().contains(PROVIDER_KEY));

    let added = gateway
        .clone()
        .oneshot(json_request(
            "POST",
            "/admin/api/providers/my-provider/models",
            ADMIN_TOKEN,
            json!({"models": [{"id": "acme/model-b"}]}),
        ))
        .await
        .unwrap();
    assert_eq!(added.status(), StatusCode::OK);
    assert_eq!(
        response_json(added).await["models"]
            .as_array()
            .unwrap()
            .len(),
        2
    );

    let models = gateway
        .clone()
        .oneshot(
            Request::builder()
                .uri("/v1/models")
                .header(header::AUTHORIZATION, format!("Bearer {GATEWAY_KEY}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let models = response_json(models).await;
    assert_eq!(models["data"].as_array().unwrap().len(), 2);
    assert_eq!(models["data"][0]["id"], "my-provider/acme/model-a");

    let removed = gateway
        .clone()
        .oneshot(json_request(
            "DELETE",
            "/admin/api/providers/my-provider/models?model=acme%2Fmodel-b",
            ADMIN_TOKEN,
            json!({}),
        ))
        .await
        .unwrap();
    assert_eq!(removed.status(), StatusCode::NO_CONTENT);

    let chat = gateway
        .clone()
        .oneshot(json_request(
            "POST",
            "/v1/chat/completions",
            GATEWAY_KEY,
            json!({
                "model": "my-provider/acme/model-a",
                "messages": [{"role": "user", "content": "hello"}],
                "reasoning_effort": "high",
                "custom_harness_field": {"preserved": true}
            }),
        ))
        .await
        .unwrap();
    assert_eq!(chat.status(), StatusCode::OK);
    assert_eq!(
        response_json(chat).await,
        json!({
            "id":"chat_1", "object":"chat.completion", "model":"my-provider/acme/model-a",
            "choices":[{"index":0,"message":{"role":"assistant","content":"hello"},"finish_reason":"stop"}]
        })
    );
    let sent = captured.lock().unwrap().clone().unwrap();
    assert_eq!(sent["model"], "acme/model-a");
    assert_eq!(sent["reasoning_effort"], "high");
    assert_eq!(sent["custom_harness_field"]["preserved"], true);

    let disable = gateway
        .clone()
        .oneshot(json_request(
            "PUT",
            "/admin/api/providers/my-provider/enabled",
            ADMIN_TOKEN,
            json!({"enabled": false}),
        ))
        .await
        .unwrap();
    assert_eq!(disable.status(), StatusCode::NO_CONTENT);

    let inventory = gateway
        .clone()
        .oneshot(
            Request::builder()
                .uri("/admin/api/providers")
                .header(header::AUTHORIZATION, format!("Bearer {ADMIN_TOKEN}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let inventory = response_json(inventory).await;
    assert_eq!(inventory["providers"][0]["enabled"], false);
    assert_eq!(
        inventory["providers"][0]["models"]
            .as_array()
            .unwrap()
            .len(),
        1
    );

    let disabled_models = gateway
        .clone()
        .oneshot(
            Request::builder()
                .uri("/v1/models")
                .header(header::AUTHORIZATION, format!("Bearer {GATEWAY_KEY}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let disabled_models = response_json(disabled_models).await;
    assert!(disabled_models["data"].as_array().unwrap().is_empty());

    let disabled_chat = gateway
        .clone()
        .oneshot(json_request(
            "POST",
            "/v1/chat/completions",
            GATEWAY_KEY,
            json!({
                "model": "my-provider/acme/model-a",
                "messages": [{"role": "user", "content": "hello"}]
            }),
        ))
        .await
        .unwrap();
    assert_eq!(disabled_chat.status(), StatusCode::NOT_FOUND);
    assert_eq!(
        disabled_chat.headers()["x-northgate-error-source"],
        "NORTHGATE"
    );
    let disabled_error = response_json(disabled_chat).await;
    assert_eq!(disabled_error["error"]["source"], "NORTHGATE");

    let enable = gateway
        .clone()
        .oneshot(json_request(
            "PUT",
            "/admin/api/providers/my-provider/enabled",
            ADMIN_TOKEN,
            json!({"enabled": true}),
        ))
        .await
        .unwrap();
    assert_eq!(enable.status(), StatusCode::NO_CONTENT);

    let enabled_models = gateway
        .oneshot(
            Request::builder()
                .uri("/v1/models")
                .header(header::AUTHORIZATION, format!("Bearer {GATEWAY_KEY}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let enabled_models = response_json(enabled_models).await;
    assert_eq!(enabled_models["data"][0]["id"], "my-provider/acme/model-a");
    upstream_task.abort();
}

#[tokio::test]
async fn anthropic_request_and_response_are_translated() {
    let captured = Arc::new(Mutex::new(None::<Value>));
    let upstream_capture = Arc::clone(&captured);
    let upstream = Router::new().route(
        "/v1/messages",
        post(move |Json(body): Json<Value>| {
            let capture = Arc::clone(&upstream_capture);
            async move {
                *capture.lock().unwrap() = Some(body);
                Json(json!({
                    "id": "msg_1", "type": "message", "model": "claude-test",
                    "content": [{"type": "text", "text": "bonjour"}],
                    "stop_reason": "end_turn", "usage": {"input_tokens": 3, "output_tokens": 2}
                }))
            }
        }),
    );
    let (upstream_address, upstream_task) = spawn(upstream).await;
    let directory = TempDir::new().unwrap();
    let store = test_store(&directory).await;
    store
        .save_provider(
            NewProvider::new(
                "Anthropic",
                format!("http://{upstream_address}/v1"),
                PROVIDER_KEY,
                UpstreamProtocol::Anthropic,
                vec![ModelMetadata {
                    upstream_id: "claude-test".to_owned(),
                    display_name: None,
                    context_window: Some(200_000),
                    max_output_tokens: Some(8_192),
                    capabilities: ModelCapabilities::default(),
                }],
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let gateway = app::router(test_config(&directory.path().join("northgate.db")), store).unwrap();
    let response = gateway
        .oneshot(json_request(
            "POST",
            "/v1/chat/completions",
            GATEWAY_KEY,
            json!({
                "model": "anthropic/claude-test",
                "messages": [
                    {"role": "system", "content": "Translate"},
                    {"role": "user", "content": "hello"}
                ],
                "max_tokens": 64
            }),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    assert_eq!(body["choices"][0]["message"]["content"], "bonjour");
    assert_eq!(body["usage"]["total_tokens"], 5);
    assert_eq!(body["model"], "anthropic/claude-test");
    let sent = captured.lock().unwrap().clone().unwrap();
    assert_eq!(sent["model"], "claude-test");
    assert_eq!(sent["system"][0]["text"], "Translate");
    assert_eq!(sent["messages"][0]["role"], "user");
    upstream_task.abort();
}

#[tokio::test]
async fn responses_stream_is_exposed_as_openai_sse() {
    let upstream = Router::new().route(
        "/v1/responses",
        post(|| async {
            (
                [(header::CONTENT_TYPE, "text/event-stream")],
                "event: response.created\ndata: {\"response\":{\"id\":\"r1\",\"model\":\"gpt-test\"}}\n\nevent: response.output_text.delta\ndata: {\"delta\":\"hello\"}\n\nevent: response.completed\ndata: {\"response\":{\"usage\":{\"input_tokens\":1,\"output_tokens\":2}}}\n\n",
            )
                .into_response()
        }),
    );
    let (upstream_address, upstream_task) = spawn(upstream).await;
    let directory = TempDir::new().unwrap();
    let store = test_store(&directory).await;
    store
        .save_provider(
            NewProvider::new(
                "Codex",
                format!("http://{upstream_address}/v1"),
                PROVIDER_KEY,
                UpstreamProtocol::Responses,
                vec![ModelMetadata {
                    upstream_id: "gpt-test".to_owned(),
                    display_name: None,
                    context_window: Some(128_000),
                    max_output_tokens: Some(16_384),
                    capabilities: ModelCapabilities::default(),
                }],
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let gateway = app::router(test_config(&directory.path().join("northgate.db")), store).unwrap();
    let response = gateway
        .oneshot(json_request(
            "POST",
            "/v1/chat/completions",
            GATEWAY_KEY,
            json!({
                "model": "codex/gpt-test",
                "messages": [{"role": "user", "content": "hello"}],
                "stream": true,
                "stream_options": {"include_usage": true}
            }),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers()[header::CONTENT_TYPE],
        "text/event-stream; charset=utf-8"
    );
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let stream = String::from_utf8(bytes.to_vec()).unwrap();
    assert!(stream.contains("chat.completion.chunk"));
    assert!(stream.contains("hello"));
    assert!(stream.contains("total_tokens"));
    assert!(stream.contains("\"model\":\"codex/gpt-test\""));
    assert!(!stream.contains("\"model\":\"gpt-test\""));
    assert_eq!(stream.matches("data: [DONE]").count(), 1);
    upstream_task.abort();
}

#[tokio::test]
async fn terminal_sse_finishes_and_releases_slot_before_upstream_eof() {
    let upstream = Router::new().route(
        "/v1/responses",
        post(|| async {
            let first = stream::once(async {
                Ok::<Bytes, Infallible>(Bytes::from_static(
                    b"event: response.completed\ndata: {\"response\":{}}\n\n",
                ))
            });
            let never = stream::pending::<Result<Bytes, Infallible>>();
            axum::response::Response::builder()
                .header(header::CONTENT_TYPE, "text/event-stream")
                .body(Body::from_stream(first.chain(never)))
                .unwrap()
        }),
    );
    let (upstream_address, upstream_task) = spawn(upstream).await;
    let directory = TempDir::new().unwrap();
    let store = test_store(&directory).await;
    store
        .save_provider(
            NewProvider::new(
                "Codex",
                format!("http://{upstream_address}/v1"),
                PROVIDER_KEY,
                UpstreamProtocol::Responses,
                vec![ModelMetadata {
                    upstream_id: "gpt-test".to_owned(),
                    display_name: None,
                    context_window: None,
                    max_output_tokens: None,
                    capabilities: ModelCapabilities::default(),
                }],
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let mut config = (*test_config(&directory.path().join("northgate.db"))).clone();
    config.max_upstream_concurrency = 1;
    config.stream_idle_timeout = None;
    let gateway = app::router(Arc::new(config), store).unwrap();

    for _ in 0..2 {
        let response = gateway
            .clone()
            .oneshot(json_request(
                "POST",
                "/v1/chat/completions",
                GATEWAY_KEY,
                json!({
                    "model":"codex/gpt-test",
                    "messages":[{"role":"user","content":"hi"}],
                    "stream":true
                }),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = tokio::time::timeout(Duration::from_secs(1), response.into_body().collect())
            .await
            .expect("downstream must finish at terminal event")
            .unwrap()
            .to_bytes();
        let body = String::from_utf8(body.to_vec()).unwrap();
        assert_eq!(body.matches("data: [DONE]").count(), 1);
    }
    upstream_task.abort();
}

#[tokio::test]
async fn bearer_is_checked_before_json_body_is_parsed() {
    let directory = TempDir::new().unwrap();
    let store = test_store(&directory).await;
    let gateway = app::router(test_config(&directory.path().join("northgate.db")), store).unwrap();
    let response = gateway
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/chat/completions")
                .header(header::AUTHORIZATION, "Bearer wrong-key-with-valid-length")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from("not-json"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(response.headers()["x-northgate-error-source"], "NORTHGATE");
    let error = response_json(response).await;
    assert_eq!(error["error"]["source"], "NORTHGATE");
    assert!(
        error["error"]["message"]
            .as_str()
            .unwrap()
            .starts_with("NORTHGATE:")
    );

    let malformed = gateway
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/chat/completions")
                .header(header::AUTHORIZATION, format!("Bearer {GATEWAY_KEY}"))
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from("not-json"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(malformed.status(), StatusCode::BAD_REQUEST);
    assert_eq!(malformed.headers()["x-northgate-error-source"], "NORTHGATE");
    let malformed = response_json(malformed).await;
    assert_eq!(malformed["error"]["source"], "NORTHGATE");
}

#[tokio::test]
async fn oversized_json_body_is_reported_as_payload_too_large() {
    let directory = TempDir::new().unwrap();
    let store = test_store(&directory).await;
    let gateway = app::router(test_config(&directory.path().join("northgate.db")), store).unwrap();
    let body = format!(
        "{{\"model\":\"codex/missing\",\"padding\":\"{}\"}}",
        "x".repeat(4 * 1024 * 1024)
    );
    let response = gateway
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/chat/completions")
                .header(header::AUTHORIZATION, format!("Bearer {GATEWAY_KEY}"))
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(response.headers()["x-northgate-error-source"], "NORTHGATE");
    let error = response_json(response).await;
    assert_eq!(error["error"]["code"], "request_too_large");
    assert_eq!(error["error"]["source"], "NORTHGATE");
}

#[tokio::test]
async fn stalled_non_stream_body_returns_gateway_timeout() {
    let upstream = Router::new().route(
        "/v1/chat/completions",
        post(|| async {
            let first = stream::once(async {
                Ok::<Bytes, Infallible>(Bytes::from_static(b"{\"id\":\"unfinished\""))
            });
            axum::response::Response::builder()
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from_stream(
                    first.chain(stream::pending::<Result<Bytes, Infallible>>()),
                ))
                .unwrap()
        }),
    );
    let (upstream_address, upstream_task) = spawn(upstream).await;
    let directory = TempDir::new().unwrap();
    let store = test_store(&directory).await;
    store
        .save_provider(
            NewProvider::new(
                "OpenAI",
                format!("http://{upstream_address}/v1"),
                PROVIDER_KEY,
                UpstreamProtocol::OpenAi,
                vec![ModelMetadata {
                    upstream_id: "model-a".to_owned(),
                    display_name: None,
                    context_window: None,
                    max_output_tokens: None,
                    capabilities: ModelCapabilities::default(),
                }],
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let mut config = (*test_config(&directory.path().join("northgate.db"))).clone();
    config.body_idle_timeout = Duration::from_millis(50);
    let gateway = app::router(Arc::new(config), store.clone()).unwrap();
    let response = tokio::time::timeout(
        Duration::from_secs(1),
        gateway.oneshot(json_request(
            "POST",
            "/v1/chat/completions",
            GATEWAY_KEY,
            json!({"model":"openai/model-a","messages":[]}),
        )),
    )
    .await
    .expect("gateway must not wait forever")
    .unwrap();
    assert_eq!(response.status(), StatusCode::GATEWAY_TIMEOUT);
    assert_eq!(response.headers()["x-northgate-error-source"], "PROVIDER");
    let error = response_json(response).await;
    assert_eq!(error["error"]["source"], "PROVIDER");
    assert!(
        error["error"]["message"]
            .as_str()
            .unwrap()
            .starts_with("PROVIDER:")
    );
    let stats = store.usage_stats(0, 3_600).await.unwrap();
    assert_eq!(stats.summary.requests, 1);
    assert_eq!(stats.summary.failed_requests, 1);
    upstream_task.abort();
}

#[tokio::test]
async fn provider_stream_errors_are_labeled_and_counted() {
    let upstream = Router::new().route(
        "/v1/chat/completions",
        post(|| async {
            (
                [(header::CONTENT_TYPE, "text/event-stream")],
                "data: {\"error\":{\"message\":\"quota exhausted\"}}\n\ndata: [DONE]\n\n",
            )
        }),
    );
    let (upstream_address, upstream_task) = spawn(upstream).await;
    let directory = TempDir::new().unwrap();
    let store = test_store(&directory).await;
    store
        .save_provider(
            NewProvider::new(
                "OpenAI",
                format!("http://{upstream_address}/v1"),
                PROVIDER_KEY,
                UpstreamProtocol::OpenAi,
                vec![ModelMetadata {
                    upstream_id: "model-a".to_owned(),
                    display_name: None,
                    context_window: None,
                    max_output_tokens: None,
                    capabilities: ModelCapabilities::default(),
                }],
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let gateway = app::router(
        test_config(&directory.path().join("northgate.db")),
        store.clone(),
    )
    .unwrap();
    let response = gateway
        .oneshot(json_request(
            "POST",
            "/v1/chat/completions",
            GATEWAY_KEY,
            json!({"model":"openai/model-a","messages":[],"stream":true}),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let body = std::str::from_utf8(&body).unwrap();
    assert!(body.contains("\"source\":\"PROVIDER\""));
    assert!(body.contains("quota exhausted"));
    assert!(body.contains("\"source\":\"PROVIDER\""));
    assert!(body.ends_with("data: [DONE]\n\n"));
    let stats = store.usage_stats(0, 3_600).await.unwrap();
    assert_eq!(stats.summary.requests, 1);
    assert_eq!(stats.summary.failed_requests, 1);
    upstream_task.abort();
}

#[tokio::test]
async fn stream_falls_back_only_before_output_starts() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let captured = Arc::clone(&attempts);
    let upstream = Router::new().route(
        "/v1/chat/completions",
        post(move |headers: HeaderMap| {
            let captured = Arc::clone(&captured);
            async move {
                captured.fetch_add(1, Ordering::SeqCst);
                if headers
                    .get(header::AUTHORIZATION)
                    .and_then(|value| value.to_str().ok())
                    == Some("Bearer stream-fallback-key")
                {
                    (
                        [(header::CONTENT_TYPE, "text/event-stream")],
                        "data: {\"id\":\"fallback\",\"object\":\"chat.completion.chunk\",\"model\":\"model-a\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"recovered\"},\"finish_reason\":null}]}\n\ndata: [DONE]\n\n",
                    )
                        .into_response()
                } else {
                    (
                        [(header::CONTENT_TYPE, "text/event-stream")],
                        "data: {\"error\":{\"message\":\"primary stream quota exhausted\"}}\n\ndata: [DONE]\n\n",
                    )
                        .into_response()
                }
            }
        }),
    );
    let (upstream_address, upstream_task) = spawn(upstream).await;
    let directory = TempDir::new().unwrap();
    let store = test_store(&directory).await;
    let provider = NewProvider::new(
        "Stream Fallback",
        format!("http://{upstream_address}/v1"),
        "stream-primary-key",
        UpstreamProtocol::OpenAi,
        vec![ModelMetadata {
            upstream_id: "model-a".to_owned(),
            display_name: None,
            context_window: None,
            max_output_tokens: None,
            capabilities: ModelCapabilities::default(),
        }],
    )
    .unwrap()
    .with_fallback_api_keys(vec!["stream-fallback-key".to_owned()])
    .unwrap();
    store.save_provider(provider).await.unwrap();
    let gateway = app::router(
        test_config(&directory.path().join("northgate.db")),
        store.clone(),
    )
    .unwrap();
    let response = gateway
        .oneshot(json_request(
            "POST",
            "/v1/chat/completions",
            GATEWAY_KEY,
            json!({"model":"stream-fallback/model-a","messages":[],"stream":true}),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let body = std::str::from_utf8(&body).unwrap();
    assert!(body.contains("recovered"));
    assert!(!body.contains("primary stream quota exhausted"));
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
    let stats = store.usage_stats(0, 3_600).await.unwrap();
    assert_eq!(stats.summary.requests, 1);
    assert_eq!(stats.summary.failed_requests, 0);
    upstream_task.abort();
}

#[tokio::test]
async fn fallback_keys_are_used_in_order_and_only_final_failure_is_reported() {
    let attempts = Arc::new(Mutex::new(Vec::<String>::new()));
    let captured = Arc::clone(&attempts);
    let upstream = Router::new().route(
        "/v1/chat/completions",
        post(move |headers: HeaderMap| {
            let captured = Arc::clone(&captured);
            async move {
                let authorization = headers
                    .get(header::AUTHORIZATION)
                    .and_then(|value| value.to_str().ok())
                    .unwrap_or_default()
                    .to_owned();
                captured.lock().unwrap().push(authorization.clone());
                match authorization.as_str() {
                    "Bearer primary-key" => (
                        StatusCode::TOO_MANY_REQUESTS,
                        Json(json!({"error":{"message":"primary quota exhausted"}})),
                    )
                        .into_response(),
                    "Bearer fallback-one-key" => (
                        StatusCode::SERVICE_UNAVAILABLE,
                        Json(json!({"error":{"message":"fallback one unavailable"}})),
                    )
                        .into_response(),
                    "Bearer fallback-two-key" => Json(json!({
                        "id":"fallback-ok",
                        "object":"chat.completion",
                        "model":"model-a",
                        "choices":[{"index":0,"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}]
                    }))
                    .into_response(),
                    "Bearer fallback-final-fail" => (
                        StatusCode::FORBIDDEN,
                        Json(json!({"error":{"message":"fallback quota exhausted"}})),
                    )
                        .into_response(),
                    _ => StatusCode::UNAUTHORIZED.into_response(),
                }
            }
        }),
    );
    let (upstream_address, upstream_task) = spawn(upstream).await;
    let directory = TempDir::new().unwrap();
    let store = test_store(&directory).await;
    let provider = NewProvider::new(
        "Fallback Provider",
        format!("http://{upstream_address}/v1"),
        "primary-key",
        UpstreamProtocol::OpenAi,
        vec![ModelMetadata {
            upstream_id: "model-a".to_owned(),
            display_name: None,
            context_window: None,
            max_output_tokens: None,
            capabilities: ModelCapabilities::default(),
        }],
    )
    .unwrap()
    .with_fallback_api_keys(vec![
        "fallback-one-key".to_owned(),
        "fallback-two-key".to_owned(),
    ])
    .unwrap();
    store.save_provider(provider).await.unwrap();
    let gateway = app::router(
        test_config(&directory.path().join("northgate.db")),
        store.clone(),
    )
    .unwrap();

    let succeeded = gateway
        .clone()
        .oneshot(json_request(
            "POST",
            "/v1/chat/completions",
            GATEWAY_KEY,
            json!({"model":"fallback-provider/model-a","messages":[]}),
        ))
        .await
        .unwrap();
    assert_eq!(succeeded.status(), StatusCode::OK);
    assert_eq!(
        response_json(succeeded).await["choices"][0]["message"]["content"],
        "ok"
    );
    assert_eq!(
        *attempts.lock().unwrap(),
        vec![
            "Bearer primary-key",
            "Bearer fallback-one-key",
            "Bearer fallback-two-key"
        ]
    );
    let stats = store.usage_stats(0, 3_600).await.unwrap();
    assert_eq!(stats.summary.requests, 1);
    assert_eq!(stats.summary.failed_requests, 0);

    let replace = gateway
        .clone()
        .oneshot(json_request(
            "PUT",
            "/admin/api/providers/fallback-provider/fallback-keys/2",
            ADMIN_TOKEN,
            json!({"api_key":"fallback-final-fail"}),
        ))
        .await
        .unwrap();
    assert_eq!(replace.status(), StatusCode::NO_CONTENT);

    let failed = gateway
        .clone()
        .oneshot(json_request(
            "POST",
            "/v1/chat/completions",
            GATEWAY_KEY,
            json!({"model":"fallback-provider/model-a","messages":[]}),
        ))
        .await
        .unwrap();
    assert_eq!(failed.status(), StatusCode::BAD_GATEWAY);
    assert_eq!(failed.headers()["x-northgate-error-source"], "PROVIDER");
    let failed = response_json(failed).await;
    let message = failed["error"]["message"].as_str().unwrap();
    assert!(message.contains("primary: HTTP 429 Too Many Requests: primary quota exhausted"));
    assert!(message.contains("fallback 1: HTTP 503 Service Unavailable: fallback one unavailable"));
    assert!(message.contains("fallback 2: HTTP 403 Forbidden: fallback quota exhausted"));
    assert!(!message.contains("primary-key"));
    assert!(!message.contains("fallback-one-key"));
    assert!(!message.contains("fallback-final-fail"));
    let stats = store.usage_stats(0, 3_600).await.unwrap();
    assert_eq!(stats.summary.requests, 2);
    assert_eq!(stats.summary.failed_requests, 1);

    let remove = gateway
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/admin/api/providers/fallback-provider/fallback-keys/1")
                .header(header::AUTHORIZATION, format!("Bearer {ADMIN_TOKEN}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(remove.status(), StatusCode::NO_CONTENT);
    let resolved = store.find_model("fallback-provider/model-a").await.unwrap();
    assert_eq!(resolved.fallback_api_keys.len(), 1);
    assert_eq!(
        resolved.fallback_api_keys[0].expose_secret(),
        "fallback-final-fail"
    );
    upstream_task.abort();
}

#[tokio::test]
async fn selected_metadata_is_enriched_and_save_rechecks_provider() {
    let list_calls = Arc::new(AtomicUsize::new(0));
    let calls = Arc::clone(&list_calls);
    let upstream = Router::new()
        .route(
            "/v1/models",
            get(move || {
                calls.fetch_add(1, Ordering::SeqCst);
                async { Json(json!({"data":[{"id":"model-a"}]})) }
            }),
        )
        .route(
            "/v1/models/model-a",
            get(|| async {
                Json(json!({
                    "id":"model-a",
                    "display_name":"Detailed Model",
                    "context_window":64000,
                    "max_output_tokens":4096,
                    "capabilities":{"tools":true,"streaming":true}
                }))
            }),
        );
    let (upstream_address, upstream_task) = spawn(upstream).await;
    let directory = TempDir::new().unwrap();
    let store = test_store(&directory).await;
    let gateway = app::router(test_config(&directory.path().join("northgate.db")), store).unwrap();
    let base_url = format!("http://{upstream_address}/v1");

    let enriched = gateway
        .clone()
        .oneshot(json_request(
            "POST",
            "/admin/api/models/enrich",
            ADMIN_TOKEN,
            json!({
                "base_url":base_url,
                "api_key":PROVIDER_KEY,
                "protocol":"openai",
                "model_ids":["model-a"]
            }),
        ))
        .await
        .unwrap();
    assert_eq!(enriched.status(), StatusCode::OK);
    let enriched = response_json(enriched).await;
    assert_eq!(enriched["models"][0]["context_window"], 64000);
    assert_eq!(enriched["models"][0]["capabilities"]["tools"], true);

    let saved = gateway
        .oneshot(json_request(
            "POST",
            "/admin/api/providers",
            ADMIN_TOKEN,
            json!({
                "name":"Detailed",
                "base_url":base_url,
                "api_key":PROVIDER_KEY,
                "protocol":"openai",
                "models":[{"id":"model-a","context_window":1}]
            }),
        ))
        .await
        .unwrap();
    assert_eq!(saved.status(), StatusCode::CREATED);
    let saved = response_json(saved).await;
    assert_eq!(saved["models"][0]["context_window"], 64000);
    assert!(list_calls.load(Ordering::SeqCst) >= 2);
    upstream_task.abort();
}

#[tokio::test]
async fn openai_and_anthropic_streams_keep_public_aliases_and_finish() {
    let upstream = Router::new()
        .route(
            "/v1/chat/completions",
            post(|| async {
                (
                    [(header::CONTENT_TYPE, "text/event-stream")],
                    "data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"model\":\"upstream-openai\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"open\"},\"finish_reason\":null}]}\n\ndata: [DONE]\n\n",
                )
            }),
        )
        .route(
            "/v1/messages",
            post(|| async {
                (
                    [(header::CONTENT_TYPE, "text/event-stream")],
                    "event: message_start\ndata: {\"message\":{\"id\":\"m1\",\"model\":\"upstream-claude\",\"usage\":{\"input_tokens\":2}}}\n\nevent: content_block_delta\ndata: {\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"anthropic\"}}\n\nevent: message_delta\ndata: {\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":3}}\n\nevent: message_stop\ndata: {}\n\n",
                )
            }),
        );
    let (upstream_address, upstream_task) = spawn(upstream).await;
    let directory = TempDir::new().unwrap();
    let store = test_store(&directory).await;
    for (name, protocol, model) in [
        ("OpenAI", UpstreamProtocol::OpenAi, "upstream-openai"),
        ("Anthropic", UpstreamProtocol::Anthropic, "upstream-claude"),
    ] {
        store
            .save_provider(
                NewProvider::new(
                    name,
                    format!("http://{upstream_address}/v1"),
                    PROVIDER_KEY,
                    protocol,
                    vec![ModelMetadata {
                        upstream_id: model.to_owned(),
                        display_name: None,
                        context_window: None,
                        max_output_tokens: None,
                        capabilities: ModelCapabilities::default(),
                    }],
                )
                .unwrap(),
            )
            .await
            .unwrap();
    }
    let gateway = app::router(test_config(&directory.path().join("northgate.db")), store).unwrap();
    for (alias, text) in [
        ("openai/upstream-openai", "open"),
        ("anthropic/upstream-claude", "anthropic"),
    ] {
        let response = gateway
            .clone()
            .oneshot(json_request(
                "POST",
                "/v1/chat/completions",
                GATEWAY_KEY,
                json!({
                    "model":alias,
                    "messages":[{"role":"user","content":"hi"}],
                    "stream":true,
                    "stream_options":{"include_usage":true}
                }),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let body = String::from_utf8(body.to_vec()).unwrap();
        assert!(body.contains(text));
        assert!(body.contains(&format!("\"model\":\"{alias}\"")));
        assert_eq!(body.matches("data: [DONE]").count(), 1);
    }
    upstream_task.abort();
}

#[tokio::test]
async fn stalled_discovery_body_returns_gateway_timeout() {
    let upstream = Router::new().route(
        "/v1/models",
        get(|| async {
            let first =
                stream::once(async { Ok::<Bytes, Infallible>(Bytes::from_static(b"{\"data\":[")) });
            axum::response::Response::builder()
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from_stream(
                    first.chain(stream::pending::<Result<Bytes, Infallible>>()),
                ))
                .unwrap()
        }),
    );
    let (upstream_address, upstream_task) = spawn(upstream).await;
    let directory = TempDir::new().unwrap();
    let store = test_store(&directory).await;
    let mut config = (*test_config(&directory.path().join("northgate.db"))).clone();
    config.body_idle_timeout = Duration::from_millis(50);
    let gateway = app::router(Arc::new(config), store).unwrap();
    let response = tokio::time::timeout(
        Duration::from_secs(1),
        gateway.oneshot(json_request(
            "POST",
            "/admin/api/discover",
            ADMIN_TOKEN,
            json!({
                "base_url":format!("http://{upstream_address}/v1"),
                "api_key":PROVIDER_KEY,
                "protocol":"openai"
            }),
        )),
    )
    .await
    .expect("discovery must not wait forever")
    .unwrap();
    assert_eq!(response.status(), StatusCode::GATEWAY_TIMEOUT);
    upstream_task.abort();
}

#[tokio::test]
async fn dashboard_stats_and_model_tests_report_usage_and_full_stream() {
    let upstream = Router::new().route(
        "/v1/chat/completions",
        post(|Json(body): Json<Value>| async move {
            if body["stream"].as_bool().unwrap_or(false) {
                let first = stream::once(async {
                    Ok::<Bytes, Infallible>(Bytes::from_static(
                        b"data: {\"id\":\"test-stream\",\"object\":\"chat.completion.chunk\",\"model\":\"model-a\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"OK\"},\"finish_reason\":null}]}\n\n",
                    ))
                });
                let rest = stream::once(async {
                    tokio::time::sleep(Duration::from_millis(200)).await;
                    Ok::<Bytes, Infallible>(Bytes::from_static(
                        b"data: {\"id\":\"test-stream\",\"object\":\"chat.completion.chunk\",\"model\":\"model-a\",\"choices\":[],\"usage\":{\"prompt_tokens\":10,\"completion_tokens\":2,\"prompt_tokens_details\":{\"cached_tokens\":4}}}\n\ndata: [DONE]\n\n",
                    ))
                });
                axum::response::Response::builder()
                    .header(header::CONTENT_TYPE, "text/event-stream")
                    .body(Body::from_stream(first.chain(rest)))
                    .unwrap()
            } else if body.get("stream_options").is_some() {
                (
                    StatusCode::BAD_REQUEST,
                    Json(json!({"error":{"message":"stream_options is only allowed when stream is true"}})),
                )
                    .into_response()
            } else {
                Json(json!({
                    "id":"test-json", "object":"chat.completion", "model":"model-a",
                    "choices":[{"index":0,"message":{"role":"assistant","content":"OK"},"finish_reason":"stop"}],
                    "usage":{"prompt_tokens":9,"completion_tokens":1,"prompt_tokens_details":{"cached_tokens":3}}
                }))
                .into_response()
            }
        }),
    );
    let (upstream_address, upstream_task) = spawn(upstream).await;
    let directory = TempDir::new().unwrap();
    let store = test_store(&directory).await;
    store
        .save_provider(
            NewProvider::new(
                "Tracked",
                format!("http://{upstream_address}/v1"),
                PROVIDER_KEY,
                UpstreamProtocol::OpenAi,
                vec![ModelMetadata {
                    upstream_id: "model-a".to_owned(),
                    display_name: None,
                    context_window: None,
                    max_output_tokens: None,
                    capabilities: ModelCapabilities::default(),
                }],
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let gateway = app::router(test_config(&directory.path().join("northgate.db")), store).unwrap();

    let plain = gateway
        .clone()
        .oneshot(json_request(
            "POST",
            "/admin/api/models/test",
            ADMIN_TOKEN,
            json!({"model":"tracked/model-a","stream":false}),
        ))
        .await
        .unwrap();
    assert_eq!(plain.status(), StatusCode::OK);
    assert!(plain.headers().contains_key("x-northgate-test-latency-ms"));
    assert_eq!(
        response_json(plain).await["choices"][0]["message"]["content"],
        "OK"
    );

    let streamed = gateway
        .clone()
        .oneshot(json_request(
            "POST",
            "/admin/api/models/test",
            ADMIN_TOKEN,
            json!({"model":"tracked/model-a","stream":true}),
        ))
        .await
        .unwrap();
    assert_eq!(streamed.status(), StatusCode::OK);
    let mut body = streamed.into_body().into_data_stream();
    let first = tokio::time::timeout(Duration::from_millis(100), body.next())
        .await
        .expect("first SSE frame must arrive before upstream completes")
        .expect("first frame")
        .unwrap();
    let mut stream_text = String::from_utf8(first.to_vec()).unwrap();
    assert!(stream_text.contains("\"content\":\"OK\""));
    while let Some(chunk) = body.next().await {
        let chunk = chunk.unwrap();
        stream_text.push_str(std::str::from_utf8(&chunk).unwrap());
    }
    assert!(stream_text.contains("\"cached_tokens\":4"));
    assert!(stream_text.ends_with("data: [DONE]\n\n"));

    let stats = gateway
        .clone()
        .oneshot(
            Request::builder()
                .uri("/admin/api/stats?range=24h")
                .header(header::AUTHORIZATION, format!("Bearer {ADMIN_TOKEN}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(stats.status(), StatusCode::OK);
    let stats = response_json(stats).await;
    assert_eq!(stats["summary"]["requests"], 2);
    assert_eq!(stats["summary"]["input_tokens"], 12);
    assert_eq!(stats["summary"]["output_tokens"], 3);
    assert_eq!(stats["summary"]["cache_tokens"], 7);
    assert_eq!(stats["models"][0]["model"], "tracked/model-a");
    assert_eq!(stats["models"][0]["active"], true);

    let protected = gateway
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/admin/api/stats/models?model=tracked%2Fmodel-a")
                .header(header::AUTHORIZATION, format!("Bearer {ADMIN_TOKEN}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(protected.status(), StatusCode::CONFLICT);

    let removed_provider = gateway
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/admin/api/providers/tracked")
                .header(header::AUTHORIZATION, format!("Bearer {ADMIN_TOKEN}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(removed_provider.status(), StatusCode::NO_CONTENT);

    let archived = gateway
        .clone()
        .oneshot(
            Request::builder()
                .uri("/admin/api/stats?range=24h")
                .header(header::AUTHORIZATION, format!("Bearer {ADMIN_TOKEN}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let archived = response_json(archived).await;
    assert_eq!(archived["models"][0]["active"], false);

    let deleted_usage = gateway
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/admin/api/stats/models?model=tracked%2Fmodel-a")
                .header(header::AUTHORIZATION, format!("Bearer {ADMIN_TOKEN}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(deleted_usage.status(), StatusCode::NO_CONTENT);

    let empty = gateway
        .oneshot(
            Request::builder()
                .uri("/admin/api/stats?range=24h")
                .header(header::AUTHORIZATION, format!("Bearer {ADMIN_TOKEN}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let empty = response_json(empty).await;
    assert_eq!(empty["summary"]["requests"], 0);
    assert!(empty["models"].as_array().unwrap().is_empty());
    upstream_task.abort();
}

#[tokio::test]
async fn bare_origin_base_url_discovers_and_proxies_under_v1() {
    let upstream = Router::new()
        .route(
            "/v1/models",
            get(|| async { Json(json!({"data": [{"id": "model-a"}]})) }),
        )
        .route(
            "/v1/chat/completions",
            post(|| async {
                Json(json!({
                    "id": "chat_1", "object": "chat.completion", "model": "model-a",
                    "choices": [{"index": 0, "message": {"role": "assistant", "content": "hello"}, "finish_reason": "stop"}]
                }))
            }),
        );
    let (upstream_address, upstream_task) = spawn(upstream).await;
    let directory = TempDir::new().unwrap();
    let store = test_store(&directory).await;
    let gateway = app::router(test_config(&directory.path().join("northgate.db")), store).unwrap();

    let saved = gateway
        .clone()
        .oneshot(json_request(
            "POST",
            "/admin/api/providers",
            ADMIN_TOKEN,
            json!({
                "name": "Bare", "base_url": format!("http://{upstream_address}"),
                "api_key": PROVIDER_KEY, "protocol": "openai",
                "models": [{"id": "model-a"}]
            }),
        ))
        .await
        .unwrap();
    assert_eq!(saved.status(), StatusCode::CREATED);

    let chat = gateway
        .oneshot(json_request(
            "POST",
            "/v1/chat/completions",
            GATEWAY_KEY,
            json!({"model": "bare/model-a", "messages": [{"role": "user", "content": "hi"}]}),
        ))
        .await
        .unwrap();
    assert_eq!(chat.status(), StatusCode::OK);
    assert_eq!(
        response_json(chat).await["choices"][0]["message"]["content"],
        "hello"
    );
    upstream_task.abort();
}

#[tokio::test]
async fn inbound_concurrency_limit_sheds_load_with_503() {
    let upstream = Router::new().route(
        "/v1/chat/completions",
        post(|| async {
            tokio::time::sleep(Duration::from_secs(10)).await;
            StatusCode::OK
        }),
    );
    let (upstream_address, upstream_task) = spawn(upstream).await;
    let directory = TempDir::new().unwrap();
    let store = test_store(&directory).await;
    store
        .save_provider(
            NewProvider::new(
                "Slow",
                format!("http://{upstream_address}/v1"),
                PROVIDER_KEY,
                UpstreamProtocol::OpenAi,
                vec![ModelMetadata {
                    upstream_id: "model-a".to_owned(),
                    display_name: None,
                    context_window: None,
                    max_output_tokens: None,
                    capabilities: ModelCapabilities::default(),
                }],
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let mut config = (*test_config(&directory.path().join("northgate.db"))).clone();
    config.max_inbound_concurrency = 1;
    let gateway = app::router(Arc::new(config), store).unwrap();

    let blocked = gateway.clone();
    let holder = tokio::spawn(async move {
        blocked
            .oneshot(json_request(
                "POST",
                "/v1/chat/completions",
                GATEWAY_KEY,
                json!({"model": "slow/model-a", "messages": []}),
            ))
            .await
            .unwrap()
    });
    tokio::time::sleep(Duration::from_millis(100)).await;

    let shed = gateway
        .clone()
        .oneshot(
            Request::builder()
                .uri("/v1/models")
                .header(header::AUTHORIZATION, format!("Bearer {GATEWAY_KEY}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(shed.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(shed.headers()["x-northgate-error-source"], "NORTHGATE");
    assert_eq!(response_json(shed).await["error"]["code"], "gateway_busy");

    let health = gateway
        .oneshot(
            Request::builder()
                .uri("/healthz")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(health.status(), StatusCode::OK);
    holder.abort();
    upstream_task.abort();
}
