use std::{
    collections::{BTreeMap, HashMap, HashSet},
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use axum::{
    Json, Router,
    body::Body,
    extract::{DefaultBodyLimit, Path, Query, Request, State, rejection::JsonRejection},
    http::{HeaderMap, HeaderName, HeaderValue, StatusCode, header},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{delete, get, post, put},
};
use bytes::{Bytes, BytesMut};
use futures_util::StreamExt;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::sync::{OwnedSemaphorePermit, RwLock, Semaphore};
use tower::limit::ConcurrencyLimitLayer;
use tower_http::{timeout::RequestBodyTimeoutLayer, trace::TraceLayer};
use zeroize::Zeroize;

use crate::{
    auth::require_bearer,
    config::Config,
    discovery::{DiscoveredModel, DiscoveryError, discover_models, enrich_models},
    error::AppError,
    protocol::{self, UpstreamProtocol},
    store::{
        MAX_FALLBACK_API_KEYS, MAX_MODELS_PER_PROVIDER, ModelCapabilities, ModelMetadata,
        ModelUsage, NewProvider, ProviderSummary, ResolvedModel, Store, StoreError, TokenUsage,
        UsageTotals,
    },
    web,
};

const MAX_SUCCESS_BODY: usize = 8 * 1024 * 1024;
const MAX_ERROR_BODY: usize = 256 * 1024;
const MAX_REQUEST_BODY: usize = 4 * 1024 * 1024;
const CLIENT_CACHE_TTL: Duration = Duration::from_secs(300);
const MAX_CACHED_ORIGINS: usize = 128;

#[derive(Clone)]
struct CachedClient {
    expires_at: Instant,
    client: Client,
}

#[derive(Clone)]
pub struct AppState {
    config: Arc<Config>,
    store: Store,
    clients: Arc<RwLock<HashMap<String, CachedClient>>>,
    upstream_slots: Arc<Semaphore>,
}

pub fn router(config: Arc<Config>, store: Store) -> anyhow::Result<Router> {
    let inbound_limit = config.max_inbound_concurrency;
    let request_body_timeout = config.request_body_timeout;
    let state = AppState {
        upstream_slots: Arc::new(Semaphore::new(config.max_upstream_concurrency)),
        config,
        store,
        clients: Arc::new(RwLock::new(HashMap::new())),
    };
    let admin = Router::new()
        .route("/admin/api/health", get(admin_health))
        .route("/admin/api/discover", post(discover))
        .route("/admin/api/models/enrich", post(enrich_selected))
        .route("/admin/api/models/test", post(test_model))
        .route("/admin/api/stats", get(usage_stats))
        .route("/admin/api/stats/models", delete(delete_model_stats))
        .route(
            "/admin/api/providers",
            get(list_providers).post(save_provider),
        )
        .route("/admin/api/providers/{slug}", delete(delete_provider))
        .route(
            "/admin/api/providers/{slug}/enabled",
            put(set_provider_enabled),
        )
        .route(
            "/admin/api/providers/{slug}/discover",
            post(discover_provider_models),
        )
        .route(
            "/admin/api/providers/{slug}/models",
            post(add_provider_models).delete(delete_provider_model),
        )
        .route(
            "/admin/api/providers/{slug}/fallback-keys/{position}",
            put(set_fallback_api_key).delete(delete_fallback_api_key),
        )
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            require_admin_before_body,
        ));
    let gateway = Router::new()
        .route("/v1/models", get(list_models))
        .route("/v1/chat/completions", post(chat_completions))
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            require_gateway_before_body,
        ));
    Ok(Router::new()
        .route("/", get(web::index))
        .route("/dashboard", get(web::dashboard))
        .route("/app.js", get(web::javascript))
        .route("/dashboard.js", get(web::dashboard_javascript))
        .route("/style.css", get(web::stylesheet))
        .route("/healthz", get(health))
        .merge(admin)
        .merge(gateway)
        .layer(DefaultBodyLimit::max(MAX_REQUEST_BODY))
        .layer(RequestBodyTimeoutLayer::new(request_body_timeout))
        .layer(ConcurrencyLimitLayer::new(inbound_limit))
        .layer(TraceLayer::new_for_http())
        .with_state(state))
}

async fn require_admin_before_body(
    State(state): State<AppState>,
    request: Request,
    next: Next,
) -> Result<Response, AppError> {
    require_bearer(request.headers(), &state.config.admin_token)?;
    Ok(next.run(request).await)
}

async fn require_gateway_before_body(
    State(state): State<AppState>,
    request: Request,
    next: Next,
) -> Result<Response, AppError> {
    require_bearer(request.headers(), &state.config.gateway_key)?;
    Ok(next.run(request).await)
}

async fn health() -> Json<Value> {
    Json(json!({"status": "ok", "service": "northgate"}))
}

async fn admin_health(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Value>, AppError> {
    require_bearer(&headers, &state.config.admin_token)?;
    Ok(Json(json!({
        "status": "ok",
        "upstream_capacity": state.config.max_upstream_concurrency,
        "upstream_available": state.upstream_slots.available_permits(),
        "first_byte_timeout_seconds": state.config.first_byte_timeout.as_secs(),
        "stream_idle_timeout_seconds": state.config.stream_idle_timeout.map(|value| value.as_secs())
    })))
}

#[derive(Deserialize)]
struct DiscoverRequest {
    base_url: String,
    api_key: String,
    protocol: UpstreamProtocol,
}

impl Drop for DiscoverRequest {
    fn drop(&mut self) {
        self.api_key.zeroize();
    }
}

async fn discover(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<DiscoverRequest>,
) -> Result<Json<Value>, AppError> {
    require_bearer(&headers, &state.config.admin_token)?;
    validate_api_key(&request.api_key)?;
    let _permit = Arc::clone(&state.upstream_slots)
        .try_acquire_owned()
        .map_err(|_| AppError::Busy)?;
    let client = upstream_client(&state, &request.base_url).await?;
    let models = discover_models(
        &client,
        request.protocol,
        &request.base_url,
        &request.api_key,
        state.config.first_byte_timeout,
        state.config.body_idle_timeout,
    )
    .await
    .map_err(map_discovery_error)?;
    Ok(Json(json!({"models": models})))
}

#[derive(Deserialize)]
struct SaveProviderRequest {
    name: String,
    base_url: String,
    api_key: String,
    #[serde(default)]
    fallback_api_keys: Vec<String>,
    protocol: UpstreamProtocol,
    models: Vec<SaveModelRequest>,
}

impl Drop for SaveProviderRequest {
    fn drop(&mut self) {
        self.api_key.zeroize();
        for api_key in &mut self.fallback_api_keys {
            api_key.zeroize();
        }
    }
}

#[derive(Deserialize)]
struct SaveModelRequest {
    id: String,
}

#[derive(Deserialize)]
struct EnrichRequest {
    base_url: String,
    api_key: String,
    protocol: UpstreamProtocol,
    model_ids: Vec<String>,
}

impl Drop for EnrichRequest {
    fn drop(&mut self) {
        self.api_key.zeroize();
    }
}

async fn enrich_selected(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(mut request): Json<EnrichRequest>,
) -> Result<Json<Value>, AppError> {
    require_bearer(&headers, &state.config.admin_token)?;
    validate_api_key(&request.api_key)?;
    let models = authoritative_selected_models(
        &state,
        request.protocol,
        &request.base_url,
        &request.api_key,
        std::mem::take(&mut request.model_ids),
    )
    .await?;
    Ok(Json(json!({"models": models})))
}

async fn save_provider(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(mut request): Json<SaveProviderRequest>,
) -> Result<(StatusCode, Json<ProviderInventory>), AppError> {
    require_bearer(&headers, &state.config.admin_token)?;
    validate_api_key(&request.api_key)?;
    if request.fallback_api_keys.len() > MAX_FALLBACK_API_KEYS {
        return Err(AppError::Invalid(format!(
            "at most {MAX_FALLBACK_API_KEYS} fallback API keys are allowed"
        )));
    }
    let mut unique_keys = HashSet::with_capacity(request.fallback_api_keys.len() + 1);
    unique_keys.insert(request.api_key.as_str());
    for api_key in &request.fallback_api_keys {
        validate_api_key(api_key)?;
        if !unique_keys.insert(api_key.as_str()) {
            return Err(AppError::Conflict(
                "provider API keys must be unique".to_owned(),
            ));
        }
    }
    let selected = authoritative_selected_models(
        &state,
        request.protocol,
        &request.base_url,
        &request.api_key,
        request
            .models
            .iter()
            .map(|model| model.id.clone())
            .collect(),
    )
    .await?;
    let models = selected
        .into_iter()
        .map(|model| ModelMetadata {
            upstream_id: model.id,
            display_name: Some(model.display_name),
            context_window: model.context_window,
            max_output_tokens: model.max_output_tokens,
            capabilities: ModelCapabilities {
                tools: model.capabilities.tools.unwrap_or(false),
                vision: model.capabilities.vision.unwrap_or(false),
                reasoning: false,
                streaming: model.capabilities.streaming.unwrap_or(true),
            },
        })
        .collect();
    let provider = NewProvider::new(
        std::mem::take(&mut request.name),
        std::mem::take(&mut request.base_url),
        std::mem::take(&mut request.api_key),
        request.protocol,
        models,
    )
    .and_then(|provider| {
        provider.with_fallback_api_keys(std::mem::take(&mut request.fallback_api_keys))
    })
    .map_err(map_store_error)?;
    let saved = state
        .store
        .save_provider(provider)
        .await
        .map_err(map_store_error)?;
    let inventory = inventory_for_provider(&state.store, saved).await?;
    Ok((StatusCode::CREATED, Json(inventory)))
}

#[derive(Deserialize)]
struct AddProviderModelsRequest {
    models: Vec<SaveModelRequest>,
}

async fn discover_provider_models(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(slug): Path<String>,
) -> Result<Json<Value>, AppError> {
    require_bearer(&headers, &state.config.admin_token)?;
    let provider = state
        .store
        .find_provider(&slug)
        .await
        .map_err(map_store_error)?;
    let _permit = Arc::clone(&state.upstream_slots)
        .try_acquire_owned()
        .map_err(|_| AppError::Busy)?;
    let client = upstream_client(&state, &provider.base_url).await?;
    let models = discover_models(
        &client,
        provider.protocol,
        &provider.base_url,
        provider.api_key.expose_secret(),
        state.config.first_byte_timeout,
        state.config.body_idle_timeout,
    )
    .await
    .map_err(map_discovery_error)?;
    Ok(Json(json!({"models": models})))
}

async fn add_provider_models(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(slug): Path<String>,
    Json(request): Json<AddProviderModelsRequest>,
) -> Result<Json<ProviderInventory>, AppError> {
    require_bearer(&headers, &state.config.admin_token)?;
    let provider = state
        .store
        .find_provider(&slug)
        .await
        .map_err(map_store_error)?;
    let selected = authoritative_selected_models(
        &state,
        provider.protocol,
        &provider.base_url,
        provider.api_key.expose_secret(),
        request.models.into_iter().map(|model| model.id).collect(),
    )
    .await?;
    let models = selected
        .into_iter()
        .map(|model| ModelMetadata {
            upstream_id: model.id,
            display_name: Some(model.display_name),
            context_window: model.context_window,
            max_output_tokens: model.max_output_tokens,
            capabilities: ModelCapabilities {
                tools: model.capabilities.tools.unwrap_or(false),
                vision: model.capabilities.vision.unwrap_or(false),
                reasoning: false,
                streaming: model.capabilities.streaming.unwrap_or(true),
            },
        })
        .collect();
    state
        .store
        .add_models(&slug, models)
        .await
        .map_err(|error| match error {
            StoreError::Conflict => {
                AppError::Conflict("one or more selected models are already registered".to_owned())
            }
            error => map_store_error(error),
        })?;
    let provider = state
        .store
        .list_providers()
        .await
        .map_err(map_store_error)?
        .into_iter()
        .find(|provider| provider.slug == slug)
        .ok_or(AppError::NotFound)?;
    Ok(Json(inventory_for_provider(&state.store, provider).await?))
}

#[derive(Deserialize)]
struct DeleteProviderModelQuery {
    model: String,
}

async fn delete_provider_model(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(slug): Path<String>,
    Query(query): Query<DeleteProviderModelQuery>,
) -> Result<StatusCode, AppError> {
    require_bearer(&headers, &state.config.admin_token)?;
    if state
        .store
        .delete_model(&slug, &query.model)
        .await
        .map_err(map_store_error)?
    {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err(AppError::NotFound)
    }
}

async fn authoritative_selected_models(
    state: &AppState,
    protocol: UpstreamProtocol,
    base_url: &str,
    api_key: &str,
    model_ids: Vec<String>,
) -> Result<Vec<DiscoveredModel>, AppError> {
    if model_ids.is_empty() || model_ids.len() > MAX_MODELS_PER_PROVIDER {
        return Err(AppError::Invalid(format!(
            "select between 1 and {MAX_MODELS_PER_PROVIDER} models"
        )));
    }
    let _permit = Arc::clone(&state.upstream_slots)
        .try_acquire_owned()
        .map_err(|_| AppError::Busy)?;
    let selected = model_ids.into_iter().collect::<HashSet<_>>();
    let client = upstream_client(state, base_url).await?;
    let discovered = discover_models(
        &client,
        protocol,
        base_url,
        api_key,
        state.config.first_byte_timeout,
        state.config.body_idle_timeout,
    )
    .await
    .map_err(map_discovery_error)?;
    let mut models = discovered
        .into_iter()
        .filter(|model| selected.contains(&model.id))
        .collect::<Vec<_>>();
    if models.len() != selected.len() {
        return Err(AppError::Invalid(
            "one or more selected models are no longer exposed by the provider".to_owned(),
        ));
    }
    enrich_models(
        &client,
        protocol,
        base_url,
        api_key,
        &mut models,
        state.config.first_byte_timeout,
        state.config.body_idle_timeout,
    )
    .await
    .map_err(map_discovery_error)?;
    Ok(models)
}

#[derive(Serialize)]
struct ProviderInventory {
    #[serde(flatten)]
    provider: ProviderSummary,
    models: Vec<InventoryModel>,
}

#[derive(Serialize)]
struct InventoryModel {
    id: String,
    public_id: String,
    display_name: Option<String>,
    context_window: Option<u64>,
    max_output_tokens: Option<u64>,
    capabilities: ModelCapabilities,
}

async fn list_providers(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Value>, AppError> {
    require_bearer(&headers, &state.config.admin_token)?;
    let providers = state
        .store
        .list_providers()
        .await
        .map_err(map_store_error)?;
    let models = state.store.list_models().await.map_err(map_store_error)?;
    let mut by_provider = BTreeMap::<String, Vec<InventoryModel>>::new();
    for model in models {
        by_provider
            .entry(model.provider_slug.clone())
            .or_default()
            .push(InventoryModel {
                id: model.upstream_id,
                public_id: model.alias,
                display_name: model.display_name,
                context_window: model.context_window,
                max_output_tokens: model.max_output_tokens,
                capabilities: model.capabilities,
            });
    }
    let inventory = providers
        .into_iter()
        .map(|provider| ProviderInventory {
            models: by_provider.remove(&provider.slug).unwrap_or_default(),
            provider,
        })
        .collect::<Vec<_>>();
    Ok(Json(json!({"providers": inventory})))
}

async fn inventory_for_provider(
    store: &Store,
    provider: ProviderSummary,
) -> Result<ProviderInventory, AppError> {
    let models = store
        .list_models()
        .await
        .map_err(map_store_error)?
        .into_iter()
        .filter(|model| model.provider_slug == provider.slug)
        .map(|model| InventoryModel {
            id: model.upstream_id,
            public_id: model.alias,
            display_name: model.display_name,
            context_window: model.context_window,
            max_output_tokens: model.max_output_tokens,
            capabilities: model.capabilities,
        })
        .collect();
    Ok(ProviderInventory { provider, models })
}

async fn delete_provider(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(slug): Path<String>,
) -> Result<StatusCode, AppError> {
    require_bearer(&headers, &state.config.admin_token)?;
    if state
        .store
        .delete_provider(&slug)
        .await
        .map_err(map_store_error)?
    {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err(AppError::NotFound)
    }
}

#[derive(Deserialize)]
struct SetProviderEnabledRequest {
    enabled: bool,
}

async fn set_provider_enabled(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(slug): Path<String>,
    Json(request): Json<SetProviderEnabledRequest>,
) -> Result<StatusCode, AppError> {
    require_bearer(&headers, &state.config.admin_token)?;
    if state
        .store
        .set_provider_enabled(&slug, request.enabled)
        .await
        .map_err(map_store_error)?
    {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err(AppError::NotFound)
    }
}

#[derive(Deserialize)]
struct SetFallbackApiKeyRequest {
    api_key: String,
}

impl Drop for SetFallbackApiKeyRequest {
    fn drop(&mut self) {
        self.api_key.zeroize();
    }
}

async fn set_fallback_api_key(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((slug, position)): Path<(String, usize)>,
    Json(mut request): Json<SetFallbackApiKeyRequest>,
) -> Result<StatusCode, AppError> {
    require_bearer(&headers, &state.config.admin_token)?;
    validate_api_key(&request.api_key)?;
    match state
        .store
        .set_fallback_api_key(&slug, position, Some(std::mem::take(&mut request.api_key)))
        .await
    {
        Ok(()) => Ok(StatusCode::NO_CONTENT),
        Err(StoreError::NotFound) => Err(AppError::NotFound),
        Err(StoreError::Conflict) => Err(AppError::Conflict(
            "fallback API key duplicates another key for this provider".to_owned(),
        )),
        Err(StoreError::InvalidInput) => Err(AppError::Invalid(format!(
            "fallback key position must be between 1 and {MAX_FALLBACK_API_KEYS}; slot 1 must exist before slot 2"
        ))),
        Err(error) => Err(map_store_error(error)),
    }
}

async fn delete_fallback_api_key(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((slug, position)): Path<(String, usize)>,
) -> Result<StatusCode, AppError> {
    require_bearer(&headers, &state.config.admin_token)?;
    match state
        .store
        .set_fallback_api_key(&slug, position, None)
        .await
    {
        Ok(()) => Ok(StatusCode::NO_CONTENT),
        Err(StoreError::NotFound) => Err(AppError::NotFound),
        Err(StoreError::InvalidInput) => Err(AppError::Invalid(format!(
            "fallback key position must be between 1 and {MAX_FALLBACK_API_KEYS}"
        ))),
        Err(error) => Err(map_store_error(error)),
    }
}

#[derive(Serialize)]
struct OpenAiModel {
    id: String,
    object: &'static str,
    created: u64,
    owned_by: String,
    context_window: Option<u64>,
    max_output_tokens: Option<u64>,
    capabilities: ModelCapabilities,
}

async fn list_models(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Value>, AppError> {
    require_bearer(&headers, &state.config.gateway_key)?;
    let models = state
        .store
        .list_enabled_models()
        .await
        .map_err(map_store_error)?
        .into_iter()
        .map(|model| OpenAiModel {
            id: model.alias,
            object: "model",
            created: 0,
            owned_by: model.provider_slug,
            context_window: model.context_window,
            max_output_tokens: model.max_output_tokens,
            capabilities: model.capabilities,
        })
        .collect::<Vec<_>>();
    Ok(Json(json!({"object": "list", "data": models})))
}

async fn chat_completions(
    State(state): State<AppState>,
    headers: HeaderMap,
    request: Result<Json<Value>, JsonRejection>,
) -> Result<Response, AppError> {
    require_bearer(&headers, &state.config.gateway_key)?;
    let Json(request) = request.map_err(map_chat_json_rejection)?;
    execute_chat(state, request).await
}

fn map_chat_json_rejection(rejection: JsonRejection) -> AppError {
    let status = rejection.status();
    let detail = rejection.body_text();
    let (kind, error) = match rejection {
        JsonRejection::JsonSyntaxError(_) => (
            "syntax",
            AppError::Invalid("request body contains malformed JSON".to_owned()),
        ),
        JsonRejection::JsonDataError(_) => (
            "data",
            AppError::Invalid("request JSON could not be deserialized".to_owned()),
        ),
        JsonRejection::MissingJsonContentType(_) => {
            ("content_type", AppError::UnsupportedMediaType)
        }
        _ if status == StatusCode::PAYLOAD_TOO_LARGE => ("body_limit", AppError::PayloadTooLarge),
        JsonRejection::BytesRejection(_) => ("body_read", AppError::RequestBodyIncomplete),
        _ => (
            "other",
            AppError::Invalid("request body could not be parsed".to_owned()),
        ),
    };
    tracing::warn!(%status, rejection = kind, detail = %detail, "chat request rejected during JSON extraction");
    error
}

#[derive(Deserialize)]
struct TestModelRequest {
    model: String,
    #[serde(default)]
    stream: bool,
}

async fn test_model(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<TestModelRequest>,
) -> Result<Response, AppError> {
    require_bearer(&headers, &state.config.admin_token)?;
    let started = Instant::now();
    let chat_request = json!({
        "model": request.model,
        "messages": [{"role": "user", "content": "Reply with OK."}],
        "max_tokens": 16,
        "stream": request.stream,
        "stream_options": {"include_usage": true}
    });
    let mut response = execute_chat(state, chat_request).await?;
    let elapsed = started.elapsed().as_millis().min(u128::from(u64::MAX));
    if let Ok(value) = HeaderValue::from_str(&elapsed.to_string()) {
        response.headers_mut().insert(
            HeaderName::from_static("x-northgate-test-latency-ms"),
            value,
        );
    }
    Ok(response)
}

#[derive(Default, Deserialize)]
struct StatsQuery {
    range: Option<String>,
}

#[derive(Deserialize)]
struct DeleteModelStatsQuery {
    model: String,
}

#[derive(Serialize)]
struct DashboardModelUsage {
    #[serde(flatten)]
    usage: ModelUsage,
    active: bool,
    enabled: bool,
}

async fn delete_model_stats(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<DeleteModelStatsQuery>,
) -> Result<StatusCode, AppError> {
    require_bearer(&headers, &state.config.admin_token)?;
    match state.store.delete_model_usage(&query.model).await {
        Ok(true) => Ok(StatusCode::NO_CONTENT),
        Ok(false) | Err(StoreError::NotFound) => Err(AppError::NotFound),
        Err(StoreError::Conflict) => Err(AppError::Conflict(
            "model is still active in the registry; remove its provider first".to_owned(),
        )),
        Err(error) => Err(map_store_error(error)),
    }
}

async fn usage_stats(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<StatsQuery>,
) -> Result<Json<Value>, AppError> {
    require_bearer(&headers, &state.config.admin_token)?;
    let now = unix_seconds()?;
    let (range, duration, series_bucket) = match query.range.as_deref().unwrap_or("24h") {
        "24h" => ("24h", 24 * 60 * 60, 60 * 60),
        "7d" => ("7d", 7 * 24 * 60 * 60, 6 * 60 * 60),
        "30d" => ("30d", 30 * 24 * 60 * 60, 24 * 60 * 60),
        "90d" => ("90d", 90 * 24 * 60 * 60, 24 * 60 * 60),
        _ => {
            return Err(AppError::Invalid(
                "range must be 24h, 7d, 30d, or 90d".to_owned(),
            ));
        }
    };
    let mut stats = state
        .store
        .usage_stats(now.saturating_sub(duration), series_bucket)
        .await
        .map_err(map_store_error)?;
    let catalog = state.store.list_models().await.map_err(map_store_error)?;
    let enabled_aliases = state
        .store
        .list_enabled_models()
        .await
        .map_err(map_store_error)?
        .into_iter()
        .map(|model| model.alias)
        .collect::<HashSet<_>>();
    let active_aliases = catalog
        .iter()
        .map(|model| model.alias.clone())
        .collect::<HashSet<_>>();
    let known = stats
        .models
        .iter()
        .map(|model| model.model.as_str())
        .collect::<HashSet<_>>();
    let missing = catalog
        .iter()
        .filter(|model| !known.contains(model.alias.as_str()))
        .map(|model| crate::store::ModelUsage {
            model: model.alias.clone(),
            provider: model.provider_slug.clone(),
            totals: UsageTotals::default(),
        })
        .collect::<Vec<_>>();
    stats.models.extend(missing);
    stats.models.sort_by(|left, right| {
        right
            .totals
            .requests
            .cmp(&left.totals.requests)
            .then_with(|| left.model.cmp(&right.model))
    });
    let models = stats
        .models
        .into_iter()
        .map(|usage| DashboardModelUsage {
            active: active_aliases.contains(&usage.model),
            enabled: enabled_aliases.contains(&usage.model),
            usage,
        })
        .collect::<Vec<_>>();
    Ok(Json(json!({
        "range": range,
        "generated_at": now,
        "summary": stats.summary,
        "series": stats.series,
        "models": models,
        "token_semantics": "provider-reported; cached input is excluded from input_tokens"
    })))
}

fn unix_seconds() -> Result<i64, AppError> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| AppError::Internal)
        .and_then(|duration| i64::try_from(duration.as_secs()).map_err(|_| AppError::Internal))
}

async fn execute_chat(state: AppState, request: Value) -> Result<Response, AppError> {
    let public_model = request
        .get("model")
        .and_then(Value::as_str)
        .filter(|model| !model.is_empty() && model.len() <= 1024)
        .ok_or_else(|| AppError::Invalid("model is required".to_owned()))?
        .to_owned();
    let resolved = state
        .store
        .find_model(&public_model)
        .await
        .map_err(map_store_error)?;
    record_usage(
        &state.store,
        &resolved.model.provider_slug,
        &public_model,
        1,
        0,
        TokenUsage::default(),
    )
    .await;
    let stats_store = state.store.clone();
    let provider_slug = resolved.model.provider_slug.clone();
    let stats_model = public_model.clone();
    let result = execute_resolved_chat(state, request, public_model, resolved).await;
    if result.as_ref().is_err_and(AppError::is_provider_failure) {
        record_usage(
            &stats_store,
            &provider_slug,
            &stats_model,
            0,
            1,
            TokenUsage::default(),
        )
        .await;
    }
    result
}

async fn execute_resolved_chat(
    state: AppState,
    request: Value,
    public_model: String,
    resolved: ResolvedModel,
) -> Result<Response, AppError> {
    let permit = Arc::clone(&state.upstream_slots)
        .try_acquire_owned()
        .map_err(|_| AppError::Busy)?;
    let ResolvedModel {
        model,
        base_url,
        protocol,
        api_key,
        fallback_api_keys,
        ..
    } = resolved;
    let preview = protocol::build_upstream_request(
        &base_url,
        protocol,
        &model.upstream_id,
        api_key.expose_secret(),
        &request,
    )
    .map_err(|error| AppError::Invalid(error.to_string()))?;
    let client = upstream_client(&state, &preview.url).await?;
    let mut keys = Vec::with_capacity(fallback_api_keys.len() + 1);
    keys.push(api_key);
    keys.extend(fallback_api_keys);
    let mut keys = keys.into_iter().enumerate();
    let mut failures = Vec::new();
    let mut all_failures_are_timeouts = true;

    while let Some((key_index, api_key)) = keys.next() {
        let attempt = send_provider_attempt(
            &client,
            &api_key,
            ProviderAttemptConfig {
                base_url: &base_url,
                protocol,
                upstream_model: &model.upstream_id,
                request: &request,
                first_byte_timeout: state.config.first_byte_timeout,
                body_idle_timeout: state.config.body_idle_timeout,
            },
        )
        .await;
        let (response, upstream) = match attempt {
            Ok(success) => success,
            Err(AttemptFailure::Northgate(error)) => return Err(error),
            Err(AttemptFailure::Provider(failure)) => {
                all_failures_are_timeouts &= failure.timeout;
                failures.push(format!("{}: {}", attempt_label(key_index), failure.message));
                continue;
            }
        };

        if upstream.stream {
            return Ok(streaming_response(
                response,
                protocol,
                upstream.include_usage,
                state.config.stream_idle_timeout,
                permit,
                StreamStats {
                    store: state.store,
                    provider_slug: model.provider_slug,
                    model_alias: public_model,
                },
                StreamRetry {
                    client,
                    base_url,
                    upstream_model: model.upstream_id,
                    request,
                    api_keys: keys.collect(),
                    first_byte_timeout: state.config.first_byte_timeout,
                    body_idle_timeout: state.config.body_idle_timeout,
                    failures,
                    current_key_index: key_index,
                },
            ));
        }

        match convert_non_stream_response(
            response,
            protocol,
            &public_model,
            state.config.body_idle_timeout,
        )
        .await
        {
            Ok((value, usage)) => {
                drop(permit);
                if let Some(usage) = usage {
                    record_usage(
                        &state.store,
                        &model.provider_slug,
                        &public_model,
                        0,
                        0,
                        usage,
                    )
                    .await;
                }
                return Ok((StatusCode::OK, Json(value)).into_response());
            }
            Err(failure) => {
                all_failures_are_timeouts &= failure.timeout;
                failures.push(format!("{}: {}", attempt_label(key_index), failure.message));
            }
        }
    }

    let message = provider_attempts_failed(&failures);
    if all_failures_are_timeouts {
        Err(AppError::UpstreamTimeout(message))
    } else {
        Err(AppError::Upstream(message))
    }
}

enum AttemptFailure {
    Northgate(AppError),
    Provider(ProviderAttemptFailure),
}

struct ProviderAttemptFailure {
    message: String,
    timeout: bool,
}

#[derive(Clone, Copy)]
struct ProviderAttemptConfig<'a> {
    base_url: &'a str,
    protocol: UpstreamProtocol,
    upstream_model: &'a str,
    request: &'a Value,
    first_byte_timeout: Duration,
    body_idle_timeout: Duration,
}

fn attempt_label(index: usize) -> String {
    if index == 0 {
        "primary".to_owned()
    } else {
        format!("fallback {index}")
    }
}

fn provider_attempts_failed(failures: &[String]) -> String {
    format!(
        "all provider attempts failed ({}): {}",
        failures.len(),
        failures.join(" | ")
    )
}

fn provider_error_detail(bytes: &[u8], api_key: &str) -> Option<String> {
    let parsed = serde_json::from_slice::<Value>(bytes).ok();
    let candidate = parsed
        .as_ref()
        .and_then(|value| {
            value
                .pointer("/error/message")
                .or_else(|| value.get("message"))
                .or_else(|| value.get("detail"))
                .or_else(|| value.get("error"))
        })
        .and_then(Value::as_str)
        .map(str::to_owned)
        .or_else(|| std::str::from_utf8(bytes).ok().map(str::to_owned))?;
    let compact = candidate.split_whitespace().collect::<Vec<_>>().join(" ");
    if compact.is_empty() {
        return None;
    }
    let redacted = compact.replace(api_key, "[REDACTED]");
    Some(redacted.chars().take(2_048).collect())
}

async fn send_provider_attempt(
    client: &Client,
    api_key: &crate::store::SecretString,
    config: ProviderAttemptConfig<'_>,
) -> Result<(reqwest::Response, protocol::UpstreamHttpRequest), AttemptFailure> {
    let upstream = protocol::build_upstream_request(
        config.base_url,
        config.protocol,
        config.upstream_model,
        api_key.expose_secret(),
        config.request,
    )
    .map_err(|error| AttemptFailure::Northgate(AppError::Invalid(error.to_string())))?;
    let mut builder = client.post(&upstream.url);
    for (name, value) in &upstream.headers {
        let name = HeaderName::from_bytes(name.as_bytes()).map_err(|_| {
            AttemptFailure::Northgate(AppError::Invalid("invalid upstream header".to_owned()))
        })?;
        let value = HeaderValue::from_str(value).map_err(|_| {
            AttemptFailure::Northgate(AppError::Invalid("invalid upstream credential".to_owned()))
        })?;
        builder = builder.header(name, value);
    }
    let response = tokio::time::timeout(
        config.first_byte_timeout,
        builder.json(&upstream.body).send(),
    )
    .await
    .map_err(|_| {
        AttemptFailure::Provider(ProviderAttemptFailure {
            message: format!(
                "timed out waiting {} seconds for response headers",
                config.first_byte_timeout.as_secs()
            ),
            timeout: true,
        })
    })?
    .map_err(|_| {
        AttemptFailure::Provider(ProviderAttemptFailure {
            message: "connection failed".to_owned(),
            timeout: false,
        })
    })?;
    if response.status().is_success() {
        return Ok((response, upstream));
    }

    let status = response.status();
    let detail = match collect_response(response, MAX_ERROR_BODY, config.body_idle_timeout).await {
        Ok(bytes) => provider_error_detail(&bytes, api_key.expose_secret()),
        Err(error) => Some(error.to_string()),
    };
    let message = detail
        .map(|detail| format!("HTTP {status}: {detail}"))
        .unwrap_or_else(|| format!("HTTP {status}"));
    Err(AttemptFailure::Provider(ProviderAttemptFailure {
        message,
        timeout: false,
    }))
}

async fn convert_non_stream_response(
    response: reqwest::Response,
    protocol: UpstreamProtocol,
    public_model: &str,
    body_idle_timeout: Duration,
) -> Result<(Value, Option<TokenUsage>), ProviderAttemptFailure> {
    let bytes = collect_response(response, MAX_SUCCESS_BODY, body_idle_timeout)
        .await
        .map_err(|error| ProviderAttemptFailure {
            timeout: matches!(error, AppError::Timeout | AppError::FirstByteTimeout(_)),
            message: error.to_string(),
        })?;
    let value: Value = serde_json::from_slice(&bytes).map_err(|_| ProviderAttemptFailure {
        message: "provider returned invalid JSON".to_owned(),
        timeout: false,
    })?;
    let usage = response_usage(protocol, &value);
    let value = match protocol {
        UpstreamProtocol::OpenAi => protocol::openai::passthrough_response(value, public_model),
        UpstreamProtocol::Anthropic => {
            protocol::anthropic::response_to_openai_chat(&value, public_model).map_err(|error| {
                ProviderAttemptFailure {
                    message: error.to_string(),
                    timeout: false,
                }
            })?
        }
        UpstreamProtocol::Responses => {
            protocol::responses::response_to_openai_chat(&value, public_model).map_err(|error| {
                ProviderAttemptFailure {
                    message: error.to_string(),
                    timeout: false,
                }
            })?
        }
    };
    Ok((value, usage))
}

fn response_usage(protocol: UpstreamProtocol, value: &Value) -> Option<TokenUsage> {
    let usage = value.get("usage")?;
    let (total_input, output_tokens, cache_tokens) = match protocol {
        UpstreamProtocol::OpenAi => (
            usage
                .get("prompt_tokens")
                .and_then(Value::as_u64)
                .or_else(|| usage.get("input_tokens").and_then(Value::as_u64))
                .unwrap_or(0),
            usage
                .get("completion_tokens")
                .and_then(Value::as_u64)
                .or_else(|| usage.get("output_tokens").and_then(Value::as_u64))
                .unwrap_or(0),
            usage["prompt_tokens_details"]["cached_tokens"]
                .as_u64()
                .or_else(|| usage["input_tokens_details"]["cached_tokens"].as_u64())
                .unwrap_or(0),
        ),
        UpstreamProtocol::Responses => (
            usage["input_tokens"].as_u64().unwrap_or(0),
            usage["output_tokens"].as_u64().unwrap_or(0),
            usage["input_tokens_details"]["cached_tokens"]
                .as_u64()
                .unwrap_or(0),
        ),
        UpstreamProtocol::Anthropic => {
            let input = usage["input_tokens"]
                .as_u64()
                .unwrap_or(0)
                .saturating_add(usage["cache_creation_input_tokens"].as_u64().unwrap_or(0));
            (
                input,
                usage["output_tokens"].as_u64().unwrap_or(0),
                usage["cache_read_input_tokens"].as_u64().unwrap_or(0),
            )
        }
    };
    Some(TokenUsage {
        input_tokens: total_input.saturating_sub(if protocol == UpstreamProtocol::Anthropic {
            0
        } else {
            cache_tokens
        }),
        output_tokens,
        cache_tokens,
    })
}

async fn record_usage(
    store: &Store,
    provider_slug: &str,
    model_alias: &str,
    requests: u64,
    failed_requests: u64,
    usage: TokenUsage,
) {
    if let Err(error) = store
        .record_usage(provider_slug, model_alias, requests, failed_requests, usage)
        .await
    {
        tracing::warn!(?error, "failed to record aggregate usage");
    }
}

async fn collect_response(
    response: reqwest::Response,
    maximum: usize,
    idle_timeout: std::time::Duration,
) -> Result<Vec<u8>, AppError> {
    if response
        .content_length()
        .is_some_and(|length| length > maximum as u64)
    {
        return Err(AppError::Upstream(
            "provider response exceeded limit".to_owned(),
        ));
    }
    let mut bytes = BytesMut::new();
    let mut stream = response.bytes_stream();
    while let Some(chunk) = tokio::time::timeout(idle_timeout, stream.next())
        .await
        .map_err(|_| AppError::Timeout)?
    {
        let chunk = chunk.map_err(|_| AppError::Upstream("provider body failed".to_owned()))?;
        if bytes.len().saturating_add(chunk.len()) > maximum {
            return Err(AppError::Upstream(
                "provider response exceeded limit".to_owned(),
            ));
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes.to_vec())
}

struct StreamStats {
    store: Store,
    provider_slug: String,
    model_alias: String,
}

struct StreamRetry {
    client: Client,
    base_url: String,
    upstream_model: String,
    request: Value,
    api_keys: Vec<(usize, crate::store::SecretString)>,
    first_byte_timeout: Duration,
    body_idle_timeout: Duration,
    failures: Vec<String>,
    current_key_index: usize,
}

fn streaming_response(
    response: reqwest::Response,
    protocol: UpstreamProtocol,
    include_usage: bool,
    idle_timeout: Option<std::time::Duration>,
    permit: OwnedSemaphorePermit,
    stats: StreamStats,
    retry: StreamRetry,
) -> Response {
    let stream = proxy_stream(
        response,
        protocol,
        include_usage,
        idle_timeout,
        permit,
        stats,
        retry,
    );
    (
        StatusCode::OK,
        [
            (header::CONTENT_TYPE, "text/event-stream; charset=utf-8"),
            (header::CACHE_CONTROL, "no-cache, no-transform"),
            (header::CONNECTION, "keep-alive"),
            (HeaderName::from_static("x-accel-buffering"), "no"),
        ],
        Body::from_stream(stream),
    )
        .into_response()
}

fn proxy_stream(
    response: reqwest::Response,
    protocol: UpstreamProtocol,
    include_usage: bool,
    idle_timeout: Option<std::time::Duration>,
    permit: OwnedSemaphorePermit,
    stats: StreamStats,
    retry: StreamRetry,
) -> impl futures_util::Stream<Item = Result<Bytes, std::io::Error>> {
    async_stream::stream! {
        let _permit = permit;
        let StreamRetry {
            client,
            base_url,
            upstream_model,
            request,
            api_keys,
            first_byte_timeout,
            body_idle_timeout,
            mut failures,
            mut current_key_index,
        } = retry;
        let mut remaining_keys = api_keys.into_iter();
        let mut current_response = response;

        let (usage, provider_failed) = 'attempt: loop {
            let mut upstream = current_response.bytes_stream();
            let mut converter = protocol::sse::OpenAiSseConverter::new(
                protocol,
                stats.model_alias.clone(),
                include_usage,
            );
            let mut emitted = false;
            let failure = loop {
                let next = if let Some(timeout) = idle_timeout {
                    tokio::time::timeout(timeout, upstream.next()).await
                } else {
                    Ok(upstream.next().await)
                };
                match next {
                    Ok(Some(Ok(chunk))) => match converter.push(&chunk) {
                        Ok(frames) => {
                            if converter.is_failed() {
                                break converter
                                    .failure_message()
                                    .unwrap_or("provider emitted a stream error")
                                    .to_owned();
                            }
                            for frame in frames {
                                emitted = true;
                                yield Ok(Bytes::from(frame));
                            }
                            if converter.is_done() {
                                break 'attempt (converter.usage(), false);
                            }
                        },
                        Err(error) => break error.to_string(),
                    },
                    Ok(Some(Err(_))) => break "upstream stream failed".to_owned(),
                    Ok(None) => match converter.finish() {
                        Ok(frames) => {
                            if converter.is_failed() {
                                break converter
                                    .failure_message()
                                    .unwrap_or("provider emitted a stream error")
                                    .to_owned();
                            }
                            for frame in frames {
                                yield Ok(Bytes::from(frame));
                            }
                            break 'attempt (converter.usage(), false);
                        },
                        Err(error) => break error.to_string(),
                    },
                    Err(_) => break "upstream stream idle timeout".to_owned(),
                }
            };

            let attempt_usage = converter.usage();
            failures.push(format!(
                "{}: {failure}",
                attempt_label(current_key_index)
            ));
            if emitted {
                let message = format!(
                    "{}; fallback stopped because stream output had already started",
                    provider_attempts_failed(&failures)
                );
                yield Ok(stream_error_frame(&message));
                break 'attempt (attempt_usage, true);
            }

            let mut next_response = None;
            for (key_index, api_key) in remaining_keys.by_ref() {
                match send_provider_attempt(
                    &client,
                    &api_key,
                    ProviderAttemptConfig {
                        base_url: &base_url,
                        protocol,
                        upstream_model: &upstream_model,
                        request: &request,
                        first_byte_timeout,
                        body_idle_timeout,
                    },
                ).await {
                    Ok((response, _)) => {
                        current_key_index = key_index;
                        next_response = Some(response);
                        break;
                    }
                    Err(AttemptFailure::Provider(failure)) => failures.push(format!(
                        "{}: {}",
                        attempt_label(key_index),
                        failure.message
                    )),
                    Err(AttemptFailure::Northgate(error)) => {
                        let message = if failures.is_empty() {
                            error.to_string()
                        } else {
                            format!(
                                "{} | fallback stopped by NORTHGATE: {error}",
                                provider_attempts_failed(&failures)
                            )
                        };
                        yield Ok(stream_error_frame_with_source("NORTHGATE", &message));
                        break 'attempt (attempt_usage, false);
                    }
                }
            }
            if let Some(response) = next_response {
                current_response = response;
                continue;
            }
            yield Ok(stream_error_frame(&provider_attempts_failed(&failures)));
            break 'attempt (attempt_usage, true);
        };

        let failed_requests = u64::from(provider_failed);
        if usage.is_some() || failed_requests != 0 {
            let usage = usage.unwrap_or_default();
            record_usage(
                &stats.store,
                &stats.provider_slug,
                &stats.model_alias,
                0,
                failed_requests,
                TokenUsage {
                    input_tokens: usage.input_tokens,
                    output_tokens: usage.output_tokens,
                    cache_tokens: usage.cache_tokens,
                },
            ).await;
        }
    }
}

fn stream_error_frame(message: &str) -> Bytes {
    stream_error_frame_with_source("PROVIDER", message)
}

fn stream_error_frame_with_source(source: &'static str, message: &str) -> Bytes {
    let code = if source == "PROVIDER" {
        "upstream_error"
    } else {
        "internal_error"
    };
    let payload = json!({"error": {
        "message": format!("{source}: {message}"),
        "type": code,
        "code": code,
        "source": source
    }});
    Bytes::from(format!("data: {payload}\n\ndata: [DONE]\n\n"))
}

fn validate_api_key(value: &str) -> Result<(), AppError> {
    if value.is_empty() || value.len() > 16 * 1024 || value.chars().any(char::is_control) {
        Err(AppError::Invalid("provider API key is invalid".to_owned()))
    } else {
        Ok(())
    }
}

async fn upstream_client(state: &AppState, target: &str) -> Result<Client, AppError> {
    let url = url::Url::parse(target)
        .map_err(|_| AppError::Invalid("provider URL is invalid".to_owned()))?;
    if !matches!(url.scheme(), "http" | "https")
        || !url.username().is_empty()
        || url.password().is_some()
        || url.fragment().is_some()
    {
        return Err(AppError::Invalid("provider URL is invalid".to_owned()));
    }
    let host = url
        .host_str()
        .ok_or_else(|| AppError::Invalid("provider URL has no host".to_owned()))?;
    let port = url
        .port_or_known_default()
        .ok_or_else(|| AppError::Invalid("provider URL has no valid port".to_owned()))?;
    let origin = format!("{}://{}:{}", url.scheme(), host.to_ascii_lowercase(), port);
    let now = Instant::now();
    if let Some(cached) = state.clients.read().await.get(&origin)
        && cached.expires_at > now
    {
        return Ok(cached.client.clone());
    }

    let addresses = resolve_origin(host, port, state.config.connect_timeout).await?;
    if !state.config.allow_private_upstreams
        && addresses.iter().any(|address| !is_public_ip(address.ip()))
    {
        return Err(AppError::Invalid(
            "provider resolves to a private or reserved address; set NORTHGATE_ALLOW_PRIVATE_UPSTREAMS=true only for trusted internal providers"
                .to_owned(),
        ));
    }
    let mut builder = Client::builder()
        .connect_timeout(state.config.connect_timeout)
        .pool_max_idle_per_host(4)
        .tcp_keepalive(Some(Duration::from_secs(30)))
        .redirect(reqwest::redirect::Policy::none())
        .no_proxy();
    if host.parse::<IpAddr>().is_err() {
        builder = builder.resolve_to_addrs(host, &addresses);
    }
    let client = builder.build().map_err(|_| AppError::Internal)?;
    let mut cache = state.clients.write().await;
    cache.retain(|_, value| value.expires_at > now);
    if cache.len() >= MAX_CACHED_ORIGINS {
        cache.clear();
    }
    cache.insert(
        origin,
        CachedClient {
            expires_at: now + CLIENT_CACHE_TTL,
            client: client.clone(),
        },
    );
    Ok(client)
}

async fn resolve_origin(
    host: &str,
    port: u16,
    timeout: Duration,
) -> Result<Vec<SocketAddr>, AppError> {
    if let Ok(address) = host.parse::<IpAddr>() {
        return Ok(vec![SocketAddr::new(address, port)]);
    }
    let resolved = tokio::time::timeout(timeout, tokio::net::lookup_host((host, port)))
        .await
        .map_err(|_| AppError::Timeout)?
        .map_err(|_| AppError::Upstream("provider DNS lookup failed".to_owned()))?;
    let mut addresses = resolved.collect::<Vec<_>>();
    addresses.sort_unstable();
    addresses.dedup();
    if addresses.is_empty() {
        return Err(AppError::Upstream(
            "provider DNS lookup returned no addresses".to_owned(),
        ));
    }
    Ok(addresses)
}

fn is_public_ip(address: IpAddr) -> bool {
    match address {
        IpAddr::V4(address) => is_public_ipv4(address),
        IpAddr::V6(address) => is_public_ipv6(address),
    }
}

fn is_public_ipv4(address: Ipv4Addr) -> bool {
    let [a, b, c, _] = address.octets();
    !(address.is_private()
        || address.is_loopback()
        || address.is_link_local()
        || address.is_broadcast()
        || address.is_documentation()
        || address.is_multicast()
        || address.is_unspecified()
        || a == 0
        || (a == 100 && (64..=127).contains(&b))
        || (a == 192 && b == 0 && c == 0)
        || (a == 198 && (18..=19).contains(&b))
        || a >= 240)
}

fn is_public_ipv6(address: Ipv6Addr) -> bool {
    if let Some(mapped) = address.to_ipv4_mapped() {
        return is_public_ipv4(mapped);
    }
    let segments = address.segments();
    !(address.is_loopback()
        || address.is_unspecified()
        || address.is_multicast()
        || address.is_unique_local()
        || address.is_unicast_link_local()
        || (segments[0] == 0x2001 && segments[1] == 0x0db8)
        || (segments[0] & 0xffc0) == 0xfec0)
}

fn map_store_error(error: StoreError) -> AppError {
    match error {
        StoreError::InvalidInput => AppError::Invalid("provider data is invalid".to_owned()),
        StoreError::NotFound => AppError::NotFound,
        StoreError::Conflict => AppError::Invalid("provider name already exists".to_owned()),
        StoreError::Database | StoreError::Encryption | StoreError::WorkerUnavailable => {
            AppError::Storage
        }
    }
}

fn map_discovery_error(error: DiscoveryError) -> AppError {
    match error {
        DiscoveryError::InvalidUrl => AppError::Invalid(error.to_string()),
        DiscoveryError::Timeout => AppError::Timeout,
        _ => AppError::Upstream(error.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::is_public_ip;
    use std::net::IpAddr;

    #[test]
    fn private_and_metadata_addresses_are_not_public() {
        for address in [
            "127.0.0.1",
            "10.0.0.1",
            "169.254.169.254",
            "192.168.1.1",
            "::1",
            "fc00::1",
            "fe80::1",
        ] {
            assert!(!is_public_ip(address.parse::<IpAddr>().unwrap()));
        }
        assert!(is_public_ip("1.1.1.1".parse().unwrap()));
        assert!(is_public_ip("2606:4700:4700::1111".parse().unwrap()));
    }
}
