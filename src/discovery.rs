use std::time::Duration;

use bytes::BytesMut;
use futures_util::StreamExt;
use reqwest::{Client, StatusCode, header};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use url::Url;

use crate::protocol::{UpstreamProtocol, build_endpoint_url};

const MAX_DISCOVERY_BYTES: usize = 4 * 1024 * 1024;
const MAX_DISCOVERED_MODELS: usize = 2_000;
const DETAIL_CONCURRENCY: usize = 1;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct DiscoveredCapabilities {
    pub streaming: Option<bool>,
    pub tools: Option<bool>,
    pub vision: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DiscoveredModel {
    pub id: String,
    pub display_name: String,
    pub context_window: Option<u64>,
    pub max_output_tokens: Option<u64>,
    pub capabilities: DiscoveredCapabilities,
}

#[derive(Debug, thiserror::Error)]
pub enum DiscoveryError {
    #[error("invalid provider URL")]
    InvalidUrl,
    #[error("provider model endpoint timed out")]
    Timeout,
    #[error("provider model endpoint returned HTTP {0}")]
    Http(StatusCode),
    #[error("provider model response exceeded 4 MiB")]
    TooLarge,
    #[error("provider model response was invalid")]
    InvalidResponse,
    #[error("provider connection failed")]
    Transport,
}

pub async fn discover_models(
    client: &Client,
    protocol: UpstreamProtocol,
    base_url: &str,
    api_key: &str,
    first_byte_timeout: Duration,
    body_idle_timeout: Duration,
) -> Result<Vec<DiscoveredModel>, DiscoveryError> {
    let endpoint = models_endpoint(base_url)?;
    let request = authorized_get(client, protocol, endpoint, api_key);
    let response = tokio::time::timeout(first_byte_timeout, request.send())
        .await
        .map_err(|_| DiscoveryError::Timeout)?
        .map_err(|_| DiscoveryError::Transport)?;
    if !response.status().is_success() {
        return Err(DiscoveryError::Http(response.status()));
    }
    let bytes = collect_bounded(response, MAX_DISCOVERY_BYTES, body_idle_timeout).await?;
    let value: Value =
        serde_json::from_slice(&bytes).map_err(|_| DiscoveryError::InvalidResponse)?;
    normalize_models(&value)
}

pub async fn enrich_models(
    client: &Client,
    protocol: UpstreamProtocol,
    base_url: &str,
    api_key: &str,
    models: &mut [DiscoveredModel],
    first_byte_timeout: Duration,
    body_idle_timeout: Duration,
) -> Result<(), DiscoveryError> {
    let base_endpoint = models_endpoint(base_url)?;
    let missing = models
        .iter()
        .enumerate()
        .filter_map(|(index, model)| {
            let incomplete = model.context_window.is_none()
                || model.max_output_tokens.is_none()
                || model.capabilities.streaming.is_none()
                || model.capabilities.tools.is_none()
                || model.capabilities.vision.is_none();
            incomplete.then_some((index, model.id.clone()))
        })
        .collect::<Vec<_>>();
    let lookups = futures_util::stream::iter(missing)
        .map(|(index, id)| {
            let base_endpoint = base_endpoint.clone();
            async move {
                let detail = fetch_model_detail(
                    client,
                    protocol,
                    &base_endpoint,
                    api_key,
                    &id,
                    first_byte_timeout,
                    body_idle_timeout,
                )
                .await;
                (index, detail)
            }
        })
        .buffer_unordered(DETAIL_CONCURRENCY)
        .collect::<Vec<_>>()
        .await;

    for (index, detail) in lookups {
        if let Some(detail) = detail {
            merge_metadata(&mut models[index], detail);
        }
    }
    Ok(())
}

async fn fetch_model_detail(
    client: &Client,
    protocol: UpstreamProtocol,
    models_endpoint: &Url,
    api_key: &str,
    id: &str,
    first_byte_timeout: Duration,
    body_idle_timeout: Duration,
) -> Option<DiscoveredModel> {
    let mut endpoint = models_endpoint.clone();
    endpoint.path_segments_mut().ok()?.push(id);
    let request = authorized_get(client, protocol, endpoint, api_key);
    let response = tokio::time::timeout(first_byte_timeout, request.send())
        .await
        .ok()?
        .ok()?;
    if !response.status().is_success() {
        return None;
    }
    let bytes = collect_bounded(response, MAX_DISCOVERY_BYTES, body_idle_timeout)
        .await
        .ok()?;
    let value: Value = serde_json::from_slice(&bytes).ok()?;
    let detail = value.get("data").unwrap_or(&value);
    normalize_model(detail).filter(|model| model.id == id)
}

fn merge_metadata(model: &mut DiscoveredModel, detail: DiscoveredModel) {
    if model.display_name == model.id && detail.display_name != detail.id {
        model.display_name = detail.display_name;
    }
    model.context_window = model.context_window.or(detail.context_window);
    model.max_output_tokens = model.max_output_tokens.or(detail.max_output_tokens);
    model.capabilities.streaming = model
        .capabilities
        .streaming
        .or(detail.capabilities.streaming);
    model.capabilities.tools = model.capabilities.tools.or(detail.capabilities.tools);
    model.capabilities.vision = model.capabilities.vision.or(detail.capabilities.vision);
}

fn authorized_get(
    client: &Client,
    protocol: UpstreamProtocol,
    endpoint: Url,
    api_key: &str,
) -> reqwest::RequestBuilder {
    let request = client
        .get(endpoint)
        .header(header::ACCEPT, "application/json");
    match protocol {
        UpstreamProtocol::Anthropic => request
            .header("x-api-key", api_key)
            .header("anthropic-version", "2023-06-01"),
        UpstreamProtocol::OpenAi | UpstreamProtocol::Responses => request.bearer_auth(api_key),
    }
}

/// Derived with the same rule as the chat endpoints so a base URL that
/// discovers models also proxies requests.
pub fn models_endpoint(base_url: &str) -> Result<Url, DiscoveryError> {
    let trimmed = base_url.trim().trim_end_matches('/');
    let endpoint = if trimmed.to_ascii_lowercase().ends_with("/models") {
        build_endpoint_url(trimmed, "/models")
            .map(|_| trimmed.to_owned())
            .map_err(|_| DiscoveryError::InvalidUrl)?
    } else {
        build_endpoint_url(trimmed, "/models").map_err(|_| DiscoveryError::InvalidUrl)?
    };
    let url = Url::parse(&endpoint).map_err(|_| DiscoveryError::InvalidUrl)?;
    if !matches!(url.scheme(), "http" | "https")
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(DiscoveryError::InvalidUrl);
    }
    Ok(url)
}

async fn collect_bounded(
    response: reqwest::Response,
    maximum: usize,
    idle_timeout: Duration,
) -> Result<Vec<u8>, DiscoveryError> {
    if response
        .content_length()
        .is_some_and(|length| length > maximum as u64)
    {
        return Err(DiscoveryError::TooLarge);
    }
    let mut output = BytesMut::new();
    let mut stream = response.bytes_stream();
    while let Some(chunk) = tokio::time::timeout(idle_timeout, stream.next())
        .await
        .map_err(|_| DiscoveryError::Timeout)?
    {
        let chunk = chunk.map_err(|_| DiscoveryError::Transport)?;
        if output.len().saturating_add(chunk.len()) > maximum {
            return Err(DiscoveryError::TooLarge);
        }
        output.extend_from_slice(&chunk);
    }
    Ok(output.to_vec())
}

fn normalize_models(value: &Value) -> Result<Vec<DiscoveredModel>, DiscoveryError> {
    let values = value
        .get("data")
        .or_else(|| value.get("models"))
        .unwrap_or(value)
        .as_array()
        .ok_or(DiscoveryError::InvalidResponse)?;
    if values.len() > MAX_DISCOVERED_MODELS {
        return Err(DiscoveryError::InvalidResponse);
    }
    let mut models = values
        .iter()
        .filter_map(normalize_model)
        .collect::<Vec<_>>();
    models.sort_by(|left, right| left.id.cmp(&right.id));
    models.dedup_by(|left, right| left.id == right.id);
    if models.is_empty() && !values.is_empty() {
        return Err(DiscoveryError::InvalidResponse);
    }
    Ok(models)
}

fn normalize_model(value: &Value) -> Option<DiscoveredModel> {
    let id = string_at(value, &["id", "model", "model_id"])?;
    if id.is_empty() || id.len() > 512 || id.chars().any(char::is_control) {
        return None;
    }
    let display_name = string_at(value, &["display_name", "name"])
        .filter(|name| name.len() <= 512 && !name.chars().any(char::is_control))
        .unwrap_or_else(|| id.clone());
    Some(DiscoveredModel {
        id,
        display_name,
        context_window: number_at(
            value,
            &[
                "context_length",
                "context_window",
                "max_context_length",
                "max_input_tokens",
                "input_token_limit",
            ],
        ),
        max_output_tokens: number_at(
            value,
            &[
                "max_output_tokens",
                "output_token_limit",
                "max_tokens_to_sample",
            ],
        ),
        capabilities: DiscoveredCapabilities {
            streaming: bool_capability(value, "streaming"),
            tools: bool_capability(value, "tools")
                .or_else(|| bool_capability(value, "tool_calling")),
            vision: bool_capability(value, "vision")
                .or_else(|| bool_capability(value, "image_input")),
        },
    })
}

fn string_at(value: &Value, names: &[&str]) -> Option<String> {
    names
        .iter()
        .find_map(|name| value.get(*name).and_then(Value::as_str))
        .map(str::to_owned)
}

fn number_at(value: &Value, names: &[&str]) -> Option<u64> {
    for name in names {
        if let Some(number) = value.get(*name).and_then(as_positive_u64) {
            return Some(number);
        }
        for parent in ["limits", "capabilities", "metadata", "architecture"] {
            if let Some(number) = value
                .get(parent)
                .and_then(|nested| nested.get(*name))
                .and_then(as_positive_u64)
            {
                return Some(number);
            }
        }
    }
    None
}

fn as_positive_u64(value: &Value) -> Option<u64> {
    value
        .as_u64()
        .or_else(|| value.as_str()?.parse().ok())
        .filter(|value| *value > 0)
}

fn bool_capability(value: &Value, name: &str) -> Option<bool> {
    if let Some(value) = value.get(name).and_then(Value::as_bool) {
        return Some(value);
    }
    if let Some(value) = value
        .get("capabilities")
        .and_then(|caps| caps.get(name))
        .and_then(Value::as_bool)
    {
        return Some(value);
    }
    value
        .get("supported_features")
        .or_else(|| value.get("capabilities"))
        .and_then(Value::as_array)
        .map(|items| items.iter().any(|item| item.as_str() == Some(name)))
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{models_endpoint, normalize_models};

    #[test]
    fn endpoint_preserves_provider_path_prefix() {
        assert_eq!(
            models_endpoint("https://example.test/api/v1/")
                .unwrap()
                .as_str(),
            "https://example.test/api/v1/models"
        );
        assert_eq!(
            models_endpoint("http://127.0.0.1:11434").unwrap().as_str(),
            "http://127.0.0.1:11434/v1/models"
        );
        assert_eq!(
            models_endpoint("https://example.test/v1beta/openai")
                .unwrap()
                .as_str(),
            "https://example.test/v1beta/openai/models"
        );
        assert_eq!(
            models_endpoint("https://example.test/v1/models/")
                .unwrap()
                .as_str(),
            "https://example.test/v1/models"
        );
        assert!(models_endpoint("https://example.test/v1?key=1").is_err());
        assert!(models_endpoint("https://user:pw@example.test/v1").is_err());
    }

    #[test]
    fn normalizes_common_model_metadata() {
        let models = normalize_models(&json!({"data": [{
            "id": "acme/model-a",
            "name": "Model A",
            "context_length": 131072,
            "limits": {"max_output_tokens": "8192"},
            "supported_features": ["streaming", "tools"]
        }]}))
        .unwrap();
        assert_eq!(models[0].id, "acme/model-a");
        assert_eq!(models[0].context_window, Some(131072));
        assert_eq!(models[0].max_output_tokens, Some(8192));
        assert_eq!(models[0].capabilities.tools, Some(true));
    }
}
