use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fmt;

/// Protocol selected when a provider is configured.
#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UpstreamProtocol {
    /// OpenAI Chat Completions.  `openai` is the API spelling; `open_ai` is
    /// accepted as a backwards-compatible input alias.
    #[serde(rename = "openai", alias = "open_ai")]
    OpenAi,
    Anthropic,
    Responses,
}

impl fmt::Debug for UpstreamProtocol {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::OpenAi => "OpenAi",
            Self::Anthropic => "Anthropic",
            Self::Responses => "Responses",
        })
    }
}

/// An application-facing description of an HTTP request.  The API key is
/// present for the transport layer, but Debug redacts it and error messages do
/// not contain this value.
#[derive(Clone, PartialEq)]
pub struct UpstreamHttpRequest {
    pub method: String,
    pub url: String,
    pub headers: Vec<(String, String)>,
    pub body: Value,
    pub stream: bool,
    pub include_usage: bool,
}

impl UpstreamHttpRequest {
    pub fn post(
        url: String,
        headers: Vec<(String, String)>,
        body: Value,
        stream: bool,
        include_usage: bool,
    ) -> Self {
        Self {
            method: "POST".into(),
            url,
            headers,
            body,
            stream,
            include_usage,
        }
    }

    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(key, _)| key.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.as_str())
    }
}

impl fmt::Debug for UpstreamHttpRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let headers = self.headers.iter().map(|(name, value)| {
            let safe = if is_secret_header(name) {
                "[REDACTED]".to_owned()
            } else {
                value.clone()
            };
            (name, safe)
        });
        f.debug_struct("UpstreamHttpRequest")
            .field("method", &self.method)
            .field("url", &self.url)
            .field("headers", &headers.collect::<Vec<_>>())
            .field("body", &self.body)
            .field("stream", &self.stream)
            .field("include_usage", &self.include_usage)
            .finish()
    }
}

fn is_secret_header(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "authorization" | "proxy-authorization" | "x-api-key" | "api-key"
    )
}

#[derive(Clone, PartialEq, Eq)]
pub struct ProtocolError {
    pub kind: &'static str,
    pub message: String,
}

impl ProtocolError {
    pub fn invalid(message: impl Into<String>) -> Self {
        Self {
            kind: "invalid_request",
            message: message.into(),
        }
    }

    pub fn endpoint(message: impl Into<String>) -> Self {
        Self {
            kind: "invalid_endpoint",
            message: message.into(),
        }
    }

    pub fn conversion(message: impl Into<String>) -> Self {
        Self {
            kind: "conversion_error",
            message: message.into(),
        }
    }

    pub fn stream(message: impl Into<String>) -> Self {
        Self {
            kind: "stream_error",
            message: message.into(),
        }
    }
}

impl fmt::Debug for ProtocolError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ProtocolError")
            .field("kind", &self.kind)
            .field("message", &self.message)
            .finish()
    }
}

impl fmt::Display for ProtocolError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.kind, self.message)
    }
}

impl std::error::Error for ProtocolError {}

/// Optional request field; an explicit JSON `null` counts as absent.
pub(crate) fn field<'a>(input: &'a serde_json::Map<String, Value>, key: &str) -> Option<&'a Value> {
    input.get(key).filter(|value| !value.is_null())
}

pub(crate) fn json_string(value: &Value) -> String {
    match value {
        Value::String(value) => value.clone(),
        _ => value.to_string(),
    }
}

pub(crate) fn content_text(value: &Value) -> String {
    match value {
        Value::String(text) => text.clone(),
        Value::Array(parts) => parts
            .iter()
            .filter_map(|part| {
                if let Some(text) = part.get("text").and_then(Value::as_str) {
                    Some(text.to_owned())
                } else {
                    part.get("content").map(json_string)
                }
            })
            .collect::<Vec<_>>()
            .join(""),
        Value::Null => String::new(),
        _ => value.to_string(),
    }
}
