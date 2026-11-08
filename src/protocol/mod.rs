//! Protocol adapters used by the Northgate request handler.
//!
//! The module deliberately contains transport-neutral request descriptions.  The
//! application can turn [`UpstreamHttpRequest`] into a reqwest request without
//! giving the protocol layer access to a client, a database, or logging.

pub mod anthropic;
mod common;
pub mod endpoint;
pub mod openai;
pub mod responses;
pub mod sse;

pub use common::{ProtocolError, UpstreamHttpRequest, UpstreamProtocol};
pub use endpoint::{build_endpoint_url, upstream_path};

/// Build an upstream request from the OpenAI Chat Completions representation
/// accepted by Northgate.
pub fn build_upstream_request(
    base_url: &str,
    protocol: UpstreamProtocol,
    upstream_model: &str,
    api_key: &str,
    chat_request: &serde_json::Value,
) -> Result<UpstreamHttpRequest, ProtocolError> {
    if upstream_model.trim().is_empty() {
        return Err(ProtocolError::invalid("upstream model is empty"));
    }
    if api_key.is_empty() {
        return Err(ProtocolError::invalid("upstream API key is empty"));
    }

    let url = build_endpoint_url(base_url, upstream_path(protocol))?;
    let (body, headers) = match protocol {
        UpstreamProtocol::OpenAi => (
            openai::chat_request_body(chat_request, upstream_model)?,
            vec![
                ("authorization".into(), format!("Bearer {api_key}")),
                ("content-type".into(), "application/json".into()),
            ],
        ),
        UpstreamProtocol::Anthropic => (
            anthropic::chat_request_body(chat_request, upstream_model)?,
            vec![
                ("x-api-key".into(), api_key.to_owned()),
                ("anthropic-version".into(), "2023-06-01".into()),
                ("content-type".into(), "application/json".into()),
            ],
        ),
        UpstreamProtocol::Responses => (
            responses::chat_request_body(chat_request, upstream_model)?,
            vec![
                ("authorization".into(), format!("Bearer {api_key}")),
                ("content-type".into(), "application/json".into()),
            ],
        ),
    };

    let stream = chat_request
        .get("stream")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    let include_usage = chat_request
        .get("stream_options")
        .and_then(|value| value.get("include_usage"))
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    Ok(UpstreamHttpRequest::post(
        url,
        headers,
        body,
        stream,
        include_usage,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn protocol_wire_names_match_admin_api() {
        assert_eq!(
            serde_json::to_string(&UpstreamProtocol::OpenAi).unwrap(),
            "\"openai\""
        );
        assert_eq!(
            serde_json::to_string(&UpstreamProtocol::Anthropic).unwrap(),
            "\"anthropic\""
        );
        assert_eq!(
            serde_json::from_str::<UpstreamProtocol>("\"open_ai\"").unwrap(),
            UpstreamProtocol::OpenAi
        );
    }

    #[test]
    fn request_builder_selects_endpoint_headers_and_stream() {
        let chat = json!({
            "model":"public/claude",
            "messages":[{"role":"user","content":"hello"}],
            "stream":true
        });
        let request = build_upstream_request(
            "https://provider.test/v1",
            UpstreamProtocol::Anthropic,
            "claude-sonnet",
            "secret-key",
            &chat,
        )
        .unwrap();
        assert_eq!(request.method, "POST");
        assert_eq!(request.url, "https://provider.test/v1/messages");
        assert_eq!(request.header("x-api-key"), Some("secret-key"));
        assert_eq!(request.body["model"], "claude-sonnet");
        assert!(request.stream);
        assert!(!format!("{request:?}").contains("secret-key"));
    }

    #[test]
    fn errors_do_not_echo_key() {
        let chat = json!({"messages": "bad"});
        let error = build_upstream_request(
            "https://provider.test",
            UpstreamProtocol::Responses,
            "model",
            "never-echo-this-key",
            &chat,
        )
        .unwrap_err();
        assert!(!error.to_string().contains("never-echo-this-key"));
        assert!(!format!("{error:?}").contains("never-echo-this-key"));
    }
}
