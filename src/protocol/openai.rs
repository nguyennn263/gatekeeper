use super::common::ProtocolError;
use serde_json::Value;

/// Clone an OpenAI Chat Completions body and replace only its model.
pub fn chat_request_body(
    chat_request: &Value,
    upstream_model: &str,
) -> Result<Value, ProtocolError> {
    let mut body = chat_request
        .as_object()
        .cloned()
        .ok_or_else(|| ProtocolError::invalid("chat request must be a JSON object"))?;
    body.insert("model".into(), Value::String(upstream_model.to_owned()));
    Ok(Value::Object(body))
}

/// OpenAI streams already have the public representation expected by Northgate.
/// The SSE module still supplies framing and bounded buffering for the transport.
pub fn passthrough_response(mut body: Value, requested_model: &str) -> Value {
    if let Some(object) = body.as_object_mut() {
        object.insert("model".into(), Value::String(requested_model.to_owned()));
    }
    body
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn changes_model_and_preserves_extensions() {
        let input = serde_json::json!({
            "model": "public/name",
            "messages": [{"role": "user", "content": "hello"}],
            "stream": true,
            "x-provider-extension": {"keep": true}
        });
        let output = chat_request_body(&input, "upstream/model").unwrap();
        assert_eq!(output["model"], "upstream/model");
        assert_eq!(output["x-provider-extension"]["keep"], true);
    }

    #[test]
    fn response_uses_public_model_alias() {
        let output = passthrough_response(
            serde_json::json!({"id":"chat_1","model":"upstream/model"}),
            "provider/model",
        );
        assert_eq!(output["model"], "provider/model");
    }
}
