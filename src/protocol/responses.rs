//! OpenAI Responses (including Codex-style tool calling) translation.

use super::common::{ProtocolError, content_text, json_string};
use serde_json::{Map, Value, json};

pub fn chat_request_body(
    chat_request: &Value,
    upstream_model: &str,
) -> Result<Value, ProtocolError> {
    let input = chat_request
        .as_object()
        .ok_or_else(|| ProtocolError::invalid("chat request must be a JSON object"))?;
    let messages = input
        .get("messages")
        .and_then(Value::as_array)
        .ok_or_else(|| ProtocolError::invalid("chat request messages must be an array"))?;
    let mut items = Vec::new();
    for message in messages {
        convert_message(message, &mut items)?;
    }

    let mut body = Map::new();
    body.insert("model".into(), Value::String(upstream_model.to_owned()));
    body.insert("input".into(), Value::Array(items));
    if let Some(value) = input.get("stream") {
        body.insert("stream".into(), value.clone());
    }
    if let Some(value) = input.get("temperature") {
        body.insert("temperature".into(), value.clone());
    }
    if let Some(value) = input.get("top_p") {
        body.insert("top_p".into(), value.clone());
    }
    if let Some(value) = input
        .get("max_completion_tokens")
        .or_else(|| input.get("max_tokens"))
    {
        body.insert("max_output_tokens".into(), value.clone());
    }
    if let Some(tools) = input.get("tools") {
        body.insert("tools".into(), convert_tools(tools)?);
    }
    if let Some(choice) = input.get("tool_choice") {
        body.insert("tool_choice".into(), convert_tool_choice(choice)?);
    }
    if let Some(value) = input.get("parallel_tool_calls") {
        body.insert("parallel_tool_calls".into(), value.clone());
    }
    if let Some(value) = input.get("reasoning_effort") {
        body.insert("reasoning".into(), json!({"effort": value}));
    }
    if let Some(value) = input.get("user") {
        body.insert("safety_identifier".into(), value.clone());
    }
    if let Some(value) = input.get("response_format") {
        body.insert("text".into(), convert_response_format(value)?);
    }
    if let Some(value) = input.get("verbosity") {
        let text = body
            .entry("text")
            .or_insert_with(|| json!({"format":{"type":"text"}}));
        let object = text
            .as_object_mut()
            .ok_or_else(|| ProtocolError::conversion("text configuration is invalid"))?;
        object.insert("verbosity".into(), value.clone());
    }
    if let Some(value) = input.get("logprobs") {
        let enabled = value
            .as_bool()
            .ok_or_else(|| ProtocolError::conversion("logprobs must be a boolean"))?;
        if enabled {
            body.insert("include".into(), json!(["message.output_text.logprobs"]));
        }
    }
    if let Some(value) = input.get("top_logprobs") {
        body.insert("top_logprobs".into(), value.clone());
    }
    for field in [
        "service_tier",
        "store",
        "metadata",
        "prompt_cache_key",
        "prompt_cache_retention",
        "safety_identifier",
    ] {
        if let Some(value) = input.get(field) {
            body.insert(field.into(), value.clone());
        }
    }
    validate_modalities(input.get("modalities"))?;
    validate_stream_options(input.get("stream_options"))?;
    reject_present(
        input,
        &[
            "seed",
            "n",
            "audio",
            "frequency_penalty",
            "presence_penalty",
            "stop",
            "prediction",
            "logit_bias",
            "function_call",
            "functions",
            "web_search_options",
        ],
    )?;
    copy_extensions(input, &mut body);
    Ok(Value::Object(body))
}

fn validate_modalities(value: Option<&Value>) -> Result<(), ProtocolError> {
    let Some(value) = value else { return Ok(()) };
    if value == &json!(["text"]) {
        Ok(())
    } else {
        Err(ProtocolError::conversion(
            "Responses adapter supports only text output modality",
        ))
    }
}

fn copy_extensions(input: &Map<String, Value>, body: &mut Map<String, Value>) {
    const CONSUMED: &[&str] = &[
        "model",
        "messages",
        "stream",
        "temperature",
        "top_p",
        "max_completion_tokens",
        "max_tokens",
        "tools",
        "tool_choice",
        "parallel_tool_calls",
        "reasoning_effort",
        "user",
        "response_format",
        "stream_options",
        "logprobs",
        "top_logprobs",
        "modalities",
        "seed",
        "n",
        "audio",
        "frequency_penalty",
        "presence_penalty",
        "stop",
        "prediction",
        "service_tier",
        "store",
        "metadata",
        "prompt_cache_key",
        "prompt_cache_retention",
        "safety_identifier",
        "verbosity",
        "logit_bias",
        "function_call",
        "functions",
        "web_search_options",
    ];
    for (key, value) in input {
        if !CONSUMED.contains(&key.as_str()) && !body.contains_key(key) {
            body.insert(key.clone(), value.clone());
        }
    }
}

fn convert_response_format(value: &Value) -> Result<Value, ProtocolError> {
    let object = value
        .as_object()
        .ok_or_else(|| ProtocolError::conversion("response_format must be an object"))?;
    match object.get("type").and_then(Value::as_str) {
        Some("text") => Ok(json!({"format":{"type":"text"}})),
        Some("json_object") => Ok(json!({"format":{"type":"json_object"}})),
        Some("json_schema") => {
            let schema = object
                .get("json_schema")
                .and_then(Value::as_object)
                .ok_or_else(|| {
                    ProtocolError::conversion("response_format.json_schema is missing")
                })?;
            let mut format = schema.clone();
            format.insert("type".into(), json!("json_schema"));
            if let Some(value) = format.remove("schema") {
                format.insert("schema".into(), value);
            }
            Ok(json!({"format": Value::Object(format)}))
        }
        _ => Err(ProtocolError::conversion(
            "unsupported response_format type",
        )),
    }
}

fn validate_stream_options(value: Option<&Value>) -> Result<(), ProtocolError> {
    let Some(value) = value else { return Ok(()) };
    let object = value
        .as_object()
        .ok_or_else(|| ProtocolError::conversion("stream_options must be an object"))?;
    if object.keys().any(|key| key != "include_usage") {
        return Err(ProtocolError::conversion(
            "Responses API does not support these stream_options",
        ));
    }
    Ok(())
}

fn reject_present(input: &Map<String, Value>, fields: &[&str]) -> Result<(), ProtocolError> {
    if let Some(field) = fields
        .iter()
        .find(|field| input.get(**field).is_some_and(|value| !value.is_null()))
    {
        return Err(ProtocolError::conversion(format!(
            "Responses API cannot represent OpenAI field `{field}`"
        )));
    }
    Ok(())
}

fn convert_message(message: &Value, output: &mut Vec<Value>) -> Result<(), ProtocolError> {
    let object = message
        .as_object()
        .ok_or_else(|| ProtocolError::conversion("message must be an object"))?;
    let role = object
        .get("role")
        .and_then(Value::as_str)
        .ok_or_else(|| ProtocolError::conversion("message role is missing"))?;
    match role {
        "system" | "developer" | "user" => {
            output.push(json!({
                "type": "message",
                "role": role,
                "content": input_content(object.get("content").unwrap_or(&Value::Null))
            }));
        }
        "assistant" => {
            let content = object.get("content").unwrap_or(&Value::Null);
            if !content.is_null() && !content_text(content).is_empty() {
                output.push(json!({
                    "type": "message",
                    "role": "assistant",
                    "content": [{"type":"output_text","text":content_text(content)}]
                }));
            }
            if let Some(tool_calls) = object.get("tool_calls").and_then(Value::as_array) {
                for call in tool_calls {
                    let function = call.get("function").ok_or_else(|| {
                        ProtocolError::conversion("tool call function is missing")
                    })?;
                    let name = function
                        .get("name")
                        .and_then(Value::as_str)
                        .ok_or_else(|| ProtocolError::conversion("tool call name is missing"))?;
                    let call_id = call
                        .get("id")
                        .and_then(Value::as_str)
                        .ok_or_else(|| ProtocolError::conversion("tool call id is missing"))?;
                    output.push(json!({
                        "type":"function_call",
                        "call_id":call_id,
                        "name":name,
                        "arguments":function.get("arguments").and_then(Value::as_str).unwrap_or("{}")
                    }));
                }
            }
        }
        "tool" => {
            let call_id = object
                .get("tool_call_id")
                .and_then(Value::as_str)
                .ok_or_else(|| ProtocolError::conversion("tool_call_id is missing"))?;
            output.push(json!({
                "type":"function_call_output",
                "call_id":call_id,
                "output":json_string(object.get("content").unwrap_or(&Value::Null))
            }));
        }
        _ => return Err(ProtocolError::conversion("unsupported message role")),
    }
    Ok(())
}

fn input_content(content: &Value) -> Value {
    match content {
        Value::String(text) => Value::String(text.clone()),
        Value::Array(parts) => Value::Array(parts.iter().map(input_part).collect()),
        Value::Null => Value::String(String::new()),
        other => Value::String(json_string(other)),
    }
}

fn input_part(part: &Value) -> Value {
    let object = match part.as_object() {
        Some(object) => object,
        None => return json!({"type":"input_text","text":json_string(part)}),
    };
    match object.get("type").and_then(Value::as_str).unwrap_or("text") {
        "text" | "input_text" => json!({
            "type":"input_text",
            "text":object.get("text").and_then(Value::as_str).unwrap_or("")
        }),
        "image_url" => {
            let image = object.get("image_url").unwrap_or(part);
            json!({
                "type":"input_image",
                "image_url":image.get("url").cloned().unwrap_or(Value::Null)
            })
        }
        _ => json!({"type":"input_text","text":content_text(part)}),
    }
}

fn convert_tools(value: &Value) -> Result<Value, ProtocolError> {
    let tools = value
        .as_array()
        .ok_or_else(|| ProtocolError::conversion("tools must be an array"))?;
    tools
        .iter()
        .map(|tool| {
            let function = tool.get("function").unwrap_or(tool);
            let name = function
                .get("name")
                .and_then(Value::as_str)
                .ok_or_else(|| ProtocolError::conversion("tool name is missing"))?;
            let mut converted = Map::new();
            converted.insert("type".into(), json!("function"));
            converted.insert("name".into(), json!(name));
            if let Some(description) = function.get("description") {
                converted.insert("description".into(), description.clone());
            }
            converted.insert(
                "parameters".into(),
                function
                    .get("parameters")
                    .cloned()
                    .unwrap_or_else(|| json!({"type":"object"})),
            );
            if let Some(strict) = function.get("strict") {
                converted.insert("strict".into(), strict.clone());
            }
            Ok(Value::Object(converted))
        })
        .collect::<Result<Vec<_>, ProtocolError>>()
        .map(Value::Array)
}

fn convert_tool_choice(value: &Value) -> Result<Value, ProtocolError> {
    match value {
        Value::String(choice) if matches!(choice.as_str(), "auto" | "none" | "required") => {
            Ok(Value::String(choice.clone()))
        }
        Value::Object(object) => {
            let function = object.get("function").unwrap_or(value);
            let name = function
                .get("name")
                .and_then(Value::as_str)
                .ok_or_else(|| ProtocolError::conversion("tool choice name is missing"))?;
            Ok(json!({"type":"function","name":name}))
        }
        _ => Err(ProtocolError::conversion("unsupported tool choice")),
    }
}

/// Convert a non-stream Responses object to an OpenAI Chat Completion.
pub fn response_to_openai_chat(
    response: &Value,
    requested_model: &str,
) -> Result<Value, ProtocolError> {
    let object = response
        .as_object()
        .ok_or_else(|| ProtocolError::conversion("Responses response must be an object"))?;
    let output = object
        .get("output")
        .and_then(Value::as_array)
        .ok_or_else(|| ProtocolError::conversion("Responses output is missing"))?;
    let mut text = String::new();
    let mut tool_calls = Vec::new();
    for item in output {
        match item.get("type").and_then(Value::as_str).unwrap_or("") {
            "message" => {
                if let Some(parts) = item.get("content").and_then(Value::as_array) {
                    for part in parts {
                        if part.get("type").and_then(Value::as_str) == Some("output_text") {
                            text.push_str(part.get("text").and_then(Value::as_str).unwrap_or(""));
                        }
                    }
                } else if let Some(value) = item.get("content") {
                    text.push_str(&content_text(value));
                }
            }
            "function_call" => tool_calls.push(json!({
                "id":item.get("call_id").or_else(|| item.get("id")).cloned().unwrap_or_else(|| json!("call_0")),
                "type":"function",
                "function":{
                    "name":item.get("name").and_then(Value::as_str).unwrap_or(""),
                    "arguments":item.get("arguments").and_then(Value::as_str).unwrap_or("{}")
                }
            })),
            _ => {}
        }
    }
    let mut message = Map::new();
    message.insert("role".into(), json!("assistant"));
    message.insert(
        "content".into(),
        if text.is_empty() {
            Value::Null
        } else {
            Value::String(text)
        },
    );
    if !tool_calls.is_empty() {
        message.insert("tool_calls".into(), Value::Array(tool_calls.clone()));
    }
    let finish_reason = if !tool_calls.is_empty() {
        "tool_calls"
    } else if object.get("status").and_then(Value::as_str) == Some("incomplete") {
        "length"
    } else {
        "stop"
    };
    let mut choice = Map::new();
    choice.insert("index".into(), json!(0));
    choice.insert("message".into(), Value::Object(message));
    choice.insert("finish_reason".into(), json!(finish_reason));
    let mut result = Map::new();
    result.insert(
        "id".into(),
        object
            .get("id")
            .cloned()
            .unwrap_or_else(|| json!("response-completion")),
    );
    result.insert("object".into(), json!("chat.completion"));
    result.insert("created".into(), json!(0));
    result.insert("model".into(), json!(requested_model));
    result.insert("choices".into(), json!([Value::Object(choice)]));
    if let Some(usage) = object.get("usage").and_then(Value::as_object) {
        let prompt = usage
            .get("input_tokens")
            .and_then(Value::as_u64)
            .unwrap_or(0);
        let completion = usage
            .get("output_tokens")
            .and_then(Value::as_u64)
            .unwrap_or(0);
        result.insert("usage".into(), json!({
            "prompt_tokens":prompt,
            "completion_tokens":completion,
            "total_tokens":usage.get("total_tokens").cloned().unwrap_or_else(|| json!(prompt + completion))
        }));
    }
    Ok(Value::Object(result))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_chat_history_and_tools_to_responses_input() {
        let request = json!({
            "messages":[
                {"role":"user","content":[{"type":"text","text":"hi"},{"type":"image_url","image_url":{"url":"https://img.test/a"}}]},
                {"role":"assistant","content":null,"tool_calls":[{"id":"call_1","type":"function","function":{"name":"lookup","arguments":"{\"q\":1}"}}]},
                {"role":"tool","tool_call_id":"call_1","content":"result"}
            ],
            "tools":[{"type":"function","function":{"name":"lookup","parameters":{"type":"object"}}}],
            "tool_choice":"required",
            "max_completion_tokens":50
        });
        let body = chat_request_body(&request, "gpt").unwrap();
        assert_eq!(body["input"][0]["content"][1]["type"], "input_image");
        assert_eq!(body["input"][1]["type"], "function_call");
        assert_eq!(body["input"][2]["type"], "function_call_output");
        assert_eq!(body["tools"][0]["type"], "function");
    }

    #[test]
    fn maps_response_output_and_usage() {
        let response = json!({
            "id":"resp_1","model":"gpt","status":"completed",
            "output":[{"type":"message","content":[{"type":"output_text","text":"done"}]},{"type":"function_call","call_id":"call_1","name":"lookup","arguments":"{\"q\":1}"}],
            "usage":{"input_tokens":3,"output_tokens":4}
        });
        let result = response_to_openai_chat(&response, "public/gpt").unwrap();
        assert_eq!(result["choices"][0]["finish_reason"], "tool_calls");
        assert_eq!(result["choices"][0]["message"]["content"], "done");
        assert_eq!(result["usage"]["total_tokens"], 7);
        assert_eq!(result["model"], "public/gpt");
    }

    #[test]
    fn maps_structured_output_reasoning_and_extensions() {
        let request = json!({
            "messages":[{"role":"user","content":"hi"}],
            "response_format":{"type":"json_schema","json_schema":{"name":"answer","schema":{"type":"object"},"strict":true}},
            "parallel_tool_calls":false,
            "reasoning_effort":"high",
            "logprobs":true,
            "top_logprobs":3,
            "prompt_cache_retention":"24h",
            "x-provider-option":"kept"
        });
        let body = chat_request_body(&request, "gpt").unwrap();
        assert_eq!(body["text"]["format"]["type"], "json_schema");
        assert_eq!(body["reasoning"]["effort"], "high");
        assert_eq!(body["parallel_tool_calls"], false);
        assert_eq!(body["top_logprobs"], 3);
        assert_eq!(body["prompt_cache_retention"], "24h");
        assert_eq!(body["x-provider-option"], "kept");
    }

    #[test]
    fn incompatible_fields_fail_instead_of_disappearing() {
        let request = json!({"messages":[], "n":2});
        let error = chat_request_body(&request, "gpt").unwrap_err();
        assert!(error.to_string().contains("`n`"));
    }
}
