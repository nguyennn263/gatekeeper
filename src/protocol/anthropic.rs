//! OpenAI Chat Completions <-> Anthropic Messages translation.

use super::common::{ProtocolError, content_text, field, json_string};
use serde_json::{Map, Value, json};

const DEFAULT_MAX_TOKENS: u64 = 4096;

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

    let mut system = Vec::new();
    let mut output_messages = Vec::with_capacity(messages.len());
    let mut saw_conversation = false;
    for message in messages {
        let object = message
            .as_object()
            .ok_or_else(|| ProtocolError::conversion("message must be an object"))?;
        let role = object
            .get("role")
            .and_then(Value::as_str)
            .ok_or_else(|| ProtocolError::conversion("message role is missing"))?;
        match role {
            "system" | "developer" => {
                if saw_conversation {
                    return Err(ProtocolError::conversion(
                        "Anthropic requires system/developer messages before conversation messages",
                    ));
                }
                system.extend(text_blocks(object.get("content").unwrap_or(&Value::Null)));
            }
            "tool" => {
                saw_conversation = true;
                let call_id = object
                    .get("tool_call_id")
                    .and_then(Value::as_str)
                    .ok_or_else(|| ProtocolError::conversion("tool_call_id is missing"))?;
                let content = anthropic_tool_result_content(object.get("content"));
                let block = json!({
                    "type": "tool_result",
                    "tool_use_id": call_id,
                    "content": content
                });
                append_anthropic_message(&mut output_messages, "user", vec![block]);
            }
            "user" | "assistant" => {
                saw_conversation = true;
                let content = convert_message_content(object, role)?;
                append_anthropic_message(&mut output_messages, role, content);
            }
            _ => return Err(ProtocolError::conversion("unsupported message role")),
        }
    }

    let mut body = Map::new();
    body.insert("model".into(), Value::String(upstream_model.to_owned()));
    body.insert("messages".into(), Value::Array(output_messages));
    body.insert(
        "max_tokens".into(),
        field(input, "max_tokens")
            .or_else(|| field(input, "max_completion_tokens"))
            .cloned()
            .unwrap_or_else(|| json!(DEFAULT_MAX_TOKENS)),
    );
    if !system.is_empty() {
        body.insert("system".into(), Value::Array(system));
    }
    if let Some(value) = field(input, "temperature") {
        body.insert("temperature".into(), value.clone());
    }
    // Claude rejects temperature and top_p together; temperature wins.
    if let Some(value) = field(input, "top_p")
        && !body.contains_key("temperature")
    {
        body.insert("top_p".into(), value.clone());
    }
    if let Some(value) = field(input, "stop") {
        body.insert(
            "stop_sequences".into(),
            if value.is_string() {
                json!([value])
            } else {
                value.clone()
            },
        );
    }
    if let Some(value) = field(input, "stream") {
        body.insert("stream".into(), value.clone());
    }
    let tools_disabled = field(input, "tool_choice").and_then(Value::as_str) == Some("none");
    if !tools_disabled && let Some(tools) = field(input, "tools") {
        body.insert("tools".into(), convert_tools(tools)?);
    }
    if !tools_disabled
        && let Some(choice) = field(input, "tool_choice")
        && let Some(converted) = convert_tool_choice(choice)?
    {
        body.insert("tool_choice".into(), converted);
    }
    if !tools_disabled
        && body.contains_key("tools")
        && let Some(parallel) = field(input, "parallel_tool_calls").and_then(Value::as_bool)
    {
        let choice = body
            .entry("tool_choice")
            .or_insert_with(|| json!({"type":"auto"}));
        let object = choice.as_object_mut().ok_or_else(|| {
            ProtocolError::conversion("parallel_tool_calls requires a compatible tool_choice")
        })?;
        object.insert("disable_parallel_tool_use".into(), Value::Bool(!parallel));
    }
    validate_stream_options(field(input, "stream_options"))?;
    if let Some(value) = field(input, "response_format") {
        if let Some(output_config) = convert_output_config(value, input)? {
            body.insert("output_config".into(), output_config);
        }
    } else if let Some(effort) = field(input, "reasoning_effort") {
        body.insert("output_config".into(), json!({"effort": effort}));
    }
    if let Some(user) = field(input, "user") {
        let user = user
            .as_str()
            .ok_or_else(|| ProtocolError::conversion("user must be a string"))?;
        body.insert("metadata".into(), json!({"user_id": user}));
    }
    validate_modalities(field(input, "modalities"))?;
    validate_store(field(input, "store"))?;
    reject_present(
        input,
        &[
            "seed",
            "n",
            "logprobs",
            "top_logprobs",
            "audio",
            "frequency_penalty",
            "presence_penalty",
            "metadata",
            "prediction",
            "service_tier",
            "logit_bias",
            "function_call",
            "functions",
            "verbosity",
            "web_search_options",
            "prompt_cache_key",
            "prompt_cache_retention",
            "safety_identifier",
        ],
        "Anthropic Messages",
    )?;
    copy_extensions(input, &mut body);
    Ok(Value::Object(body))
}

fn convert_output_config(
    response_format: &Value,
    input: &Map<String, Value>,
) -> Result<Option<Value>, ProtocolError> {
    let object = response_format
        .as_object()
        .ok_or_else(|| ProtocolError::conversion("response_format must be an object"))?;
    let mut output = Map::new();
    match object.get("type").and_then(Value::as_str) {
        Some("text") => {}
        Some("json_schema") => {
            let schema = object.get("json_schema").cloned().ok_or_else(|| {
                ProtocolError::conversion("response_format.json_schema is missing")
            })?;
            let mut format = schema
                .as_object()
                .cloned()
                .ok_or_else(|| ProtocolError::conversion("json_schema must be an object"))?;
            format.insert("type".into(), json!("json_schema"));
            output.insert("format".into(), Value::Object(format));
        }
        Some("json_object") => {
            return Err(ProtocolError::conversion(
                "Anthropic structured output requires response_format.type=json_schema",
            ));
        }
        _ => {
            return Err(ProtocolError::conversion(
                "unsupported response_format type",
            ));
        }
    }
    if let Some(effort) = field(input, "reasoning_effort") {
        output.insert("effort".into(), effort.clone());
    }
    Ok((!output.is_empty()).then_some(Value::Object(output)))
}

fn validate_modalities(value: Option<&Value>) -> Result<(), ProtocolError> {
    let Some(value) = value else { return Ok(()) };
    if value == &json!(["text"]) {
        Ok(())
    } else {
        Err(ProtocolError::conversion(
            "Anthropic adapter supports only text output modality",
        ))
    }
}

fn validate_store(value: Option<&Value>) -> Result<(), ProtocolError> {
    match value {
        None | Some(Value::Bool(false)) | Some(Value::Null) => Ok(()),
        Some(Value::Bool(true)) => Err(ProtocolError::conversion(
            "Anthropic Messages cannot provide OpenAI store=true semantics",
        )),
        Some(_) => Err(ProtocolError::conversion("store must be a boolean")),
    }
}

fn copy_extensions(input: &Map<String, Value>, body: &mut Map<String, Value>) {
    const CONSUMED: &[&str] = &[
        "model",
        "messages",
        "max_tokens",
        "max_completion_tokens",
        "temperature",
        "top_p",
        "stop",
        "stream",
        "tools",
        "tool_choice",
        "parallel_tool_calls",
        "stream_options",
        "response_format",
        "seed",
        "n",
        "logprobs",
        "top_logprobs",
        "reasoning_effort",
        "audio",
        "frequency_penalty",
        "presence_penalty",
        "modalities",
        "store",
        "metadata",
        "prediction",
        "service_tier",
        "user",
        "logit_bias",
        "function_call",
        "functions",
        "verbosity",
        "web_search_options",
        "prompt_cache_key",
        "prompt_cache_retention",
        "safety_identifier",
    ];
    for (key, value) in input {
        if !CONSUMED.contains(&key.as_str()) && !body.contains_key(key) {
            body.insert(key.clone(), value.clone());
        }
    }
}

/// Merge consecutive same-role messages; Anthropic rejects empty content.
fn append_anthropic_message(output: &mut Vec<Value>, role: &str, mut blocks: Vec<Value>) {
    if blocks.is_empty() {
        return;
    }
    if let Some(last) = output.last_mut()
        && last.get("role").and_then(Value::as_str) == Some(role)
        && let Some(content) = last.get_mut("content").and_then(Value::as_array_mut)
    {
        content.append(&mut blocks);
        return;
    }
    output.push(json!({"role": role, "content": blocks}));
}

fn validate_stream_options(value: Option<&Value>) -> Result<(), ProtocolError> {
    let Some(value) = value else { return Ok(()) };
    let object = value
        .as_object()
        .ok_or_else(|| ProtocolError::conversion("stream_options must be an object"))?;
    if object.keys().any(|key| key != "include_usage") {
        return Err(ProtocolError::conversion(
            "Anthropic Messages does not support these stream_options",
        ));
    }
    Ok(())
}

fn reject_present(
    input: &Map<String, Value>,
    fields: &[&str],
    protocol: &str,
) -> Result<(), ProtocolError> {
    if let Some(field) = fields
        .iter()
        .find(|field| input.get(**field).is_some_and(|value| !value.is_null()))
    {
        return Err(ProtocolError::conversion(format!(
            "{protocol} cannot represent OpenAI field `{field}`"
        )));
    }
    Ok(())
}

/// Anthropic rejects empty text blocks, which OpenAI clients commonly send.
fn text_block(text: &str) -> Option<Value> {
    (!text.is_empty()).then(|| json!({"type": "text", "text": text}))
}

fn text_blocks(value: &Value) -> Vec<Value> {
    match value {
        Value::String(text) => text_block(text).into_iter().collect(),
        Value::Array(parts) => parts
            .iter()
            .filter_map(|part| text_block(part.get("text")?.as_str()?))
            .collect(),
        Value::Null => Vec::new(),
        other => text_block(&json_string(other)).into_iter().collect(),
    }
}

fn convert_message_content(
    message: &Map<String, Value>,
    role: &str,
) -> Result<Vec<Value>, ProtocolError> {
    let mut blocks = match message.get("content") {
        Some(Value::Array(parts)) => parts
            .iter()
            .map(convert_content_part)
            .filter_map(Result::transpose)
            .collect::<Result<Vec<_>, _>>()?,
        Some(content) => text_blocks(content),
        None => Vec::new(),
    };
    if role == "assistant"
        && let Some(tool_calls) = message.get("tool_calls").and_then(Value::as_array)
    {
        for tool_call in tool_calls {
            let function = tool_call
                .get("function")
                .ok_or_else(|| ProtocolError::conversion("tool call function is missing"))?;
            let name = function
                .get("name")
                .and_then(Value::as_str)
                .ok_or_else(|| ProtocolError::conversion("tool call name is missing"))?;
            let id = tool_call
                .get("id")
                .and_then(Value::as_str)
                .ok_or_else(|| ProtocolError::conversion("tool call id is missing"))?;
            let input = parse_json_arguments(function.get("arguments"));
            blocks.push(json!({"type": "tool_use", "id": id, "name": name, "input": input}));
        }
    }
    Ok(blocks)
}

fn convert_content_part(part: &Value) -> Result<Option<Value>, ProtocolError> {
    let object = part
        .as_object()
        .ok_or_else(|| ProtocolError::conversion("content part must be an object"))?;
    match object.get("type").and_then(Value::as_str).unwrap_or("text") {
        "text" | "input_text" => Ok(text_block(
            object.get("text").and_then(Value::as_str).unwrap_or(""),
        )),
        "image_url" => {
            let image = object.get("image_url").unwrap_or(part);
            let url = image
                .get("url")
                .and_then(Value::as_str)
                .ok_or_else(|| ProtocolError::conversion("image URL is missing"))?;
            Ok(Some(json!({"type": "image", "source": image_source(url)?})))
        }
        "tool_result" => Ok(Some(json!({
            "type": "tool_result",
            "tool_use_id": object.get("tool_use_id").and_then(Value::as_str).unwrap_or(""),
            "content": object.get("content").cloned().unwrap_or(Value::Null)
        }))),
        _ => Ok(text_block(&content_text(part))),
    }
}

/// Inline images must be sent as base64 with a media type, not as `data:` URLs.
fn image_source(url: &str) -> Result<Value, ProtocolError> {
    let Some(data_url) = url.strip_prefix("data:") else {
        return Ok(json!({"type": "url", "url": url}));
    };
    let (header, data) = data_url
        .split_once(',')
        .ok_or_else(|| ProtocolError::conversion("image data URL is malformed"))?;
    let mut parameters = header.split(';');
    let media_type = parameters.next().unwrap_or("").trim();
    if media_type.is_empty() || !parameters.any(|parameter| parameter.trim() == "base64") {
        return Err(ProtocolError::conversion(
            "image data URL must be base64 encoded with a media type",
        ));
    }
    Ok(json!({"type": "base64", "media_type": media_type, "data": data}))
}

fn anthropic_tool_result_content(value: Option<&Value>) -> Value {
    match value.unwrap_or(&Value::Null) {
        Value::String(text) => Value::String(text.clone()),
        Value::Array(parts) => Value::Array(
            parts
                .iter()
                .filter_map(|part| {
                    part.get("text")
                        .map(|text| json!({"type": "text", "text": text}))
                })
                .collect(),
        ),
        other => Value::String(json_string(other)),
    }
}

fn parse_json_arguments(value: Option<&Value>) -> Value {
    let text = value.and_then(Value::as_str).unwrap_or("{}");
    serde_json::from_str(text).unwrap_or_else(|_| Value::String(text.to_owned()))
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
            Ok(json!({
                "name": name,
                "description": function.get("description").cloned().unwrap_or(Value::Null),
                "input_schema": function.get("parameters").cloned().unwrap_or_else(|| json!({"type":"object"}))
            }))
        })
        .collect::<Result<Vec<_>, ProtocolError>>()
        .map(Value::Array)
}

fn convert_tool_choice(value: &Value) -> Result<Option<Value>, ProtocolError> {
    match value {
        Value::String(choice) => match choice.as_str() {
            "auto" => Ok(Some(json!({"type": "auto"}))),
            "required" => Ok(Some(json!({"type": "any"}))),
            "none" => Ok(None),
            _ => Err(ProtocolError::conversion("unsupported tool choice")),
        },
        Value::Object(object) => {
            let function = object.get("function").unwrap_or(value);
            let name = function
                .get("name")
                .and_then(Value::as_str)
                .ok_or_else(|| ProtocolError::conversion("tool choice name is missing"))?;
            Ok(Some(json!({"type": "tool", "name": name})))
        }
        _ => Err(ProtocolError::conversion(
            "tool choice must be a string or object",
        )),
    }
}

/// Convert an Anthropic non-stream message response to an OpenAI completion.
pub fn response_to_openai_chat(
    response: &Value,
    requested_model: &str,
) -> Result<Value, ProtocolError> {
    let object = response
        .as_object()
        .ok_or_else(|| ProtocolError::conversion("Anthropic response must be an object"))?;
    let content = object
        .get("content")
        .and_then(Value::as_array)
        .ok_or_else(|| ProtocolError::conversion("Anthropic response content is missing"))?;
    let mut text = String::new();
    let mut tool_calls = Vec::new();
    for (index, block) in content.iter().enumerate() {
        match block.get("type").and_then(Value::as_str).unwrap_or("") {
            "text" => text.push_str(block.get("text").and_then(Value::as_str).unwrap_or("")),
            "tool_use" => {
                let input = block.get("input").cloned().unwrap_or_else(|| json!({}));
                tool_calls.push(json!({
                    "id": block.get("id").and_then(Value::as_str).unwrap_or(if index == 0 { "call_0" } else { "call" }),
                    "type": "function",
                    "function": {
                        "name": block.get("name").and_then(Value::as_str).unwrap_or(""),
                        "arguments": if input.is_string() { input.as_str().unwrap_or("{}").to_owned() } else { input.to_string() }
                    }
                }));
            }
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
        message.insert("tool_calls".into(), Value::Array(tool_calls));
    }

    let stop_reason = object.get("stop_reason").and_then(Value::as_str);
    let finish_reason = match stop_reason {
        Some("tool_use") => "tool_calls",
        Some("max_tokens") => "length",
        Some("stop_sequence") => "stop",
        Some(_) | None => "stop",
    };
    let mut choice = Map::new();
    choice.insert("index".into(), json!(0));
    choice.insert("message".into(), Value::Object(message));
    choice.insert("finish_reason".into(), json!(finish_reason));
    let mut output = Map::new();
    output.insert(
        "id".into(),
        object
            .get("id")
            .cloned()
            .unwrap_or_else(|| json!("anthropic-completion")),
    );
    output.insert("object".into(), json!("chat.completion"));
    output.insert("created".into(), json!(0));
    output.insert("model".into(), json!(requested_model));
    output.insert("choices".into(), json!([Value::Object(choice)]));
    if let Some(usage) = object.get("usage") {
        output.insert("usage".into(), openai_usage(usage));
    }
    Ok(Value::Object(output))
}

/// OpenAI `prompt_tokens` covers all input, so cache reads and writes are folded in.
pub(crate) fn openai_usage(usage: &Value) -> Value {
    let number = |key: &str| usage.get(key).and_then(Value::as_u64).unwrap_or(0);
    let cached = number("cache_read_input_tokens");
    let prompt_tokens = number("input_tokens")
        .saturating_add(number("cache_creation_input_tokens"))
        .saturating_add(cached);
    let completion_tokens = number("output_tokens");
    json!({
        "prompt_tokens": prompt_tokens,
        "completion_tokens": completion_tokens,
        "total_tokens": prompt_tokens.saturating_add(completion_tokens),
        "prompt_tokens_details": {"cached_tokens": cached}
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn converts_multimodal_tools_and_tool_results() {
        let request = json!({
            "messages": [
                {"role":"system","content":"be brief"},
                {"role":"user","content":[{"type":"text","text":"look"},{"type":"image_url","image_url":{"url":"https://img.test/a.png"}}]},
                {"role":"assistant","content":null,"tool_calls":[{"id":"call_1","type":"function","function":{"name":"weather","arguments":"{\"city\":\"Hanoi\"}"}}]},
                {"role":"tool","tool_call_id":"call_1","content":"sunny"}
            ],
            "tools":[{"type":"function","function":{"name":"weather","description":"forecast","parameters":{"type":"object"}}}],
            "tool_choice":{"type":"function","function":{"name":"weather"}},
            "max_tokens":123,
            "stream":true
        });
        let body = chat_request_body(&request, "claude").unwrap();
        assert_eq!(body["model"], "claude");
        assert_eq!(body["messages"][0]["content"][1]["type"], "image");
        assert_eq!(body["messages"][1]["content"][0]["type"], "tool_use");
        assert_eq!(body["messages"][2]["content"][0]["type"], "tool_result");
        assert_eq!(body["tools"][0]["input_schema"]["type"], "object");
    }

    #[test]
    fn converts_response_tool_use_and_usage() {
        let response = json!({
            "id":"msg_1","model":"claude","stop_reason":"tool_use",
            "content":[{"type":"tool_use","id":"toolu_1","name":"weather","input":{"city":"Hanoi"}}],
            "usage":{"input_tokens":4,"output_tokens":7}
        });
        let output = response_to_openai_chat(&response, "public/claude").unwrap();
        assert_eq!(output["choices"][0]["finish_reason"], "tool_calls");
        assert_eq!(
            output["choices"][0]["message"]["tool_calls"][0]["function"]["name"],
            "weather"
        );
        assert_eq!(output["usage"]["total_tokens"], 11);
        assert_eq!(output["model"], "public/claude");
    }

    #[test]
    fn groups_consecutive_tool_results() {
        let request = json!({
            "messages": [
                {"role":"assistant","tool_calls":[
                    {"id":"a","function":{"name":"one","arguments":"{}"}},
                    {"id":"b","function":{"name":"two","arguments":"{}"}}
                ]},
                {"role":"tool","tool_call_id":"a","content":"first"},
                {"role":"tool","tool_call_id":"b","content":"second"}
            ]
        });
        let body = chat_request_body(&request, "claude").unwrap();
        assert_eq!(body["messages"].as_array().unwrap().len(), 2);
        assert_eq!(body["messages"][1]["content"].as_array().unwrap().len(), 2);
    }

    #[test]
    fn preserves_extensions_and_rejects_unrepresentable_fields() {
        let request = json!({
            "messages":[{"role":"user","content":"hi"}],
            "tools":[{"type":"function","function":{"name":"lookup","parameters":{"type":"object"}}}],
            "parallel_tool_calls":false,
            "x-anthropic-option":"kept"
        });
        let body = chat_request_body(&request, "claude").unwrap();
        assert_eq!(body["tool_choice"]["disable_parallel_tool_use"], true);
        assert_eq!(body["x-anthropic-option"], "kept");

        let invalid = json!({"messages":[], "response_format":{"type":"json_object"}});
        assert!(chat_request_body(&invalid, "claude").is_err());
        let cache_retention = json!({"messages":[], "prompt_cache_retention":"24h"});
        assert!(chat_request_body(&cache_retention, "claude").is_err());
    }

    #[test]
    fn null_optional_fields_are_treated_as_absent() {
        let request = json!({
            "messages":[{"role":"user","content":"hi"}],
            "max_tokens": null,
            "temperature": null,
            "top_p": null,
            "stop": null,
            "tools": null,
            "tool_choice": null,
            "user": null,
            "response_format": null,
            "reasoning_effort": null,
            "store": null
        });
        let body = chat_request_body(&request, "claude").unwrap();
        assert_eq!(body["max_tokens"], 4096);
        for key in [
            "temperature",
            "top_p",
            "stop_sequences",
            "tools",
            "tool_choice",
            "metadata",
            "output_config",
        ] {
            assert!(body.get(key).is_none(), "{key} should be absent");
        }
    }

    #[test]
    fn temperature_wins_over_top_p() {
        let request = json!({
            "messages":[{"role":"user","content":"hi"}],
            "temperature": 0.2,
            "top_p": 0.9
        });
        let body = chat_request_body(&request, "claude").unwrap();
        assert_eq!(body["temperature"], 0.2);
        assert!(body.get("top_p").is_none());

        let only_top_p = json!({"messages":[{"role":"user","content":"hi"}], "top_p": 0.9});
        let body = chat_request_body(&only_top_p, "claude").unwrap();
        assert_eq!(body["top_p"], 0.9);
    }

    #[test]
    fn empty_text_is_dropped_and_data_urls_become_base64() {
        let request = json!({
            "messages":[
                {"role":"user","content":[
                    {"type":"text","text":""},
                    {"type":"image_url","image_url":{"url":"data:image/png;base64,AAAA"}}
                ]},
                {"role":"assistant","content":"","tool_calls":[
                    {"id":"call_1","function":{"name":"f","arguments":"{}"}}
                ]},
                {"role":"tool","tool_call_id":"call_1","content":"ok"},
                {"role":"assistant","content":""}
            ]
        });
        let body = chat_request_body(&request, "claude").unwrap();
        let messages = body["messages"].as_array().unwrap();
        assert_eq!(
            messages.len(),
            3,
            "empty trailing assistant message is skipped"
        );
        assert_eq!(messages[0]["content"].as_array().unwrap().len(), 1);
        assert_eq!(messages[0]["content"][0]["source"]["type"], "base64");
        assert_eq!(
            messages[0]["content"][0]["source"]["media_type"],
            "image/png"
        );
        assert_eq!(messages[0]["content"][0]["source"]["data"], "AAAA");
        assert_eq!(messages[1]["content"].as_array().unwrap().len(), 1);
        assert_eq!(messages[1]["content"][0]["type"], "tool_use");

        let remote = json!({"messages":[{"role":"user","content":[
            {"type":"image_url","image_url":{"url":"https://img.test/a.png"}}
        ]}]});
        let body = chat_request_body(&remote, "claude").unwrap();
        assert_eq!(body["messages"][0]["content"][0]["source"]["type"], "url");
        let malformed = json!({"messages":[{"role":"user","content":[
            {"type":"image_url","image_url":{"url":"data:image/png,notbase64"}}
        ]}]});
        assert!(chat_request_body(&malformed, "claude").is_err());
    }

    #[test]
    fn usage_includes_cached_tokens_in_prompt_total() {
        let response = json!({
            "id":"msg_1","content":[{"type":"text","text":"hi"}],"stop_reason":"end_turn",
            "usage":{"input_tokens":4,"output_tokens":7,"cache_read_input_tokens":10,"cache_creation_input_tokens":2}
        });
        let output = response_to_openai_chat(&response, "public/claude").unwrap();
        assert_eq!(output["usage"]["prompt_tokens"], 16);
        assert_eq!(output["usage"]["total_tokens"], 23);
        assert_eq!(
            output["usage"]["prompt_tokens_details"]["cached_tokens"],
            10
        );
    }

    #[test]
    fn none_tool_choice_stays_disabled_with_parallel_flag() {
        let request = json!({
            "messages":[{"role":"user","content":"hi"}],
            "tools":[{"type":"function","function":{"name":"lookup","parameters":{"type":"object"}}}],
            "tool_choice":"none",
            "parallel_tool_calls":true
        });
        let body = chat_request_body(&request, "claude").unwrap();
        assert!(body.get("tool_choice").is_none());
        assert!(body.get("tools").is_none());
    }

    #[test]
    fn maps_json_schema_and_reasoning_effort() {
        let request = json!({
            "messages":[],
            "response_format":{"type":"json_schema","json_schema":{"name":"answer","schema":{"type":"object"},"strict":true}},
            "reasoning_effort":"high"
        });
        let body = chat_request_body(&request, "claude").unwrap();
        assert_eq!(body["output_config"]["format"]["type"], "json_schema");
        assert_eq!(body["output_config"]["effort"], "high");
    }
}
