//! Bounded, incremental SSE adapters.
//!
//! Each converter accepts arbitrary byte chunks and returns complete OpenAI SSE
//! frames.  It never buffers more than the configured event limit and emits the
//! terminal `[DONE]` frame at most once.

use super::common::{ProtocolError, UpstreamProtocol};
use serde_json::{Value, json};
use std::collections::HashMap;

pub const DEFAULT_MAX_EVENT_BYTES: usize = 1024 * 1024;
const MAX_BATCH_BYTES: usize = 4 * 1024 * 1024;
const MAX_EVENTS_PER_PUSH: usize = 4096;
pub type SseOutput = String;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ReportedUsage {
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub cache_tokens: u64,
}

/// Dispatcher used by the application stream body.  It exposes one small
/// interface while selecting passthrough or a protocol-specific converter.
pub struct OpenAiSseConverter {
    inner: SseConverter,
}

enum SseConverter {
    OpenAi(OpenAiSsePassthrough),
    Anthropic(AnthropicSseConverter),
    Responses(ResponsesSseConverter),
}

impl OpenAiSseConverter {
    pub fn new(protocol: UpstreamProtocol, model: impl Into<String>, include_usage: bool) -> Self {
        let model = model.into();
        let inner = match protocol {
            UpstreamProtocol::OpenAi => SseConverter::OpenAi(
                OpenAiSsePassthrough::with_default_limit_for_model(model)
                    .expect("positive SSE limit"),
            ),
            UpstreamProtocol::Anthropic => SseConverter::Anthropic(
                AnthropicSseConverter::with_default_limit_and_usage(model, include_usage)
                    .expect("positive SSE limit"),
            ),
            UpstreamProtocol::Responses => SseConverter::Responses(
                ResponsesSseConverter::with_default_limit_and_usage(model, include_usage)
                    .expect("positive SSE limit"),
            ),
        };
        Self { inner }
    }

    pub fn push(&mut self, bytes: &[u8]) -> Result<Vec<SseOutput>, ProtocolError> {
        match &mut self.inner {
            SseConverter::OpenAi(converter) => converter.push(bytes),
            SseConverter::Anthropic(converter) => converter.push(bytes),
            SseConverter::Responses(converter) => converter.push(bytes),
        }
    }

    pub fn finish(&mut self) -> Result<Vec<SseOutput>, ProtocolError> {
        match &mut self.inner {
            SseConverter::OpenAi(converter) => converter.finish(),
            SseConverter::Anthropic(converter) => converter.finish(),
            SseConverter::Responses(converter) => converter.finish(),
        }
    }

    pub fn is_done(&self) -> bool {
        match &self.inner {
            SseConverter::OpenAi(converter) => converter.is_done(),
            SseConverter::Anthropic(converter) => converter.is_done(),
            SseConverter::Responses(converter) => converter.is_done(),
        }
    }

    pub fn is_failed(&self) -> bool {
        match &self.inner {
            SseConverter::OpenAi(converter) => converter.is_failed(),
            SseConverter::Anthropic(converter) => converter.is_failed(),
            SseConverter::Responses(converter) => converter.is_failed(),
        }
    }

    pub fn failure_message(&self) -> Option<&str> {
        match &self.inner {
            SseConverter::OpenAi(converter) => converter.failure_message(),
            SseConverter::Anthropic(converter) => converter.failure_message(),
            SseConverter::Responses(converter) => converter.failure_message(),
        }
    }

    pub fn usage(&self) -> Option<ReportedUsage> {
        match &self.inner {
            SseConverter::OpenAi(converter) => converter.usage(),
            SseConverter::Anthropic(converter) => converter.usage(),
            SseConverter::Responses(converter) => converter.usage(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SseEvent {
    pub event: Option<String>,
    pub data: String,
}

struct SseParser {
    buffer: Vec<u8>,
    event: Option<String>,
    data: String,
    event_bytes: usize,
    max_event_bytes: usize,
}

impl SseParser {
    fn new(max_event_bytes: usize) -> Result<Self, ProtocolError> {
        if max_event_bytes == 0 {
            return Err(ProtocolError::stream("SSE event limit must be positive"));
        }
        Ok(Self {
            buffer: Vec::new(),
            event: None,
            data: String::new(),
            event_bytes: 0,
            max_event_bytes,
        })
    }

    fn push(&mut self, mut bytes: &[u8]) -> Result<Vec<SseEvent>, ProtocolError> {
        let mut events = Vec::new();
        let mut batch_bytes = 0_usize;
        while !bytes.is_empty() {
            let room = self.max_event_bytes.saturating_sub(self.buffer.len());
            if room == 0 {
                return Err(ProtocolError::stream("SSE line exceeds buffer limit"));
            }
            let take = room.min(bytes.len());
            self.buffer.extend_from_slice(&bytes[..take]);
            bytes = &bytes[take..];
            while let Some((line_end, terminator_len)) = find_line_end(&self.buffer) {
                let line = self.buffer[..line_end].to_vec();
                self.buffer.drain(..line_end + terminator_len);
                let previous_events = events.len();
                self.consume_line(&line, &mut events)?;
                if events.len() > previous_events {
                    batch_bytes = batch_bytes.saturating_add(
                        events
                            .last()
                            .map(|event| {
                                event.data.len()
                                    + event.event.as_ref().map(String::len).unwrap_or(0)
                            })
                            .unwrap_or(0),
                    );
                    if events.len() > MAX_EVENTS_PER_PUSH || batch_bytes > MAX_BATCH_BYTES {
                        return Err(ProtocolError::stream("SSE network batch exceeds limit"));
                    }
                }
            }
        }
        Ok(events)
    }

    fn finish(&mut self) -> Result<Vec<SseEvent>, ProtocolError> {
        let mut events = Vec::new();
        if !self.buffer.is_empty() {
            if self.buffer.last() == Some(&b'\r') {
                self.buffer.pop();
                let line = std::mem::take(&mut self.buffer);
                self.consume_line(&line, &mut events)?;
                self.consume_line(b"", &mut events)?;
            } else {
                let line = std::mem::take(&mut self.buffer);
                self.consume_line(&line, &mut events)?;
            }
        }
        if self.event.is_some() || !self.data.is_empty() {
            self.emit_event(&mut events);
        }
        Ok(events)
    }

    fn consume_line(
        &mut self,
        line: &[u8],
        events: &mut Vec<SseEvent>,
    ) -> Result<(), ProtocolError> {
        self.event_bytes = self
            .event_bytes
            .saturating_add(line.len())
            .checked_add(1)
            .ok_or_else(|| ProtocolError::stream("SSE event size overflow"))?;
        if self.event_bytes > self.max_event_bytes {
            return Err(ProtocolError::stream("SSE event exceeds buffer limit"));
        }
        let line = std::str::from_utf8(line)
            .map_err(|_| ProtocolError::stream("SSE stream is not UTF-8"))?;
        if line.is_empty() {
            if self.event.is_some() || !self.data.is_empty() {
                self.emit_event(events);
            } else {
                self.event_bytes = 0;
            }
            return Ok(());
        }
        if line.starts_with(':') {
            return Ok(());
        }
        let (field, value) = line
            .split_once(':')
            .map(|(field, value)| (field, value.strip_prefix(' ').unwrap_or(value)))
            .unwrap_or((line, ""));
        match field {
            "event" => self.event = Some(value.to_owned()),
            "data" => {
                if !self.data.is_empty() {
                    self.data.push('\n');
                }
                self.data.push_str(value);
            }
            _ => {}
        }
        Ok(())
    }

    fn emit_event(&mut self, events: &mut Vec<SseEvent>) {
        events.push(SseEvent {
            event: self.event.take(),
            data: std::mem::take(&mut self.data),
        });
        self.event_bytes = 0;
    }
}

fn find_line_end(buffer: &[u8]) -> Option<(usize, usize)> {
    for (index, byte) in buffer.iter().enumerate() {
        if *byte == b'\n' {
            return Some((
                index.saturating_sub((index > 0 && buffer[index - 1] == b'\r') as usize),
                1,
            ));
        }
        if *byte == b'\r' {
            if index + 1 == buffer.len() {
                return None;
            }
            if index + 1 < buffer.len() && buffer[index + 1] == b'\n' {
                return Some((index, 2));
            }
            return Some((index, 1));
        }
    }
    None
}

fn sse_data(data: &str) -> String {
    data.split('\n')
        .map(|line| format!("data: {line}\n"))
        .collect::<String>()
        + "\n"
}

fn done_frame() -> String {
    sse_data("[DONE]")
}

fn error_frame() -> String {
    sse_data(
        r#"{"error":{"message":"PROVIDER: upstream stream error","type":"upstream_error","code":"upstream_error","source":"PROVIDER"}}"#,
    )
}

fn chunk(id: &str, model: &str, delta: Value, finish_reason: Option<&str>) -> String {
    sse_data(
        &json!({
            "id": id,
            "object": "chat.completion.chunk",
            "created": 0,
            "model": model,
            "choices": [{"index": 0, "delta": delta, "finish_reason": finish_reason}]
        })
        .to_string(),
    )
}

fn usage_chunk(id: &str, model: &str, input: u64, output: u64) -> String {
    sse_data(
        &json!({
            "id": id,
            "object": "chat.completion.chunk",
            "created": 0,
            "model": model,
            "choices": [],
            "usage": {"prompt_tokens": input, "completion_tokens": output, "total_tokens": input + output}
        })
        .to_string(),
    )
}

fn event_type(event: &SseEvent, value: &Value) -> Option<String> {
    event
        .event
        .clone()
        .or_else(|| value.get("type").and_then(Value::as_str).map(str::to_owned))
}

fn parse_event(event: &SseEvent) -> Result<Value, ProtocolError> {
    serde_json::from_str(&event.data)
        .map_err(|_| ProtocolError::stream("invalid upstream SSE JSON"))
}

fn provider_error_message(value: &Value) -> Option<String> {
    value
        .pointer("/error/message")
        .or_else(|| value.pointer("/response/error/message"))
        .or_else(|| value.get("message"))
        .or_else(|| value.get("error"))
        .and_then(Value::as_str)
        .map(str::to_owned)
}

/// Incremental Anthropic Messages to OpenAI Chat Completions stream converter.
pub struct AnthropicSseConverter {
    parser: SseParser,
    id: String,
    model: String,
    started: bool,
    finish_emitted: bool,
    done: bool,
    failed: bool,
    failure_message: Option<String>,
    input_tokens: u64,
    output_tokens: u64,
    cache_tokens: u64,
    saw_usage: bool,
    include_usage: bool,
    tool_indices: HashMap<u64, usize>,
    next_tool_index: usize,
}

impl AnthropicSseConverter {
    pub fn new(model: impl Into<String>, max_event_bytes: usize) -> Result<Self, ProtocolError> {
        Ok(Self {
            parser: SseParser::new(max_event_bytes)?,
            id: "anthropic-stream".into(),
            model: model.into(),
            started: false,
            finish_emitted: false,
            done: false,
            failed: false,
            failure_message: None,
            input_tokens: 0,
            output_tokens: 0,
            cache_tokens: 0,
            saw_usage: false,
            include_usage: true,
            tool_indices: HashMap::new(),
            next_tool_index: 0,
        })
    }

    pub fn with_default_limit(model: impl Into<String>) -> Result<Self, ProtocolError> {
        Self::new(model, DEFAULT_MAX_EVENT_BYTES)
    }

    pub fn with_default_limit_and_usage(
        model: impl Into<String>,
        include_usage: bool,
    ) -> Result<Self, ProtocolError> {
        let mut converter = Self::new(model, DEFAULT_MAX_EVENT_BYTES)?;
        converter.include_usage = include_usage;
        Ok(converter)
    }

    pub fn push(&mut self, bytes: &[u8]) -> Result<Vec<SseOutput>, ProtocolError> {
        if self.done {
            return Ok(Vec::new());
        }
        let events = self.parser.push(bytes)?;
        let mut output = Vec::new();
        for event in events {
            self.process(event, &mut output)?;
            if self.done {
                break;
            }
        }
        Ok(output)
    }

    pub fn finish(&mut self) -> Result<Vec<SseOutput>, ProtocolError> {
        if self.done {
            return Ok(Vec::new());
        }
        let events = self.parser.finish()?;
        let mut output = Vec::new();
        for event in events {
            self.process(event, &mut output)?;
            if self.done {
                return Ok(output);
            }
        }
        if !self.finish_emitted {
            output.push(chunk(&self.id, &self.model, json!({}), Some("stop")));
            self.finish_emitted = true;
        }
        output.push(done_frame());
        self.done = true;
        Ok(output)
    }

    pub fn is_done(&self) -> bool {
        self.done
    }

    pub fn is_failed(&self) -> bool {
        self.failed
    }

    pub fn failure_message(&self) -> Option<&str> {
        self.failure_message.as_deref()
    }

    pub fn usage(&self) -> Option<ReportedUsage> {
        self.saw_usage.then_some(ReportedUsage {
            input_tokens: self.input_tokens,
            output_tokens: self.output_tokens,
            cache_tokens: self.cache_tokens,
        })
    }

    fn process(
        &mut self,
        event: SseEvent,
        output: &mut Vec<SseOutput>,
    ) -> Result<(), ProtocolError> {
        if event.data.trim() == "[DONE]" {
            self.terminate(output);
            return Ok(());
        }
        let value = match parse_event(&event) {
            Ok(value) => value,
            Err(_) => {
                self.failure_message = Some("invalid upstream SSE JSON".to_owned());
                self.terminate_error(output);
                return Ok(());
            }
        };
        let Some(kind) = event_type(&event, &value) else {
            return Ok(());
        };
        match kind.as_str() {
            "message_start" => {
                if let Some(message) = value.get("message") {
                    self.id = message
                        .get("id")
                        .and_then(Value::as_str)
                        .unwrap_or(&self.id)
                        .to_owned();
                    if let Some(usage) = message.get("usage") {
                        let uncached = usage["input_tokens"].as_u64().unwrap_or(0);
                        let created = usage["cache_creation_input_tokens"].as_u64().unwrap_or(0);
                        self.input_tokens = uncached.saturating_add(created);
                        self.cache_tokens = usage["cache_read_input_tokens"].as_u64().unwrap_or(0);
                        self.saw_usage = true;
                    }
                }
                if !self.started {
                    output.push(chunk(
                        &self.id,
                        &self.model,
                        json!({"role":"assistant","content":""}),
                        None,
                    ));
                    self.started = true;
                }
            }
            "content_block_start" => {
                let index = value.get("index").and_then(Value::as_u64).unwrap_or(0);
                if let Some(block) = value.get("content_block") {
                    if block.get("type").and_then(Value::as_str) == Some("tool_use") {
                        let tool_index = self.tool_index(index);
                        output.push(chunk(
                            &self.id,
                            &self.model,
                            json!({"tool_calls":[{"index":tool_index,"id":block.get("id").cloned().unwrap_or(Value::Null),"type":"function","function":{"name":block.get("name").cloned().unwrap_or(Value::String(String::new())),"arguments":""}}]}),
                            None,
                        ));
                    }
                }
            }
            "content_block_delta" => {
                let delta = value.get("delta").unwrap_or(&Value::Null);
                match delta.get("type").and_then(Value::as_str) {
                    Some("text_delta") => {
                        if let Some(text) = delta.get("text").and_then(Value::as_str) {
                            output.push(chunk(
                                &self.id,
                                &self.model,
                                json!({"content":text}),
                                None,
                            ));
                        }
                    }
                    Some("input_json_delta") => {
                        let block = value.get("index").and_then(Value::as_u64).unwrap_or(0);
                        let tool_index = self.tool_index(block);
                        output.push(chunk(&self.id, &self.model, json!({"tool_calls":[{"index":tool_index,"function":{"arguments":delta.get("partial_json").and_then(Value::as_str).unwrap_or("")}}]}), None));
                    }
                    _ => {}
                }
            }
            "message_delta" => {
                if let Some(usage) = value.get("usage") {
                    let output_tokens = usage
                        .get("output_tokens")
                        .and_then(Value::as_u64)
                        .unwrap_or(0);
                    self.output_tokens = output_tokens;
                    self.saw_usage = true;
                    if let Some(reason) = value["delta"]["stop_reason"].as_str() {
                        self.emit_finish(output, reason);
                    }
                    if self.include_usage {
                        output.push(usage_chunk(
                            &self.id,
                            &self.model,
                            self.input_tokens,
                            output_tokens,
                        ));
                    }
                } else if let Some(reason) = value["delta"]["stop_reason"].as_str() {
                    self.emit_finish(output, reason);
                }
            }
            "message_stop" => self.terminate(output),
            "error" => {
                self.failure_message = provider_error_message(&value);
                self.terminate_error(output);
            }
            "ping" => {}
            _ => {}
        }
        Ok(())
    }

    fn tool_index(&mut self, block: u64) -> usize {
        if let Some(index) = self.tool_indices.get(&block) {
            return *index;
        }
        let index = self.next_tool_index;
        self.next_tool_index += 1;
        self.tool_indices.insert(block, index);
        index
    }

    fn emit_finish(&mut self, output: &mut Vec<SseOutput>, reason: &str) {
        if self.finish_emitted {
            return;
        }
        let reason = match reason {
            "tool_use" => "tool_calls",
            "max_tokens" => "length",
            "stop_sequence" => "stop",
            _ => "stop",
        };
        output.push(chunk(&self.id, &self.model, json!({}), Some(reason)));
        self.finish_emitted = true;
    }

    fn terminate(&mut self, output: &mut Vec<SseOutput>) {
        if !self.finish_emitted {
            self.emit_finish(output, "end_turn");
        }
        if !self.done {
            output.push(done_frame());
            self.done = true;
        }
    }

    fn terminate_error(&mut self, output: &mut Vec<SseOutput>) {
        self.failed = true;
        self.failure_message
            .get_or_insert_with(|| "upstream stream error".to_owned());
        if !self.done {
            output.push(error_frame());
            output.push(done_frame());
            self.done = true;
        }
    }
}

/// Incremental OpenAI Responses to OpenAI Chat Completions stream converter.
pub struct ResponsesSseConverter {
    parser: SseParser,
    id: String,
    model: String,
    started: bool,
    finish_emitted: bool,
    done: bool,
    failed: bool,
    failure_message: Option<String>,
    input_tokens: u64,
    output_tokens: u64,
    cache_tokens: u64,
    saw_usage: bool,
    include_usage: bool,
    saw_tool_call: bool,
    tool_indices: HashMap<String, usize>,
    tool_arguments: HashMap<String, String>,
    next_tool_index: usize,
}

impl ResponsesSseConverter {
    pub fn new(model: impl Into<String>, max_event_bytes: usize) -> Result<Self, ProtocolError> {
        Ok(Self {
            parser: SseParser::new(max_event_bytes)?,
            id: "responses-stream".into(),
            model: model.into(),
            started: false,
            finish_emitted: false,
            done: false,
            failed: false,
            failure_message: None,
            input_tokens: 0,
            output_tokens: 0,
            cache_tokens: 0,
            saw_usage: false,
            include_usage: true,
            saw_tool_call: false,
            tool_indices: HashMap::new(),
            tool_arguments: HashMap::new(),
            next_tool_index: 0,
        })
    }

    pub fn with_default_limit(model: impl Into<String>) -> Result<Self, ProtocolError> {
        Self::new(model, DEFAULT_MAX_EVENT_BYTES)
    }

    pub fn with_default_limit_and_usage(
        model: impl Into<String>,
        include_usage: bool,
    ) -> Result<Self, ProtocolError> {
        let mut converter = Self::new(model, DEFAULT_MAX_EVENT_BYTES)?;
        converter.include_usage = include_usage;
        Ok(converter)
    }

    pub fn push(&mut self, bytes: &[u8]) -> Result<Vec<SseOutput>, ProtocolError> {
        if self.done {
            return Ok(Vec::new());
        }
        let events = self.parser.push(bytes)?;
        let mut output = Vec::new();
        for event in events {
            self.process(event, &mut output)?;
            if self.done {
                break;
            }
        }
        Ok(output)
    }

    pub fn finish(&mut self) -> Result<Vec<SseOutput>, ProtocolError> {
        if self.done {
            return Ok(Vec::new());
        }
        let events = self.parser.finish()?;
        let mut output = Vec::new();
        for event in events {
            self.process(event, &mut output)?;
            if self.done {
                return Ok(output);
            }
        }
        self.terminate(&mut output);
        Ok(output)
    }

    pub fn is_done(&self) -> bool {
        self.done
    }

    pub fn is_failed(&self) -> bool {
        self.failed
    }

    pub fn failure_message(&self) -> Option<&str> {
        self.failure_message.as_deref()
    }

    pub fn usage(&self) -> Option<ReportedUsage> {
        self.saw_usage.then_some(ReportedUsage {
            input_tokens: self.input_tokens,
            output_tokens: self.output_tokens,
            cache_tokens: self.cache_tokens,
        })
    }

    fn process(
        &mut self,
        event: SseEvent,
        output: &mut Vec<SseOutput>,
    ) -> Result<(), ProtocolError> {
        if event.data.trim() == "[DONE]" {
            self.terminate(output);
            return Ok(());
        }
        let value = match parse_event(&event) {
            Ok(value) => value,
            Err(_) => {
                self.failure_message = Some("invalid upstream SSE JSON".to_owned());
                self.terminate_error(output);
                return Ok(());
            }
        };
        let Some(kind) = event_type(&event, &value) else {
            return Ok(());
        };
        match kind.as_str() {
            "response.created" => {
                if let Some(response) = value.get("response") {
                    self.id = response
                        .get("id")
                        .and_then(Value::as_str)
                        .unwrap_or(&self.id)
                        .to_owned();
                }
                self.emit_role(output);
            }
            "response.output_item.added" => {
                let item = value.get("item").unwrap_or(&Value::Null);
                match item.get("type").and_then(Value::as_str) {
                    Some("function_call") => {
                        self.saw_tool_call = true;
                        let key = item
                            .get("id")
                            .or_else(|| item.get("call_id"))
                            .and_then(Value::as_str)
                            .unwrap_or("tool")
                            .to_owned();
                        let index = self.tool_index(&key);
                        output.push(chunk(&self.id, &self.model, json!({"tool_calls":[{"index":index,"id":item.get("call_id").or_else(|| item.get("id")).cloned().unwrap_or(Value::Null),"type":"function","function":{"name":item.get("name").cloned().unwrap_or(Value::String(String::new())),"arguments":""}}]}), None));
                    }
                    Some("message") => self.emit_role(output),
                    _ => {}
                }
            }
            "response.output_text.delta" => {
                self.emit_started(output);
                if let Some(delta) = value.get("delta").and_then(Value::as_str) {
                    output.push(chunk(&self.id, &self.model, json!({"content":delta}), None));
                }
            }
            "response.function_call_arguments.delta" => {
                self.saw_tool_call = true;
                let key = value
                    .get("item_id")
                    .and_then(Value::as_str)
                    .unwrap_or("tool")
                    .to_owned();
                let index = self.tool_index(&key);
                let delta = value.get("delta").and_then(Value::as_str).unwrap_or("");
                self.tool_arguments.entry(key).or_default().push_str(delta);
                output.push(chunk(
                    &self.id,
                    &self.model,
                    json!({"tool_calls":[{"index":index,"function":{"arguments":delta}}]}),
                    None,
                ));
            }
            "response.function_call_arguments.done" => {
                let key = value
                    .get("item_id")
                    .and_then(Value::as_str)
                    .unwrap_or("tool")
                    .to_owned();
                let arguments = value
                    .get("arguments")
                    .and_then(Value::as_str)
                    .unwrap_or("{}");
                self.emit_completed_arguments(output, key, arguments);
            }
            "response.output_item.done" => {
                let item = value.get("item").unwrap_or(&Value::Null);
                if item.get("type").and_then(Value::as_str) == Some("function_call") {
                    let key = item
                        .get("id")
                        .or_else(|| item.get("call_id"))
                        .and_then(Value::as_str)
                        .unwrap_or("tool")
                        .to_owned();
                    let arguments = item
                        .get("arguments")
                        .and_then(Value::as_str)
                        .unwrap_or("{}");
                    self.emit_completed_arguments(output, key, arguments);
                }
            }
            "response.completed" => {
                let response = value.get("response").unwrap_or(&value);
                self.capture_usage(response);
                self.emit_finish(
                    output,
                    if self.saw_tool_call {
                        "tool_calls"
                    } else {
                        "stop"
                    },
                );
                self.emit_usage(output, response);
                self.terminate(output);
            }
            "response.incomplete" => {
                let response = value.get("response").unwrap_or(&value);
                self.capture_usage(response);
                self.emit_finish(output, "length");
                self.emit_usage(output, response);
                self.terminate(output);
            }
            "response.failed" | "error" => {
                self.failure_message = provider_error_message(&value);
                self.terminate_error(output);
            }
            _ => {}
        }
        Ok(())
    }

    fn emit_started(&mut self, output: &mut Vec<SseOutput>) {
        if !self.started {
            self.emit_role(output);
        }
    }

    fn emit_role(&mut self, output: &mut Vec<SseOutput>) {
        if self.started {
            return;
        }
        output.push(chunk(
            &self.id,
            &self.model,
            json!({"role":"assistant","content":""}),
            None,
        ));
        self.started = true;
    }

    fn tool_index(&mut self, key: &str) -> usize {
        if let Some(index) = self.tool_indices.get(key) {
            return *index;
        }
        let index = self.next_tool_index;
        self.next_tool_index += 1;
        self.tool_indices.insert(key.to_owned(), index);
        index
    }

    fn emit_completed_arguments(
        &mut self,
        output: &mut Vec<SseOutput>,
        key: String,
        completed: &str,
    ) {
        let streamed = self
            .tool_arguments
            .get(&key)
            .map(String::as_str)
            .unwrap_or("");
        let Some(suffix) = completed.strip_prefix(streamed) else {
            self.failure_message = Some("invalid completed tool arguments".to_owned());
            self.terminate_error(output);
            return;
        };
        if !suffix.is_empty() {
            let suffix = suffix.to_owned();
            let index = self.tool_index(&key);
            output.push(chunk(
                &self.id,
                &self.model,
                json!({"tool_calls":[{"index":index,"function":{"arguments":suffix}}]}),
                None,
            ));
        }
        self.tool_arguments.insert(key, completed.to_owned());
    }

    fn capture_usage(&mut self, response: &Value) {
        let Some(usage) = response.get("usage") else {
            return;
        };
        let total_input = usage["input_tokens"].as_u64().unwrap_or(self.input_tokens);
        self.cache_tokens = usage["input_tokens_details"]["cached_tokens"]
            .as_u64()
            .unwrap_or(self.cache_tokens);
        self.input_tokens = total_input.saturating_sub(self.cache_tokens);
        self.output_tokens = usage["output_tokens"]
            .as_u64()
            .unwrap_or(self.output_tokens);
        self.saw_usage = true;
    }

    fn emit_usage(&self, output: &mut Vec<SseOutput>, response: &Value) {
        if !self.include_usage {
            return;
        }
        if let Some(usage) = response.get("usage").and_then(Value::as_object) {
            let input = usage
                .get("input_tokens")
                .and_then(Value::as_u64)
                .unwrap_or(self.input_tokens);
            let completion = usage
                .get("output_tokens")
                .and_then(Value::as_u64)
                .unwrap_or(0);
            output.push(usage_chunk(&self.id, &self.model, input, completion));
        }
    }

    fn emit_finish(&mut self, output: &mut Vec<SseOutput>, reason: &str) {
        if !self.finish_emitted {
            output.push(chunk(&self.id, &self.model, json!({}), Some(reason)));
            self.finish_emitted = true;
        }
    }

    fn terminate(&mut self, output: &mut Vec<SseOutput>) {
        self.emit_started(output);
        if !self.finish_emitted {
            self.emit_finish(
                output,
                if self.saw_tool_call {
                    "tool_calls"
                } else {
                    "stop"
                },
            );
        }
        if !self.done {
            output.push(done_frame());
            self.done = true;
        }
    }

    fn terminate_error(&mut self, output: &mut Vec<SseOutput>) {
        self.failed = true;
        self.failure_message
            .get_or_insert_with(|| "upstream stream error".to_owned());
        if !self.done {
            output.push(error_frame());
            output.push(done_frame());
            self.done = true;
        }
    }
}

/// A bounded OpenAI SSE framer.  It preserves upstream `data` and `event`
/// values while making arbitrary byte chunking safe for the downstream socket.
pub struct OpenAiSsePassthrough {
    parser: SseParser,
    done: bool,
    failed: bool,
    failure_message: Option<String>,
    public_model: Option<String>,
    usage: Option<ReportedUsage>,
}

impl OpenAiSsePassthrough {
    pub fn new(max_event_bytes: usize) -> Result<Self, ProtocolError> {
        Ok(Self {
            parser: SseParser::new(max_event_bytes)?,
            done: false,
            failed: false,
            failure_message: None,
            public_model: None,
            usage: None,
        })
    }

    pub fn with_default_limit() -> Result<Self, ProtocolError> {
        Self::new(DEFAULT_MAX_EVENT_BYTES)
    }

    pub fn with_default_limit_for_model(model: impl Into<String>) -> Result<Self, ProtocolError> {
        Ok(Self {
            parser: SseParser::new(DEFAULT_MAX_EVENT_BYTES)?,
            done: false,
            failed: false,
            failure_message: None,
            public_model: Some(model.into()),
            usage: None,
        })
    }

    pub fn push(&mut self, bytes: &[u8]) -> Result<Vec<SseOutput>, ProtocolError> {
        if self.done {
            return Ok(Vec::new());
        }
        let events = self.parser.push(bytes)?;
        Ok(self.render(events))
    }

    pub fn finish(&mut self) -> Result<Vec<SseOutput>, ProtocolError> {
        if self.done {
            return Ok(Vec::new());
        }
        let events = self.parser.finish()?;
        let mut output = self.render(events);
        if !self.done {
            output.push(done_frame());
            self.done = true;
        }
        Ok(output)
    }

    pub fn is_done(&self) -> bool {
        self.done
    }

    pub fn is_failed(&self) -> bool {
        self.failed
    }

    pub fn failure_message(&self) -> Option<&str> {
        self.failure_message.as_deref()
    }

    pub fn usage(&self) -> Option<ReportedUsage> {
        self.usage
    }

    fn render(&mut self, events: Vec<SseEvent>) -> Vec<SseOutput> {
        let mut output = Vec::new();
        for event in events {
            if event.data.trim() == "[DONE]" {
                if !self.done {
                    output.push(done_frame());
                    self.done = true;
                }
                break;
            }
            let data = match serde_json::from_str::<Value>(&event.data) {
                Ok(mut value) => {
                    self.capture_usage(&value);
                    if let Some(error) = value.get_mut("error").and_then(Value::as_object_mut) {
                        self.failed = true;
                        let message = error
                            .get("message")
                            .and_then(Value::as_str)
                            .unwrap_or("upstream stream error");
                        self.failure_message = Some(message.to_owned());
                        if !message.starts_with("PROVIDER:") {
                            error.insert(
                                "message".into(),
                                Value::String(format!("PROVIDER: {message}")),
                            );
                        }
                        error.insert("source".into(), Value::String("PROVIDER".into()));
                        error
                            .entry("type")
                            .or_insert_with(|| Value::String("upstream_error".into()));
                        error
                            .entry("code")
                            .or_insert_with(|| Value::String("upstream_error".into()));
                    }
                    if let (Some(object), Some(model)) =
                        (value.as_object_mut(), self.public_model.clone())
                    {
                        object.insert("model".into(), Value::String(model));
                    }
                    value.to_string()
                }
                Err(_) => event.data,
            };
            let mut frame = String::new();
            if let Some(name) = event.event {
                frame.push_str("event: ");
                frame.push_str(&name);
                frame.push('\n');
            }
            frame.push_str(&sse_data(&data));
            output.push(frame);
        }
        output
    }

    fn capture_usage(&mut self, value: &Value) {
        let Some(usage) = value.get("usage") else {
            return;
        };
        let total_input = usage["prompt_tokens"]
            .as_u64()
            .or_else(|| usage["input_tokens"].as_u64())
            .unwrap_or(0);
        let cache_tokens = usage["prompt_tokens_details"]["cached_tokens"]
            .as_u64()
            .or_else(|| usage["input_tokens_details"]["cached_tokens"].as_u64())
            .unwrap_or(0);
        let output_tokens = usage["completion_tokens"]
            .as_u64()
            .or_else(|| usage["output_tokens"].as_u64())
            .unwrap_or(0);
        self.usage = Some(ReportedUsage {
            input_tokens: total_input.saturating_sub(cache_tokens),
            output_tokens,
            cache_tokens,
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn join(frames: &[String]) -> String {
        frames.concat()
    }

    fn streamed_tool_arguments(frames: &[String]) -> String {
        frames
            .iter()
            .flat_map(|frame| frame.lines())
            .filter_map(|line| line.strip_prefix("data: "))
            .filter_map(|data| serde_json::from_str::<Value>(data).ok())
            .filter_map(|value| {
                value["choices"][0]["delta"]["tool_calls"][0]["function"]["arguments"]
                    .as_str()
                    .map(str::to_owned)
            })
            .collect()
    }

    #[test]
    fn parser_handles_split_utf8_safe_ascii_frames_and_multiline_data() {
        let mut passthrough = OpenAiSsePassthrough::new(1024).unwrap();
        assert!(
            passthrough
                .push(b"event: message\ndata: {\"a\":")
                .unwrap()
                .is_empty()
        );
        let frames = passthrough.push(b"1}\ndata: {\"b\":2}\n\n").unwrap();
        assert_eq!(frames.len(), 1);
        assert_eq!(
            frames[0],
            "event: message\ndata: {\"a\":1}\ndata: {\"b\":2}\n\n"
        );
    }

    #[test]
    fn anthropic_tool_arguments_survive_split_frames_and_done_is_once() {
        let mut converter = AnthropicSseConverter::new("claude", 4096).unwrap();
        let first = b"event: message_start\ndata: {\"message\":{\"id\":\"m\",\"model\":\"claude\",\"usage\":{\"input_tokens\":2}}}\n\nevent: content_block_start\ndata: {\"index\":0,\"content_block\":{\"type\":\"tool_use\",\"id\":\"t\",\"name\":\"f\"}}\n\n";
        let second = b"event: content_block_delta\ndata: {\"index\":0,\"delta\":{\"type\":\"input_json_delta\",\"partial_json\":\"{\\\"x\\\"\"}}\n\n";
        let mut output = converter.push(&first[..37]).unwrap();
        output.extend(converter.push(&first[37..]).unwrap());
        output.extend(converter.push(&second[..18]).unwrap());
        output.extend(converter.push(&second[18..]).unwrap());
        output.extend(converter.push(b"event: message_delta\ndata: {\"delta\":{\"stop_reason\":\"tool_use\"},\"usage\":{\"output_tokens\":3}}\n\n").unwrap());
        output.extend(
            converter
                .push(b"event: message_stop\ndata: {}\n\n")
                .unwrap(),
        );
        output.extend(converter.finish().unwrap());
        output.extend(converter.finish().unwrap());
        let all = join(&output);
        assert!(all.contains("chat.completion.chunk"));
        assert!(!all.contains("partial_json"));
        assert_eq!(all.matches("data: [DONE]").count(), 1);
        assert!(all.contains("tool_calls"));
    }

    #[test]
    fn responses_stream_maps_text_and_usage() {
        let mut converter = ResponsesSseConverter::new("gpt", 4096).unwrap();
        let mut output = converter
            .push(b"event: response.created\ndata: {\"response\":{\"id\":\"r\",\"model\":\"gpt\"}}\n\n")
            .unwrap();
        output.extend(
            converter
                .push(b"event: response.output_text.delta\ndata: {\"delta\":\"hi\"}\n\n")
                .unwrap(),
        );
        output.extend(converter.push(b"event: response.completed\ndata: {\"response\":{\"usage\":{\"input_tokens\":1,\"output_tokens\":2}}}\n\n").unwrap());
        output.extend(converter.finish().unwrap());
        assert!(join(&output).contains("hi"));
        assert!(join(&output).contains("total_tokens"));
        assert_eq!(join(&output).matches("data: [DONE]").count(), 1);
    }

    #[test]
    fn bounded_parser_rejects_large_partial_frame() {
        let mut passthrough = OpenAiSsePassthrough::new(4).unwrap();
        assert!(passthrough.push(b"data: too-large").is_err());
    }

    #[test]
    fn parser_accepts_large_network_chunk_of_small_events() {
        let mut passthrough = OpenAiSsePassthrough::new(64).unwrap();
        let batch = "data: {}\n\n".repeat(100);
        let frames = passthrough.push(batch.as_bytes()).unwrap();
        assert_eq!(frames.len(), 100);
    }

    #[test]
    fn terminal_events_mark_converters_done_without_eof() {
        let mut anthropic = AnthropicSseConverter::new("public/claude", 1024).unwrap();
        let frames = anthropic
            .push(b"event: message_stop\ndata: {}\n\n")
            .unwrap();
        assert!(anthropic.is_done());
        assert_eq!(join(&frames).matches("data: [DONE]").count(), 1);

        let mut responses = ResponsesSseConverter::new("public/gpt", 1024).unwrap();
        let frames = responses
            .push(b"event: response.completed\ndata: {\"response\":{}}\n\n")
            .unwrap();
        assert!(responses.is_done());
        assert_eq!(join(&frames).matches("data: [DONE]").count(), 1);
    }

    #[test]
    fn responses_tool_arguments_done_supplies_full_arguments_without_deltas() {
        let mut converter = ResponsesSseConverter::new("public/gpt", 4096).unwrap();
        let mut frames = converter
            .push(b"event: response.output_item.added\ndata: {\"item\":{\"type\":\"function_call\",\"id\":\"item_1\",\"call_id\":\"call_1\",\"name\":\"lookup\"}}\n\n")
            .unwrap();
        frames.extend(
            converter
                .push(b"event: response.function_call_arguments.done\ndata: {\"item_id\":\"item_1\",\"arguments\":\"{\\\"q\\\":1}\"}\n\n")
                .unwrap(),
        );
        assert_eq!(streamed_tool_arguments(&frames), "{\"q\":1}");
    }

    #[test]
    fn responses_arguments_done_appends_authoritative_missing_suffix() {
        let mut converter = ResponsesSseConverter::new("public/gpt", 4096).unwrap();
        let mut frames = converter
            .push(b"event: response.function_call_arguments.delta\ndata: {\"item_id\":\"item_1\",\"delta\":\"{\\\"q\\\":\"}\n\n")
            .unwrap();
        frames.extend(
            converter
                .push(b"event: response.function_call_arguments.done\ndata: {\"item_id\":\"item_1\",\"arguments\":\"{\\\"q\\\":1}\"}\n\n")
                .unwrap(),
        );
        assert_eq!(streamed_tool_arguments(&frames), "{\"q\":1}");
    }

    #[test]
    fn parser_rejects_excessive_event_count_in_one_push() {
        let mut passthrough = OpenAiSsePassthrough::new(64).unwrap();
        let batch = "data: {}\n\n".repeat(MAX_EVENTS_PER_PUSH + 1);
        assert!(passthrough.push(batch.as_bytes()).is_err());
    }

    #[test]
    fn parser_counts_event_names_toward_batch_limit() {
        let mut passthrough = OpenAiSsePassthrough::new(DEFAULT_MAX_EVENT_BYTES).unwrap();
        let name = "x".repeat(900_000);
        let batch = format!("event: {name}\ndata: {{}}\n\n").repeat(5);
        assert!(passthrough.push(batch.as_bytes()).is_err());
    }
}
