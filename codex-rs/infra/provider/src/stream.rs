use std::collections::BTreeMap;
use std::num::NonZeroUsize;

use serde_json::Value;
use serde_json::json;

use crate::TranslationError;

type Result<T> = std::result::Result<T, TranslationError>;

/// One streaming completion attempt. The caller supplies a unique Responses ID
/// and a byte budget from its provider profile. Feed decoded SSE JSON chunks,
/// then call finish only upon receiving the provider's explicit [DONE] marker.
pub struct ChatStream {
    response_id: String,
    provider_id: Option<String>,
    remaining: usize,
    sequence: u64,
    created: bool,
    ended: bool,
    finish_reason: Option<String>,
    items: Vec<Value>,
    text_index: Option<usize>,
    reasoning_index: Option<usize>,
    tools: BTreeMap<u64, usize>,
    usage: Value,
}

impl ChatStream {
    pub fn new(response_id: String, budget: NonZeroUsize) -> Self {
        Self {
            response_id,
            provider_id: None,
            remaining: budget.get(),
            sequence: 0,
            created: false,
            ended: false,
            finish_reason: None,
            items: Vec::new(),
            text_index: None,
            reasoning_index: None,
            tools: BTreeMap::new(),
            usage: Value::Null,
        }
    }

    pub fn push(&mut self, chunk: &Value) -> Result<Vec<Value>> {
        if self.ended {
            return Err(TranslationError::Invalid(
                "chunk after terminal event".to_owned(),
            ));
        }
        // A conversion failure terminates this attempt; callers cannot continue
        // from a partially applied chunk and accidentally produce success.
        self.ended = true;
        let events = self.convert(chunk)?;
        self.ended = false;
        Ok(events)
    }

    fn convert(&mut self, chunk: &Value) -> Result<Vec<Value>> {
        self.remaining = self
            .remaining
            .checked_sub(chunk.to_string().len())
            .ok_or_else(|| {
                TranslationError::Invalid("completion exceeds byte budget".to_owned())
            })?;
        if !chunk["error"].is_null() {
            return Err(TranslationError::Invalid(format!(
                "provider error: {}",
                chunk["error"]
            )));
        }
        let id = chunk["id"]
            .as_str()
            .ok_or_else(|| TranslationError::Invalid("chunk.id".to_owned()))?;
        if let Some(previous) = &self.provider_id {
            if previous != id {
                return Err(TranslationError::Invalid(
                    "completion ID changed".to_owned(),
                ));
            }
        } else {
            self.provider_id = Some(id.to_owned());
        }
        let mut events = Vec::new();
        if !self.created {
            events.push(self.event(json!({"type": "response.created", "response": {
                "id": self.response_id, "object": "response", "status": "in_progress", "output": [],
            }})));
            self.created = true;
        }
        if !chunk["usage"].is_null() {
            let usage = &chunk["usage"];
            for field in ["prompt_tokens", "completion_tokens", "total_tokens"] {
                if usage[field].as_u64().is_none() {
                    return Err(TranslationError::Invalid(format!("usage.{field}")));
                }
            }
            self.usage = json!({
                "input_tokens": usage["prompt_tokens"],
                "output_tokens": usage["completion_tokens"],
                "total_tokens": usage["total_tokens"],
                "input_tokens_details": {"cached_tokens": usage.pointer("/prompt_tokens_details/cached_tokens")
                    .or_else(|| usage.get("prompt_cache_hit_tokens")).cloned().unwrap_or(json!(0))},
                "output_tokens_details": {"reasoning_tokens": usage.pointer("/completion_tokens_details/reasoning_tokens")
                    .cloned().unwrap_or(json!(0))},
            });
        }
        for choice in chunk["choices"]
            .as_array()
            .ok_or_else(|| TranslationError::Invalid("chunk.choices".to_owned()))?
        {
            if choice["index"].as_u64() != Some(0) {
                return Err(TranslationError::Unsupported(
                    "multiple completion choices".to_owned(),
                ));
            }
            if self.finish_reason.is_some() {
                return Err(TranslationError::Invalid(
                    "choice after finish_reason".to_owned(),
                ));
            }
            let delta = &choice["delta"];
            if let Some(reasoning) = delta
                .get("reasoning_content")
                .filter(|value| !value.is_null())
            {
                let text = reasoning.as_str().ok_or_else(|| {
                    TranslationError::Invalid("delta.reasoning_content".to_owned())
                })?;
                if !text.is_empty() {
                    let index = match self.reasoning_index {
                        Some(index) => index,
                        None => {
                            if !self.items.is_empty() {
                                return Err(TranslationError::Invalid(
                                    "reasoning started after assistant output".to_owned(),
                                ));
                            }
                            let index = self.items.len();
                            let item = json!({"type": "reasoning", "status": "in_progress",
                                "id": format!("{}_reasoning_{index}", self.response_id), "summary": [],
                                "content": [{"type": "reasoning_text", "text": ""}], "encrypted_content": null});
                            events.push(self.event(json!({"type": "response.output_item.added", "output_index": index, "item": item})));
                            self.items.push(item);
                            self.reasoning_index = Some(index);
                            index
                        }
                    };
                    append(&mut self.items[index]["content"][0]["text"], text)?;
                    events.push(self.event(
                        json!({"type": "response.reasoning_text.delta", "output_index": index,
                        "item_id": self.items[index]["id"], "content_index": 0, "delta": text}),
                    ));
                }
            }
            if let Some(text) = delta["content"].as_str().filter(|text| !text.is_empty()) {
                let index = match self.text_index {
                    Some(index) => index,
                    None => {
                        let index = self.items.len();
                        let item = json!({"type": "message", "role": "assistant", "status": "in_progress",
                            "id": format!("{}_message_{index}", self.response_id),
                            "content": [{"type": "output_text", "text": "", "annotations": []}]});
                        events.push(self.event(json!({"type": "response.output_item.added", "output_index": index, "item": item})));
                        self.items.push(item);
                        self.text_index = Some(index);
                        index
                    }
                };
                append(&mut self.items[index]["content"][0]["text"], text)?;
                events.push(self.event(
                    json!({"type": "response.output_text.delta", "output_index": index,
                    "item_id": self.items[index]["id"], "content_index": 0, "delta": text}),
                ));
            }
            if let Some(calls) = delta.get("tool_calls").filter(|value| !value.is_null()) {
                for call in calls
                    .as_array()
                    .ok_or_else(|| TranslationError::Invalid("delta.tool_calls".to_owned()))?
                {
                    let tool_index = call["index"]
                        .as_u64()
                        .ok_or_else(|| TranslationError::Invalid("tool index".to_owned()))?;
                    let index = match self.tools.get(&tool_index) {
                        Some(index) => *index,
                        None => {
                            let call_id = call["id"].as_str().ok_or_else(|| {
                                TranslationError::Invalid("tool call ID".to_owned())
                            })?;
                            let name = call["function"]["name"]
                                .as_str()
                                .ok_or_else(|| TranslationError::Invalid("tool name".to_owned()))?;
                            let index = self.items.len();
                            let item = json!({"type": "function_call", "status": "in_progress",
                                "id": format!("{}_function_{index}", self.response_id),
                                "call_id": call_id, "name": name, "arguments": ""});
                            events.push(self.event(json!({"type": "response.output_item.added", "output_index": index, "item": item})));
                            self.items.push(item);
                            self.tools.insert(tool_index, index);
                            index
                        }
                    };
                    if call
                        .get("id")
                        .is_some_and(|id| !id.is_null() && id != &self.items[index]["call_id"])
                        || call["function"].get("name").is_some_and(|name| {
                            !name.is_null() && name != &self.items[index]["name"]
                        })
                    {
                        return Err(TranslationError::Invalid(
                            "tool identity changed within stream".to_owned(),
                        ));
                    }
                    if let Some(arguments) = call["function"]["arguments"].as_str() {
                        append(&mut self.items[index]["arguments"], arguments)?;
                        events.push(self.event(json!({"type": "response.function_call_arguments.delta",
                            "output_index": index, "item_id": self.items[index]["id"], "delta": arguments})));
                    }
                }
            }
            if let Some(reason) = choice["finish_reason"].as_str() {
                self.finish_reason = Some(reason.to_owned());
            }
        }
        Ok(events)
    }

    /// A network EOF without [DONE] must be reported by the transport, not
    /// converted into a completed response. Usage may arrive after finish_reason.
    pub fn finish(&mut self) -> Result<Vec<Value>> {
        if self.ended {
            return Err(TranslationError::Invalid(
                "attempt already ended".to_owned(),
            ));
        }
        self.ended = true;
        let reason = self
            .finish_reason
            .as_deref()
            .ok_or_else(|| TranslationError::Invalid("missing finish_reason".to_owned()))?;
        let incomplete = match reason {
            "stop" | "tool_calls" => None,
            "length" => Some("max_output_tokens"),
            "content_filter" => Some("content_filter"),
            other => {
                return Err(TranslationError::Unsupported(format!(
                    "finish_reason {other}"
                )));
            }
        };
        let mut events = Vec::new();
        if incomplete.is_none() {
            for index in 0..self.items.len() {
                self.items[index]["status"] = json!("completed");
                if self.text_index == Some(index) {
                    events.push(self.event(
                        json!({"type": "response.output_text.done", "output_index": index,
                        "item_id": self.items[index]["id"], "content_index": 0,
                        "text": self.items[index]["content"][0]["text"]}),
                    ));
                } else if self.reasoning_index == Some(index) {
                    let text = self.items[index]["content"][0]["text"]
                        .as_str()
                        .ok_or_else(|| {
                            TranslationError::Invalid("reasoning accumulator".to_owned())
                        })?;
                    self.items[index]["encrypted_content"] =
                        json!(format!("{}{text}", crate::CHAT_REASONING_PREFIX));
                    events.push(self.event(json!({"type": "response.reasoning_text.done", "output_index": index,
                        "item_id": self.items[index]["id"], "content_index": 0, "text": self.items[index]["content"][0]["text"]})));
                } else {
                    events.push(self.event(json!({"type": "response.function_call_arguments.done", "output_index": index,
                        "item_id": self.items[index]["id"], "arguments": self.items[index]["arguments"]})));
                }
                events.push(self.event(json!({"type": "response.output_item.done", "output_index": index, "item": self.items[index]})));
            }
        }
        let status = if incomplete.is_some() {
            "incomplete"
        } else {
            "completed"
        };
        events.push(
            self.event(json!({"type": format!("response.{status}"), "response": {
                "id": self.response_id, "object": "response", "status": status,
                "output": self.items, "usage": self.usage,
                "incomplete_details": incomplete.map(|reason| json!({"reason": reason})),
                "provider_response_id": self.provider_id,
            }})),
        );
        Ok(events)
    }

    fn event(&mut self, mut event: Value) -> Value {
        event["sequence_number"] = json!(self.sequence);
        event["response_id"] = json!(self.response_id);
        self.sequence += 1;
        event
    }
}

fn append(value: &mut Value, delta: &str) -> Result<()> {
    let Value::String(text) = value else {
        return Err(TranslationError::Invalid("stream accumulator".to_owned()));
    };
    text.push_str(delta);
    Ok(())
}
