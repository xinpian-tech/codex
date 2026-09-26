use serde_json::Value;
use serde_json::json;

#[derive(Debug, thiserror::Error)]
pub enum TranslationError {
    #[error("missing or invalid {0}")]
    Invalid(String),
    #[error("chat adapter does not support {0}")]
    Unsupported(String),
}

type Result<T> = std::result::Result<T, TranslationError>;

/// Converts a full-context Responses request into a streaming Chat Completions
/// request. The model comes from the Agent's resolved provider binding.
/// Unsupported content is reported instead of silently deleting conversation
/// items. Transport, continuation storage and provider capabilities are separate.
pub fn translate_chat_request(request: &Value, model: &str) -> Result<Value> {
    if !request.is_object() || model.is_empty() {
        return Err(TranslationError::Invalid("request/model".to_owned()));
    }
    if request
        .get("previous_response_id")
        .is_some_and(|value| !value.is_null())
    {
        return Err(TranslationError::Unsupported(
            "previous_response_id; send full context".to_owned(),
        ));
    }
    let mut messages = Vec::<Value>::new();
    if let Some(instructions) = request.get("instructions").filter(|value| !value.is_null()) {
        messages.push(json!({"role": "system", "content": text(instructions, "instructions")?}));
    }
    let input = request
        .get("input")
        .ok_or_else(|| TranslationError::Invalid("input".to_owned()))?;
    if let Some(input) = input.as_str() {
        messages.push(json!({"role": "user", "content": input}));
    } else {
        let mut reasoning = None::<String>;
        for item in input
            .as_array()
            .ok_or_else(|| TranslationError::Invalid("input".to_owned()))?
        {
            match item
                .get("type")
                .and_then(Value::as_str)
                .unwrap_or("message")
            {
                "reasoning" => {
                    if let Some(continuation) = item
                        .get("encrypted_content")
                        .filter(|value| !value.is_null())
                    {
                        let continuation = text(continuation, "reasoning continuation")?
                            .strip_prefix(crate::CHAT_REASONING_PREFIX)
                            .ok_or_else(|| {
                                TranslationError::Unsupported(
                                    "foreign reasoning continuation".to_owned(),
                                )
                            })?;
                        reasoning.get_or_insert_default().push_str(continuation);
                        continue;
                    }
                    let parts = item["content"].as_array().ok_or_else(|| {
                        TranslationError::Unsupported(
                            "reasoning without original content".to_owned(),
                        )
                    })?;
                    let pending = reasoning.get_or_insert_default();
                    for part in parts {
                        if part["type"] != "reasoning_text" {
                            return Err(TranslationError::Unsupported(
                                "reasoning content type".to_owned(),
                            ));
                        }
                        pending.push_str(text(&part["text"], "reasoning.text")?);
                    }
                }
                "message" => {
                    let role = text(&item["role"], "message.role")?;
                    let role = match role {
                        "developer" => "system",
                        "system" | "user" | "assistant" => role,
                        other => {
                            return Err(TranslationError::Unsupported(format!(
                                "message role {other}"
                            )));
                        }
                    };
                    let mut message = json!({"role": role, "content": content(&item["content"])?});
                    if let Some(reasoning) = reasoning.take() {
                        if role != "assistant" {
                            return Err(TranslationError::Invalid(
                                "reasoning must precede assistant output".to_owned(),
                            ));
                        }
                        message["reasoning_content"] = json!(reasoning);
                    }
                    messages.push(message);
                }
                "function_call" => {
                    if item.get("namespace").is_some_and(|value| !value.is_null()) {
                        return Err(TranslationError::Unsupported(
                            "namespaced function call".to_owned(),
                        ));
                    }
                    let call = json!({
                        "id": text(&item["call_id"], "function_call.call_id")?,
                        "type": "function",
                        "function": {
                            "name": text(&item["name"], "function_call.name")?,
                            "arguments": text(&item["arguments"], "function_call.arguments")?,
                        },
                    });
                    if let Some(reasoning) = reasoning.take() {
                        messages.push(json!({"role": "assistant", "content": null, "reasoning_content": reasoning}));
                    } else if messages
                        .last()
                        .is_none_or(|message| message["role"] != "assistant")
                    {
                        messages.push(json!({"role": "assistant", "content": null}));
                    }
                    let message = messages
                        .last_mut()
                        .and_then(Value::as_object_mut)
                        .ok_or_else(|| TranslationError::Invalid("assistant message".to_owned()))?;
                    message
                        .entry("tool_calls")
                        .or_insert_with(|| json!([]))
                        .as_array_mut()
                        .ok_or_else(|| TranslationError::Invalid("tool_calls".to_owned()))?
                        .push(call);
                }
                "function_call_output" => {
                    if reasoning.is_some() {
                        return Err(TranslationError::Invalid(
                            "reasoning missing assistant output".to_owned(),
                        ));
                    }
                    messages.push(json!({
                    "role": "tool",
                    "tool_call_id": text(&item["call_id"], "function_call_output.call_id")?,
                    "content": content(&item["output"])? ,
                    }));
                }
                other => return Err(TranslationError::Unsupported(format!("input item {other}"))),
            }
        }
        if reasoning.is_some() {
            return Err(TranslationError::Invalid(
                "unfinished reasoning item".to_owned(),
            ));
        }
    }
    let mut result = json!({
        "model": model,
        "messages": messages,
        "stream": true,
        "stream_options": {"include_usage": true},
    });
    if let Some(tools) = request.get("tools").filter(|value| !value.is_null()) {
        let tools = tools
            .as_array()
            .ok_or_else(|| TranslationError::Invalid("tools".to_owned()))?;
        let mut converted = Vec::with_capacity(tools.len());
        for tool in tools {
            if tool["type"] != "function" {
                return Err(TranslationError::Unsupported(format!(
                    "tool type {}",
                    tool["type"]
                )));
            }
            let mut function = json!({"name": text(&tool["name"], "tool.name")?});
            for field in ["description", "parameters", "strict"] {
                if let Some(value) = tool.get(field) {
                    function[field] = value.clone();
                }
            }
            converted.push(json!({"type": "function", "function": function}));
        }
        result["tools"] = Value::Array(converted);
    }
    if let Some(choice) = request.get("tool_choice").filter(|value| !value.is_null()) {
        result["tool_choice"] = match choice.as_str() {
            Some("auto" | "none" | "required") => choice.clone(),
            Some(other) => {
                return Err(TranslationError::Unsupported(format!(
                    "tool_choice {other}"
                )));
            }
            None if choice["type"] == "function" => json!({
                "type": "function", "function": {"name": text(&choice["name"], "tool_choice.name")?},
            }),
            None => return Err(TranslationError::Invalid("tool_choice".to_owned())),
        };
    }
    for field in ["temperature", "top_p", "parallel_tool_calls"] {
        if let Some(value) = request.get(field).filter(|value| !value.is_null()) {
            result[field] = value.clone();
        }
    }
    if let Some(tokens) = request
        .get("max_output_tokens")
        .filter(|value| !value.is_null())
    {
        result["max_tokens"] = tokens.clone();
    }
    if let Some(effort) = request.pointer("/reasoning/effort") {
        result["reasoning_effort"] = effort.clone();
    }
    if let Some(format) = request.pointer("/text/format") {
        match format["type"].as_str() {
            Some("text") => {}
            Some("json_object") => result["response_format"] = json!({"type": "json_object"}),
            other => {
                return Err(TranslationError::Unsupported(format!(
                    "text format {other:?}"
                )));
            }
        }
    }
    Ok(result)
}

fn text<'a>(value: &'a Value, field: &str) -> Result<&'a str> {
    value
        .as_str()
        .ok_or_else(|| TranslationError::Invalid(field.to_owned()))
}

fn content(value: &Value) -> Result<String> {
    if let Some(text) = value.as_str() {
        return Ok(text.to_owned());
    }
    let items = value
        .as_array()
        .ok_or_else(|| TranslationError::Invalid("content".to_owned()))?;
    let mut output = String::new();
    for item in items {
        match item["type"].as_str() {
            Some("input_text" | "output_text" | "text") => {
                output.push_str(text(&item["text"], "content.text")?)
            }
            other => {
                return Err(TranslationError::Unsupported(format!(
                    "content part {other:?}"
                )));
            }
        }
    }
    Ok(output)
}
