use std::collections::BTreeSet;

use serde_json::Value;
use serde_json::json;

use crate::TranslationError;

type Result<T> = std::result::Result<T, TranslationError>;

/// Wraps Responses freeform tools in a single string function argument. Apply
/// before namespace normalization, and restore after namespace decoding.
#[derive(Default)]
pub struct CustomTools {
    names: BTreeSet<(Option<String>, String)>,
    active: BTreeSet<String>,
}

impl CustomTools {
    pub fn normalize(request: &mut Value) -> Result<Self> {
        let mut custom = Self::default();
        if let Some(tools) = request.get_mut("tools").and_then(Value::as_array_mut) {
            for tool in tools {
                if tool["type"] == "namespace" {
                    let namespace = tool["name"]
                        .as_str()
                        .ok_or_else(|| TranslationError::Invalid("namespace.name".to_owned()))?
                        .to_owned();
                    let children = tool["tools"]
                        .as_array_mut()
                        .ok_or_else(|| TranslationError::Invalid("namespace.tools".to_owned()))?;
                    for child in children {
                        custom.definition(child, Some(&namespace))?;
                    }
                } else {
                    custom.definition(tool, /*namespace*/ None)?;
                }
            }
        }
        if let Some(items) = request.get_mut("input").and_then(Value::as_array_mut) {
            for item in items {
                if item["type"] == "custom_tool_call" {
                    custom.names.insert(identity(item)?);
                    let input = item["input"].as_str().ok_or_else(|| {
                        TranslationError::Invalid("custom_tool_call.input".to_owned())
                    })?;
                    item["arguments"] = json!(json!({"input": input}).to_string());
                    item["type"] = json!("function_call");
                    if let Some(object) = item.as_object_mut() {
                        object.remove("input");
                    }
                } else if item["type"] == "custom_tool_call_output" {
                    item["type"] = json!("function_call_output");
                }
            }
        }
        if let Some(choice) = request.get_mut("tool_choice")
            && choice["type"] == "custom"
        {
            choice["type"] = json!("function");
        }
        Ok(custom)
    }

    fn definition(&mut self, tool: &mut Value, namespace: Option<&str>) -> Result<()> {
        if tool["type"] != "custom" {
            return Ok(());
        }
        let name = tool["name"]
            .as_str()
            .ok_or_else(|| TranslationError::Invalid("custom tool name".to_owned()))?;
        self.names
            .insert((namespace.map(str::to_owned), name.to_owned()));
        let description = tool["description"].as_str().unwrap_or_default();
        let format = tool.get("format").map(Value::to_string).unwrap_or_default();
        *tool = json!({
            "type": "function", "name": name,
            "description": format!("{description}\nPass the tool's complete freeform input in the input string. Tool format: {format}"),
            "parameters": {"type": "object", "properties": {"input": {"type": "string"}},
                "required": ["input"], "additionalProperties": false},
        });
        Ok(())
    }

    pub(crate) fn restore(&mut self, mut event: Value) -> Result<Vec<Value>> {
        let kind = event["type"].as_str().unwrap_or_default().to_owned();
        if let Some(item) = event.get_mut("item")
            && item["type"] == "function_call"
            && self.names.contains(&identity(item)?)
        {
            if let Some(id) = item["id"].as_str() {
                self.active.insert(id.to_owned());
            }
            self.restore_item(item, &kind)?;
        }
        if event["item_id"]
            .as_str()
            .is_some_and(|id| self.active.contains(id))
        {
            match kind.as_str() {
                "response.function_call_arguments.delta" => return Ok(Vec::new()),
                "response.function_call_arguments.done" => {
                    let input = decode(&event["arguments"])?;
                    let mut delta = event.clone();
                    delta["type"] = json!("response.custom_tool_call_input.delta");
                    delta["delta"] = json!(input);
                    event["type"] = json!("response.custom_tool_call_input.done");
                    event["input"] = json!(input);
                    if let Some(object) = delta.as_object_mut() {
                        object.remove("arguments");
                    }
                    if let Some(object) = event.as_object_mut() {
                        object.remove("arguments");
                    }
                    return Ok(vec![delta, event]);
                }
                _ => {}
            }
        }
        if let Some(items) = event
            .pointer_mut("/response/output")
            .and_then(Value::as_array_mut)
        {
            for item in items {
                if item["type"] == "function_call" && self.names.contains(&identity(item)?) {
                    self.restore_item(item, &kind)?;
                }
            }
        }
        Ok(vec![event])
    }

    fn restore_item(&self, item: &mut Value, event: &str) -> Result<()> {
        let input = if event == "response.output_item.added" || event == "response.incomplete" {
            String::new()
        } else {
            decode(&item["arguments"])?
        };
        item["type"] = json!("custom_tool_call");
        item["input"] = json!(input);
        if let Some(object) = item.as_object_mut() {
            object.remove("arguments");
        }
        Ok(())
    }
}

fn identity(item: &Value) -> Result<(Option<String>, String)> {
    let name = item["name"]
        .as_str()
        .ok_or_else(|| TranslationError::Invalid("tool identity name".to_owned()))?;
    Ok((
        item["namespace"].as_str().map(str::to_owned),
        name.to_owned(),
    ))
}

fn decode(arguments: &Value) -> Result<String> {
    let arguments = arguments
        .as_str()
        .ok_or_else(|| TranslationError::Invalid("custom tool arguments".to_owned()))?;
    let value: Value = serde_json::from_str(arguments)
        .map_err(|error| TranslationError::Invalid(format!("custom tool input: {error}")))?;
    value["input"]
        .as_str()
        .map(str::to_owned)
        .ok_or_else(|| TranslationError::Invalid("custom tool input string".to_owned()))
}
