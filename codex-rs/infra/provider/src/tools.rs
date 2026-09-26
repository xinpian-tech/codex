use std::collections::BTreeMap;

use serde_json::Value;
use serde_json::json;

use crate::TranslationError;

/// Per-request mapping from Chat Completions names to Responses namespaces.
/// Aliases depend on identity, not definition order, so history remains stable
/// when the available tool list changes between requests.
#[derive(Default)]
pub struct ToolNames {
    names: BTreeMap<String, (String, String)>,
}

impl ToolNames {
    pub fn normalize(request: &mut Value) -> Result<Self, TranslationError> {
        let mut names = Self::default();
        if let Some(tools) = request.get_mut("tools").filter(|tools| !tools.is_null()) {
            let definitions = tools
                .as_array()
                .ok_or_else(|| TranslationError::Invalid("tools".to_owned()))?;
            let mut flattened = Vec::new();
            for definition in definitions {
                if definition["type"] == "namespace" {
                    let namespace = definition["name"]
                        .as_str()
                        .ok_or_else(|| TranslationError::Invalid("namespace.name".to_owned()))?;
                    let children = definition["tools"]
                        .as_array()
                        .ok_or_else(|| TranslationError::Invalid("namespace.tools".to_owned()))?;
                    for child in children {
                        let mut child = child.clone();
                        let name = child["name"]
                            .as_str()
                            .ok_or_else(|| TranslationError::Invalid("tool.name".to_owned()))?;
                        let qualified = format!("{namespace}.{name}");
                        child["name"] = json!(names.register(namespace, name)?);
                        let description = definition["description"].as_str().unwrap_or_default();
                        let own = child["description"].as_str().unwrap_or_default();
                        child["description"] = json!(format!("{qualified}\n{description}\n{own}"));
                        flattened.push(child);
                    }
                } else {
                    flattened.push(definition.clone());
                }
            }
            *tools = Value::Array(flattened);
        }
        if let Some(items) = request.get_mut("input").and_then(Value::as_array_mut) {
            for item in items {
                if item["type"] == "function_call"
                    && let Some(namespace) = item["namespace"].as_str()
                {
                    let name = item["name"].as_str().ok_or_else(|| {
                        TranslationError::Invalid("function_call.name".to_owned())
                    })?;
                    item["name"] = json!(names.register(namespace, name)?);
                    item.as_object_mut()
                        .ok_or_else(|| TranslationError::Invalid("function_call".to_owned()))?
                        .remove("namespace");
                }
            }
        }
        if let Some(choice) = request.get_mut("tool_choice")
            && choice["type"] == "function"
            && let Some(namespace) = choice["namespace"].as_str()
        {
            let name = choice["name"]
                .as_str()
                .ok_or_else(|| TranslationError::Invalid("tool_choice.name".to_owned()))?;
            choice["name"] = json!(names.register(namespace, name)?);
            choice
                .as_object_mut()
                .ok_or_else(|| TranslationError::Invalid("tool_choice".to_owned()))?
                .remove("namespace");
        }
        Ok(names)
    }

    fn register(&mut self, namespace: &str, name: &str) -> Result<String, TranslationError> {
        let identity = (namespace.to_owned(), name.to_owned());
        let encoded = serde_json::to_vec(&identity)
            .map_err(|error| TranslationError::Invalid(error.to_string()))?;
        let alias = blake3::hash(&encoded).to_hex().to_string();
        if self
            .names
            .get(&alias)
            .is_some_and(|existing| existing != &identity)
        {
            return Err(TranslationError::Invalid("tool alias collision".to_owned()));
        }
        self.names.insert(alias.clone(), identity);
        Ok(alias)
    }

    pub(crate) fn restore(&self, event: &mut Value) {
        if let Some(item) = event.get_mut("item") {
            self.restore_item(item);
        }
        if let Some(items) = event
            .pointer_mut("/response/output")
            .and_then(Value::as_array_mut)
        {
            for item in items {
                self.restore_item(item);
            }
        }
    }

    fn restore_item(&self, item: &mut Value) {
        if item["type"] == "function_call"
            && let Some(alias) = item["name"].as_str()
            && let Some((namespace, name)) = self.names.get(alias)
        {
            item["name"] = json!(name);
            item["namespace"] = json!(namespace);
        }
    }
}
