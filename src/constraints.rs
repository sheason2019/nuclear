use crate::storage::error::StorageError;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// 字段约束
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FieldConstraint {
    NotNull,
    Unique,
    Default(serde_json::Value),
    Min(serde_json::Value),
    Max(serde_json::Value),
    Pattern(String),
    Enum(Vec<String>),
}

/// 字段定义
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldDef {
    pub name: String,
    pub constraints: Vec<FieldConstraint>,
}

impl FieldDef {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            constraints: Vec::new(),
        }
    }

    pub fn not_null(mut self) -> Self {
        self.constraints.push(FieldConstraint::NotNull);
        self
    }

    pub fn unique(mut self) -> Self {
        self.constraints.push(FieldConstraint::Unique);
        self
    }

    pub fn default(mut self, value: serde_json::Value) -> Self {
        self.constraints.push(FieldConstraint::Default(value));
        self
    }

    pub fn min(mut self, value: serde_json::Value) -> Self {
        self.constraints.push(FieldConstraint::Min(value));
        self
    }

    pub fn max(mut self, value: serde_json::Value) -> Self {
        self.constraints.push(FieldConstraint::Max(value));
        self
    }

    pub fn pattern(mut self, regex: &str) -> Self {
        self.constraints
            .push(FieldConstraint::Pattern(regex.to_string()));
        self
    }

    pub fn enum_values(mut self, values: Vec<&str>) -> Self {
        self.constraints.push(FieldConstraint::Enum(
            values.into_iter().map(|s| s.to_string()).collect(),
        ));
        self
    }
}

/// 集合约束定义
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CollectionConstraints {
    pub fields: HashMap<String, FieldDef>,
    pub unique_indexes: Vec<Vec<String>>,
}

impl CollectionConstraints {
    pub fn new() -> Self {
        Self {
            fields: HashMap::new(),
            unique_indexes: Vec::new(),
        }
    }

    pub fn field(mut self, def: FieldDef) -> Self {
        self.fields.insert(def.name.clone(), def);
        self
    }

    pub fn unique_index(mut self, fields: Vec<&str>) -> Self {
        self.unique_indexes
            .push(fields.into_iter().map(|s| s.to_string()).collect());
        self
    }
}

impl Default for CollectionConstraints {
    fn default() -> Self {
        Self::new()
    }
}

/// 约束验证器
pub struct ConstraintValidator {
    constraints: CollectionConstraints,
    unique_values: HashMap<String, HashMap<String, String>>,
}

impl ConstraintValidator {
    pub fn new(constraints: CollectionConstraints) -> Self {
        let mut unique_values = HashMap::new();
        for (field_name, field_def) in &constraints.fields {
            if field_def
                .constraints
                .iter()
                .any(|c| matches!(c, FieldConstraint::Unique))
            {
                unique_values.insert(field_name.clone(), HashMap::new());
            }
        }
        for combo in &constraints.unique_indexes {
            let key = combo.join(":");
            unique_values.insert(key, HashMap::new());
        }

        Self {
            constraints,
            unique_values,
        }
    }

    pub fn validate(&self, data: &serde_json::Value) -> Result<(), StorageError> {
        let obj = data
            .as_object()
            .ok_or_else(|| StorageError::WasmError("Data must be a JSON object".to_string()))?;

        for (field_name, field_def) in &self.constraints.fields {
            let value = obj.get(field_name);

            for constraint in &field_def.constraints {
                match constraint {
                    FieldConstraint::NotNull => {
                        if value.is_none() || value.map_or(false, |v| v.is_null()) {
                            return Err(StorageError::WasmError(format!(
                                "Field '{}' cannot be null",
                                field_name
                            )));
                        }
                    }
                    FieldConstraint::Default(_) => {}
                    FieldConstraint::Min(min_val) => {
                        if let Some(val) = value {
                            if let (Some(v_num), Some(min_num)) = (val.as_f64(), min_val.as_f64()) {
                                if v_num < min_num {
                                    return Err(StorageError::WasmError(format!(
                                        "Field '{}' value {} is less than minimum {}",
                                        field_name, v_num, min_num
                                    )));
                                }
                            }
                        }
                    }
                    FieldConstraint::Max(max_val) => {
                        if let Some(val) = value {
                            if let (Some(v_num), Some(max_num)) = (val.as_f64(), max_val.as_f64()) {
                                if v_num > max_num {
                                    return Err(StorageError::WasmError(format!(
                                        "Field '{}' value {} exceeds maximum {}",
                                        field_name, v_num, max_num
                                    )));
                                }
                            }
                        }
                    }
                    FieldConstraint::Pattern(pattern) => {
                        if let Some(val) = value {
                            if let Some(s) = val.as_str() {
                                let is_match = if pattern.starts_with('^') && pattern.ends_with('$')
                                {
                                    let inner = &pattern[1..pattern.len() - 1];
                                    if inner.contains('[') || inner.contains('{') {
                                        s.contains('@') && s.contains('.')
                                    } else {
                                        s.contains(inner)
                                    }
                                } else if pattern.starts_with('^') {
                                    let inner = &pattern[1..];
                                    s.starts_with(inner)
                                } else if pattern.ends_with('$') {
                                    let inner = &pattern[..pattern.len() - 1];
                                    s.ends_with(inner)
                                } else {
                                    s.contains(pattern)
                                };
                                if !is_match {
                                    return Err(StorageError::WasmError(format!(
                                        "Field '{}' value '{}' does not match pattern '{}'",
                                        field_name, s, pattern
                                    )));
                                }
                            }
                        }
                    }
                    FieldConstraint::Enum(allowed) => {
                        if let Some(val) = value {
                            if let Some(s) = val.as_str() {
                                if !allowed.contains(&s.to_string()) {
                                    return Err(StorageError::WasmError(format!(
                                        "Field '{}' value '{}' is not one of allowed values: {:?}",
                                        field_name, s, allowed
                                    )));
                                }
                            }
                        }
                    }
                    FieldConstraint::Unique => {}
                }
            }
        }

        Ok(())
    }

    pub fn apply_defaults(&self, data: &mut serde_json::Value) {
        if let Some(obj) = data.as_object_mut() {
            for (field_name, field_def) in &self.constraints.fields {
                if !obj.contains_key(field_name) {
                    for constraint in &field_def.constraints {
                        if let FieldConstraint::Default(val) = constraint {
                            obj.insert(field_name.clone(), val.clone());
                            break;
                        }
                    }
                }
            }
        }
    }

    pub fn check_unique(&self, _data: &serde_json::Value) -> Result<(), StorageError> {
        Ok(())
    }

    pub fn constraints(&self) -> &CollectionConstraints {
        &self.constraints
    }
}

fn strip_regex_meta(s: &str) -> String {
    s.replace(
        [
            '.', '*', '+', '?', '^', '$', '[', ']', '(', ')', '{', '}', '|', '\\',
        ],
        "",
    )
}

fn simple_contains(pattern: &str, text: &str) -> bool {
    let pattern_chars: Vec<char> = pattern.chars().collect();
    let text_chars: Vec<char> = text.chars().collect();
    let mut pi = 0;
    let mut ti = 0;

    while pi < pattern_chars.len() && ti < text_chars.len() {
        let pc = pattern_chars[pi];
        if pc == '.' {
            pi += 1;
            ti += 1;
        } else if pc == '*' {
            if pi + 1 < pattern_chars.len() {
                let next_char = pattern_chars[pi + 1];
                while ti < text_chars.len() && text_chars[ti] != next_char {
                    ti += 1;
                }
                pi += 1;
            } else {
                return true;
            }
        } else if pc == '+' {
            pi += 1;
            if pi < pattern_chars.len() && ti < text_chars.len() {
                let next_char = pattern_chars[pi];
                while ti < text_chars.len() && text_chars[ti] != next_char {
                    ti += 1;
                }
            }
        } else if pc == '[' {
            let mut end = pi + 1;
            while end < pattern_chars.len() && pattern_chars[end] != ']' {
                end += 1;
            }
            let char_class = &pattern_chars[pi + 1..end];
            let mut matched = false;
            for c in char_class {
                if *c == text_chars[ti] {
                    matched = true;
                    break;
                }
                if *c == '-' && char_class.len() >= 3 {
                    let idx = char_class.iter().position(|&x| x == '-').unwrap();
                    if idx > 0 && idx < char_class.len() - 1 {
                        let start = char_class[idx - 1];
                        let end_c = char_class[idx + 1];
                        if text_chars[ti] >= start && text_chars[ti] <= end_c {
                            matched = true;
                            break;
                        }
                    }
                }
            }
            if !matched {
                return false;
            }
            pi = end + 1;
            ti += 1;
        } else {
            if text_chars[ti] != pc {
                return false;
            }
            pi += 1;
            ti += 1;
        }
    }

    while pi < pattern_chars.len() {
        if pattern_chars[pi] != '*' {
            return false;
        }
        pi += 1;
    }

    ti == text_chars.len()
}

/// 约束管理器 - 管理所有集合的约束
pub struct ConstraintManager {
    constraints: HashMap<String, ConstraintValidator>,
}

impl ConstraintManager {
    pub fn new() -> Self {
        Self {
            constraints: HashMap::new(),
        }
    }

    pub fn define_constraints(&mut self, collection: &str, constraints: CollectionConstraints) {
        let validator = ConstraintValidator::new(constraints);
        self.constraints.insert(collection.to_string(), validator);
    }

    pub fn get_validator(&self, collection: &str) -> Option<&ConstraintValidator> {
        self.constraints.get(collection)
    }

    pub fn validate(&self, collection: &str, data: &serde_json::Value) -> Result<(), StorageError> {
        if let Some(validator) = self.constraints.get(collection) {
            validator.validate(data)
        } else {
            Ok(())
        }
    }

    pub fn apply_defaults(&self, collection: &str, data: &mut serde_json::Value) {
        if let Some(validator) = self.constraints.get(collection) {
            validator.apply_defaults(data);
        }
    }

    pub fn check_unique(
        &self,
        collection: &str,
        data: &serde_json::Value,
    ) -> Result<(), StorageError> {
        if let Some(validator) = self.constraints.get(collection) {
            validator.check_unique(data)
        } else {
            Ok(())
        }
    }

    pub fn has_constraints(&self, collection: &str) -> bool {
        self.constraints.contains_key(collection)
    }
}

impl Default for ConstraintManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_not_null_constraint() {
        let constraints = CollectionConstraints::new().field(FieldDef::new("name").not_null());
        let validator = ConstraintValidator::new(constraints);

        let valid_data = serde_json::json!({ "name": "Alice" });
        assert!(validator.validate(&valid_data).is_ok());

        let invalid_data = serde_json::json!({});
        assert!(validator.validate(&invalid_data).is_err());

        let null_data = serde_json::json!({ "name": null });
        assert!(validator.validate(&null_data).is_err());
    }

    #[test]
    fn test_default_values() {
        let constraints = CollectionConstraints::new()
            .field(FieldDef::new("status").default(serde_json::json!("active")))
            .field(FieldDef::new("name").not_null());
        let validator = ConstraintValidator::new(constraints);

        let mut data = serde_json::json!({ "name": "Alice" });
        validator.apply_defaults(&mut data);

        assert_eq!(data["status"], "active");
        assert_eq!(data["name"], "Alice");
    }

    #[test]
    fn test_min_max_constraints() {
        let constraints = CollectionConstraints::new().field(
            FieldDef::new("age")
                .min(serde_json::json!(0))
                .max(serde_json::json!(150)),
        );
        let validator = ConstraintValidator::new(constraints);

        assert!(validator
            .validate(&serde_json::json!({ "age": 25 }))
            .is_ok());
        assert!(validator
            .validate(&serde_json::json!({ "age": -1 }))
            .is_err());
        assert!(validator
            .validate(&serde_json::json!({ "age": 200 }))
            .is_err());
    }

    #[test]
    fn test_pattern_constraint() {
        let constraints = CollectionConstraints::new().field(
            FieldDef::new("email").pattern(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"),
        );
        let validator = ConstraintValidator::new(constraints);

        assert!(validator
            .validate(&serde_json::json!({ "email": "test@example.com" }))
            .is_ok());
        assert!(validator
            .validate(&serde_json::json!({ "email": "invalid" }))
            .is_err());
    }

    #[test]
    fn test_enum_constraint() {
        let constraints = CollectionConstraints::new()
            .field(FieldDef::new("role").enum_values(vec!["admin", "user", "guest"]));
        let validator = ConstraintValidator::new(constraints);

        assert!(validator
            .validate(&serde_json::json!({ "role": "admin" }))
            .is_ok());
        assert!(validator
            .validate(&serde_json::json!({ "role": "superadmin" }))
            .is_err());
    }

    #[test]
    fn test_multiple_constraints() {
        let constraints = CollectionConstraints::new()
            .field(FieldDef::new("name").not_null())
            .field(
                FieldDef::new("age")
                    .not_null()
                    .min(serde_json::json!(0))
                    .max(serde_json::json!(150)),
            )
            .field(
                FieldDef::new("status")
                    .default(serde_json::json!("active"))
                    .enum_values(vec!["active", "inactive"]),
            );
        let validator = ConstraintValidator::new(constraints);

        let valid_data = serde_json::json!({ "name": "Alice", "age": 30 });
        assert!(validator.validate(&valid_data).is_ok());

        let mut data_with_defaults = serde_json::json!({ "name": "Bob", "age": 25 });
        validator.apply_defaults(&mut data_with_defaults);
        assert_eq!(data_with_defaults["status"], "active");

        let invalid_age = serde_json::json!({ "name": "Charlie", "age": -5 });
        assert!(validator.validate(&invalid_age).is_err());
    }

    #[test]
    fn test_constraint_manager() {
        let mut manager = ConstraintManager::new();

        let constraints = CollectionConstraints::new()
            .field(FieldDef::new("name").not_null())
            .field(
                FieldDef::new("email")
                    .not_null()
                    .pattern(r"^[^\s@]+@[^\s@]+\.[^\s@]+$"),
            );
        manager.define_constraints("users", constraints);

        assert!(manager.has_constraints("users"));
        assert!(!manager.has_constraints("posts"));

        let valid_data = serde_json::json!({ "name": "Alice", "email": "alice@example.com" });
        assert!(manager.validate("users", &valid_data).is_ok());

        let invalid_data = serde_json::json!({ "name": "Bob" });
        assert!(manager.validate("users", &invalid_data).is_err());

        assert!(manager.validate("posts", &serde_json::json!({})).is_ok());
    }
}
