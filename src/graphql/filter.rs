use serde_json::Value;

#[derive(Debug, Clone)]
pub enum FilterOp {
    Eq(Value),
    Ne(Value),
    Gt(Value),
    Gte(Value),
    Lt(Value),
    Lte(Value),
    Contains(String),
    In(Vec<Value>),
}

pub struct Filter {
    conditions: std::collections::HashMap<String, FilterOp>,
}

impl Filter {
    pub fn from_json(filter: &Value) -> Option<Self> {
        let obj = filter.as_object()?;
        let mut conditions = std::collections::HashMap::new();

        for (field, value) in obj {
            if let Some(op) = Self::parse_filter_op(value) {
                conditions.insert(field.clone(), op);
            }
        }

        if conditions.is_empty() {
            None
        } else {
            Some(Self { conditions })
        }
    }

    fn parse_filter_op(value: &Value) -> Option<FilterOp> {
        let obj = value.as_object()?;

        for (op_name, op_value) in obj {
            match op_name.as_str() {
                "eq" => return Some(FilterOp::Eq(op_value.clone())),
                "ne" => return Some(FilterOp::Ne(op_value.clone())),
                "gt" => return Some(FilterOp::Gt(op_value.clone())),
                "gte" => return Some(FilterOp::Gte(op_value.clone())),
                "lt" => return Some(FilterOp::Lt(op_value.clone())),
                "lte" => return Some(FilterOp::Lte(op_value.clone())),
                "contains" => {
                    if let Some(s) = op_value.as_str() {
                        return Some(FilterOp::Contains(s.to_string()));
                    }
                }
                "in" => {
                    if let Some(arr) = op_value.as_array() {
                        return Some(FilterOp::In(arr.clone()));
                    }
                }
                _ => {}
            }
        }

        None
    }

    pub fn matches(&self, data: &Value) -> bool {
        for (field, op) in &self.conditions {
            let field_value = data.get(field);

            match op {
                FilterOp::Eq(expected) => {
                    if field_value != Some(expected) {
                        return false;
                    }
                }
                FilterOp::Ne(expected) => {
                    if field_value == Some(expected) {
                        return false;
                    }
                }
                FilterOp::Gt(expected) => {
                    if !Self::compare_gt(field_value, expected) {
                        return false;
                    }
                }
                FilterOp::Gte(expected) => {
                    if !Self::compare_gte(field_value, expected) {
                        return false;
                    }
                }
                FilterOp::Lt(expected) => {
                    if !Self::compare_lt(field_value, expected) {
                        return false;
                    }
                }
                FilterOp::Lte(expected) => {
                    if !Self::compare_lte(field_value, expected) {
                        return false;
                    }
                }
                FilterOp::Contains(substr) => {
                    if let Some(s) = field_value.and_then(|v| v.as_str()) {
                        if !s.contains(substr) {
                            return false;
                        }
                    } else {
                        return false;
                    }
                }
                FilterOp::In(values) => {
                    if !values.contains(field_value.unwrap_or(&Value::Null)) {
                        return false;
                    }
                }
            }
        }

        true
    }

    fn compare_gt(actual: Option<&Value>, expected: &Value) -> bool {
        match (actual, expected) {
            (Some(Value::Number(a)), Value::Number(b)) => {
                a.as_f64().unwrap_or(0.0) > b.as_f64().unwrap_or(0.0)
            }
            (Some(Value::String(a)), Value::String(b)) => a > b,
            _ => false,
        }
    }

    fn compare_gte(actual: Option<&Value>, expected: &Value) -> bool {
        match (actual, expected) {
            (Some(Value::Number(a)), Value::Number(b)) => {
                a.as_f64().unwrap_or(0.0) >= b.as_f64().unwrap_or(0.0)
            }
            (Some(Value::String(a)), Value::String(b)) => a >= b,
            _ => false,
        }
    }

    fn compare_lt(actual: Option<&Value>, expected: &Value) -> bool {
        match (actual, expected) {
            (Some(Value::Number(a)), Value::Number(b)) => {
                a.as_f64().unwrap_or(0.0) < b.as_f64().unwrap_or(0.0)
            }
            (Some(Value::String(a)), Value::String(b)) => a < b,
            _ => false,
        }
    }

    fn compare_lte(actual: Option<&Value>, expected: &Value) -> bool {
        match (actual, expected) {
            (Some(Value::Number(a)), Value::Number(b)) => {
                a.as_f64().unwrap_or(0.0) <= b.as_f64().unwrap_or(0.0)
            }
            (Some(Value::String(a)), Value::String(b)) => a <= b,
            _ => false,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum SortOrder {
    Asc,
    Desc,
}

pub struct Sort {
    fields: Vec<(String, SortOrder)>,
}

impl Sort {
    pub fn from_json(order_by: &Value) -> Option<Self> {
        let obj = order_by.as_object()?;
        let mut fields = Vec::new();

        for (field, direction) in obj {
            let order = match direction.as_str() {
                Some("ASC") => SortOrder::Asc,
                Some("DESC") => SortOrder::Desc,
                _ => continue,
            };
            fields.push((field.clone(), order));
        }

        if fields.is_empty() {
            None
        } else {
            Some(Self { fields })
        }
    }

    pub fn sort<T, F>(&self, items: &mut Vec<T>, get_field: F)
    where
        F: Fn(&T, &str) -> Option<serde_json::Value>,
    {
        items.sort_by(|a, b| {
            for (field, order) in &self.fields {
                let val_a = get_field(a, field);
                let val_b = get_field(b, field);

                let cmp = Self::compare_values(&val_a, &val_b);
                let cmp = if *order == SortOrder::Desc {
                    cmp.reverse()
                } else {
                    cmp
                };

                if cmp != std::cmp::Ordering::Equal {
                    return cmp;
                }
            }
            std::cmp::Ordering::Equal
        });
    }

    fn compare_values(a: &Option<Value>, b: &Option<Value>) -> std::cmp::Ordering {
        match (a, b) {
            (None, None) => std::cmp::Ordering::Equal,
            (None, Some(_)) => std::cmp::Ordering::Less,
            (Some(_), None) => std::cmp::Ordering::Greater,
            (Some(a), Some(b)) => match (a, b) {
                (Value::Null, Value::Null) => std::cmp::Ordering::Equal,
                (Value::Bool(a), Value::Bool(b)) => a.cmp(b),
                (Value::Number(a), Value::Number(b)) => {
                    let a = a.as_f64().unwrap_or(0.0);
                    let b = b.as_f64().unwrap_or(0.0);
                    a.partial_cmp(&b).unwrap_or(std::cmp::Ordering::Equal)
                }
                (Value::String(a), Value::String(b)) => a.cmp(b),
                _ => std::cmp::Ordering::Equal,
            },
        }
    }
}
