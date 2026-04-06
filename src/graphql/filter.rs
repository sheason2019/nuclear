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
    conditions: std::collections::HashMap<String, Vec<FilterOp>>,
}

impl Filter {
    pub fn from_json(filter: &Value) -> Option<Self> {
        let obj = filter.as_object()?;
        let mut conditions = std::collections::HashMap::new();

        for (field, value) in obj {
            if let Some(ops) = Self::parse_filter_ops(value) {
                conditions.insert(field.clone(), ops);
            }
        }

        if conditions.is_empty() {
            None
        } else {
            Some(Self { conditions })
        }
    }

    fn parse_filter_ops(value: &Value) -> Option<Vec<FilterOp>> {
        let obj = value.as_object()?;
        if obj.is_empty() {
            return None;
        }
        let mut ops = Vec::new();

        for (op_name, op_value) in obj {
            match op_name.as_str() {
                "eq" => ops.push(FilterOp::Eq(op_value.clone())),
                "ne" => ops.push(FilterOp::Ne(op_value.clone())),
                "gt" => ops.push(FilterOp::Gt(op_value.clone())),
                "gte" => ops.push(FilterOp::Gte(op_value.clone())),
                "lt" => ops.push(FilterOp::Lt(op_value.clone())),
                "lte" => ops.push(FilterOp::Lte(op_value.clone())),
                "contains" => {
                    if let Some(s) = op_value.as_str() {
                        ops.push(FilterOp::Contains(s.to_string()));
                    }
                }
                "in" => {
                    if let Some(arr) = op_value.as_array() {
                        ops.push(FilterOp::In(arr.clone()));
                    }
                }
                _ => {}
            }
        }

        if ops.is_empty() { None } else { Some(ops) }
    }

    pub fn matches(&self, data: &Value) -> bool {
        for (field, ops) in &self.conditions {
            let field_value = data.get(field);

            for op in ops {
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

#[cfg(test)]
mod filter_tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_filter_eq_match() {
        let filter_json = json!({"name": {"eq": "Alice"}});
        let filter = Filter::from_json(&filter_json).unwrap();
        let data = json!({"name": "Alice", "age": 25});
        assert!(filter.matches(&data));
    }

    #[test]
    fn test_filter_eq_no_match() {
        let filter_json = json!({"name": {"eq": "Alice"}});
        let filter = Filter::from_json(&filter_json).unwrap();
        let data = json!({"name": "Bob", "age": 25});
        assert!(!filter.matches(&data));
    }

    #[test]
    fn test_filter_ne_match() {
        let filter_json = json!({"name": {"ne": "Alice"}});
        let filter = Filter::from_json(&filter_json).unwrap();
        let data = json!({"name": "Bob", "age": 25});
        assert!(filter.matches(&data));
    }

    #[test]
    fn test_filter_ne_no_match() {
        let filter_json = json!({"name": {"ne": "Alice"}});
        let filter = Filter::from_json(&filter_json).unwrap();
        let data = json!({"name": "Alice", "age": 25});
        assert!(!filter.matches(&data));
    }

    #[test]
    fn test_filter_gt_number() {
        let filter_json = json!({"age": {"gt": 20}});
        let filter = Filter::from_json(&filter_json).unwrap();
        let data = json!({"name": "Alice", "age": 25});
        assert!(filter.matches(&data));
    }

    #[test]
    fn test_filter_gt_no_match() {
        let filter_json = json!({"age": {"gt": 30}});
        let filter = Filter::from_json(&filter_json).unwrap();
        let data = json!({"name": "Alice", "age": 25});
        assert!(!filter.matches(&data));
    }

    #[test]
    fn test_filter_gte() {
        let filter_json = json!({"age": {"gte": 25}});
        let filter = Filter::from_json(&filter_json).unwrap();
        let data = json!({"name": "Alice", "age": 25});
        assert!(filter.matches(&data));
    }

    #[test]
    fn test_filter_lt() {
        let filter_json = json!({"age": {"lt": 30}});
        let filter = Filter::from_json(&filter_json).unwrap();
        let data = json!({"name": "Alice", "age": 25});
        assert!(filter.matches(&data));
    }

    #[test]
    fn test_filter_lte() {
        let filter_json = json!({"age": {"lte": 25}});
        let filter = Filter::from_json(&filter_json).unwrap();
        let data = json!({"name": "Alice", "age": 25});
        assert!(filter.matches(&data));
    }

    #[test]
    fn test_filter_contains() {
        let filter_json = json!({"name": {"contains": "li"}});
        let filter = Filter::from_json(&filter_json).unwrap();
        let data = json!({"name": "Alice", "age": 25});
        assert!(filter.matches(&data));
    }

    #[test]
    fn test_filter_contains_no_match() {
        let filter_json = json!({"name": {"contains": "xyz"}});
        let filter = Filter::from_json(&filter_json).unwrap();
        let data = json!({"name": "Alice", "age": 25});
        assert!(!filter.matches(&data));
    }

    #[test]
    fn test_filter_in() {
        let filter_json = json!({"name": {"in": ["Alice", "Bob"]}});
        let filter = Filter::from_json(&filter_json).unwrap();
        let data = json!({"name": "Alice", "age": 25});
        assert!(filter.matches(&data));
    }

    #[test]
    fn test_filter_in_no_match() {
        let filter_json = json!({"name": {"in": ["Bob", "Charlie"]}});
        let filter = Filter::from_json(&filter_json).unwrap();
        let data = json!({"name": "Alice", "age": 25});
        assert!(!filter.matches(&data));
    }

    #[test]
    fn test_filter_multiple_conditions() {
        let filter_json = json!({"age": {"gte": 20, "lte": 30}});
        let filter = Filter::from_json(&filter_json).unwrap();
        let data = json!({"name": "Alice", "age": 25});
        assert!(filter.matches(&data));
    }

    #[test]
    fn test_filter_range_excludes_upper() {
        let filter_json = json!({"age": {"gte": 20, "lte": 30}});
        let filter = Filter::from_json(&filter_json).unwrap();
        let data = json!({"name": "Bob", "age": 35});
        assert!(!filter.matches(&data));
    }

    #[test]
    fn test_filter_range_excludes_lower() {
        let filter_json = json!({"age": {"gte": 20, "lte": 30}});
        let filter = Filter::from_json(&filter_json).unwrap();
        let data = json!({"name": "Charlie", "age": 15});
        assert!(!filter.matches(&data));
    }

    #[test]
    fn test_filter_multiple_fields() {
        let filter_json = json!({"name": {"eq": "Alice"}, "age": {"gt": 20}});
        let filter = Filter::from_json(&filter_json).unwrap();
        let data = json!({"name": "Alice", "age": 25});
        assert!(filter.matches(&data));
    }

    #[test]
    fn test_filter_missing_field() {
        let filter_json = json!({"email": {"eq": "alice@example.com"}});
        let filter = Filter::from_json(&filter_json).unwrap();
        let data = json!({"name": "Alice", "age": 25});
        assert!(!filter.matches(&data));
    }

    #[test]
    fn test_filter_empty_json_returns_none() {
        let filter_json = json!({});
        assert!(Filter::from_json(&filter_json).is_none());
    }

    #[test]
    fn test_filter_gt_string() {
        let filter_json = json!({"name": {"gt": "Bob"}});
        let filter = Filter::from_json(&filter_json).unwrap();
        let data = json!({"name": "Charlie"});
        assert!(filter.matches(&data));
    }

    #[test]
    fn test_filter_lte_string() {
        let filter_json = json!({"name": {"lte": "Bob"}});
        let filter = Filter::from_json(&filter_json).unwrap();
        let data = json!({"name": "Alice"});
        assert!(filter.matches(&data));
    }
}

#[cfg(test)]
mod sort_tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_sort_asc_numbers() {
        let sort_json = json!({"age": "ASC"});
        let sort = Sort::from_json(&sort_json).unwrap();
        let mut items = vec![
            json!({"name": "Charlie", "age": 28}),
            json!({"name": "Alice", "age": 25}),
            json!({"name": "Bob", "age": 30}),
        ];
        sort.sort(&mut items, |item, field| item.get(field).cloned());
        assert_eq!(items[0].get("name").unwrap(), "Alice");
        assert_eq!(items[1].get("name").unwrap(), "Charlie");
        assert_eq!(items[2].get("name").unwrap(), "Bob");
    }

    #[test]
    fn test_sort_desc_numbers() {
        let sort_json = json!({"age": "DESC"});
        let sort = Sort::from_json(&sort_json).unwrap();
        let mut items = vec![
            json!({"name": "Charlie", "age": 28}),
            json!({"name": "Alice", "age": 25}),
            json!({"name": "Bob", "age": 30}),
        ];
        sort.sort(&mut items, |item, field| item.get(field).cloned());
        assert_eq!(items[0].get("name").unwrap(), "Bob");
        assert_eq!(items[1].get("name").unwrap(), "Charlie");
        assert_eq!(items[2].get("name").unwrap(), "Alice");
    }

    #[test]
    fn test_sort_asc_strings() {
        let sort_json = json!({"name": "ASC"});
        let sort = Sort::from_json(&sort_json).unwrap();
        let mut items = vec![
            json!({"name": "Charlie", "age": 28}),
            json!({"name": "Alice", "age": 25}),
            json!({"name": "Bob", "age": 30}),
        ];
        sort.sort(&mut items, |item, field| item.get(field).cloned());
        assert_eq!(items[0].get("name").unwrap(), "Alice");
        assert_eq!(items[1].get("name").unwrap(), "Bob");
        assert_eq!(items[2].get("name").unwrap(), "Charlie");
    }

    #[test]
    fn test_sort_desc_strings() {
        let sort_json = json!({"name": "DESC"});
        let sort = Sort::from_json(&sort_json).unwrap();
        let mut items = vec![
            json!({"name": "Charlie", "age": 28}),
            json!({"name": "Alice", "age": 25}),
            json!({"name": "Bob", "age": 30}),
        ];
        sort.sort(&mut items, |item, field| item.get(field).cloned());
        assert_eq!(items[0].get("name").unwrap(), "Charlie");
        assert_eq!(items[1].get("name").unwrap(), "Bob");
        assert_eq!(items[2].get("name").unwrap(), "Alice");
    }

    #[test]
    fn test_sort_multiple_fields() {
        let sort_json = json!({"age": "ASC", "name": "ASC"});
        let sort = Sort::from_json(&sort_json).unwrap();
        let mut items = vec![
            json!({"name": "Charlie", "age": 25}),
            json!({"name": "Alice", "age": 25}),
            json!({"name": "Bob", "age": 30}),
        ];
        sort.sort(&mut items, |item, field| item.get(field).cloned());
        assert_eq!(items[0].get("name").unwrap(), "Alice");
        assert_eq!(items[1].get("name").unwrap(), "Charlie");
        assert_eq!(items[2].get("name").unwrap(), "Bob");
    }

    #[test]
    fn test_sort_empty_returns_none() {
        let sort_json = json!({});
        assert!(Sort::from_json(&sort_json).is_none());
    }

    #[test]
    fn test_sort_with_none_values() {
        let sort_json = json!({"age": "ASC"});
        let sort = Sort::from_json(&sort_json).unwrap();
        let mut items = vec![
            json!({"name": "Charlie", "age": 28}),
            json!({"name": "Alice"}),
            json!({"name": "Bob", "age": 30}),
        ];
        sort.sort(&mut items, |item, field| item.get(field).cloned());
        assert_eq!(items[0].get("name").unwrap(), "Alice");
        assert_eq!(items[1].get("name").unwrap(), "Charlie");
        assert_eq!(items[2].get("name").unwrap(), "Bob");
    }

    #[test]
    fn test_sort_already_sorted() {
        let sort_json = json!({"age": "ASC"});
        let sort = Sort::from_json(&sort_json).unwrap();
        let mut items = vec![
            json!({"name": "Alice", "age": 25}),
            json!({"name": "Charlie", "age": 28}),
            json!({"name": "Bob", "age": 30}),
        ];
        sort.sort(&mut items, |item, field| item.get(field).cloned());
        assert_eq!(items[0].get("name").unwrap(), "Alice");
        assert_eq!(items[1].get("name").unwrap(), "Charlie");
        assert_eq!(items[2].get("name").unwrap(), "Bob");
    }

    #[test]
    fn test_sort_reverse_sorted() {
        let sort_json = json!({"age": "ASC"});
        let sort = Sort::from_json(&sort_json).unwrap();
        let mut items = vec![
            json!({"name": "Bob", "age": 30}),
            json!({"name": "Charlie", "age": 28}),
            json!({"name": "Alice", "age": 25}),
        ];
        sort.sort(&mut items, |item, field| item.get(field).cloned());
        assert_eq!(items[0].get("name").unwrap(), "Alice");
        assert_eq!(items[1].get("name").unwrap(), "Charlie");
        assert_eq!(items[2].get("name").unwrap(), "Bob");
    }
}
