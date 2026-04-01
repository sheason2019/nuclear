use async_graphql::*;
use serde::{Deserialize, Serialize};

/// JSON标量类型，用于存储无schema数据
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Json(pub serde_json::Value);

#[Scalar]
impl ScalarType for Json {
    fn parse(value: Value) -> InputValueResult<Self> {
        match value {
            Value::Object(_)
            | Value::List(_)
            | Value::String(_)
            | Value::Number(_)
            | Value::Boolean(_)
            | Value::Null => Ok(Json(serde_json::to_value(&value)?)),
            _ => Err(InputValueError::expected_type(value)),
        }
    }

    fn to_value(&self) -> Value {
        serde_json::from_value(self.0.clone()).unwrap_or(Value::Null)
    }
}

/// DateTime标量类型
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct DateTime(pub chrono::DateTime<chrono::Utc>);

#[Scalar]
impl ScalarType for DateTime {
    fn parse(value: Value) -> InputValueResult<Self> {
        if let Value::String(s) = value {
            let dt = chrono::DateTime::parse_from_rfc3339(&s)?.with_timezone(&chrono::Utc);
            Ok(DateTime(dt))
        } else {
            Err(InputValueError::expected_type(value))
        }
    }

    fn to_value(&self) -> Value {
        Value::String(self.0.to_rfc3339())
    }
}
