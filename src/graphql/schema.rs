use async_graphql::*;
use futures::stream::{self, Stream};
use super::scalars::{Json, DateTime};
use crate::api::database::GraphqlDatabase;

#[derive(SimpleObject, Debug, Clone)]
pub struct Meta {
    pub id: ID,
    pub created_at: DateTime,
    pub updated_at: DateTime,
}

#[derive(Debug, Clone)]
pub struct Record {
    pub data: serde_json::Value,
    pub meta: Meta,
}

#[Object]
impl Record {
    async fn field(&self, name: String) -> Option<Json> {
        self.data.get(&name).map(|v| Json(v.clone()))
    }
    
    async fn data(&self) -> Json {
        Json(self.data.clone())
    }
    
    async fn _meta(&self) -> &Meta {
        &self.meta
    }
}

pub struct QueryRoot;

#[Object]
impl QueryRoot {
    async fn records(
        &self,
        ctx: &Context<'_>,
        collection: String,
        _filter: Option<Json>,
        _order_by: Option<Json>,
        first: Option<i32>,
        offset: Option<i32>,
    ) -> Result<Vec<Record>> {
        let db = ctx.data::<GraphqlDatabase>()?;
        let records = db.get_records(&collection).await
            .map_err(|e| Error::new(e.to_string()))?;
        
        let mut result: Vec<Record> = records.into_iter().map(|(id, record_data)| {
            Record {
                data: record_data.fields,
                meta: Meta {
                    id: id.into(),
                    created_at: DateTime(chrono::DateTime::from_timestamp_millis(record_data.meta.created_at as i64)
                        .unwrap_or_default()
                        .with_timezone(&chrono::Utc)),
                    updated_at: DateTime(chrono::DateTime::from_timestamp_millis(record_data.meta.updated_at as i64)
                        .unwrap_or_default()
                        .with_timezone(&chrono::Utc)),
                },
            }
        }).collect();
        
        if let Some(offset) = offset {
            if offset as usize >= result.len() {
                return Ok(vec![]);
            }
            result = result.split_off(offset as usize);
        }
        
        if let Some(first) = first {
            result.truncate(first as usize);
        }
        
        Ok(result)
    }

    async fn record(
        &self, 
        ctx: &Context<'_>, 
        collection: String,
        id: ID
    ) -> Result<Option<Record>> {
        let db = ctx.data::<GraphqlDatabase>()?;
        if let Some(record_data) = db.get_record(&collection, &id).await
            .map_err(|e| Error::new(e.to_string()))? 
        {
            Ok(Some(Record {
                data: record_data.fields,
                meta: Meta {
                    id: id.into(),
                    created_at: DateTime(chrono::DateTime::from_timestamp_millis(record_data.meta.created_at as i64)
                        .unwrap_or_default()
                        .with_timezone(&chrono::Utc)),
                    updated_at: DateTime(chrono::DateTime::from_timestamp_millis(record_data.meta.updated_at as i64)
                        .unwrap_or_default()
                        .with_timezone(&chrono::Utc)),
                },
            }))
        } else {
            Ok(None)
        }
    }

    async fn records_aggregate(
        &self, 
        ctx: &Context<'_>,
        collection: String
    ) -> Result<RecordsAggregate> {
        let db = ctx.data::<GraphqlDatabase>()?;
        let count = db.count_records(&collection).await
            .map_err(|e| Error::new(e.to_string()))?;
        Ok(RecordsAggregate { count })
    }
}

#[derive(SimpleObject)]
pub struct RecordsAggregate {
    pub count: i32,
}

pub struct MutationRoot;

#[Object]
impl MutationRoot {
    async fn create_record(
        &self, 
        ctx: &Context<'_>,
        collection: String,
        data: Json
    ) -> Result<Record> {
        let db = ctx.data::<GraphqlDatabase>()?;
        let record_data = db.create_record(&collection, data.0).await
            .map_err(|e| Error::new(e.to_string()))?;
        
        Ok(Record {
            data: record_data.fields,
            meta: Meta {
                id: record_data.meta.id.into(),
                created_at: DateTime(chrono::DateTime::from_timestamp_millis(record_data.meta.created_at as i64)
                    .unwrap_or_default()
                    .with_timezone(&chrono::Utc)),
                updated_at: DateTime(chrono::DateTime::from_timestamp_millis(record_data.meta.updated_at as i64)
                    .unwrap_or_default()
                    .with_timezone(&chrono::Utc)),
            },
        })
    }

    async fn update_record(
        &self, 
        ctx: &Context<'_>,
        collection: String,
        id: ID,
        data: Json
    ) -> Result<Option<Record>> {
        let db = ctx.data::<GraphqlDatabase>()?;
        if let Some(record_data) = db.update_record(&collection, &id, data.0).await
            .map_err(|e| Error::new(e.to_string()))? 
        {
            Ok(Some(Record {
                data: record_data.fields,
                meta: Meta {
                    id: id.into(),
                    created_at: DateTime(chrono::DateTime::from_timestamp_millis(record_data.meta.created_at as i64)
                        .unwrap_or_default()
                        .with_timezone(&chrono::Utc)),
                    updated_at: DateTime(chrono::DateTime::from_timestamp_millis(record_data.meta.updated_at as i64)
                        .unwrap_or_default()
                        .with_timezone(&chrono::Utc)),
                },
            }))
        } else {
            Ok(None)
        }
    }

    async fn delete_record(
        &self, 
        ctx: &Context<'_>,
        collection: String,
        id: ID
    ) -> Result<bool> {
        let db = ctx.data::<GraphqlDatabase>()?;
        db.delete_record(&collection, &id).await
            .map_err(|e| Error::new(e.to_string()))
    }
}

pub struct SubscriptionRoot;

#[Subscription]
impl SubscriptionRoot {
    async fn record_created(
        &self, 
        ctx: &Context<'_>,
        collection: String
    ) -> impl Stream<Item = Record> {
        let _ = (ctx, collection);
        stream::iter(vec![])
    }

    async fn record_updated(
        &self, 
        ctx: &Context<'_>,
        collection: String
    ) -> impl Stream<Item = Record> {
        let _ = (ctx, collection);
        stream::iter(vec![])
    }

    async fn record_deleted(
        &self, 
        ctx: &Context<'_>,
        collection: String
    ) -> impl Stream<Item = ID> {
        let _ = (ctx, collection);
        stream::iter(vec![])
    }
}