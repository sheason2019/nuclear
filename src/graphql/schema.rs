use async_graphql::*;
use futures::stream::{self, Stream};
use super::scalars::{Json, DateTime};

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
        filter: Option<Json>,
        order_by: Option<Json>,
        first: Option<i32>,
        offset: Option<i32>,
    ) -> Result<Vec<Record>> {
        let _ = (ctx, collection, filter, order_by, first, offset);
        todo!()
    }

    async fn record(
        &self, 
        ctx: &Context<'_>, 
        collection: String,
        id: ID
    ) -> Result<Option<Record>> {
        let _ = (ctx, collection, id);
        todo!()
    }

    async fn records_aggregate(
        &self, 
        ctx: &Context<'_>,
        collection: String
    ) -> Result<RecordsAggregate> {
        let _ = (ctx, collection);
        todo!()
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
        let _ = (ctx, collection, data);
        todo!()
    }

    async fn update_record(
        &self, 
        ctx: &Context<'_>,
        collection: String,
        id: ID,
        data: Json
    ) -> Result<Option<Record>> {
        let _ = (ctx, collection, id, data);
        todo!()
    }

    async fn delete_record(
        &self, 
        ctx: &Context<'_>,
        collection: String,
        id: ID
    ) -> Result<bool> {
        let _ = (ctx, collection, id);
        todo!()
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