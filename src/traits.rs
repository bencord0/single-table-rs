use crate::types;
use async_trait::async_trait;

#[async_trait]
pub trait Database {
    fn table_name(&self) -> String;

    fn sync_create_table(&self);
    fn sync_delete_table(&self);

    async fn create_table(&self) -> types::CreateTableResult;
    async fn delete_table(&self) -> types::DeleteTableResult;
    async fn describe_table(&self) -> types::DescribeTableResult;
    async fn scan<S: Into<String> + Send>(
        &self,
        index: Option<S>,
        limit: Option<i64>,
    ) -> types::ScanResult;

    async fn get_item<S: Into<String> + Send>(&self, pk: S, sk: Option<S>) -> types::GetItemResult;
    async fn put_item<H: Into<types::HashMap> + Key + Send>(
        &self,
        hashmap: H,
    ) -> types::PutItemResult;
    async fn query<S: Into<String> + Send>(
        &self,
        index: Option<S>,
        pk: S,
        sk: S,
    ) -> types::QueryResult;
}

pub trait Key {
    fn key(&self) -> (String, String);
    fn model_key(&self) -> (String, String);
}

impl Key for types::HashMap {
    fn key(&self) -> (String, String) {
        (
            match self.get("pk") {
                Some(pk) => match &pk.s {
                    Some(s) => s.clone(),
                    None => "".to_string(),
                },
                None => "".to_string(),
            },
            match self.get("sk") {
                Some(sk) => match &sk.s {
                    Some(s) => s.clone(),
                    None => "".to_string(),
                },
                None => "".to_string(),
            },
        )
    }

    fn model_key(&self) -> (String, String) {
        (
            match self.get("model") {
                Some(model) => match &model.s {
                    Some(s) => s.clone(),
                    None => "".to_string(),
                },
                None => "".to_string(),
            },
            match self.get("sk") {
                Some(sk) => match &sk.s {
                    Some(s) => s.clone(),
                    None => "".to_string(),
                },
                None => "".to_string(),
            },
        )
    }
}

#[async_trait]
pub trait SecurityTokens {
    async fn get_caller_identity(&self) -> types::GetCallerIdentityResult;
}
