use async_trait::async_trait;
use crate::types;

#[async_trait]
pub trait Database {
    fn table_name(&self) -> String;

    fn sync_create_table(&self);
    fn sync_delete_table(&self);

    async fn create_table(&self) -> types::CreateTableResult;
    async fn delete_table(&self) -> types::DeleteTableResult;

    async fn get_item<S: Into<String> + Send>(&self, pk: S, sk: Option<S>) -> types::GetItemResult;
    async fn put_item<H: Into<types::HashMap> + Key + Send>(&self, hashmap: H) -> types::PutItemResult;
    async fn query<S: Into<String> + Send>(&self, pk: S, sk: S) -> types::QueryResult;
}

pub trait Key {
    fn key(&self) -> (String, Option<String>);
}

impl Key for types::HashMap {
    fn key(&self) -> (String, Option<String>) {
        (
            match self.get("pk") {
                Some(pk) => match &pk.s {
                    Some(s) => s.clone(),
                    None => "".to_string(),
                },
                None => "".to_string(),
            },
            match self.get("sk") {
                Some(sk) => sk.s.clone(),
                None => None,
            },
        )
    }
}
