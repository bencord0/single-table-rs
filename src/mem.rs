use async_mutex::Mutex;
use async_trait::async_trait;
use std::collections::BTreeMap;
use uuid::Uuid;

use crate::{
    traits::{Database, Key},
    types::*,
};

pub struct MemoryDB(Mutex<BTreeMap<(String, Option<String>), HashMap>>, String);

pub fn memorydb() -> MemoryDB {
    MemoryDB(Mutex::new(BTreeMap::new()), {
        let uuid = Uuid::new_v4();
        format!("single-table-{}", uuid.to_hyphenated())
    })
}

#[async_trait]
impl Database for MemoryDB {
    fn table_name(&self) -> String {
        self.1.clone()
    }

    async fn delete_table(&self) -> DeleteTableResult {
        self.0.lock().await.clear();
        Ok(Default::default())
    }

    fn sync_delete_table(&self) {
        let _ = smol::run(async { self.delete_table().await });
    }

    async fn create_table(&self) -> CreateTableResult {
        Ok(Default::default())
    }

    fn sync_create_table(&self) {
        let _ = smol::run(async { self.create_table().await });
    }

    async fn describe_table(&self) -> DescribeTableResult {
        Ok(Default::default())
    }

    async fn get_item<S>(&self, pk: S, sk: Option<S>) -> GetItemResult
    where
        S: Into<String> + Send,
    {
        let key = (pk.into(), sk.map(|sk| sk.into()));

        let db = self.0.lock().await;
        let item = db.get(&key);

        Ok(GetItemOutput {
            item: item.cloned(),
            ..Default::default()
        })
    }

    async fn put_item<H>(&self, item: H) -> PutItemResult
    where
        H: Into<HashMap> + Key + Send,
    {
        let hash_map = item.into();
        let key = hash_map.key();
        self.0.lock().await.insert(key, hash_map);

        Ok(Default::default())
    }

    async fn query<S>(&self, pk: S, sk: S) -> QueryResult
    where
        S: Into<String> + Send,
    {
        let pk = pk.into();
        let sk = sk.into();
        let mut items: Vec<HashMap> = vec![];
        let mut count: i64 = 0;

        let db = self.0.lock().await;
        for (key, hash_map) in db.iter() {
            if key.0 == pk {
                if let Some(sortkey) = &key.1 {
                    if !sortkey.starts_with(&sk) {
                        continue;
                    }
                }

                count = match count.checked_add(1) {
                    Some(count) => {
                        items.push(hash_map.clone());
                        count
                    }
                    None => break,
                };
            }
        }

        Ok(QueryOutput {
            items: Some(items),
            count: Some(count),
            ..Default::default()
        })
    }
}
