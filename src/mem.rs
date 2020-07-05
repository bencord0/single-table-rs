use async_mutex::Mutex;
use async_trait::async_trait;
use std::collections::BTreeMap;
use uuid::Uuid;

use crate::{
    traits::{Database, Key},
    types::*,
};

pub struct MemoryDB {
    table_name: String,
    table: Mutex<BTreeMap<(String, String), HashMap>>,
    index: Mutex<BTreeMap<(String, String), HashMap>>,
}

pub fn memorydb() -> MemoryDB {
    MemoryDB {
        table_name: {
            let uuid = Uuid::new_v4();
            format!("single-table-{}", uuid.to_hyphenated())
        },
        table: Mutex::new(BTreeMap::new()),
        index: Mutex::new(BTreeMap::new()),
    }
}

#[async_trait]
impl Database for MemoryDB {
    fn table_name(&self) -> String {
        self.table_name.clone()
    }

    async fn delete_table(&self) -> DeleteTableResult {
        self.table.lock().await.clear();
        self.index.lock().await.clear();
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

    async fn scan<S>(&self, index: Option<S>, limit: Option<i64>) -> ScanResult
    where
        S: Into<String> + Send,
    {
        let mut items: Vec<HashMap> = vec![];

        let index = index.map(|s| s.into());
        let db = match &index {
            None => self.table.lock().await,
            Some(_) => self.index.lock().await,
        };

        for (i, item) in db.values().cloned().enumerate() {
            if let Some(limit) = limit {
                if i as i64 >= limit {
                    break;
                }
            }

            items.push(item);
        }

        let count = Some(items.len() as i64);
        let scanned_count = Some(items.len() as i64);

        Ok(ScanOutput {
            items: Some(items),
            count,
            scanned_count,
            ..Default::default()
        })
    }

    async fn get_item<S>(&self, pk: S, sk: S) -> GetItemResult
    where
        S: Into<String> + Send,
    {
        let key = (pk.into(), sk.into());

        let db = self.table.lock().await;
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
        self.table
            .lock()
            .await
            .insert(hash_map.key(), hash_map.clone());
        self.index
            .lock()
            .await
            .insert(hash_map.model_key(), hash_map.clone());

        Ok(Default::default())
    }

    async fn query<S>(&self, index: Option<S>, pk: S, sk: S) -> QueryResult
    where
        S: Into<String> + Send,
    {
        let index = index.map(|s| s.into());
        let pk = pk.into();
        let sk = sk.into();
        let mut items: Vec<HashMap> = vec![];
        let mut count: i64 = 0;

        let db = match &index {
            None => self.table.lock().await,
            Some(_) => self.index.lock().await,
        };

        'maxitems: for (key, hash_map) in db.iter() {
            if key.0 == pk && key.1.starts_with(&sk) {
                count = match count.checked_add(1) {
                    Some(count) => {
                        items.push(hash_map.clone());
                        count
                    }
                    None => break 'maxitems,
                };
            }
        }

        Ok(QueryOutput {
            items: Some(items),
            count: Some(count),
            ..Default::default()
        })
    }

    async fn transact_write_items(
        &self,
        transact_items: Vec<TransactWriteItem>,
    ) -> TransactWriteItemsResult {
        for transact_item in transact_items {
            if let Some(put_op) = transact_item.put {
                let hashmap = put_op.item;
                let _ = self.put_item(hashmap).await;
            }
        }

        Ok(Default::default())
    }
}
