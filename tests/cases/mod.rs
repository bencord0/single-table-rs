use async_std::task::sleep;
use async_trait::async_trait;
use futures_intrusive::sync::Semaphore;
use rusoto_dynamodb::DynamoDbClient;
use smol_timeout::TimeoutExt;
use std::time::Duration;

use single_table::*;
use traits::{Database, Key};

mod database;

fn dynamodb() -> TemporaryDatabase<ddb::DDB> {
    // Connect to dynamodb-local
    // There's a script in `/scripts/start-ddb.sh` to set one up with docker
    let endpoint_url = env::ensure_var("AWS_ENDPOINT_URL");
    let region = env::resolve_region(None, Some(endpoint_url)).unwrap();
    let db = ddb::DDB::new(DynamoDbClient::new(region), {
        let uuid = uuid::Uuid::new_v4();
        format!("single-table-{}", uuid.to_hyphenated())
    });

    // Create a temporary database table that will be deleted on Drop
    {
        let db = TemporaryDatabase::new(db);
        db.sync_create_table();
        db
    }
}

struct TemporaryDatabase<DB: Database + Send + Sync>(DB, Semaphore);

impl<DB: Database + Send + Sync> TemporaryDatabase<DB> {
    fn new(db: DB) -> Self {
        // Create an async aware semaphore to allow some parallel access to the db
        let semaphore = Semaphore::new(true, 5);
        Self(db, semaphore)
    }

    fn sync_create_table(&self) {
        if let None = smol::run(async { self.create_table().timeout(Duration::from_secs(2)).await })
        {
            panic!("sync_create_table: timed out");
        };
    }

    fn sync_delete_table(&self) {
        if let None = smol::run(async { self.delete_table().timeout(Duration::from_secs(1)).await })
        {
            panic!("sync_delete_table: timed out");
        };
    }
}

impl<DB: Database + Send + Sync> Drop for TemporaryDatabase<DB> {
    fn drop(&mut self) {
        self.sync_delete_table();
    }
}

#[async_trait]
impl<DB: Database + Send + Sync> Database for TemporaryDatabase<DB> {
    fn table_name(&self) -> String {
        self.0.table_name()
    }

    async fn create_table(&self) -> types::CreateTableResult {
        // create_table using dynamodb local can fail with a 500 error
        // panicked at 'create_table: Unknown(BufferedHttpResponse {status: 500, ... }
        //
        // Use a semaphore to require exclusive access and a small gap between requests
        // to protect the db when creating tables.
        let _sem = self.1.acquire(5).await;
        sleep(Duration::from_millis(200)).await;
        Ok(self.0.create_table().await.expect("create_table"))
    }

    async fn delete_table(&self) -> types::DeleteTableResult {
        let _sem = self.1.acquire(1).await;
        self.0.delete_table().await
    }

    async fn describe_table(&self) -> types::DescribeTableResult {
        let _sem = self.1.acquire(1).await;
        self.0.describe_table().await
    }

    async fn scan<S: Into<String> + Send>(
        &self,
        index: Option<S>,
        limit: Option<i64>,
    ) -> types::ScanResult {
        let _sem = self.1.acquire(1).await;
        self.0.scan(index, limit).await
    }

    async fn get_item<S: Into<String> + Send>(&self, pk: S, sk: S) -> types::GetItemResult {
        let _sem = self.1.acquire(1).await;
        self.0.get_item(pk, sk).await
    }

    async fn put_item<H: Into<types::HashMap> + Key + Send>(
        &self,
        hashmap: H,
    ) -> types::PutItemResult {
        let _sem = self.1.acquire(1).await;
        self.0.put_item(hashmap).await
    }

    async fn query<S: Into<String> + Send>(
        &self,
        index: Option<S>,
        pk: S,
        sk: S,
    ) -> types::QueryResult {
        let _sem = self.1.acquire(1).await;
        self.0.query(index, pk, sk).await
    }

    async fn transact_write_items(
        &self,
        transact_items: Vec<types::TransactWriteItem>,
    ) -> types::TransactWriteItemsResult {
        let _sem = self.1.acquire(1).await;
        self.0.transact_write_items(transact_items).await
    }
}
