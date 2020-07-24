use async_trait::async_trait;
use futures_intrusive::sync::{Semaphore, SemaphoreReleaser};
use once_cell::sync::Lazy;
use smol_timeout::TimeoutExt;
use std::{error::Error, time::Duration};

use single_table::*;
use traits::{Database, Key};

mod database;

type TestResult = Result<(), Box<dyn Error>>;

#[cfg(feature = "external_database")]
pub use rusoto_dynamodb::DynamoDbClient;

#[cfg(feature = "external_database")]
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

fn memorydb() -> TemporaryDatabase<mem::MemoryDB> {
    let memdb = mem::memorydb();
    let db = TemporaryDatabase::new(memdb);

    db.sync_create_table();
    db
}

struct TemporaryDatabase<DB: Database + Send + Sync>(DB);

const SEMSIZE: usize = 20;
static SEMAPHORE: Lazy<Semaphore> = Lazy::new(|| {
    // Create an async aware semaphore to allow some parallel access to the db
    Semaphore::new(true, SEMSIZE)
});

impl<DB: Database + Send + Sync> TemporaryDatabase<DB> {
    fn new(db: DB) -> Self {
        Self(db)
    }

    async fn acquire(&self) -> SemaphoreReleaser<'_> {
        SEMAPHORE.acquire(1 as usize).await
    }

    async fn acquire_all(&self) -> SemaphoreReleaser<'_> {
        SEMAPHORE.acquire(SEMSIZE).await
    }

    fn sync_create_table(&self) {
        if let None = smol::run(self.create_table().timeout(Duration::from_secs(2))) {
            panic!(
                r#"
sync_create_table: timed out
Do you need to start the database?

    $ ./scripts/start-ddb.sh
    $ export AWS_ENDPOINT_URL=http://localhost:2000

"#
            );
        };
    }

    fn sync_delete_table(&self) {
        let _ = smol::run(self.delete_table().timeout(Duration::from_secs(1)));
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
        match {
            // lightweight acquire, drop at end of block
            let _sem = self.acquire().await;
            self.0.create_table().await
        } {
            // Successfully created the table on first try
            Ok(table_result) => Ok(table_result),

            // Failed to create the table.
            // Acquire the full semaphore to wait for the db to settle
            Err(_) => {
                let _sem = self.acquire_all().await;
                Ok(self.0.create_table().await.expect("create_table"))
            }
        }
    }

    async fn delete_table(&self) -> types::DeleteTableResult {
        let _sem = self.acquire().await;
        self.0.delete_table().await
    }

    async fn describe_table(&self) -> types::DescribeTableResult {
        let _sem = self.acquire().await;
        self.0.describe_table().await
    }

    async fn scan<S: Into<String> + Send>(
        &self,
        index: Option<S>,
        limit: Option<i64>,
    ) -> types::ScanResult {
        let _sem = self.acquire().await;
        self.0.scan(index, limit).await
    }

    async fn get_item<S: Into<String> + Send>(&self, pk: S, sk: S) -> types::GetItemResult {
        let _sem = self.acquire().await;
        self.0.get_item(pk, sk).await
    }

    async fn put_item<H: Into<types::HashMap> + Key + Send>(
        &self,
        hashmap: H,
    ) -> types::PutItemResult {
        let _sem = self.acquire().await;
        self.0.put_item(hashmap).await
    }

    async fn query<S: Into<String> + Send>(
        &self,
        index: Option<S>,
        pk: S,
        sk: S,
    ) -> types::QueryResult {
        let _sem = self.acquire().await;
        self.0.query(index, pk, sk).await
    }

    async fn transact_write_items(
        &self,
        transact_items: Vec<types::TransactWriteItem>,
    ) -> types::TransactWriteItemsResult {
        let _sem = self.acquire().await;
        self.0.transact_write_items(transact_items).await
    }
}

fn insert_models(db: &impl Database) -> TestResult {
    let foo: Model = Model::new("foo", 1);
    let bar: SubModel = SubModel::new("bar", foo.clone());
    let baz: SubModel = SubModel::new("baz", foo.clone());

    let items: Vec<types::HashMap> = vec![
        serde_dynamodb::to_hashmap(&foo)?,
        serde_dynamodb::to_hashmap(&bar)?,
        serde_dynamodb::to_hashmap(&baz)?,
    ];

    smol::run(futures::future::join_all(
        items.iter().map(|item| db.put_item(item.clone())),
    ));

    Ok(())
}
