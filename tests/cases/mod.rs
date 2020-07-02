use async_trait::async_trait;
use rusoto_dynamodb::DynamoDbClient;

use single_table::*;
use traits::{Database, Key};

mod database;

fn dynamodb() -> TemporaryDatabase<ddb::DDB> {
    // Connect to dynamodb-local
    // There's a script in `/scripts/start-ddb.sh` to set one up with docker
    let endpoint_url = env::ensure_var("AWS_ENDPOINT_URL");
    let region = env::resolve_region(None, Some(endpoint_url)).unwrap();
    let db = ddb::DDB::new(
        DynamoDbClient::new(region),
        {
            let uuid = uuid::Uuid::new_v4();
            format!("single-table-{}", uuid.to_hyphenated())
        }
    );

    // Create a temporary database table that will be deleted on Drop
    db.sync_create_table();
    TemporaryDatabase(db)
}

struct TemporaryDatabase<DB: Database + Send + Sync>(DB);

#[async_trait]
impl<DB: Database + Send + Sync> Database for TemporaryDatabase<DB> {
    fn table_name(&self) -> String { self.0.table_name() }
    fn sync_create_table(&self) { self.0.sync_create_table() }
    fn sync_delete_table(&self) { self.0.sync_delete_table() }

    async fn create_table(&self) -> types::CreateTableResult {
        self.0.create_table().await
    }

    async fn delete_table(&self) -> types::DeleteTableResult {
        self.0.delete_table().await
    }

    async fn describe_table(&self) -> types::DescribeTableResult {
        self.0.describe_table().await
    }

    async fn scan<S: Into<String> + Send>(&self, index_name: Option<S>, limit: Option<i64>) -> types::ScanResult {
        self.0.scan(index_name, limit).await
    }

    async fn get_item<S: Into<String> + Send>(&self, pk: S, sk: Option<S>) -> types::GetItemResult {
        self.0.get_item(pk, sk).await
    }

    async fn put_item<H: Into<types::HashMap> + Key + Send>(
        &self,
        hashmap: H,
    ) -> types::PutItemResult {
        self.0.put_item(hashmap).await
    }

    async fn query<S: Into<String> + Send>(&self, pk: S, sk: S) -> types::QueryResult {
        self.0.query(pk, sk).await
    }
}

impl<DB: Database + Send + Sync> Drop for TemporaryDatabase<DB> {
    fn drop(&mut self) {
        self.0.sync_delete_table();
    }
}
