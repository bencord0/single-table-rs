use async_trait::async_trait;
use rusoto_core::Region;
use smol_timeout::TimeoutExt;
use std::{env, time::Duration};

#[rustfmt::skip]
use rusoto_dynamodb::{
    DynamoDb,
    DynamoDbClient,
    KeySchemaElement,
    ProvisionedThroughput,

    AttributeDefinition,
    AttributeValue,

};

use crate::{
    traits::{
        Database,
        Key,
    },
    types::*,
};

pub struct DDB(DynamoDbClient, String);

pub fn dynamodb() -> DDB {
    env::set_var("AWS_ACCESS_KEY_ID", "local");
    env::set_var("AWS_SECRET_ACCESS_KEY", "local");

    let region = Region::Custom {
        name: "local".to_owned(),
        endpoint: "http://localhost:2000".to_owned(),
    };

    DDB(DynamoDbClient::new(region), {
        let uuid = uuid::Uuid::new_v4();
        format!("single-table-{}", uuid.to_hyphenated().to_string())
    })
}

#[async_trait]
impl Database for DDB {

    fn table_name(&self) -> String {
        self.1.clone()
    }

    async fn delete_table(
        &self,
    ) -> DeleteTableResult {
        self.0
            .delete_table(DeleteTableInput {
                table_name: self.table_name(),
                ..Default::default()
            })
            .await
    }

    fn sync_delete_table(&self) {
        if let None = smol::run(async {
            self.delete_table()
                .timeout(Duration::from_secs(1))
                .await
        }) {
            panic!("sync_delete_table: timed out");
        };
    }

    async fn create_table(
        &self,
    ) -> CreateTableResult {
        self.0
            .create_table(CreateTableInput {
                table_name: self.table_name(),
                key_schema: vec![
                    KeySchemaElement {
                        attribute_name: "pk".to_string(),
                        key_type: "HASH".to_string(),
                    },
                    KeySchemaElement {
                        attribute_name: "sk".to_string(),
                        key_type: "RANGE".to_string(),
                    },
                ],
                attribute_definitions: vec![
                    AttributeDefinition {
                        attribute_name: "pk".to_string(),
                        attribute_type: "S".to_string(),
                    },
                    AttributeDefinition {
                        attribute_name: "sk".to_string(),
                        attribute_type: "S".to_string(),
                    },
                ],
                provisioned_throughput: Some(ProvisionedThroughput {
                    read_capacity_units: 1,
                    write_capacity_units: 1,
                }),
                ..Default::default()
            }).await
    }

    fn sync_create_table(&self) {
        if let None = smol::run(async {
            self.create_table()
                .timeout(Duration::from_secs(1))
                .await
        }) {
            panic!("sync_create_table: timed out");
        };
    }


    async fn get_item<S>(
        &self,
        pk: S,
        sk: Option<S>,
    ) -> GetItemResult
    where
        S: Into<String> + Send,
    {
        let mut key = HashMap::new();
        key.insert(
            "pk".to_string(),
            AttributeValue {
                s: Some(pk.into()),
                ..Default::default()
            },
        );
        if let Some(sk) = sk {
            key.insert(
                "sk".to_string(),
                AttributeValue {
                    s: Some(sk.into()),
                    ..Default::default()
                },
            );
        }

        self.0
            .get_item(GetItemInput {
                table_name: self.table_name(),
                key,
                ..Default::default()
            })
            .await
    }


    async fn put_item<H>(
        &self,
        item: H,
    ) -> PutItemResult
    where
        H: Into<HashMap> + Key + Send
    {
        self.0
            .put_item(PutItemInput {
                table_name: self.table_name(),
                item: item.into(),
                ..Default::default()
            })
            .await
    }

    async fn query<S>(&self, pk: S, sk: S) -> QueryResult
    where
        S: Into<String> + Send,
    {
        let keys = "pk = :pk AND begins_with(sk, :sk)".to_string();
        let mut values = HashMap::new();
        values.insert(
            ":pk".to_string(),
            AttributeValue {
                s: Some(pk.into()),
                ..Default::default()
            },
        );
        values.insert(
            ":sk".to_string(),
            AttributeValue {
                s: Some(sk.into()),
                ..Default::default()
            },
        );

        self.0
            .query(QueryInput {
                table_name: self.table_name(),
                key_condition_expression: Some(keys),
                expression_attribute_values: Some(values),
                ..Default::default()
            })
            .await
    }
}
