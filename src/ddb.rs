use async_trait::async_trait;

#[rustfmt::skip]
use rusoto_dynamodb::{
    DynamoDb,
    DynamoDbClient,
    KeySchemaElement,
    ProvisionedThroughput,

    AttributeDefinition,
    AttributeValue,

    GlobalSecondaryIndex,
    Projection,
};

use crate::{
    traits::{make_key, Database, Key},
    types::*,
};

pub struct DDB(DynamoDbClient, String);

impl DDB {
    pub fn new<T: Into<String>>(client: DynamoDbClient, table_name: T) -> Self {
        Self(client, table_name.into())
    }
}

#[async_trait]
impl Database for DDB {
    fn table_name(&self) -> String {
        self.1.clone()
    }

    async fn delete_table(&self) -> DeleteTableResult {
        self.0
            .delete_table(DeleteTableInput {
                table_name: self.table_name(),
                ..Default::default()
            })
            .await
    }

    async fn create_table(&self) -> CreateTableResult {
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
                    AttributeDefinition {
                        attribute_name: "model".to_string(),
                        attribute_type: "S".to_string(),
                    },
                ],
                global_secondary_indexes: Some(vec![GlobalSecondaryIndex {
                    index_name: "model".to_string(),
                    key_schema: vec![
                        KeySchemaElement {
                            attribute_name: "model".to_string(),
                            key_type: "HASH".to_string(),
                        },
                        KeySchemaElement {
                            attribute_name: "sk".to_string(),
                            key_type: "RANGE".to_string(),
                        },
                    ],
                    projection: Projection {
                        projection_type: Some("ALL".to_string()),
                        ..Default::default()
                    },
                    provisioned_throughput: Some(ProvisionedThroughput {
                        read_capacity_units: 1,
                        write_capacity_units: 1,
                    }),
                }]),
                provisioned_throughput: Some(ProvisionedThroughput {
                    read_capacity_units: 1,
                    write_capacity_units: 1,
                }),
                ..Default::default()
            })
            .await
    }

    async fn describe_table(&self) -> DescribeTableResult {
        self.0
            .describe_table(DescribeTableInput {
                table_name: self.table_name(),
                ..Default::default()
            })
            .await
    }

    async fn scan<S>(&self, index: Option<S>, limit: Option<i64>) -> ScanResult
    where
        S: Into<String> + Send,
    {
        let index_name: Option<String> = index.map(|idx| idx.into());
        self.0
            .scan(ScanInput {
                table_name: self.table_name(),
                index_name,
                limit,
                ..Default::default()
            })
            .await
    }

    async fn get_item<S>(&self, pk: S, sk: S) -> GetItemResult
    where
        S: Into<String> + Send,
    {
        let key = make_key(pk, sk);
        self.0
            .get_item(GetItemInput {
                table_name: self.table_name(),
                key,
                ..Default::default()
            })
            .await
    }

    async fn put_item<H>(&self, item: H) -> PutItemResult
    where
        H: Into<HashMap> + Key + Send,
    {
        self.0
            .put_item(PutItemInput {
                table_name: self.table_name(),
                item: item.into(),
                ..Default::default()
            })
            .await
    }

    async fn query<S>(&self, index: Option<S>, pk: S, sk: S) -> QueryResult
    where
        S: Into<String> + Send,
    {
        let index_name = index.map(|s| s.into());

        let key_condition_expression = Some("#pk = :pk AND begins_with(#sk, :sk)".to_string());
        let expression_attribute_values = {
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
            Some(values)
        };

        type Names = std::collections::HashMap<String, String>;
        let expression_attribute_names = match index_name.as_ref() {
            None => {
                let mut names = Names::new();
                names.insert("#pk".to_string(), "pk".to_string());
                names.insert("#sk".to_string(), "sk".to_string());
                Some(names)
            }
            Some(index) if index == "model" => {
                let mut names = Names::new();
                names.insert("#pk".to_string(), "model".to_string());
                names.insert("#sk".to_string(), "sk".to_string());
                Some(names)
            }
            Some(_) => None,
        };

        self.0
            .query(QueryInput {
                table_name: self.table_name(),
                index_name,
                key_condition_expression,
                expression_attribute_names,
                expression_attribute_values,
                ..Default::default()
            })
            .await
    }

    async fn transact_write_items(
        &self,
        transact_items: Vec<TransactWriteItem>,
    ) -> TransactWriteItemsResult {
        self.0
            .transact_write_items(TransactWriteItemsInput {
                transact_items,
                ..Default::default()
            })
            .await
    }
}
