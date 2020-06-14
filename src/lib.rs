use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Model {
    name: String,
    a_number: i32,

    created_at: DateTime<Utc>,
    pk: String,
    sk: String,
}

impl Model {
    pub fn new<S>(name: S, a_number: i32) -> Self
    where
        S: Into<String>,
    {
        let name = name.into();
        Self {
            pk: format!("model#{}", name.clone()),
            sk: format!("model#{}", name.clone()),

            name: name,
            created_at: Utc::now(),
            a_number,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SubModel {
    name: String,
    model: Model,

    created_at: DateTime<Utc>,

    pk: String,
    sk: String,
}

impl SubModel {
    pub fn new<'a, S>(name: S, model: Model) -> Self
    where
        S: Into<String>,
    {
        let name = name.into();
        Self {
            pk: format!("model#{}", model.name()),
            sk: format!("model#{}#submodel#{}", model.name(), &name),

            name: name,
            model: model,
            created_at: Utc::now(),
        }
    }
}

pub mod ddb {
    use rusoto_core::{Region, RusotoError};
    use std::env;

    #[rustfmt::skip]
    use rusoto_dynamodb::{
        DynamoDb,
        DynamoDbClient,
        KeySchemaElement,
        ProvisionedThroughput,

        AttributeDefinition,
        AttributeValue,

        CreateTableError, CreateTableInput, CreateTableOutput,
        DeleteTableError, DeleteTableInput, DeleteTableOutput,
        GetItemError, GetItemInput, GetItemOutput,
        PutItemError, PutItemInput, PutItemOutput,
        QueryError, QueryInput, QueryOutput,
    };

    pub struct DDB(DynamoDbClient, String);
    pub type HashMap = std::collections::HashMap<String, AttributeValue>;

    pub fn dynamodb() -> DDB {
        env::set_var("AWS_ACCESS_KEY_ID", "local");
        env::set_var("AWS_SECRET_ACCESS_KEY", "local");

        let region = Region::Custom {
            name: "local".to_owned(),
            endpoint: "http://localhost:1000".to_owned(),
        };

        DDB(DynamoDbClient::new(region), {
            let uuid = uuid::Uuid::new_v4();
            format!("models-rs-{}", uuid.to_hyphenated().to_string())
        })
    }

    impl DDB {
        pub fn table_name(&self) -> String {
            self.1.clone()
        }

        pub async fn delete_table(
            &self,
        ) -> Result<DeleteTableOutput, RusotoError<DeleteTableError>> {
            self.0
                .delete_table(DeleteTableInput {
                    table_name: self.table_name(),
                    ..Default::default()
                })
                .await
        }

        pub fn sync_delete_table(&self) {
            let _ = smol::run(async { self.delete_table().await });
        }

        pub async fn create_table(
            &self,
        ) -> Result<CreateTableOutput, RusotoError<CreateTableError>> {
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
                })
                .await
        }

        pub fn sync_create_table(&self) {
            let _ = smol::run(async { self.create_table().await });
        }

        pub async fn get_item<S>(
            &self,
            pk: S,
            sk: Option<S>,
        ) -> Result<GetItemOutput, RusotoError<GetItemError>>
        where
            S: Into<String>,
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

        pub async fn put_item(
            &self,
            item: HashMap,
        ) -> Result<PutItemOutput, RusotoError<PutItemError>> {
            self.0
                .put_item(PutItemInput {
                    table_name: self.table_name(),
                    item,
                    ..Default::default()
                })
                .await
        }

        pub async fn query<S>(&self, pk: S, sk: S) -> Result<QueryOutput, RusotoError<QueryError>>
        where
            S: Into<String>,
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_none() {
        let ddb = ddb::dynamodb();
        ddb.sync_create_table();

        if let Ok(get_item_output) =
            smol::run(async { ddb.get_item("model#foo", Some("model#foo")).await })
        {
            let item = get_item_output.item;
            assert_eq!(item, None);
        }

        ddb.sync_delete_table();
    }

    #[test]
    fn test_put_get_some() {
        let ddb = ddb::dynamodb();
        ddb.sync_create_table();

        let model = Model::new("foo", 1);

        let hashmap: ddb::HashMap = match serde_dynamodb::to_hashmap(&model) {
            Ok(hashmap) => hashmap,
            Err(_) => ddb::HashMap::new(),
        };

        if let Ok(put_item_output) = smol::run(async { ddb.put_item(hashmap).await }) {
            println!("{:?}", put_item_output);
        }

        if let Ok(get_item_output) =
            smol::run(async { ddb.get_item("model#foo", Some("model#foo")).await })
        {
            let item = match get_item_output.item {
                Some(item) => item,
                None => ddb::HashMap::new(),
            };

            let model: serde_dynamodb::error::Result<Model> = serde_dynamodb::from_hashmap(item);
            assert!(model.is_ok());

            if let Ok(model) = model {
                println!("{:?}", model);
            }
        }

        ddb.sync_delete_table();
    }

    #[test]
    fn test_get_submodels() {
        let ddb = ddb::dynamodb();
        ddb.sync_create_table();

        let foo: Model = Model::new("foo", 1);
        let bar: SubModel = SubModel::new("bar", foo.clone());
        let baz: SubModel = SubModel::new("baz", foo.clone());

        smol::run(async {
            let _ = futures::join!(
                {
                    match serde_dynamodb::to_hashmap(&foo) {
                        Ok(hashmap) => ddb.put_item(hashmap),
                        Err(_) => return,
                    }
                },
                {
                    match serde_dynamodb::to_hashmap(&bar) {
                        Ok(hashmap) => ddb.put_item(hashmap),
                        Err(_) => return,
                    }
                },
                {
                    match serde_dynamodb::to_hashmap(&baz) {
                        Ok(hashmap) => ddb.put_item(hashmap),
                        Err(_) => return,
                    }
                },
            );
        });

        let items: rusoto_dynamodb::QueryOutput =
            match smol::run(async { ddb.query("model#foo", "model#foo#submodel#").await }) {
                Ok(items) => items,
                Err(_) => return,
            };
        assert_eq!(items.count, Some(2));

        let mut submodels: Vec<SubModel> = vec![];
        if let Some(items) = items.items {
            for item in items {
                let sm: SubModel = match serde_dynamodb::from_hashmap(item) {
                    Ok(sm) => sm,
                    Err(_) => return,
                };
                submodels.push(sm);
            }
        }

        println!("{:#?}", submodels);
        assert_eq!(submodels.len(), 2);
        assert_eq!(submodels[0].name, "bar");
        assert_eq!(submodels[1].name, "baz");

        ddb.sync_delete_table();
    }
}
