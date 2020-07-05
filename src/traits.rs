use crate::types;
use async_trait::async_trait;

#[async_trait]
pub trait Database: TransactionalOperations {
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

    async fn get_item<S: Into<String> + Send>(&self, pk: S, sk: S) -> types::GetItemResult;
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

    async fn transact_write_items(
        &self,
        transact_items: Vec<types::TransactWriteItem>,
    ) -> types::TransactWriteItemsResult;
}

pub trait TransactionalOperations {
    fn condition_check_exists<PK, SK, M>(&self, pk: PK, sk: SK, model: M) -> types::TransactWriteItem
    where
        PK: Into<String> + Send,
        SK: Into<String> + Send,
        M: Into<String> + Send;

    fn put(&self, hashmap: types::HashMap) -> types::TransactWriteItem;
}

impl<T: Database> TransactionalOperations for T {
    fn condition_check_exists<PK, SK, M>(&self, pk: PK, sk: SK, model: M) -> types::TransactWriteItem
    where
        PK: Into<String> + Send,
        SK: Into<String> + Send,
        M: Into<String> + Send,
    {
        let key = make_key(pk, sk);
        let condition_expression = "model = :model".to_string();

        let expression_attribute_values = {
            let mut values = types::HashMap::new();
            values.insert(
                ":model".to_string(),
                types::AttributeValue {
                    s: Some(model.into()),
                    ..Default::default()
                },
            );
            Some(values)
        };

        types::TransactWriteItem {
            condition_check: Some(types::ConditionCheck {
                table_name: self.table_name(),

                key,

                condition_expression,
                expression_attribute_values,

                ..Default::default()
            }),
            ..Default::default()
        }
    }

    fn put(&self, hashmap: types::HashMap) -> types::TransactWriteItem {
        types::TransactWriteItem {
            put: Some(types::Put {
                table_name: self.table_name(),
                item: hashmap,
                ..Default::default()
            }),
            ..Default::default()
        }
    }
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

pub fn make_key<PK, SK>(pk: PK, sk: SK) -> types::HashMap
where
    PK: Into<String>,
    SK: Into<String>,
{
    let pk = pk.into();
    let sk = sk.into();

    let mut key = types::HashMap::new();

    key.insert(
        "pk".to_string(),
        types::AttributeValue {
            s: Some(pk),
            ..Default::default()
        },
    );

    key.insert(
        "sk".to_string(),
        types::AttributeValue {
            s: Some(sk.into()),
            ..Default::default()
        },
    );

    key
}

#[async_trait]
pub trait SecurityTokens {
    async fn get_caller_identity(&self) -> types::GetCallerIdentityResult;
}
