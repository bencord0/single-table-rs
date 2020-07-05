use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use std::error::Error;

pub mod args;
pub mod ddb;
pub mod env;
pub mod mem;
pub mod sts;
pub mod traits;
pub mod types;

pub use ddb::DDB;
pub use sts::STS;
pub use traits::{Database, SecurityTokens};

#[derive(thiserror::Error, Debug)]
enum ProgramError {
    #[error("item not found: {0}")]
    GetNone(String),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Model {
    name: String,
    a_number: i32,

    created_at: DateTime<Utc>,

    // These are used as dynamodb key attributes
    pk: String,
    sk: String,
    model: String,
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
            model: "model".to_string(),

            name: name,
            created_at: Utc::now(),
            a_number,
        }
    }

    pub fn from_hashmap(hashmap: &types::HashMap) -> serde_dynamodb::error::Result<Self> {
        serde_dynamodb::from_hashmap(hashmap.to_owned())
    }

    pub fn to_hashmap(&self) -> serde_dynamodb::error::Result<types::HashMap> {
        serde_dynamodb::to_hashmap(&self)
    }

    pub async fn get<DB, S>(db: &DB, name: S) -> Result<Self, Box<dyn Error>>
    where
        DB: Database,
        S: Into<String>,
    {
        let name = name.into();
        let pk = format!("model#{}", name);
        let sk = pk.clone();

        let res = db.get_item(pk, sk).await?;
        if let Some(hashmap) = res.item {
            return Ok(Self::from_hashmap(&hashmap)?);
        }

        Err(Box::new(ProgramError::GetNone(name)))
    }

    pub async fn save<DB>(&mut self, db: &DB) -> Result<(), Box<dyn Error>>
    where
        DB: Database,
    {
        let hashmap = self.to_hashmap()?;
        let _ = db.put_item(hashmap).await?;

        Ok(())
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn value(&self) -> i32 {
        self.a_number
    }

    pub fn pk(&self) -> String {
        self.pk.clone()
    }

    pub fn sk(&self) -> String {
        self.sk.clone()
    }

    pub fn model(&self) -> String {
        self.model.clone()
    }
}

impl traits::Key for Model {
    fn key(&self) -> (String, String) {
        (self.pk(), self.sk())
    }

    fn model_key(&self) -> (String, String) {
        (self.model(), self.sk())
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SubModel {
    name: String,
    parent: String,

    created_at: DateTime<Utc>,

    pk: String,
    sk: String,
    model: String,
}

impl SubModel {
    pub fn new<'a, S>(name: S, parent: Model) -> Self
    where
        S: Into<String>,
    {
        let name = name.into();
        Self {
            pk: format!("model#{}", parent.name()),
            sk: format!("model#{}#submodel#{}", parent.name(), &name),
            model: "submodel".to_string(),

            name: name,
            parent: parent.sk(),
            created_at: Utc::now(),
        }
    }

    pub fn from_hashmap(hashmap: &types::HashMap) -> serde_dynamodb::error::Result<Self> {
        serde_dynamodb::from_hashmap(hashmap.to_owned())
    }

    pub fn to_hashmap(&self) -> serde_dynamodb::error::Result<types::HashMap> {
        serde_dynamodb::to_hashmap(&self)
    }

    pub async fn get<DB, S>(db: &DB, parent: S, name: S) -> Result<Self, Box<dyn Error>>
    where
        DB: Database,
        S: Into<String>,
    {
        let parent = parent.into();
        let name = name.into();
        let pk = format!("model#{}", parent);
        let sk = format!("model#{}#submodel#{}", parent, name);

        let res = db.get_item(pk, sk).await?;
        if let Some(hashmap) = res.item {
            return Ok(Self::from_hashmap(&hashmap)?);
        }

        Err(Box::new(ProgramError::GetNone(name)))
    }

    pub async fn save<DB>(&mut self, db: &DB) -> Result<(), Box<dyn Error>>
    where
        DB: Database,
    {
        let hashmap = self.to_hashmap()?;
        let res = db
            .transact_write_items(vec![
                db.condition_check_exists(&self.parent, &self.parent, "model"),
                db.put(hashmap),
            ])
            .await?;

        println!("{:?}", res);
        Ok(())
    }

    pub fn name(&self) -> String {
        self.name.clone()
    }

    pub fn pk(&self) -> String {
        self.pk.clone()
    }

    pub fn sk(&self) -> String {
        self.sk.clone()
    }

    pub fn model(&self) -> String {
        self.model.clone()
    }
}

impl traits::Key for SubModel {
    fn key(&self) -> (String, String) {
        (self.pk(), self.sk())
    }

    fn model_key(&self) -> (String, String) {
        (self.model(), self.sk())
    }
}
