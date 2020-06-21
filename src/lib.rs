use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

pub mod ddb;
pub mod types;
pub mod traits;

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

    pub fn name(&self) -> String {
        self.name.clone()
    }
}
