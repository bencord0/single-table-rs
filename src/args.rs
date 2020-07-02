use clap::Clap;
use std::{
    error::Error,
    str::FromStr,
};

#[derive(Clap, Debug)]
#[clap(version, author)]
pub struct Opts {
    #[clap(long, env = "AWS_ENDPOINT_URL")]
    pub aws_endpoint_url: Option<String>,

    #[clap(long, env = "AWS_REGION")]
    pub aws_region: Option<String>,

    /// The DynamoDB Table Name (you only need one)
    #[clap(long, default_value = "single-table")]
    pub table_name: String,

    #[clap(subcommand)]
    pub commands: Commands,
}

#[derive(Clap, Debug)]
pub enum Commands {
    /// Create the DynamoDB Table using the predefined schema.
    Create,
    /// Discribe the DynamoDB Table schema.
    Describe,
    /// Scan for all items in the DynamoDB Table (or an index).
    Scan(ScanOpts),

    /// Put Items into the DynamdoDB Table.
    Put(PutOpts),
    /// Get an Item by `pk` and optional `sk`.
    Get(GetOpts),
    /// Query for Items by `pk` and optional `sk`.
    Query(QueryOpts),

    /// Return details about the current IAM user credentials.
    /// This is a demonstration of other rusoto APIs.
    #[clap(name = "whoami")]
    WhoAmI,
}

#[derive(Clap, Debug)]
pub struct ScanOpts {
    #[clap(long)]
    pub index: Option<String>,

    #[clap(long)]
    pub limit: Option<i64>,
}

#[derive(Clap, Debug)]
pub struct GetOpts {
    pub pk: String,
    pub sk: Option<String>,
}

#[derive(Clap, Debug)]
pub struct PutOpts {
    #[clap(long)]
    pub models: Vec<ModelOpts>,

    #[clap(long)]
    pub submodels: Vec<SubmodelOpts>,
}

#[derive(Clap, Debug)]
pub struct QueryOpts {
    pub pk: String,
    pub sk: Option<String>,
}

#[derive(Clap, Debug)]
pub struct ModelOpts {
    pub name: String,
    pub a_version: i32,
}

impl FromStr for ModelOpts {
    type Err = Box<dyn Error>;
    fn from_str(s: &str) -> Result<ModelOpts, Self::Err> {
        let pos = s
            .find(':')
            .ok_or_else(|| format!("invalid input, expected 'String:i32'; no ':' found in '{}'", s))?;

        Ok(ModelOpts {
            name: s[..pos].parse()?,
            a_version: s[pos + 1..].parse()?,
        })
    }
}

#[derive(Clap, Debug)]
pub struct SubmodelOpts {
    pub name: String,
    pub parent: String,
}

impl FromStr for SubmodelOpts {
    type Err = Box<dyn Error>;
    fn from_str(s: &str) -> Result<SubmodelOpts, Self::Err> {
        let pos = s
            .find(':')
            .ok_or_else(|| format!("invalid input, expected 'String:String; no ':' found in '{}'", s))?;

        Ok(SubmodelOpts {
            name: s[..pos].parse()?,
            parent: s[pos + 1..].parse()?,
        })
    }
}
