use clap::Clap;

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

    /// Put a Model into the DynamdoDB Table.
    PutModel(PutModelOpts),
    /// Put a SubModel into the DynamdoDB Table.
    #[clap(name = "put-submodel")]
    PutSubModel(PutSubModelOpts),

    /// Get a Model by `name`.
    GetModel(GetModelOpts),
    /// Get a SubModel by `parent` Model and `name`.
    #[clap(name = "get-submodel")]
    GetSubModel(GetSubModelOpts),
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
pub struct GetModelOpts {
    pub name: String,
}

#[derive(Clap, Debug)]
pub struct GetSubModelOpts {
    pub parent: String,
    pub name: String,
}

#[derive(Clap, Debug)]
pub struct QueryOpts {
    pub pk: String,
    pub sk: Option<String>,

    #[clap(long)]
    pub index: Option<String>,
}

#[derive(Clap, Debug)]
pub struct PutModelOpts {
    pub name: String,
    pub a_version: i32,
}

#[derive(Clap, Debug)]
pub struct PutSubModelOpts {
    pub name: String,
    pub parent: String,
}
