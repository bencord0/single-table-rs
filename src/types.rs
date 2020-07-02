pub use rusoto_core::RusotoError;
use std::collections;

#[rustfmt::skip]
pub use rusoto_dynamodb::{
    AttributeValue,

    CreateTableError, CreateTableInput, CreateTableOutput,
    DeleteTableError, DeleteTableInput, DeleteTableOutput,
    DescribeTableError, DescribeTableInput, DescribeTableOutput,
    ScanError, ScanInput, ScanOutput,
    GetItemError, GetItemInput, GetItemOutput,
    PutItemError, PutItemInput, PutItemOutput,
    QueryError, QueryInput, QueryOutput,
};

#[rustfmt::skip]
pub use rusoto_sts::{
    GetCallerIdentityError, GetCallerIdentityRequest, GetCallerIdentityResponse,
};

// Dynamodb
pub type HashMap = collections::HashMap<String, AttributeValue>;
pub type CreateTableResult = Result<CreateTableOutput, RusotoError<CreateTableError>>;
pub type DeleteTableResult = Result<DeleteTableOutput, RusotoError<DeleteTableError>>;
pub type DescribeTableResult = Result<DescribeTableOutput, RusotoError<DescribeTableError>>;
pub type ScanResult = Result<ScanOutput, RusotoError<ScanError>>;
pub type GetItemResult = Result<GetItemOutput, RusotoError<GetItemError>>;
pub type PutItemResult = Result<PutItemOutput, RusotoError<PutItemError>>;
pub type QueryResult = Result<QueryOutput, RusotoError<QueryError>>;

// STS
pub type GetCallerIdentityResult = Result<GetCallerIdentityResponse, RusotoError<GetCallerIdentityError>>;
