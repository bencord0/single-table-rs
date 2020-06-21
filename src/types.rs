use std::collections;
pub use rusoto_core::RusotoError;

#[rustfmt::skip]
pub use rusoto_dynamodb::{
    AttributeValue,

    CreateTableError, CreateTableInput, CreateTableOutput,
    DeleteTableError, DeleteTableInput, DeleteTableOutput,
    GetItemError, GetItemInput, GetItemOutput,
    PutItemError, PutItemInput, PutItemOutput,
    QueryError, QueryInput, QueryOutput,
};

pub type HashMap = collections::HashMap<String, AttributeValue>;
pub type CreateTableResult = Result<CreateTableOutput, RusotoError<CreateTableError>>;
pub type DeleteTableResult = Result<DeleteTableOutput, RusotoError<DeleteTableError>>;
pub type GetItemResult = Result<GetItemOutput, RusotoError<GetItemError>>;
pub type PutItemResult = Result<PutItemOutput, RusotoError<PutItemError>>;
pub type QueryResult = Result<QueryOutput, RusotoError<QueryError>>;
