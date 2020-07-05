# Single table patterns in Rust

This is an experiment to help me figure out how to implement
single-table patterns in Rust with rusoto.

Getting your data model right with dynamodb can be tricky.
If you get it right, you can support multiple access patterns,
while minimising the number of requests to the datastore.

For a written overview of the basic ideas, see [this article][1].

[1]: https://www.alexdebrie.com/posts/dynamodb-single-table/

## What's in this repository

`single-table` is a rust program that serves as a demonstration of the single-table pattern.

This is not a library that you can use directly in another application,
 you will have to design application specific methods and traits for
 your own models that map to dynamodb requests.

If you need a tool for analytics, or require access patterns
that cannot be planned a priori, then consider other datastores
such as elasticsearch or a relational database.

## Usage

```bash
$ cargo run -- --help
single-table 0.1.0

USAGE:
    single-table [OPTIONS] <SUBCOMMAND>

FLAGS:
    -h, --help       Prints help information
    -V, --version    Prints version information

OPTIONS:
        --aws-endpoint-url <aws-endpoint-url>     [env: AWS_ENDPOINT_URL=http://localhost:2000]
        --aws-region <aws-region>                 [env: AWS_REGION=]
        --table-name <table-name>                The DynamoDB Table Name (you only need one) [default: single-table]

SUBCOMMANDS:
    create          Create the DynamoDB Table using the predefined schema
    describe        Discribe the DynamoDB Table schema
    get-model       Get a Model by `name`
    get-submodel    Get a SubModel by `parent` Model and `name`
    help            Prints this message or the help of the given subcommand(s)
    put-model       Put a Model into the DynamdoDB Table
    put-submodel    Put a SubModel into the DynamdoDB Table
    query           Query for Items by `pk` and optional `sk`
    scan            Scan for all items in the DynamoDB Table (or an index)
    whoami          Return details about the current IAM user credentials. This is a demonstration of other rusoto APIs
```

### Start the database

If you have docker, start a copy of [`dynamodb-local`][2] with the provided script.

```bash
$ ./scripts/start-ddb.sh
```

This will launch the `amazon/dynamodb-local` docker container image, and expose a DynamoDB
compatible API service on port `2000`.

Depending on your own preference, you can also use [`localstack`][3], which exposes port
`4566` by default.

Either way, you can now set `AWS_ENDPOINT_URL` to an appropriate location. If running
within AWS, leave this value unset to use the SDK default endpoint for the region.
You can override the AWS region by setting `AWS_REGION`.

```bash
export AWS_ENDPOINT_URL=http://localhost:2000
```

[2]: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBLocal.html
[3]: https://localstack.io

### Create the DynamoDB Table

Use the `create` subcommand to create the DynamoDB Table.

```bash
$ cargo run -- create --help
single-table-create
Create the DynamoDB Table using the predefined schema

USAGE:
    single-table create

FLAGS:
    -h, --help       Prints help information
    -V, --version    Prints version information
```

```bash
$ cargo run -- create
```

Check the Table has been created successfully with the `describe` command.

```bash
$ cargo run -- describe --help
single-table-describe
Discribe the DynamoDB Table schema

USAGE:
    single-table describe

FLAGS:
    -h, --help       Prints help information
    -V, --version    Prints version information
```

```bash
$ cargo run -- describe
```

### Put Items into the Table

```bash
single-table-put-model
Put a Model into the DynamdoDB Table

USAGE:
    single-table put-model <name> <a-version>

ARGS:
    <name>
    <a-version>

FLAGS:
    -h, --help       Prints help information
    -V, --version    Prints version information
```

```bash
$ cargo run -- put-model foo 1
$ cargo run -- put-model bar 2
$ cargo run -- put-model baz 3
```

You can put "Model"s using this command. The specification string for each model will be specific to
your application, perhaps it comes from a HTML form or REST API. Types are enforced by the Rust type
system (`String` and `i64` in this example), and the DynamoDB AttributeValues (JSON objects keyed by type).

```bash
single-table-put-submodel
Put a SubModel into the DynamdoDB Table

USAGE:
    single-table put-submodel <name> <parent>

ARGS:
    <name>
    <parent>

FLAGS:
    -h, --help       Prints help information
    -V, --version    Prints version information
```

```bash
$ cargo run -- put-submodel abc foo
```

You can put "SubModel"s using this command. This is a demonstration of using Transactional Writes
in DynamoDB. The `parent` argument must refer to the `name` of an existing `Model` which is enforced
by the transaction write.

### Get Items

Use the `get-model` and `get-submodel` commands to retrieve specific Items.

#### get-model by PK

In this example, only `Model`s can be retrieved if you only specify the `pk`.

```bash
single-table-get-model
Get a Model by `name`

USAGE:
    single-table get-model <name>

ARGS:
    <name>

FLAGS:
    -h, --help       Prints help information
    -V, --version    Prints version information
```

```bash
$ cargo run -- get foo
Model {
    name: "foo",
    a_number: 1,
    ... and other fields
}
```

#### get-submodel by PK and SK

```bash
single-table-get-submodel
Get a SubModel by `parent` Model and `name`

USAGE:
    single-table get-submodel <parent> <name>

ARGS:
    <parent>
    <name>

FLAGS:
    -h, --help       Prints help information
    -V, --version    Prints version information
```

In this example, `SubModel`s can be retrieved if you specify both the `pk` and `sk`.

```bash
$ cargo run -- get foo bar
SubModel {
    name: "bar",
    parent: "model#foo",
    ... and other fields
}
```

### Query Items

DynamoDB's `Query` API is used to retrieve multiple items (with the same `pk`). If you can organize items
to be stored within the same partition, this can be an efficient access pattern to avoid N+1 requests, or
worse, full table `Scan`s.

In this example, both `Model`s and `SubModel`s can be simultaneously retrieved for a specific `pk`.

```bash
$ cargo run -- query foo
Model {
    name: "foo",
    a_number: 1,
    ... and other fields
}
SubModel {
    name: "bar",
    parent: "model#foo",
    ... and other fields
}
```

Querying by `GSI` is also available.

```bash
$ cargo run -- query --index=model foo bar
SubModel {
    name: "bar",
    parent: "model#foo",
    ... and other fields
}
```

### Scan the whole table

```bash
$ cargo run -- scan --help
single-table-scan
Scan for all items in the DynamoDB Table (or an index)

USAGE:
    single-table scan [OPTIONS]

FLAGS:
    -h, --help       Prints help information
    -V, --version    Prints version information

OPTIONS:
        --index <index>
        --limit <limit>
```
