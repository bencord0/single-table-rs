use rstest::rstest;
use rstest_reuse::*;

use single_table::*;
use traits::{Database, TransactionalOperations};

use super::*;

struct State<DB>
where
    DB: Database + Send + Sync,
{
    db: TemporaryDatabase<DB>,
}

#[cfg(feature = "external_database")]
impl State<ddb::DDB> {
    fn new() -> Self {
        Self { db: dynamodb() }
    }
}

impl State<mem::MemoryDB> {
    fn new() -> Self {
        Self { db: memorydb() }
    }
}

#[template]
#[rstest(state,
    #[cfg(feature = "external_database")]
    case::ddb(State::<ddb::DDB>::new()),
    case::mem(State::<mem::MemoryDB>::new()),
)]
fn state<DB>(state: State<DB>)
where
    DB: Database + Send + Sync,
{
}

#[apply(state)]
fn test_get_none<DB>(state: State<DB>) -> TestResult
where
    DB: Database + Send + Sync,
{
    let get_item_output = smol::run(state.db.get_item("model#foo", "model#foo"))?;

    let item = get_item_output.item;
    assert_eq!(item, None);

    Ok(())
}

#[apply(state)]
fn test_put_get_some<DB>(state: State<DB>) -> TestResult
where
    DB: Database + Send + Sync,
{
    let model = Model::new("foo", 1);

    let hashmap: types::HashMap =
        serde_dynamodb::to_hashmap(&model).unwrap_or_else(|_| types::HashMap::new());

    let put_item_output = smol::run(state.db.put_item(hashmap))?;
    println!("{:?}", put_item_output);

    let get_item_output = smol::run(state.db.get_item("model#foo", "model#foo"))?;
    let item = get_item_output
        .item
        .unwrap_or_else(|| types::HashMap::new());

    let model = Model::from_hashmap(&item)?;
    println!("{:?}", model);
    assert_eq!(model.name(), "foo");
    assert_eq!(model.value(), 1);

    Ok(())
}

#[apply(state)]
fn test_query_submodels<DB>(state: State<DB>) -> TestResult
where
    DB: Database + Send + Sync,
{
    insert_models(&state.db)?;

    let items: rusoto_dynamodb::QueryOutput =
        smol::run(state.db.query(None, "model#foo", "model#foo#submodel#"))?;
    assert_eq!(items.count, Some(2));

    let mut submodels: Vec<SubModel> = vec![];
    for item in items.items.ok_or(".items is Some")? {
        let sm = SubModel::from_hashmap(&item)?;
        submodels.push(sm);
    }

    println!("{:#?}", submodels);
    assert_eq!(submodels.len(), 2);
    assert_eq!(submodels[0].name(), "bar");
    assert_eq!(submodels[1].name(), "baz");

    Ok(())
}

#[apply(state)]
fn test_query_index_model<DB>(state: State<DB>) -> TestResult
where
    DB: Database + Send + Sync,
{
    insert_models(&state.db)?;

    let items: rusoto_dynamodb::QueryOutput =
        smol::run(state.db.query(Some("model"), "model", "model#foo"))?;
    assert_eq!(items.count, Some(1));

    let mut models: Vec<Model> = vec![];
    for item in items.items.ok_or(".items is Some")? {
        let sm = Model::from_hashmap(&item)?;
        models.push(sm);
    }

    println!("{:#?}", models);
    assert_eq!(models.len(), 1);
    assert_eq!(models[0].name(), "foo");

    Ok(())
}

#[apply(state)]
fn test_query_index_submodel<DB>(state: State<DB>) -> TestResult
where
    DB: Database + Send + Sync,
{
    insert_models(&state.db)?;

    let items: rusoto_dynamodb::QueryOutput = smol::run(state.db.query(
        Some("model"),
        "submodel",
        "model#foo#submodel#bar",
    ))?;
    assert_eq!(items.count, Some(1));

    let mut submodels: Vec<SubModel> = vec![];
    for item in items.items.ok_or(".items is Some")? {
        let sm = SubModel::from_hashmap(&item)?;
        submodels.push(sm);
    }

    println!("{:#?}", submodels);
    assert_eq!(submodels.len(), 1);
    assert_eq!(submodels[0].name(), "bar");

    Ok(())
}

#[apply(state)]
fn test_scan<DB>(state: State<DB>) -> TestResult
where
    DB: Database + Send + Sync,
{
    insert_models(&state.db)?;

    let items: rusoto_dynamodb::ScanOutput = smol::run(state.db.scan(None::<String>, None))?;
    assert_eq!(items.count, Some(3));
    assert_eq!(items.scanned_count, Some(3));

    println!("{:#?}", items.items);
    Ok(())
}

#[apply(state)]
fn test_scan_index<DB>(state: State<DB>) -> TestResult
where
    DB: Database + Send + Sync,
{
    insert_models(&state.db)?;

    let items: rusoto_dynamodb::ScanOutput = smol::run(state.db.scan(Some("model"), None))?;
    assert_eq!(items.count, Some(3));
    assert_eq!(items.scanned_count, Some(3));

    println!("{:#?}", items.items);
    Ok(())
}

#[apply(state)]
fn test_scan_limit<DB>(state: State<DB>) -> TestResult
where
    DB: Database + Send + Sync,
{
    insert_models(&state.db)?;

    let items: rusoto_dynamodb::ScanOutput = smol::run(state.db.scan(None::<String>, Some(1)))?;
    assert_eq!(items.count, Some(1));
    assert_eq!(items.scanned_count, Some(1));

    println!("{:#?}", items.items);
    Ok(())
}

#[apply(state)]
fn test_transact_write_items<DB>(state: State<DB>) -> TestResult
where
    DB: Database + Send + Sync,
{
    let table_name = smol::run(state.db.describe_table())?;
    assert!(table_name.table.is_some());

    let mut foo: Model = Model::new("foo", 1);
    let bar: SubModel = SubModel::new("bar", foo.clone());

    let _ = smol::run(foo.save(&state.db))?;
    let _ = smol::run(state.db.transact_write_items(vec![
        state.db.condition_check_exists(foo.pk(), foo.sk(), foo.model()),
        state.db.put(bar.to_hashmap()?),
    ]))?;

    let res = smol::run(SubModel::get(&state.db, "foo", "bar"))?;
    println!("{:?}", res);
    assert_eq!(res.name(), "bar");

    Ok(())
}
