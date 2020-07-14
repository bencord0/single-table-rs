use rstest::rstest;
use rstest_reuse::*;

use single_table::*;
use traits::Database;

use super::*;

#[template]
#[rstest(db,
    #[cfg(feature = "external_database")]
    case::ddb(dynamodb()),
    case::mem(memorydb()),
)]
fn database(db: impl Database) {}

#[apply(database)]
fn test_get_none(db: impl Database) -> TestResult {
    let get_item_output = smol::run(db.get_item("model#foo", "model#foo"))?;

    let item = get_item_output.item;
    assert_eq!(item, None);

    Ok(())
}

#[apply(database)]
fn test_put_get_some(db: impl Database) -> TestResult {
    let model = Model::new("foo", 1);

    let hashmap: types::HashMap =
        serde_dynamodb::to_hashmap(&model).unwrap_or_else(|_| types::HashMap::new());

    let put_item_output = smol::run(db.put_item(hashmap))?;
    println!("{:?}", put_item_output);

    let get_item_output = smol::run(db.get_item("model#foo", "model#foo"))?;
    let item = get_item_output
        .item
        .unwrap_or_else(|| types::HashMap::new());

    let model = Model::from_hashmap(&item)?;
    println!("{:?}", model);
    assert_eq!(model.name(), "foo");
    assert_eq!(model.value(), 1);

    Ok(())
}

#[apply(database)]
fn test_query_submodels(db: impl Database) -> TestResult {
    insert_models(&db)?;

    let items: rusoto_dynamodb::QueryOutput =
        smol::run(db.query(None, "model#foo", "model#foo#submodel#"))?;
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

#[apply(database)]
fn test_query_index_model(db: impl Database) -> TestResult {
    insert_models(&db)?;

    let items: rusoto_dynamodb::QueryOutput =
        smol::run(db.query(Some("model"), "model", "model#foo"))?;
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

#[apply(database)]
fn test_query_index_submodel(db: impl Database) -> TestResult {
    insert_models(&db)?;

    let items: rusoto_dynamodb::QueryOutput =
        smol::run(db.query(Some("model"), "submodel", "model#foo#submodel#bar"))?;
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

#[apply(database)]
fn test_scan(db: impl Database) -> TestResult {
    insert_models(&db)?;

    let items: rusoto_dynamodb::ScanOutput = smol::run(db.scan(None::<String>, None))?;
    assert_eq!(items.count, Some(3));
    assert_eq!(items.scanned_count, Some(3));

    println!("{:#?}", items.items);
    Ok(())
}

#[apply(database)]
fn test_scan_index(db: impl Database) -> TestResult {
    insert_models(&db)?;

    let items: rusoto_dynamodb::ScanOutput = smol::run(db.scan(Some("model"), None))?;
    assert_eq!(items.count, Some(3));
    assert_eq!(items.scanned_count, Some(3));

    println!("{:#?}", items.items);
    Ok(())
}

#[apply(database)]
fn test_scan_limit(db: impl Database) -> TestResult {
    insert_models(&db)?;

    let items: rusoto_dynamodb::ScanOutput = smol::run(db.scan(None::<String>, Some(1)))?;
    assert_eq!(items.count, Some(1));
    assert_eq!(items.scanned_count, Some(1));

    println!("{:#?}", items.items);
    Ok(())
}

#[apply(database)]
fn test_transact_write_items(db: impl Database) -> TestResult {
    let table_name = smol::run(db.describe_table())?;
    assert!(table_name.table.is_some());

    let mut foo: Model = Model::new("foo", 1);
    let bar: SubModel = SubModel::new("bar", foo.clone());

    let _ = smol::run(foo.save(&db))?;
    let _ = smol::run(db.transact_write_items(vec![
        db.condition_check_exists(foo.pk(), foo.sk(), foo.model()),
        db.put(bar.to_hashmap()?),
    ]))?;

    let res = smol::run(SubModel::get(&db, "foo", "bar"))?;
    println!("{:?}", res);
    assert_eq!(res.name(), "bar");

    Ok(())
}
