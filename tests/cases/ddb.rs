use single_table::*;
use traits::Database;

#[test]
fn test_get_none() -> Result<(), Box<dyn std::error::Error>> {
    let ddb = ddb::dynamodb();
    ddb.sync_create_table();

    let get_item_output =
        smol::run(ddb.get_item("model#foo", Some("model#foo")))?;

    let item = get_item_output.item;
    assert_eq!(item, None);

    ddb.sync_delete_table();
    Ok(())
}

#[test]
fn test_put_get_some() -> Result<(), Box<dyn std::error::Error>> {
    let ddb = ddb::dynamodb();
    ddb.sync_create_table();

    let model = Model::new("foo", 1);

    let hashmap: types::HashMap =
        serde_dynamodb::to_hashmap(&model).unwrap_or_else(|_| types::HashMap::new());

    let put_item_output = smol::run(ddb.put_item(hashmap))?;
    println!("{:?}", put_item_output);

    let get_item_output =
        smol::run(ddb.get_item("model#foo", Some("model#foo")))?;
    let item = get_item_output.item.unwrap_or_else(|| types::HashMap::new());

    let model: Model = serde_dynamodb::from_hashmap(item)?;
    println!("{:?}", model);
    assert_eq!(model.name(), "foo");
    assert_eq!(model.value(), 1);

    ddb.sync_delete_table();
    Ok(())
}

#[test]
fn test_get_submodels() -> Result<(), Box<dyn std::error::Error>> {
    let ddb = ddb::dynamodb();
    ddb.sync_create_table();

    let foo: Model = Model::new("foo", 1);
    let bar: SubModel = SubModel::new("bar", foo.clone());
    let baz: SubModel = SubModel::new("baz", foo.clone());

    let items: Vec<types::HashMap> = vec![
        serde_dynamodb::to_hashmap(&foo)?,
        serde_dynamodb::to_hashmap(&bar)?,
        serde_dynamodb::to_hashmap(&baz)?,
    ];

    smol::run(futures::future::join_all(
        items.iter().map(|item| ddb.put_item(item.clone())),
    ));

    let items: rusoto_dynamodb::QueryOutput =
        smol::run(ddb.query("model#foo", "model#foo#submodel#"))?;
    assert_eq!(items.count, Some(2));

    let mut submodels: Vec<SubModel> = vec![];
    for item in items.items.ok_or(".items is Some")? {
        let sm: SubModel = serde_dynamodb::from_hashmap(item)?;
        submodels.push(sm);
    }

    println!("{:#?}", submodels);
    assert_eq!(submodels.len(), 2);
    assert_eq!(submodels[0].name(), "bar");
    assert_eq!(submodels[1].name(), "baz");

    ddb.sync_delete_table();
    Ok(())
}
