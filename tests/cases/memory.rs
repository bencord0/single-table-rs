use single_table::*;
use traits::Database;

#[test]
fn test_get_none() {
    let ddb = mem::memorydb();
    ddb.sync_create_table();

    if let Ok(get_item_output) =
        smol::run(async { ddb.get_item("model#foo", Some("model#foo")).await })
    {
        let item = get_item_output.item;
        assert_eq!(item, None);
    } else {
        assert!(false);
    }

    ddb.sync_delete_table();
}

#[test]
fn test_put_get_some() {
    let ddb = mem::memorydb();
    ddb.sync_create_table();

    let model = Model::new("foo", 1);

    let hashmap: types::HashMap = match serde_dynamodb::to_hashmap(&model) {
        Ok(hashmap) => hashmap,
        Err(_) => types::HashMap::new(),
    };

    if let Ok(put_item_output) = smol::run(async { ddb.put_item(hashmap).await }) {
        println!("{:?}", put_item_output);
    }

    if let Ok(get_item_output) =
        smol::run(async { ddb.get_item("model#foo", Some("model#foo")).await })
    {
        let item = match get_item_output.item {
            Some(item) => item,
            None => types::HashMap::new(),
        };

        let model: serde_dynamodb::error::Result<Model> = serde_dynamodb::from_hashmap(item);
        assert!(model.is_ok());

        if let Ok(model) = model {
            println!("{:?}", model);
        }
    } else {
        assert!(false);
    }

    ddb.sync_delete_table();
}

#[test]
fn test_get_submodels() {
    let ddb = mem::memorydb();
    ddb.sync_create_table();

    let foo: Model = Model::new("foo", 1);
    let bar: SubModel = SubModel::new("bar", foo.clone());
    let baz: SubModel = SubModel::new("baz", foo.clone());

    smol::run(async {
        let _ = futures::join!(
            {
                match serde_dynamodb::to_hashmap(&foo) {
                    Ok(hashmap) => ddb.put_item(hashmap),
                    Err(_) => return,
                }
            },
            {
                match serde_dynamodb::to_hashmap(&bar) {
                    Ok(hashmap) => ddb.put_item(hashmap),
                    Err(_) => return,
                }
            },
            {
                match serde_dynamodb::to_hashmap(&baz) {
                    Ok(hashmap) => ddb.put_item(hashmap),
                    Err(_) => return,
                }
            },
        );
    });

    let items: rusoto_dynamodb::QueryOutput =
        match smol::run(async { ddb.query("model#foo", "model#foo#submodel#").await }) {
            Ok(items) => items,
            Err(_) => return,
        };
    assert_eq!(items.count, Some(2));

    let mut submodels: Vec<SubModel> = vec![];
    if let Some(items) = items.items {
        for item in items {
            let sm: SubModel = match serde_dynamodb::from_hashmap(item) {
                Ok(sm) => sm,
                Err(_) => return,
            };
            submodels.push(sm);
        }
    } else {
        assert!(false);
    }

    println!("{:#?}", submodels);
    assert_eq!(submodels.len(), 2);
    assert_eq!(submodels[0].name(), "bar");
    assert_eq!(submodels[1].name(), "baz");

    ddb.sync_delete_table();
}
