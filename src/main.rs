use clap::Clap;
use rusoto_dynamodb::DynamoDbClient;
use rusoto_sts::StsClient;
use single_table::{args::*, env, Database, Model, SecurityTokens, SubModel};
use std::error::Error;

fn main() -> Result<(), Box<dyn Error>> {
    let opts = Opts::parse();
    println!("{:?}", opts);

    let region = env::resolve_region(opts.aws_region.clone(), opts.aws_endpoint_url.clone())?;
    println!("{:?}", region);

    let db = single_table::DDB::new(DynamoDbClient::new(region.clone()), &opts.table_name);
    let sts = single_table::STS::new(StsClient::new(region.clone()));

    smol::run(async {
        match opts.commands {
            Commands::Create => create(db).await?,
            Commands::Describe => describe(db).await?,

            Commands::GetModel(opts) => get_model(db, opts).await?,
            Commands::GetSubModel(opts) => get_submodel(db, opts).await?,

            Commands::Query(opts) => query(db, opts).await?,
            Commands::Scan(opts) => scan(db, opts).await?,

            Commands::PutModel(opts) => put_model(db, opts).await?,
            Commands::PutSubModel(opts) => put_submodel(db, opts).await?,

            Commands::WhoAmI => whoami(sts).await?,
        }
        Ok(())
    })
}

async fn create<DB: Database>(db: DB) -> Result<(), Box<dyn Error>> {
    println!("table name: {}", db.table_name());
    let res = db.create_table().await;

    println!("res: {:?}", res);
    Ok(())
}

async fn describe<DB: Database>(db: DB) -> Result<(), Box<dyn Error>> {
    let res = db.describe_table().await;

    println!("{}: {:#?}", db.table_name(), res);
    Ok(())
}

async fn get_model<DB: Database>(db: DB, opts: GetModelOpts) -> Result<(), Box<dyn Error>> {
    let res = Model::get(&db, opts.name).await?;
    println!("{:#?}", res);

    Ok(())
}

async fn get_submodel<DB: Database>(db: DB, opts: GetSubModelOpts) -> Result<(), Box<dyn Error>> {
    let res = SubModel::get(&db, opts.parent, opts.name).await?;
    println!("{:#?}", res);

    Ok(())
}

async fn query<DB: Database>(db: DB, opts: QueryOpts) -> Result<(), Box<dyn Error>> {
    let (pk, sk) = match &opts.index {
        Some(index) if index == "model" => match opts.sk {
            Some(sk) => (
                "submodel".to_string(),
                format!("model#{}#submodel#{}", opts.pk, sk),
            ),
            None => ("model".to_string(), format!("model#{}", opts.pk)),
        },
        None | Some(_) => {
            let pk = format!("model#{}", opts.pk);
            let sk = match opts.sk {
                Some(sk) => format!("model#{}#submodel#{}", opts.pk, sk),
                None => format!("model#{}", opts.pk),
            };
            (pk, sk)
        }
    };

    let res = db.query(opts.index, pk, sk).await?;
    if let Some(hashmaps) = res.items {
        for hashmap in hashmaps {
            let _ = Model::from_hashmap(&hashmap).map(|item| println!("{:#?}", item));

            let _ = SubModel::from_hashmap(&hashmap).map(|item| println!("{:#?}", item));
        }
    }

    Ok(())
}

async fn scan<DB: Database>(db: DB, opts: ScanOpts) -> Result<(), Box<dyn Error>> {
    let index = opts.index.clone();
    let res = db.scan(opts.index, opts.limit).await?;

    println!("TableName: {}", db.table_name());
    if let Some(index) = index {
        println!("IndexName: {}", index);
    }

    if let Some(hashmaps) = res.items {
        for hashmap in hashmaps {
            let _ = Model::from_hashmap(&hashmap).map(|item| println!("{:#?}", item));

            let _ = SubModel::from_hashmap(&hashmap).map(|item| println!("{:#?}", item));
        }
    }

    Ok(())
}

async fn put_model<DB: Database>(db: DB, opts: PutModelOpts) -> Result<(), Box<dyn Error>> {
    let mut model = Model::new(opts.name, opts.a_version);
    let res = model.save(&db).await?;
    println!("{:#?}", res);

    Ok(())
}

async fn put_submodel<DB: Database>(db: DB, opts: PutSubModelOpts) -> Result<(), Box<dyn Error>> {
    let parent = Model::get(&db, &opts.parent).await?;
    let mut submodel = SubModel::new(opts.name, parent);

    let res = submodel.save(&db).await?;
    println!("{:#?}", res);

    Ok(())
}

async fn whoami<STS: SecurityTokens>(sts: STS) -> Result<(), Box<dyn Error>> {
    let caller_id = sts.get_caller_identity().await?;
    println!("{:?}", caller_id);
    Ok(())
}
