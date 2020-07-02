use clap::Clap;
use rusoto_dynamodb::DynamoDbClient;
use rusoto_sts::StsClient;
use single_table::{
    args::{Commands, GetOpts, Opts, PutOpts, QueryOpts, ScanOpts},
    ddb::DDB,
    env,
    sts::STS,
    traits::{
        Database,
        SecurityTokens,
    },
    Model, SubModel,
};
use std::{
    error::Error,
};

fn main() -> Result<(), Box<dyn Error>> {
    let opts = Opts::parse();
    println!("{:?}", opts);

    let region = env::resolve_region(opts.aws_region.clone(), opts.aws_endpoint_url.clone())?;
    println!("{:?}", region);

    let db = DDB::new(
        DynamoDbClient::new(region.clone()),
        &opts.table_name,
    );
    let sts = STS::new(StsClient::new(region.clone()));

    smol::run(async {
        match opts.commands {
            Commands::Create => create(db).await?,
            Commands::Describe => describe(db).await?,
            Commands::Scan(opts) => scan(db, opts).await?,

            Commands::Put(opts) => put(db, opts).await?,
            Commands::Get(opts) => get(db, opts).await?,
            Commands::Query(opts) => query(db, opts).await?,

            Commands::WhoAmI => whoami(sts).await?,
        }
        Ok(())
    })
}

async fn create(db: DDB) -> Result<(), Box<dyn Error>> {
    println!("table name: {}", db.table_name());
    let res = db.create_table().await;

    println!("res: {:?}", res);
    Ok(())
}

async fn describe(db: DDB) -> Result<(), Box<dyn Error>> {
    let res = db.describe_table().await;

    println!("{}: {:#?}", db.table_name(), res);
    Ok(())
}

async fn scan(db: DDB, opts: ScanOpts) -> Result<(), Box<dyn Error>> {
    let index = opts.index.clone();
    let res = db.scan(opts.index, opts.limit).await?;

    println!("TableName: {}", db.table_name());
    if let Some(index) = index {
        println!("IndexName: {}", index);
    }

    if let Some(hashmaps) = res.items {
        for hashmap in hashmaps {
            let _ = Model::from_hashmap(&hashmap)
                .map(|item| println!("{:#?}", item));

            let _ = SubModel::from_hashmap(&hashmap)
                .map(|item| println!("{:#?}", item));
        }
    }

    Ok(())
}

async fn put(db: DDB, opts: PutOpts) -> Result<(), Box<dyn Error>> {
    let mut hashmaps = vec![];

    for opt in opts.models {
        let model = Model::new(opt.name, opt.a_version);
        let hashmap = serde_dynamodb::to_hashmap(&model)?;
        hashmaps.push(hashmap);
    }

    for opt in opts.submodels {
        let submodel = SubModel::new(opt.name, Model::new(opt.parent, 0));
        let hashmap = serde_dynamodb::to_hashmap(&submodel)?;
        hashmaps.push(hashmap);
    }

    for hashmap in hashmaps.iter().cloned() {
        let res = db.put_item(hashmap).await?;
        println!("{:#?}", res);
    }
    Ok(())
}

async fn get(db: DDB, opts: GetOpts) -> Result<(), Box<dyn Error>> {
    let pk = format!("model#{}", opts.pk);
    let sk = match opts.sk {
        Some(sk) => format!("model#{}#submodel#{}", opts.pk, sk),
        None => format!("model#{}", opts.pk),
    };

    let res = db.get_item(pk, Some(sk)).await?;
    if let Some(hashmap) = res.item {
        let _ = Model::from_hashmap(&hashmap)
            .map(|item| println!("{:#?}", item));

        let _ = SubModel::from_hashmap(&hashmap)
            .map(|item| println!("{:#?}", item));
    } else {
        println!("{:?}", res);
    }

    Ok(())
}

async fn query(db: DDB, opts: QueryOpts) -> Result<(), Box<dyn Error>> {
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

async fn whoami(sts: STS) -> Result<(), Box<dyn Error>> {
    let caller_id = sts.get_caller_identity().await?;
    println!("{:?}", caller_id);
    Ok(())
}
