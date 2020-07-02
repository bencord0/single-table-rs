use clap::Clap;
use rusoto_dynamodb::DynamoDbClient;
use rusoto_sts::StsClient;
use single_table::{
    args::{
        Commands, Opts, PutOpts,
    },
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
            Commands::WhoAmI => whoami(sts).await?,
            Commands::Put(opts) => put(db, opts).await?,
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

async fn whoami(sts: STS) -> Result<(), Box<dyn Error>> {
    let caller_id = sts.get_caller_identity().await?;
    println!("{:?}", caller_id);
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
