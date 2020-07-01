use rusoto_core::Region;

#[rustfmt::skip]
use rusoto_sts::{
        Sts,
        StsClient,
};

use crate::{
    env,
    types::*,
};

pub async fn get_caller_identity() -> GetCallerIdentityResult {
    env::set_default_var("AWS_ACCESS_KEY_ID" ,"local").unwrap();
    env::set_default_var("AWS_SECRET_ACCESS_KEY", "local").unwrap();

    let region = Region::Custom {
        name: "local".to_string(),
        endpoint: "http://localhost:2000".to_string(),
    };

    let sts = StsClient::new(region);

    sts.get_caller_identity(GetCallerIdentityRequest{
        ..Default::default()
    }).await
}
