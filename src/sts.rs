use async_trait::async_trait;

#[rustfmt::skip]
use rusoto_sts::{
    Sts,
    StsClient,
};

use crate::{
    types::*,
    traits::SecurityTokens,
};

pub struct STS(StsClient);

impl STS {
    pub fn new(client: StsClient) -> Self {
        Self(client)
    }
}

#[async_trait]
impl SecurityTokens for STS {
    async fn get_caller_identity(&self) -> GetCallerIdentityResult {
        self.0.get_caller_identity(GetCallerIdentityRequest{
            ..Default::default()
        }).await
    }
}
