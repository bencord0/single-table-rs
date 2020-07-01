use clap::Clap;
use std::{
    error::Error,
    str::FromStr,
};

#[derive(Clap, Debug)]
#[clap(version, author)]
pub struct Opts {
    #[clap(long, env = "AWS_ENDPOINT_URL")]
    pub aws_endpoint_url: Option<String>,

    #[clap(long, env = "AWS_REGION")]
    pub aws_region: Option<String>,

    #[clap(long, default_value = "single-table")]
    pub table_name: String,

    #[clap(subcommand)]
    pub commands: Commands,
}

#[derive(Clap, Debug)]
pub enum Commands {
    Create,
    Describe,
    Put(PutOpts),

    #[clap(name = "whoami")]
    WhoAmI,
}

#[derive(Clap, Debug)]
pub struct PutOpts {
    #[clap(long)]
    pub models: Vec<ModelOpts>,

    #[clap(long)]
    pub submodels: Vec<SubmodelOpts>,
}

#[derive(Clap, Debug)]
pub struct ModelOpts {
    pub name: String,
    pub a_version: i32,
}

impl FromStr for ModelOpts {
    type Err = Box<dyn Error>;
    fn from_str(s: &str) -> Result<ModelOpts, Self::Err> {
        let pos = s
            .find(':')
            .ok_or_else(|| format!("invalid input, expected 'String:i32'; no ':' found in '{}'", s))?;

        Ok(ModelOpts {
            name: s[..pos].parse()?,
            a_version: s[pos + 1..].parse()?,
        })
    }
}

#[derive(Clap, Debug)]
pub struct SubmodelOpts {
    pub name: String,
    pub parent: String,
}

impl FromStr for SubmodelOpts {
    type Err = Box<dyn Error>;
    fn from_str(s: &str) -> Result<SubmodelOpts, Self::Err> {
        let pos = s
            .find(':')
            .ok_or_else(|| format!("invalid input, expected 'String:String; no ':' found in '{}'", s))?;

        Ok(SubmodelOpts {
            name: s[..pos].parse()?,
            parent: s[pos + 1..].parse()?,
        })
    }
}
