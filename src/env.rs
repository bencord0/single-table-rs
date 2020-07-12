use rusoto_core::Region;
use std::{env, ffi::OsStr, str::FromStr};

pub fn set_default_var<K: AsRef<OsStr>, V: AsRef<OsStr>>(
    key: K,
    val: V,
) -> Result<String, env::VarError> {
    let key = key.as_ref();
    match env::var(key.clone()) {
        Ok(s) => Ok(s),
        Err(env::VarError::NotPresent) => {
            env::set_var(key, &val);

            let v = val.as_ref();
            match v.to_str() {
                Some(s) => Ok(s.to_string()),
                None => Err(env::VarError::NotUnicode(v.to_os_string())),
            }
        }
        Err(e) => Err(e),
    }
}

// Retrieve environment variable, or panic
pub fn ensure_var<K: AsRef<OsStr>>(key: K) -> String {
    env::var(key.as_ref()).expect(
        r#"

Set AWS_ENDPOINT_URL to run tests.
    example: `export AWS_ENDPOINT_URL=http://localhost:2000`

"#,
    )
}

pub fn resolve_region(
    aws_region: Option<String>,
    aws_endpoint_url: Option<String>,
) -> Result<Region, Box<dyn std::error::Error>> {
    let region: Region = match (aws_region.as_ref(), aws_endpoint_url.as_ref()) {
        // User fully specified Region details
        (Some(region), Some(endpoint)) => Region::Custom {
            name: region.to_string(),
            endpoint: endpoint.to_string(),
        },

        // User specified a region, use the default endpoint for that region
        (Some(region), None) => Region::from_str(&region)?,

        // User specified a custom endpoint, useful for dynamodb-local
        (None, Some(endpoint)) => {
            set_default_var("AWS_ACCESS_KEY_ID", "local")?;
            set_default_var("AWS_SECRET_ACCESS_KEY", "local")?;

            Region::Custom {
                name: "local".to_string(),
                endpoint: endpoint.to_string(),
            }
        }

        // User did not specify connection details, use the SDK defaults
        (None, None) => Region::default(),
    };

    Ok(region)
}
