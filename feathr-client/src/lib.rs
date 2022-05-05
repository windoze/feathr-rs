mod project;
mod error;
mod feature;
mod feature_builder;
mod model;
mod source;
mod utils;
mod job_client;
mod registry_client;

use log::debug;
pub use project::{AnchorGroupBuilder, FeathrProject};
pub use error::Error;
pub use feature::{AnchorFeature, DerivedFeature, Feature};
pub use feature_builder::{AnchorFeatureBuilder, DerivedFeatureBuilder};
pub use model::*;
pub use source::*;
pub use utils::ExtDuration;
pub use job_client::*;
pub use registry_client::{FeatureRegistry, FeathrApiClient, PurviewClient};

/// Log if `Result` is an error
pub(crate) trait Logged {
    fn log(self) -> Self;
}

impl<T, E> Logged for std::result::Result<T, E>
where
    E: std::fmt::Debug,
{
    fn log(self) -> Self {
        if let Err(e) = &self {
            debug!("---TraceError--- {:#?}", e)
        }
        self
    }
}

#[cfg(test)]
mod tests {
    use crate::*;

    #[test]
    fn it_works() {
        let proj = FeathrProject::new();
        let s = proj.jdbc_source_builder("h1")
            .set_auth(JdbcSourceAuth::Userpass)
            .set_url("jdbc:sqlserver://bet-test.database.windows.net:1433;database=bet-test")
            .set_dbtable("AzureRegions")
            .build()
            .unwrap();
        let g1 = proj.group_builder("g1").set_source(s).build().unwrap();
        let k1 = TypedKey::new("c1", ValueType::INT32);
        let k2 = TypedKey::new("c2", ValueType::INT32);
        let f = proj
            .anchor_builder(&g1, "f1")
            .set_type(FeatureType::INT32)
            .set_transform("x".into())
            .set_keys(&[k1, k2])
            .build()
            .unwrap();
        proj
            .derived_builder("d1")
            .add_input(f)
            .set_transform("1".into())
            .set_type(FeatureType::INT32)
            .build()
            .unwrap();
        let s = proj.get_feature_config().unwrap();
        println!("{}", s);
    }
}
