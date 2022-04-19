mod client;
mod error;
mod feature;
mod feature_builder;
mod model;
mod source;
mod utils;
mod job_client;
mod registry_client;

pub use client::{AnchorGroupBuilder, FeathrClient};
pub use error::Error;
pub use feature::{AnchorFeature, DerivedFeature, Feature};
pub use feature_builder::{AnchorFeatureBuilder, DerivedFeatureBuilder};
pub use model::*;
pub use source::*;
pub use utils::ExtDuration;
pub use job_client::{JobClient, AzureSynapseClient, DatabricksClient};
pub use registry_client::{FeatureRegistry, FeathrApiClient, PurviewClient};

#[cfg(test)]
mod tests {
    use crate::{FeathrClient, FeatureType};

    #[test]
    fn it_works() {
        let client = FeathrClient::new();
        let s = client
            .hdfs_source_builder("h1")
            .set_path("somepath")
            .build()
            .unwrap();
        let g1 = client.group_builder("g1").set_source(s).build().unwrap();
        let f = client
            .anchor_builder(&g1, "f1")
            .set_type(FeatureType::INT32)
            .set_transform("x".into())
            .build()
            .unwrap();
        client
            .derived_builder("d1")
            .add_input(f)
            .set_transform("1".into())
            .set_type(FeatureType::INT32)
            .build()
            .unwrap();
        let s = client.get_feature_config().unwrap();
        println!("{}", s);
    }
}
