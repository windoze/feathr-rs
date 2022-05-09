mod project;
mod error;
mod feature;
mod feature_builder;
mod model;
mod source;
mod observation;
mod feature_query;
mod job_config;
mod utils;
mod job_client;
mod registry_client;
mod client;

use log::debug;
pub use project::{AnchorGroupBuilder, FeathrProject};
pub use error::Error;
pub use feature::{AnchorFeature, DerivedFeature, Feature};
pub use feature_builder::{AnchorFeatureBuilder, DerivedFeatureBuilder};
pub use model::*;
pub use source::*;
pub use observation::*;
pub use feature_query::*;
pub use job_config::*;
pub use utils::ExtDuration;
pub use job_client::*;
pub use registry_client::{FeatureRegistry, FeathrApiClient, PurviewClient};
pub use client::FeathrClient;

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

