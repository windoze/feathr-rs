mod feathr_api_client;
mod purview_client;

use async_trait::async_trait;

pub use feathr_api_client::FeathrApiClient;
pub use purview_client::PurviewClient;

use crate::{FeathrProject, Error};

// TODO:
#[async_trait]
pub trait FeatureRegistry {
    async fn save_project(&self, project: &FeathrProject) -> Result<(), Error>;
    async fn load_project(&self) -> Result<FeathrProject, Error>;
}