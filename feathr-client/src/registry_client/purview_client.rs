use async_trait::async_trait;

use crate::{FeatureRegistry, FeathrProject, Error};

pub struct PurviewClient {}

#[async_trait]
impl FeatureRegistry for PurviewClient {
    async fn save_project(&self, project: &FeathrProject) -> Result<(), Error> {
        todo!()
    }

    async fn load_project(&self, name: &str) -> Result<FeathrProject, Error> {
        todo!()
    }
}