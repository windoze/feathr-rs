use async_trait::async_trait;

use crate::{FeatureRegistry, FeathrProject, Error, YamlSource};

pub struct FeathrApiClient;

impl FeathrApiClient {
    pub fn from_var_source(var_source: &YamlSource) -> Result<Self, crate::Error> {
        // TODO:
        Ok(Self)
    }
}

#[async_trait]
impl FeatureRegistry for FeathrApiClient {
    async fn save_project(&self, project: &FeathrProject) -> Result<(), Error> {
        todo!()
    }

    async fn load_project(&self, name: &str) -> Result<FeathrProject, Error> {
        todo!()
    }
}