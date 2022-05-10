use std::sync::Arc;

use async_trait::async_trait;

use crate::{FeatureRegistry, FeathrProject, Error, VarSource};

pub struct FeathrApiClient;

impl FeathrApiClient {
    /**
     * Create Api Client from a VarSource
     */
    pub async fn from_var_source(_var_source: Arc<dyn VarSource + Send + Sync>) -> Result<Self, crate::Error>
    {
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