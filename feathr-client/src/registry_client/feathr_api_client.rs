use async_trait::async_trait;

use crate::{FeatureRegistry, FeathrProject, Error, VarSource};

pub struct FeathrApiClient;

impl FeathrApiClient {
    /**
     * Create Api Client from a VarSource
     */
    pub fn from_var_source<T>(_var_source: &T) -> Result<Self, crate::Error>
    where
        T: VarSource
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