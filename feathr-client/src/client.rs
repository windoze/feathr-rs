use std::sync::RwLock;
use std::{collections::HashMap, sync::Arc};

use indexmap::IndexMap;
use serde::ser::SerializeStruct;
use serde::Serialize;

use crate::feature::{AnchorFeature, AnchorFeatureImpl, DerivedFeature, DerivedFeatureImpl};
use crate::feature_builder::{AnchorFeatureBuilder, DerivedFeatureBuilder};
use crate::{Error, Feature, HdfsSourceBuilder, JdbcSourceBuilder, Source, SourceImpl};

#[derive(Debug)]
pub struct FeathrClient {
    input_context: Source,
    inner: Arc<RwLock<FeathrClientImpl>>,
}

impl FeathrClient {
    pub fn new() -> Self {
        let inner = Arc::new(RwLock::new(FeathrClientImpl {
            anchor_groups: Default::default(),
            derivations: Default::default(),
            sources: Default::default(),
        }));
        Self {
            input_context: Source {
                inner: Arc::new(SourceImpl::INPUT_CONTEXT()),
            },
            inner,
        }
    }

    pub fn get_anchor(&self, group: &str, name: &str) -> Result<AnchorFeature, Error> {
        let r = self.inner.read()?;
        Ok(AnchorFeature {
            owner: self.inner.clone(),
            inner: r.get_anchor(group, name)?,
        })
    }

    pub fn get_derived(&self, name: &str) -> Result<DerivedFeature, Error> {
        let r = self.inner.read()?;
        Ok(DerivedFeature {
            owner: self.inner.clone(),
            inner: r.get_derived(name)?,
        })
    }

    pub fn group_builder(&self, name: &str) -> AnchorGroupBuilder {
        AnchorGroupBuilder::new(self.inner.clone(), name)
    }

    pub fn anchor_builder(&self, group: &str, name: &str) -> AnchorFeatureBuilder {
        AnchorFeatureBuilder::new(self.inner.clone(), group, name)
    }

    pub fn derived_builder(&self, name: &str) -> DerivedFeatureBuilder {
        DerivedFeatureBuilder::new(self.inner.clone(), name)
    }

    pub fn hdfs_source_builder(&self, name: &str) -> HdfsSourceBuilder {
        HdfsSourceBuilder::new(self.inner.clone(), name)
    }

    pub fn jdbc_source_builder(&self, name: &str) -> JdbcSourceBuilder {
        JdbcSourceBuilder::new(self.inner.clone(), name)
    }

    #[allow(non_snake_case)]
    pub fn INPUT_CONTEXT(&self) -> Source {
        self.input_context.clone()
    }

    pub fn get_secret_keys(&self) -> Result<Vec<String>, Error> {
        Ok(self.inner.read()?.get_secret_keys())
    }

    pub fn get_feature_config(&self) -> Result<String, Error> {
        let r = self.inner.read()?;
        let s = serde_json::to_string_pretty(&*r).unwrap();
        Ok(s)
    }
}

#[derive(Debug, Serialize)]
pub(crate) struct FeathrClientImpl {
    #[serde(rename = "anchors")]
    anchor_groups: HashMap<String, AnchorGroup>,
    derivations: HashMap<String, Arc<DerivedFeatureImpl>>,
    sources: HashMap<String, Arc<SourceImpl>>,
}

impl FeathrClientImpl {
    fn get_anchor(&self, group: &str, name: &str) -> Result<Arc<AnchorFeatureImpl>, Error> {
        let g = self
            .anchor_groups
            .get(group)
            .ok_or_else(|| Error::AnchorGroupNotFound(group.to_string()))?;
        g.get(name)
    }

    fn get_derived(&self, name: &str) -> Result<Arc<DerivedFeatureImpl>, Error> {
        self.derivations
            .get(name)
            .ok_or_else(|| Error::FeatureNotFound(name.to_string()))
            .map(|r| r.to_owned())
    }

    fn insert_anchor(
        &mut self,
        group: &str,
        f: AnchorFeatureImpl,
    ) -> Result<Arc<AnchorFeatureImpl>, Error> {
        Ok(self
            .anchor_groups
            .get_mut(group)
            .ok_or_else(|| Error::AnchorGroupNotFound(group.to_string()))?
            .insert(f))
    }

    fn insert_derived(&mut self, f: DerivedFeatureImpl) -> Arc<DerivedFeatureImpl> {
        let name = f.base.name.clone();
        let ret = Arc::new(f);
        self.derivations.insert(name, ret.clone());
        ret
    }

    fn insert_source(&mut self, s: SourceImpl) -> Arc<SourceImpl> {
        let name = s.name.clone();
        let ret = Arc::new(s);
        self.sources.insert(name, ret.clone());
        ret
    }

    fn get_secret_keys(&self) -> Vec<String> {
        self.sources
            .iter()
            .map(|(_, s)| s.get_secret_keys().into_iter())
            .flatten()
            .collect()
    }
}

#[derive(Debug, Default)]
struct AnchorGroup {
    name: String,
    anchors: IndexMap<String, Arc<AnchorFeatureImpl>>,
    source: String,
    registry_tags: HashMap<String, String>,
}

impl AnchorGroup {
    fn insert(&mut self, f: AnchorFeatureImpl) -> Arc<AnchorFeatureImpl> {
        let name = f.base.name.clone();
        let ret = Arc::new(f);
        self.anchors.insert(name, ret.clone());
        ret
    }

    fn get(&self, name: &str) -> Result<Arc<AnchorFeatureImpl>, Error> {
        Ok(self
            .anchors
            .get(name)
            .ok_or_else(|| Error::FeatureNotFound(name.to_string()))?
            .to_owned())
    }
}

impl Serialize for AnchorGroup {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("JdbcAuth", 3)?;
        state.serialize_field("source", &self.source)?;
        #[derive(Serialize)]
        struct Key {
            #[serde(rename = "sqlExpr")]
            sql_expr: Vec<String>,
        }
        let key = Key {
            sql_expr: self
                .anchors
                .get_index(0)
                .map(|(_, v)| v.get_key_alias())
                .unwrap_or_default(),
        };
        state.serialize_field("key", &key)?;
        state.serialize_field("features", &self.anchors.clone())?;
        state.end()
    }
}

pub struct AnchorGroupBuilder {
    owner: Arc<RwLock<FeathrClientImpl>>,
    name: String,
    source: Option<String>,
    registry_tags: HashMap<String, String>,
}

impl AnchorGroupBuilder {
    fn new(owner: Arc<RwLock<FeathrClientImpl>>, name: &str) -> Self {
        Self {
            owner,
            name: name.to_string(),
            source: None,
            registry_tags: Default::default(),
        }
    }

    pub fn set_source(&mut self, source: Source) -> &mut Self {
        self.source = Some(source.get_name());
        self
    }

    pub fn add_registry_tag(&mut self, key: &str, value: &str) -> &mut Self {
        self.registry_tags
            .insert(key.to_string(), value.to_string());
        self
    }

    pub fn build(&mut self) -> Result<String, Error> {
        let group = AnchorGroup {
            name: self.name.clone(),
            anchors: Default::default(),
            source: self
                .source
                .clone()
                .unwrap_or_else(|| "INPUT_CONTEXT".to_string()),
            registry_tags: self.registry_tags.clone(),
        };

        let name = group.name.clone();
        let mut w = self.owner.write()?;
        w.anchor_groups.entry(name.clone()).or_insert(group);
        Ok(name)
    }
}

pub(crate) trait FeathrClientModifier {
    fn insert_anchor(&self, group: &str, anchor: AnchorFeatureImpl)
        -> Result<AnchorFeature, Error>;
    fn insert_derived(&self, derived: DerivedFeatureImpl) -> Result<DerivedFeature, Error>;
    fn insert_source(&self, source: SourceImpl) -> Result<Source, Error>;
}

impl FeathrClientModifier for Arc<RwLock<FeathrClientImpl>> {
    fn insert_anchor(
        &self,
        group: &str,
        anchor: AnchorFeatureImpl,
    ) -> Result<AnchorFeature, Error> {
        let mut w = self.write()?;
        Ok(AnchorFeature {
            owner: self.clone(),
            inner: w.insert_anchor(group, anchor)?,
        })
    }

    fn insert_derived(&self, derived: DerivedFeatureImpl) -> Result<DerivedFeature, Error> {
        let mut w = self.write()?;
        Ok(DerivedFeature {
            owner: self.clone(),
            inner: w.insert_derived(derived),
        })
    }

    fn insert_source(&self, source: SourceImpl) -> Result<Source, Error> {
        let mut w = self.write()?;
        Ok(Source {
            inner: w.insert_source(source),
        })
    }
}
