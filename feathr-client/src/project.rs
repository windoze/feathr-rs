use std::sync::RwLock;
use std::{collections::HashMap, sync::Arc};

use chrono::{DateTime, Utc};
use indexmap::IndexMap;
use serde::ser::SerializeStruct;
use serde::Serialize;

use crate::feature::{AnchorFeature, AnchorFeatureImpl, DerivedFeature, DerivedFeatureImpl};
use crate::feature_builder::{AnchorFeatureBuilder, DerivedFeatureBuilder};
use crate::{
    DateTimeResolution, Error, Feature, FeatureQuery, FeatureType, HdfsSourceBuilder,
    JdbcSourceBuilder, ObservationSettings, Source, SourceImpl, SubmitGenerationJobRequestBuilder,
    SubmitJoiningJobRequestBuilder, TypedKey,
};

/**
 * A Feathr Project is the container of all anchor features, anchor groups, derived features, and data sources.
 */
#[derive(Debug)]
pub struct FeathrProject {
    input_context: Source,
    inner: Arc<RwLock<FeathrProjectImpl>>,
}

impl FeathrProject {
    /**
     * Create a new Feathr project with name
     */
    pub fn new(name: &str) -> Self {
        let inner = Arc::new(RwLock::new(FeathrProjectImpl {
            name: name.to_string(),
            anchor_groups: Default::default(),
            derivations: Default::default(),
            sources: Default::default(),
        }));
        FeathrProject {
            input_context: Source {
                inner: Arc::new(SourceImpl::INPUT_CONTEXT()),
            },
            inner,
        }
    }

    /**
     * Retrieve anchor feature with `name` from specified group
     */
    pub fn get_anchor(&self, group: &str, name: &str) -> Result<AnchorFeature, Error> {
        let r = self.inner.read()?;
        Ok(AnchorFeature {
            owner: self.inner.clone(),
            inner: r.get_anchor(group, name)?,
        })
    }

    /**
     * Retrieve derived feature with `name`
     */
    pub fn get_derived(&self, name: &str) -> Result<DerivedFeature, Error> {
        let r = self.inner.read()?;
        Ok(DerivedFeature {
            owner: self.inner.clone(),
            inner: r.get_derived(name)?,
        })
    }

    /**
     * Retrieve anchor group with `name`
     */
    pub fn get_anchor_group(&self, name: &str) -> Result<AnchorGroup, Error> {
        let g = self
            .inner
            .read()?
            .anchor_groups
            .get(name)
            .ok_or_else(|| Error::AnchorGroupNotFound(name.to_string()))?
            .clone();
        Ok(AnchorGroup {
            owner: self.inner.clone(),
            inner: g,
        })
    }

    /**
     * Start creating an anchor group, with given name and data source
     */
    pub fn anchor_group(&self, name: &str, source: Source) -> AnchorGroupBuilder {
        AnchorGroupBuilder::new(self.inner.clone(), name, source)
    }

    /**
     * Start creating a derived feature with given name and feature type
     */
    pub fn derived(&self, name: &str, feature_type: FeatureType) -> DerivedFeatureBuilder {
        DerivedFeatureBuilder::new(self.inner.clone(), name, feature_type)
    }

    /**
     * Start creating a HDFS data source with given name
     */
    pub fn hdfs_source(&self, name: &str, path: &str) -> HdfsSourceBuilder {
        HdfsSourceBuilder::new(self.inner.clone(), name, path)
    }

    /**
     * Start creating a JDBC data source with given name
     */
    pub fn jdbc_source(&self, name: &str, url: &str) -> JdbcSourceBuilder {
        JdbcSourceBuilder::new(self.inner.clone(), name, url)
    }

    /**
     * Returns the placeholder data source
     */
    #[allow(non_snake_case)]
    pub fn INPUT_CONTEXT(&self) -> Source {
        self.input_context.clone()
    }

    /**
     * Creates the Spark job request for a feature-joining job
     */
    pub fn feature_join_job<O, Q>(
        &self,
        observation_settings: O,
        feature_query: &[&Q],
        output: &str,
    ) -> Result<SubmitJoiningJobRequestBuilder, Error>
    where
        O: Into<ObservationSettings>,
        Q: Into<FeatureQuery> + Clone,
    {
        let ob = observation_settings.into();
        Ok(SubmitJoiningJobRequestBuilder::new_join(
            format!("{}_feathr_feature_join_job", self.inner.read()?.name),
            ob.observation_path.to_string(),
            self.get_feature_config()?,
            self.get_feature_join_config(ob, feature_query, output)?,
            self.get_secret_keys()?,
        ))
    }

    /**
     * Creates the Spark job request for a feature-generation job
     */
    pub fn feature_gen_job(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        step: DateTimeResolution,
    ) -> Result<SubmitGenerationJobRequestBuilder, Error> {
        Ok(SubmitGenerationJobRequestBuilder::new_gen(
            format!(
                "{}_feathr_feature_materialization_job",
                self.inner.read()?.name
            ),
            Default::default(), // TODO:
            self.get_feature_config()?,
            self.get_secret_keys()?,
            start,
            end,
            step,
        ))
    }

    pub(crate) fn get_secret_keys(&self) -> Result<Vec<String>, Error> {
        Ok(self.inner.read()?.get_secret_keys())
    }

    pub(crate) fn get_feature_config(&self) -> Result<String, Error> {
        let r = self.inner.read()?;
        let s = serde_json::to_string_pretty(&*r).unwrap();
        Ok(s)
    }

    pub(crate) fn get_feature_join_config<O, Q>(
        &self,
        observation_settings: O,
        feature_query: &[&Q],
        output: &str,
    ) -> Result<String, Error>
    where
        O: Into<ObservationSettings>,
        Q: Into<FeatureQuery> + Clone,
    {
        // TODO: Validate feature names

        #[derive(Clone, Debug, Serialize)]
        #[serde(rename_all = "camelCase")]
        struct FeatureJoinConfig {
            #[serde(flatten)]
            observation_settings: ObservationSettings,
            feature_list: Vec<FeatureQuery>,
            output_path: String,
        }
        let cfg = FeatureJoinConfig {
            observation_settings: observation_settings.into(),
            feature_list: feature_query
                .into_iter()
                .map(|&q| q.to_owned().into())
                .collect(),
            output_path: output.to_string(),
        };
        Ok(serde_json::to_string_pretty(&cfg)?)
    }
}

#[derive(Debug, Serialize)]
pub(crate) struct FeathrProjectImpl {
    #[serde(skip_serializing)]
    name: String,
    #[serde(rename = "anchors")]
    anchor_groups: HashMap<String, Arc<RwLock<AnchorGroupImpl>>>,
    derivations: HashMap<String, Arc<DerivedFeatureImpl>>,
    sources: HashMap<String, Arc<SourceImpl>>,
}

impl FeathrProjectImpl {
    fn get_anchor(&self, group: &str, name: &str) -> Result<Arc<AnchorFeatureImpl>, Error> {
        let g = self
            .anchor_groups
            .get(group)
            .ok_or_else(|| Error::AnchorGroupNotFound(group.to_string()))?;
        g.read()?.get(name)
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
            .write()?
            .insert(f)?)
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

#[allow(dead_code)]
#[derive(Debug)]
struct AnchorGroupImpl {
    name: String,
    anchors: IndexMap<String, Arc<AnchorFeatureImpl>>,
    source: Source,
    registry_tags: HashMap<String, String>,
}

impl AnchorGroupImpl {
    fn insert(&mut self, f: AnchorFeatureImpl) -> Result<Arc<AnchorFeatureImpl>, Error> {
        if self.source != Source::INPUT_CONTEXT()
            && (f.get_key().is_empty() || f.get_key() == vec![TypedKey::DUMMY_KEY()])
        {
            return Err(Error::DummyKeyUsedWithoutInputContext(f.get_name()));
        }

        if !self.anchors.is_empty() && (f.get_key_alias() != self.get_key_alias()) {
            return Err(Error::InvalidKeyAlias(f.get_name(), self.name.clone()));
        }

        let name = f.base.name.clone();
        let ret = Arc::new(f);
        self.anchors.insert(name, ret.clone());
        Ok(ret)
    }

    fn get(&self, name: &str) -> Result<Arc<AnchorFeatureImpl>, Error> {
        Ok(self
            .anchors
            .get(name)
            .ok_or_else(|| Error::FeatureNotFound(name.to_string()))?
            .to_owned())
    }

    fn get_key_alias(&self) -> Vec<String> {
        self.anchors
            .get_index(0)
            .map(|(_, v)| v.get_key_alias())
            .unwrap_or_default()
    }
}

impl Serialize for AnchorGroupImpl {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("JdbcAuth", 3)?;
        state.serialize_field("source", &self.source.get_name())?;
        #[derive(Serialize)]
        struct Key {
            #[serde(rename = "sqlExpr")]
            sql_expr: Vec<String>,
        }
        let key = Key {
            sql_expr: self.get_key_alias(),
        };
        state.serialize_field("key", &key)?;
        state.serialize_field("features", &self.anchors.clone())?;
        state.end()
    }
}

pub struct AnchorGroup {
    owner: Arc<RwLock<FeathrProjectImpl>>,
    inner: Arc<RwLock<AnchorGroupImpl>>,
}

impl AnchorGroup {
    pub fn anchor(
        &self,
        name: &str,
        feature_type: FeatureType,
    ) -> Result<AnchorFeatureBuilder, Error> {
        Ok(AnchorFeatureBuilder::new(
            self.owner.clone(),
            &self.inner.read()?.name,
            name,
            feature_type,
        ))
    }

    pub fn get_anchor(&self, name: &str) -> Result<AnchorFeature, Error> {
        let r = self.inner.read()?;
        Ok(AnchorFeature {
            owner: self.owner.clone(),
            inner: r.get(name)?,
        })
    }
}

pub struct AnchorGroupBuilder {
    owner: Arc<RwLock<FeathrProjectImpl>>,
    name: String,
    source: Source,
    registry_tags: HashMap<String, String>,
}

impl AnchorGroupBuilder {
    fn new(owner: Arc<RwLock<FeathrProjectImpl>>, name: &str, source: Source) -> Self {
        Self {
            owner,
            name: name.to_string(),
            source: source,
            registry_tags: Default::default(),
        }
    }

    pub fn add_registry_tag(&mut self, key: &str, value: &str) -> &mut Self {
        self.registry_tags
            .insert(key.to_string(), value.to_string());
        self
    }

    pub fn build(&mut self) -> Result<AnchorGroup, Error> {
        let group = AnchorGroupImpl {
            name: self.name.clone(),
            anchors: Default::default(),
            source: self.source.clone(),
            registry_tags: self.registry_tags.clone(),
        };

        let name = group.name.clone();
        let mut w = self.owner.write()?;
        let g = Arc::new(RwLock::new(group));
        w.anchor_groups.entry(name.clone()).or_insert(g.clone());
        Ok(AnchorGroup {
            owner: self.owner.clone(),
            inner: g,
        })
    }
}

pub(crate) trait FeathrProjectModifier {
    fn insert_anchor(&self, group: &str, anchor: AnchorFeatureImpl)
        -> Result<AnchorFeature, Error>;
    fn insert_derived(&self, derived: DerivedFeatureImpl) -> Result<DerivedFeature, Error>;
    fn insert_source(&self, source: SourceImpl) -> Result<Source, Error>;
}

impl FeathrProjectModifier for Arc<RwLock<FeathrProjectImpl>> {
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

#[cfg(test)]
mod tests {
    use crate::*;

    #[test]
    fn it_works() {
        let proj = FeathrProject::new("p1");
        let s = proj
            .jdbc_source(
                "h1",
                "jdbc:sqlserver://bet-test.database.windows.net:1433;database=bet-test",
            )
            .auth(JdbcSourceAuth::Userpass)
            .dbtable("AzureRegions")
            .build()
            .unwrap();
        let g1 = proj.anchor_group("g1", s).build().unwrap();
        let k1 = TypedKey::new("c1", ValueType::INT32);
        let k2 = TypedKey::new("c2", ValueType::INT32);
        let f = g1
            .anchor("f1", FeatureType::INT32)
            .unwrap()
            .transform("x")
            .keys(&[&k1, &k2])
            .build()
            .unwrap();
        proj.derived("d1", FeatureType::INT32)
            .add_input(&f)
            .transform("1")
            .build()
            .unwrap();
        let s = proj.get_feature_config().unwrap();
        println!("{}", s);
    }
}
