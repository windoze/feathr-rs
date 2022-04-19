use std::{sync::{Arc, RwLock}, collections::HashMap};

use crate::{client::{FeathrClientImpl, FeathrClientModifier}, FeatureType, Transformation, TypedKey, feature::{FeatureBase, AnchorFeatureImpl, AnchorFeature, InputFeature, DerivedFeatureImpl, DerivedFeature, Feature}, Error};

#[derive(Debug)]
pub struct AnchorFeatureBuilder {
    pub(crate) owner: Arc<RwLock<FeathrClientImpl>>,
    group: String,
    name: String,
    feature_type: Option<FeatureType>,
    transform: Option<Transformation>,
    keys: Vec<TypedKey>,
    feature_alias: String,
    registry_tags: HashMap<String, String>,
}

impl AnchorFeatureBuilder {
    pub(crate) fn new(owner: Arc<RwLock<FeathrClientImpl>>, group: &str, name: &str) -> Self {
        Self {
            owner,
            group: group.to_string(),
            name: name.to_string(),
            feature_type: None,
            transform: None,
            keys: Default::default(),
            feature_alias: name.to_string(),
            registry_tags: Default::default(),
        }
    }

    pub fn set_type(&mut self, feature_type: FeatureType) -> &mut Self {
        self.feature_type = Some(feature_type);
        self
    }

    pub fn set_transform(&mut self, transform: Transformation) -> &mut Self {
        self.transform = Some(transform);
        self
    }

    pub fn add_tag(&mut self, key: &str, value: &str) -> &mut Self {
        self.registry_tags
            .insert(key.to_string(), value.to_string());
        self
    }

    pub fn build(&mut self) -> Result<AnchorFeature, Error> {
        let anchor = AnchorFeatureImpl {
            base: FeatureBase {
                name: self.name.clone(),
                feature_type: self
                    .feature_type
                    .as_ref()
                    .ok_or_else(|| Error::MissingFeatureType(self.name.clone()))?
                    .to_owned(),
                transform: self
                    .transform
                    .as_ref()
                    .ok_or_else(|| Error::MissingTransformation(self.name.clone()))?
                    .to_owned(),
                key: if self.keys.is_empty() {
                    vec![TypedKey::DUMMY_KEY()]
                } else {
                    self.keys.clone()
                },
                feature_alias: self.feature_alias.clone(),
                key_alias: self
                    .keys
                    .iter()
                    .map(|k| {
                        k.key_column_alias
                            .as_ref()
                            .unwrap_or(&k.key_column)
                            .to_owned()
                    })
                    .collect(),
                registry_tags: self.registry_tags.clone(),
            },
        };
        self.owner.insert_anchor(&self.group, anchor)
    }
}
#[derive(Debug)]
pub struct DerivedFeatureBuilder {
    pub(crate) owner: Arc<RwLock<FeathrClientImpl>>,
    name: String,
    feature_type: Option<FeatureType>,
    transform: Option<Transformation>,
    keys: Vec<TypedKey>,
    feature_alias: String,
    registry_tags: HashMap<String, String>,
    input_features: Vec<InputFeature>,
}

impl DerivedFeatureBuilder {
    pub(crate) fn new(owner: Arc<RwLock<FeathrClientImpl>>, name: &str) -> Self {
        Self {
            owner,
            name: name.to_string(),
            feature_type: None,
            transform: None,
            keys: Default::default(),
            feature_alias: name.to_string(),
            registry_tags: Default::default(),
            input_features: Default::default(),
        }
    }

    pub fn set_type(&mut self, feature_type: FeatureType) -> &mut Self {
        self.feature_type = Some(feature_type);
        self
    }

    pub fn set_transform(&mut self, transform: Transformation) -> &mut Self {
        self.transform = Some(transform);
        self
    }

    pub fn add_tag(&mut self, key: &str, value: &str) -> &mut Self {
        self.registry_tags
            .insert(key.to_string(), value.to_string());
        self
    }

    pub fn add_input<T: Feature>(&mut self, feature: T) -> &mut Self {
        self.input_features.push(InputFeature {
            key: feature.get_key(),
            name: feature.get_name(),
        });
        self
    }

    pub fn build(&mut self) -> Result<DerivedFeature, Error> {
        let derived = DerivedFeatureImpl {
            base: FeatureBase {
                name: self.name.clone(),
                feature_type: self
                    .feature_type
                    .as_ref()
                    .ok_or_else(|| Error::MissingFeatureType(self.name.clone()))?
                    .to_owned(),
                transform: self
                    .transform
                    .as_ref()
                    .ok_or_else(|| Error::MissingTransformation(self.name.clone()))?
                    .to_owned(),
                key: if self.keys.is_empty() {
                    vec![TypedKey::DUMMY_KEY()]
                } else {
                    self.keys.clone()
                },
                feature_alias: self.feature_alias.clone(),
                key_alias: self
                    .keys
                    .iter()
                    .map(|k| {
                        k.key_column_alias
                            .as_ref()
                            .unwrap_or(&k.key_column)
                            .to_owned()
                    })
                    .collect(),
                registry_tags: self.registry_tags.clone(),
            },
            inputs: self.input_features.clone(),
        };
        self.owner.insert_derived(derived)
    }
}
