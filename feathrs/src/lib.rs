use std::collections::HashMap;
use std::fmt::Debug;
use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
};

use chrono::{DateTime, Duration, TimeZone, Utc};
use feathr_client::JobId;
use futures::future::join_all;
use pyo3::exceptions::{PyKeyError, PyRuntimeError, PyValueError};
use pyo3::types::{PyDateTime, PyList};
use pyo3::{exceptions::PyTypeError, prelude::*, pyclass::CompareOp};

mod utils;

#[pyclass]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum ValueType {
    UNSPECIFIED,
    BOOL,
    INT32,
    INT64,
    FLOAT,
    DOUBLE,
    STRING,
    BYTES,
}

#[pymethods]
impl ValueType {
    fn __repr__(&self) -> String {
        format!("{:#?}", &self)
    }

    fn __richcmp__(&self, other: &Self, op: CompareOp) -> PyResult<bool> {
        match op {
            CompareOp::Eq => Ok(self == other),
            CompareOp::Ne => Ok(self != other),
            _ => Err(PyTypeError::new_err("Unsupported")),
        }
    }

    fn __hash__(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        self.hash(&mut hasher);
        hasher.finish()
    }
}

impl From<feathr_client::ValueType> for ValueType {
    fn from(v: feathr_client::ValueType) -> Self {
        match v {
            feathr_client::ValueType::UNSPECIFIED => ValueType::UNSPECIFIED,
            feathr_client::ValueType::BOOL => ValueType::BOOL,
            feathr_client::ValueType::INT32 => ValueType::INT32,
            feathr_client::ValueType::INT64 => ValueType::INT64,
            feathr_client::ValueType::FLOAT => ValueType::FLOAT,
            feathr_client::ValueType::DOUBLE => ValueType::DOUBLE,
            feathr_client::ValueType::STRING => ValueType::STRING,
            feathr_client::ValueType::BYTES => ValueType::BYTES,
        }
    }
}

impl Into<feathr_client::ValueType> for ValueType {
    fn into(self) -> feathr_client::ValueType {
        match self {
            ValueType::UNSPECIFIED => feathr_client::ValueType::UNSPECIFIED,
            ValueType::BOOL => feathr_client::ValueType::BOOL,
            ValueType::INT32 => feathr_client::ValueType::INT32,
            ValueType::INT64 => feathr_client::ValueType::INT64,
            ValueType::FLOAT => feathr_client::ValueType::FLOAT,
            ValueType::DOUBLE => feathr_client::ValueType::DOUBLE,
            ValueType::STRING => feathr_client::ValueType::STRING,
            ValueType::BYTES => feathr_client::ValueType::BYTES,
        }
    }
}

#[pyclass]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum VectorType {
    TENSOR,
}

#[pymethods]
impl VectorType {
    fn __repr__(&self) -> String {
        format!("{:#?}", &self)
    }

    fn __richcmp__(&self, other: &Self, op: CompareOp) -> PyResult<bool> {
        match op {
            CompareOp::Eq => Ok(self == other),
            CompareOp::Ne => Ok(self != other),
            _ => Err(PyTypeError::new_err("Unsupported")),
        }
    }

    fn __hash__(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        self.hash(&mut hasher);
        hasher.finish()
    }
}

impl From<feathr_client::VectorType> for VectorType {
    fn from(_: feathr_client::VectorType) -> Self {
        VectorType::TENSOR
    }
}

impl Into<feathr_client::VectorType> for VectorType {
    fn into(self) -> feathr_client::VectorType {
        feathr_client::VectorType::TENSOR
    }
}

#[pyclass]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum TensorCategory {
    DENSE,
    SPARSE,
}

#[pymethods]
impl TensorCategory {
    fn __repr__(&self) -> String {
        format!("{:#?}", &self)
    }

    fn __richcmp__(&self, other: &Self, op: CompareOp) -> PyResult<bool> {
        match op {
            CompareOp::Eq => Ok(self == other),
            CompareOp::Ne => Ok(self != other),
            _ => Err(PyTypeError::new_err("Unsupported")),
        }
    }

    fn __hash__(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        self.hash(&mut hasher);
        hasher.finish()
    }
}

impl From<feathr_client::TensorCategory> for TensorCategory {
    fn from(v: feathr_client::TensorCategory) -> Self {
        match v {
            feathr_client::TensorCategory::DENSE => TensorCategory::DENSE,
            feathr_client::TensorCategory::SPARSE => TensorCategory::SPARSE,
        }
    }
}

impl Into<feathr_client::TensorCategory> for TensorCategory {
    fn into(self) -> feathr_client::TensorCategory {
        match self {
            TensorCategory::DENSE => feathr_client::TensorCategory::DENSE,
            TensorCategory::SPARSE => feathr_client::TensorCategory::SPARSE,
        }
    }
}

#[pyclass]
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct FeatureType {
    #[pyo3(get)]
    type_: VectorType,
    #[pyo3(get)]
    tensor_category: TensorCategory,
    #[pyo3(get)]
    dimension_type: Vec<ValueType>,
    #[pyo3(get)]
    val_type: ValueType,
}

#[allow(non_snake_case)]
#[pymethods]
impl FeatureType {
    #[classattr]
    pub const BOOLEAN: FeatureType = FeatureType {
        type_: VectorType::TENSOR,
        tensor_category: TensorCategory::DENSE,
        dimension_type: vec![],
        val_type: ValueType::BOOL,
    };
    #[classattr]
    pub const INT32: FeatureType = FeatureType {
        type_: VectorType::TENSOR,
        tensor_category: TensorCategory::DENSE,
        dimension_type: vec![],
        val_type: ValueType::INT32,
    };
    #[classattr]
    pub const INT64: FeatureType = FeatureType {
        type_: VectorType::TENSOR,
        tensor_category: TensorCategory::DENSE,
        dimension_type: vec![],
        val_type: ValueType::INT64,
    };
    #[classattr]
    pub const FLOAT: FeatureType = FeatureType {
        type_: VectorType::TENSOR,
        tensor_category: TensorCategory::DENSE,
        dimension_type: vec![],
        val_type: ValueType::FLOAT,
    };
    #[classattr]
    pub const DOUBLE: FeatureType = FeatureType {
        type_: VectorType::TENSOR,
        tensor_category: TensorCategory::DENSE,
        dimension_type: vec![],
        val_type: ValueType::DOUBLE,
    };
    #[classattr]
    pub const STRING: FeatureType = FeatureType {
        type_: VectorType::TENSOR,
        tensor_category: TensorCategory::DENSE,
        dimension_type: vec![],
        val_type: ValueType::STRING,
    };
    #[classattr]
    pub const BYTES: FeatureType = FeatureType {
        type_: VectorType::TENSOR,
        tensor_category: TensorCategory::DENSE,
        dimension_type: vec![],
        val_type: ValueType::BYTES,
    };
    #[classattr]
    pub fn INT32_VECTOR() -> Self {
        FeatureType {
            type_: VectorType::TENSOR,
            tensor_category: TensorCategory::DENSE,
            dimension_type: vec![ValueType::INT32],
            val_type: ValueType::BOOL,
        }
    }
    #[classattr]
    pub fn INT64_VECTOR() -> Self {
        FeatureType {
            type_: VectorType::TENSOR,
            tensor_category: TensorCategory::DENSE,
            dimension_type: vec![ValueType::INT32],
            val_type: ValueType::BOOL,
        }
    }
    #[classattr]
    pub fn FLOAT_VECTOR() -> Self {
        FeatureType {
            type_: VectorType::TENSOR,
            tensor_category: TensorCategory::DENSE,
            dimension_type: vec![ValueType::INT32],
            val_type: ValueType::BOOL,
        }
    }
    #[classattr]
    pub fn DOUBLE_VECTOR() -> Self {
        FeatureType {
            type_: VectorType::TENSOR,
            tensor_category: TensorCategory::DENSE,
            dimension_type: vec![ValueType::INT32],
            val_type: ValueType::BOOL,
        }
    }

    fn __repr__(&self) -> String {
        format!("{:#?}", &self)
    }

    fn __richcmp__(&self, other: &Self, op: CompareOp) -> PyResult<bool> {
        match op {
            CompareOp::Eq => Ok(self == other),
            CompareOp::Ne => Ok(self != other),
            _ => Err(PyTypeError::new_err("Unsupported")),
        }
    }

    fn __hash__(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        self.hash(&mut hasher);
        hasher.finish()
    }
}

impl From<feathr_client::FeatureType> for FeatureType {
    fn from(v: feathr_client::FeatureType) -> Self {
        Self {
            type_: v.type_.into(),
            tensor_category: v.tensor_category.into(),
            dimension_type: v.dimension_type.into_iter().map(|t| t.into()).collect(),
            val_type: v.val_type.into(),
        }
    }
}

impl Into<feathr_client::FeatureType> for FeatureType {
    fn into(self) -> feathr_client::FeatureType {
        feathr_client::FeatureType {
            type_: self.type_.into(),
            tensor_category: self.tensor_category.into(),
            dimension_type: self.dimension_type.into_iter().map(|t| t.into()).collect(),
            val_type: self.val_type.into(),
        }
    }
}

#[pyclass]
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct TypedKey {
    #[pyo3(get)]
    key_column: String,
    #[pyo3(get)]
    key_column_type: ValueType,
    #[pyo3(get)]
    full_name: Option<String>,
    #[pyo3(get)]
    description: Option<String>,
    #[pyo3(get)]
    key_column_alias: Option<String>,
}

#[allow(non_snake_case)]
#[pymethods]
impl TypedKey {
    #[new]
    #[args(full_name = "None", description = "None")]
    fn new(
        key_column: &str,
        key_column_type: ValueType,
        full_name: Option<String>,
        description: Option<String>,
    ) -> Self {
        Self {
            key_column: key_column.to_string(),
            key_column_type,
            full_name,
            description,
            key_column_alias: Some(key_column.to_string()),
        }
    }

    #[classattr]
    fn DUMMY_KEY() -> TypedKey {
        TypedKey {
            key_column: "NOT_NEEDED".to_string(),
            key_column_type: ValueType::UNSPECIFIED,
            full_name: Some("feathr.dummy_typedkey".to_string()),
            description: Some("A dummy typed key for passthrough/request feature.".to_string()),
            key_column_alias: None,
        }
    }

    fn as_key(&self, key_column_alias: &str) -> Self {
        let mut ret = self.clone();
        ret.key_column_alias = Some(key_column_alias.to_string());
        ret
    }

    fn __repr__(&self) -> String {
        format!("{:#?}", &self)
    }

    fn __richcmp__(&self, other: &Self, op: CompareOp) -> PyResult<bool> {
        match op {
            CompareOp::Eq => Ok(self == other),
            CompareOp::Ne => Ok(self != other),
            _ => Err(PyTypeError::new_err("Unsupported")),
        }
    }

    fn __hash__(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        self.hash(&mut hasher);
        hasher.finish()
    }
}

impl From<feathr_client::TypedKey> for TypedKey {
    fn from(v: feathr_client::TypedKey) -> Self {
        Self {
            key_column: v.key_column,
            key_column_type: v.key_column_type.into(),
            full_name: v.full_name,
            description: v.description,
            key_column_alias: v.key_column_alias,
        }
    }
}

impl Into<feathr_client::TypedKey> for TypedKey {
    fn into(self) -> feathr_client::TypedKey {
        feathr_client::TypedKey {
            key_column: self.key_column,
            key_column_type: self.key_column_type.into(),
            full_name: self.full_name,
            description: self.description,
            key_column_alias: self.key_column_alias,
        }
    }
}

#[allow(non_camel_case_types)]
#[pyclass]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum Aggregation {
    // No operation
    NOP,
    // Average
    AVG,
    MAX,
    MIN,
    SUM,
    UNION,
    // Element-wise average, typically used in array type value, i.e. 1d dense tensor
    ELEMENTWISE_AVG,
    ELEMENTWISE_MIN,
    ELEMENTWISE_MAX,
    ELEMENTWISE_SUM,
    // Pick the latest value according to its timestamp
    LATEST,
}

#[pymethods]
impl Aggregation {
    fn __repr__(&self) -> String {
        format!("{:#?}", &self)
    }

    fn __richcmp__(&self, other: &Self, op: CompareOp) -> PyResult<bool> {
        match op {
            CompareOp::Eq => Ok(self == other),
            CompareOp::Ne => Ok(self != other),
            _ => Err(PyTypeError::new_err("Unsupported")),
        }
    }

    fn __hash__(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        self.hash(&mut hasher);
        hasher.finish()
    }
}

impl From<feathr_client::Aggregation> for Aggregation {
    fn from(v: feathr_client::Aggregation) -> Self {
        match v {
            feathr_client::Aggregation::NOP => Aggregation::NOP,
            feathr_client::Aggregation::AVG => Aggregation::AVG,
            feathr_client::Aggregation::MAX => Aggregation::MAX,
            feathr_client::Aggregation::MIN => Aggregation::MIN,
            feathr_client::Aggregation::SUM => Aggregation::SUM,
            feathr_client::Aggregation::UNION => Aggregation::UNION,
            feathr_client::Aggregation::ELEMENTWISE_AVG => Aggregation::ELEMENTWISE_AVG,
            feathr_client::Aggregation::ELEMENTWISE_MIN => Aggregation::ELEMENTWISE_MIN,
            feathr_client::Aggregation::ELEMENTWISE_MAX => Aggregation::ELEMENTWISE_MAX,
            feathr_client::Aggregation::ELEMENTWISE_SUM => Aggregation::ELEMENTWISE_SUM,
            feathr_client::Aggregation::LATEST => Aggregation::LATEST,
        }
    }
}

impl Into<feathr_client::Aggregation> for Aggregation {
    fn into(self) -> feathr_client::Aggregation {
        match self {
            Aggregation::NOP => feathr_client::Aggregation::NOP,
            Aggregation::AVG => feathr_client::Aggregation::AVG,
            Aggregation::MAX => feathr_client::Aggregation::MAX,
            Aggregation::MIN => feathr_client::Aggregation::MIN,
            Aggregation::SUM => feathr_client::Aggregation::SUM,
            Aggregation::UNION => feathr_client::Aggregation::UNION,
            Aggregation::ELEMENTWISE_AVG => feathr_client::Aggregation::ELEMENTWISE_AVG,
            Aggregation::ELEMENTWISE_MIN => feathr_client::Aggregation::ELEMENTWISE_MIN,
            Aggregation::ELEMENTWISE_MAX => feathr_client::Aggregation::ELEMENTWISE_MAX,
            Aggregation::ELEMENTWISE_SUM => feathr_client::Aggregation::ELEMENTWISE_SUM,
            Aggregation::LATEST => feathr_client::Aggregation::LATEST,
        }
    }
}

#[pyclass]
#[derive(Clone, Debug, PartialEq, Eq)]
struct Transformation(feathr_client::Transformation);

#[pymethods]
impl Transformation {
    #[new]
    fn from_str(s: &str) -> Self {
        Self(feathr_client::Transformation::from(s))
    }

    #[staticmethod]
    fn window_agg(def_expr: &str, agg_func: Aggregation, window: &str) -> PyResult<Self> {
        Ok(Self(
            feathr_client::Transformation::window_agg(
                def_expr,
                agg_func.into(),
                utils::str_to_dur(window)?,
            )
            .map_err(|e| PyValueError::new_err(format!("{:#?}", e)))?,
        ))
    }

    fn __repr__(&self) -> String {
        format!("{:#?}", &self)
    }

    fn __richcmp__(&self, other: &Self, op: CompareOp) -> PyResult<bool> {
        match op {
            CompareOp::Eq => Ok(self == other),
            CompareOp::Ne => Ok(self != other),
            _ => Err(PyTypeError::new_err("Unsupported")),
        }
    }
}

impl From<feathr_client::Transformation> for Transformation {
    fn from(v: feathr_client::Transformation) -> Self {
        Self(v)
    }
}

impl Into<feathr_client::Transformation> for Transformation {
    fn into(self) -> feathr_client::Transformation {
        self.0
    }
}

#[pyclass]
#[derive(Clone, Debug, Eq, PartialEq)]
struct Source(feathr_client::Source);

#[pymethods]
impl Source {
    #[getter]
    fn get_name(&self) -> String {
        self.0.get_name()
    }

    #[getter]
    pub fn get_secret_keys(&self) -> Vec<String> {
        self.0.get_secret_keys()
    }

    #[getter]
    pub fn get_preprocessing(&self) -> Option<String> {
        self.0.get_preprocessing()
    }

    #[allow(non_snake_case)]
    #[classattr]
    pub fn INPUT_CONTEXT() -> Self {
        Self(feathr_client::Source::INPUT_CONTEXT())
    }

    fn __repr__(&self) -> String {
        format!("{:#?}", &self)
    }

    fn __richcmp__(&self, other: &Self, op: CompareOp) -> PyResult<bool> {
        match op {
            CompareOp::Eq => Ok(self == other),
            CompareOp::Ne => Ok(self != other),
            _ => Err(PyTypeError::new_err("Unsupported")),
        }
    }
}

impl From<feathr_client::Source> for Source {
    fn from(v: feathr_client::Source) -> Self {
        Self(v)
    }
}

impl Into<feathr_client::Source> for Source {
    fn into(self) -> feathr_client::Source {
        self.0
    }
}

#[pyclass]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
enum JdbcSourceAuth {
    Anonymous,
    Userpass,
    Token,
}

#[pymethods]
impl JdbcSourceAuth {
    fn __repr__(&self) -> String {
        format!("{:#?}", &self)
    }

    fn __richcmp__(&self, other: &Self, op: CompareOp) -> PyResult<bool> {
        match op {
            CompareOp::Eq => Ok(self == other),
            CompareOp::Ne => Ok(self != other),
            _ => Err(PyTypeError::new_err("Unsupported")),
        }
    }

    fn __hash__(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        self.hash(&mut hasher);
        hasher.finish()
    }
}

impl From<feathr_client::JdbcSourceAuth> for JdbcSourceAuth {
    fn from(v: feathr_client::JdbcSourceAuth) -> Self {
        match v {
            feathr_client::JdbcSourceAuth::Anonymous => JdbcSourceAuth::Anonymous,
            feathr_client::JdbcSourceAuth::Userpass => JdbcSourceAuth::Userpass,
            feathr_client::JdbcSourceAuth::Token => JdbcSourceAuth::Token,
        }
    }
}

impl Into<feathr_client::JdbcSourceAuth> for JdbcSourceAuth {
    fn into(self) -> feathr_client::JdbcSourceAuth {
        match self {
            JdbcSourceAuth::Anonymous => feathr_client::JdbcSourceAuth::Anonymous,
            JdbcSourceAuth::Userpass => feathr_client::JdbcSourceAuth::Userpass,
            JdbcSourceAuth::Token => feathr_client::JdbcSourceAuth::Token,
        }
    }
}

#[pyclass]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DateTimeResolution {
    Daily,
    Hourly,
}

#[pymethods]
impl DateTimeResolution {
    fn __repr__(&self) -> String {
        format!("{:#?}", &self)
    }

    fn __richcmp__(&self, other: &Self, op: CompareOp) -> PyResult<bool> {
        match op {
            CompareOp::Eq => Ok(self == other),
            CompareOp::Ne => Ok(self != other),
            _ => Err(PyTypeError::new_err("Unsupported")),
        }
    }
}

impl Into<feathr_client::DateTimeResolution> for DateTimeResolution {
    fn into(self) -> feathr_client::DateTimeResolution {
        match self {
            DateTimeResolution::Daily => feathr_client::DateTimeResolution::Daily,
            DateTimeResolution::Hourly => feathr_client::DateTimeResolution::Hourly,
        }
    }
}

#[pyclass]
#[derive(Clone, Debug)]
pub struct RedisSink(feathr_client::RedisSink);

#[pymethods]
impl RedisSink {
    #[new]
    #[args(streaming = "false", streaming_timeout = "None")]
    fn new(table_name: &str, streaming: bool, streaming_timeout: Option<i64>) -> Self {
        Self(feathr_client::RedisSink {
            table_name: table_name.to_string(),
            streaming,
            streaming_timeout: streaming_timeout.map(|i| Duration::seconds(i)),
        })
    }
}

#[pyclass]
#[derive(Clone, Debug)]
struct ObservationSettings(feathr_client::ObservationSettings);

#[pymethods]
impl ObservationSettings {
    #[new]
    #[args(timestamp_column = "None", format = "None")]
    fn new(observation_path: &str, timestamp_column: Option<&str>, format: Option<&str>) -> Self {
        if let Some(timestamp_column) = timestamp_column {
            if let Some(format) = format {
                Self(feathr_client::ObservationSettings::new(
                    observation_path,
                    timestamp_column,
                    format,
                ))
            } else {
                Self(feathr_client::ObservationSettings::new(
                    observation_path,
                    timestamp_column,
                    "epoch",
                ))
            }
        } else {
            Self(feathr_client::ObservationSettings::from_path(
                observation_path,
            ))
        }
    }
}

#[pyclass]
#[derive(Clone, Debug)]
struct FeatureQuery(feathr_client::FeatureQuery);

#[pymethods]
impl FeatureQuery {
    #[new]
    fn new(names: &PyList, keys: Vec<TypedKey>) -> Self {
        let keys: Vec<feathr_client::TypedKey> = keys.into_iter().map(|k| k.into()).collect();
        let keys: Vec<&feathr_client::TypedKey> = keys.iter().map(|k| k).collect();
        let mut n: Vec<String> = vec![];
        for name in names.into_iter() {
            if let Ok(name) = name.extract::<String>() {
                n.push(name);
            } else if let Ok(feature) = name.extract::<AnchorFeature>() {
                n.push(feature.0.to_string())
            } else if let Ok(feature) = name.extract::<DerivedFeature>() {
                n.push(feature.0.to_string())
            }
        }
        Self(feathr_client::FeatureQuery::new(&n, &keys))
    }

    #[staticmethod]
    fn by_name(names: Vec<&str>) -> Self {
        Self(feathr_client::FeatureQuery::by_name(&names))
    }
}

#[pyclass]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JobStatus {
    Starting,
    Running,
    Success,
    Failed,
}

#[pymethods]
impl JobStatus {
    fn __repr__(&self) -> String {
        format!("{:#?}", &self)
    }

    fn __richcmp__(&self, other: &Self, op: CompareOp) -> PyResult<bool> {
        match op {
            CompareOp::Eq => Ok(self == other),
            CompareOp::Ne => Ok(self != other),
            _ => Err(PyTypeError::new_err("Unsupported")),
        }
    }
}

impl From<feathr_client::JobStatus> for JobStatus {
    fn from(v: feathr_client::JobStatus) -> Self {
        match v {
            feathr_client::JobStatus::Starting => JobStatus::Starting,
            feathr_client::JobStatus::Running => JobStatus::Running,
            feathr_client::JobStatus::Success => JobStatus::Success,
            feathr_client::JobStatus::Failed => JobStatus::Failed,
        }
    }
}

#[pyclass]
#[derive(Clone, Debug)]
struct AnchorFeature(feathr_client::AnchorFeature);

#[pymethods(extends=Feature)]
impl AnchorFeature {
    #[getter]
    fn get_name(&self) -> String {
        feathr_client::Feature::get_name(&self.0)
    }
    #[getter]
    fn get_type(&self) -> FeatureType {
        feathr_client::Feature::get_type(&self.0).into()
    }
    #[getter]
    fn get_key(&self) -> Vec<TypedKey> {
        feathr_client::Feature::get_key(&self.0)
            .into_iter()
            .map(|k| k.into())
            .collect()
    }
    #[getter]
    fn get_transformation(&self) -> Transformation {
        feathr_client::Feature::get_transformation(&self.0).into()
    }
    #[getter]
    fn get_key_alias(&self) -> Vec<String> {
        feathr_client::Feature::get_key_alias(&self.0)
    }
    #[getter]
    fn get_registry_tags(&self) -> HashMap<String, String> {
        feathr_client::Feature::get_registry_tags(&self.0)
    }

    fn with_key(&self, group: &str, key_alias: Vec<&str>) -> PyResult<Self> {
        Ok(self
            .0
            .with_key(group, &key_alias)
            .map_err(|e| PyValueError::new_err(format!("{:#?}", e)))?
            .into())
    }

    fn as_feature(&self, group: &str, feature_alias: &str) -> PyResult<Self> {
        Ok(self
            .0
            .as_feature(group, feature_alias)
            .map_err(|e| PyValueError::new_err(format!("{:#?}", e)))?
            .into())
    }

    fn __repr__(&self) -> String {
        format!("{:#?}", &self)
    }
}

impl From<feathr_client::AnchorFeature> for AnchorFeature {
    fn from(v: feathr_client::AnchorFeature) -> Self {
        Self(v)
    }
}

impl Into<feathr_client::AnchorFeature> for AnchorFeature {
    fn into(self) -> feathr_client::AnchorFeature {
        self.0
    }
}

#[pyclass]
#[derive(Clone, Debug)]
struct DerivedFeature(feathr_client::DerivedFeature);

#[pymethods]
impl DerivedFeature {
    #[getter]
    fn get_name(&self) -> String {
        feathr_client::Feature::get_name(&self.0)
    }
    #[getter]
    fn get_type(&self) -> FeatureType {
        feathr_client::Feature::get_type(&self.0).into()
    }
    #[getter]
    fn get_key(&self) -> Vec<TypedKey> {
        feathr_client::Feature::get_key(&self.0)
            .into_iter()
            .map(|k| k.into())
            .collect()
    }
    #[getter]
    fn get_transformation(&self) -> Transformation {
        feathr_client::Feature::get_transformation(&self.0).into()
    }
    #[getter]
    fn get_key_alias(&self) -> Vec<String> {
        feathr_client::Feature::get_key_alias(&self.0)
    }
    #[getter]
    fn get_registry_tags(&self) -> HashMap<String, String> {
        feathr_client::Feature::get_registry_tags(&self.0)
    }

    fn with_key(&self, key_alias: Vec<&str>) -> PyResult<Self> {
        Ok(self
            .0
            .with_key(&key_alias)
            .map_err(|e| PyValueError::new_err(format!("{:#?}", e)))?
            .into())
    }

    fn as_feature(&self, feature_alias: &str) -> PyResult<Self> {
        Ok(self
            .0
            .as_feature(feature_alias)
            .map_err(|e| PyValueError::new_err(format!("{:#?}", e)))?
            .into())
    }
    fn __repr__(&self) -> String {
        format!("{:#?}", &self)
    }
}

impl From<feathr_client::DerivedFeature> for DerivedFeature {
    fn from(v: feathr_client::DerivedFeature) -> Self {
        Self(v)
    }
}

impl Into<feathr_client::DerivedFeature> for DerivedFeature {
    fn into(self) -> feathr_client::DerivedFeature {
        self.0
    }
}

#[pyclass]
#[derive(Clone, Debug)]
struct AnchorGroup(feathr_client::AnchorGroup);

#[pymethods]
impl AnchorGroup {
    #[args(keys = "None", registry_tags = "None")]
    fn anchor(
        &self,
        name: &str,
        feature_type: FeatureType,
        transform: &PyAny,
        keys: Option<Vec<TypedKey>>,
        registry_tags: Option<HashMap<String, String>>,
    ) -> PyResult<AnchorFeature> {
        let mut builder = self
            .0
            .anchor(name, feature_type.into())
            .map_err(|e| PyValueError::new_err(format!("{:#?}", e)))?;
        if let Ok(transform) = transform.extract::<String>() {
            builder.transform(transform);
        } else if let Ok(transform) = transform.extract::<Transformation>() {
            builder.transform(transform);
        } else {
            return Err(PyValueError::new_err(
                "`transform` must be string or Transformation object",
            ));
        }
        if let Some(keys) = keys {
            let keys: Vec<feathr_client::TypedKey> = keys.into_iter().map(|k| k.into()).collect();
            let k: Vec<&feathr_client::TypedKey> = keys.iter().map(|k| k).collect();
            builder.keys(&k);
        }
        if let Some(registry_tags) = registry_tags {
            for (key, value) in registry_tags.into_iter() {
                builder.add_tag(&key, &value);
            }
        }
        Ok(builder
            .build()
            .map_err(|e| PyValueError::new_err(format!("{:#?}", e)))?
            .into())
    }

    fn __getitem__(&self, key: &str) -> PyResult<AnchorFeature> {
        Ok(self
            .0
            .get_anchor(key)
            .map_err(|_| PyKeyError::new_err(key.to_string()))?
            .into())
    }
}

impl From<feathr_client::AnchorGroup> for AnchorGroup {
    fn from(v: feathr_client::AnchorGroup) -> Self {
        Self(v)
    }
}

impl Into<feathr_client::AnchorGroup> for AnchorGroup {
    fn into(self) -> feathr_client::AnchorGroup {
        self.0
    }
}

#[pyclass]
struct FeathrProject(feathr_client::FeathrProject, FeathrClient);

#[pymethods]
impl FeathrProject {
    pub fn get_anchor_group(&self, name: &str) -> PyResult<AnchorGroup> {
        Ok(self
            .0
            .get_anchor_group(name)
            .map_err(|_| PyKeyError::new_err(name.to_string()))?
            .into())
    }

    pub fn get_derived(&self, name: &str) -> PyResult<DerivedFeature> {
        Ok(self
            .0
            .get_derived(name)
            .map_err(|_| PyKeyError::new_err(name.to_string()))?
            .into())
    }

    #[args(registry_tags = "None")]
    pub fn anchor_group(
        &self,
        name: &str,
        source: Source,
        registry_tags: Option<HashMap<String, String>>,
    ) -> PyResult<AnchorGroup> {
        let mut builder = self.0.anchor_group(name, source.into());
        if let Some(registry_tags) = registry_tags {
            for (key, value) in registry_tags.into_iter() {
                builder.add_registry_tag(&key, &value);
            }
        }
        Ok(builder
            .build()
            .map_err(|e| PyValueError::new_err(format!("{:#?}", e)))?
            .into())
    }

    #[args(keys = "None", registry_tags = "None")]
    pub fn derived(
        &self,
        name: &str,
        feature_type: FeatureType,
        transform: &PyAny,
        inputs: &PyList,
        keys: Option<Vec<TypedKey>>,
        registry_tags: Option<HashMap<String, String>>,
    ) -> PyResult<DerivedFeature> {
        let mut builder = self.0.derived(name, feature_type.into());
        if let Ok(transform) = transform.extract::<String>() {
            builder.transform(transform);
        } else if let Ok(transform) = transform.extract::<Transformation>() {
            builder.transform(transform);
        } else {
            return Err(PyValueError::new_err(
                "`transform` must be string or Transformation object",
            ));
        }
        if let Some(keys) = keys {
            let keys: Vec<feathr_client::TypedKey> = keys.into_iter().map(|k| k.into()).collect();
            let k: Vec<&feathr_client::TypedKey> = keys.iter().map(|k| k).collect();
            builder.keys(&k);
        }
        for f in inputs.iter() {
            if let Ok(f) = f.extract::<AnchorFeature>() {
                let f: feathr_client::AnchorFeature = f.to_owned().into();
                builder.add_input(&f);
            } else if let Ok(f) = f.extract::<DerivedFeature>() {
                let f: feathr_client::DerivedFeature = f.to_owned().into();
                builder.add_input(&f);
            } else {
                return Err(PyTypeError::new_err(
                    "Inputs must be list of AnchorFeature or DerivedFeature",
                ));
            }
        }
        if let Some(registry_tags) = registry_tags {
            for (key, value) in registry_tags.into_iter() {
                builder.add_tag(&key, &value);
            }
        }
        Ok(builder
            .build()
            .map_err(|e| PyValueError::new_err(format!("{:#?}", e)))?
            .into())
    }

    #[args(
        timestamp_column = "None",
        timestamp_column_format = "None",
        preprocessing = "None"
    )]
    pub fn hdfs_source(
        &self,
        name: &str,
        path: &str,
        timestamp_column: Option<String>,
        timestamp_column_format: Option<String>,
        preprocessing: Option<String>, // TODO: Use PyCallable?
    ) -> PyResult<Source> {
        let mut builder = self.0.hdfs_source(name, path);
        if let Some(timestamp_column) = timestamp_column {
            if let Some(timestamp_column_format) = timestamp_column_format {
                builder.time_window(&timestamp_column, &timestamp_column_format);
            } else {
                return Err(PyValueError::new_err(
                    "timestamp_column_format must not be omitted",
                ));
            }
        }

        if let Some(preprocessing) = preprocessing {
            builder.preprocessing(&preprocessing);
        }

        Ok(builder
            .build()
            .map_err(|e| PyValueError::new_err(format!("{:#?}", e)))?
            .into())
    }

    #[args(
        dbtable = "None",
        query = "None",
        auth = "None",
        timestamp_column = "None",
        timestamp_column_format = "None",
        preprocessing = "None"
    )]
    pub fn jdbc_source(
        &self,
        name: &str,
        url: &str,
        dbtable: Option<String>,
        query: Option<String>,
        auth: Option<JdbcSourceAuth>,
        timestamp_column: Option<String>,
        timestamp_column_format: Option<String>,
        preprocessing: Option<String>, // TODO: Use PyCallable?
    ) -> PyResult<Source> {
        let mut builder = self.0.jdbc_source(name, url);

        if let Some(dbtable) = dbtable {
            builder.dbtable(&dbtable);
        } else {
            if let Some(query) = query {
                builder.query(&query);
            } else {
                return Err(PyValueError::new_err(
                    "dbtable and query cannot be both omitted",
                ));
            }
        }

        if let Some(auth) = auth {
            builder.auth(auth.into());
        }

        if let Some(timestamp_column) = timestamp_column {
            if let Some(timestamp_column_format) = timestamp_column_format {
                builder.time_window(&timestamp_column, &timestamp_column_format);
            } else {
                return Err(PyValueError::new_err(
                    "timestamp_column_format must not be omitted",
                ));
            }
        }

        if let Some(preprocessing) = preprocessing {
            builder.preprocessing(&preprocessing);
        }

        Ok(builder
            .build()
            .map_err(|e| PyValueError::new_err(format!("{:#?}", e)))?
            .into())
    }

    fn get_offline_features(
        &self,
        observation: &PyAny,
        feature_query: &PyList,
        output: &str,
    ) -> PyResult<u64> {
        let observation: ObservationSettings = observation.extract()?;
        let observation = observation.0;
        let mut queries: Vec<feathr_client::FeatureQuery> = vec![];
        for f in feature_query.into_iter() {
            let q = if let Ok(s) = f.extract::<String>() {
                feathr_client::FeatureQuery::by_name(&[&s])
            } else if let Ok(f) = f.extract::<FeatureQuery>() {
                f.0
            } else {
                return Err(PyValueError::new_err(format!(
                    "feature_query must be list of strings or FeatureQuery objects"
                )));
            };
            queries.push(q);
        }
        let queries: Vec<&feathr_client::FeatureQuery> = queries.iter().map(|q| q).collect();
        let request = self
            .0
            .feature_join_job(observation, &queries, output)
            .map_err(|e| PyRuntimeError::new_err(format!("{:#?}", e)))?
            .output_path(output)
            .build();
        let client = self.1 .0.clone();

        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                Ok(client
                    .submit_job(request)
                    .await
                    .map_err(|e| PyRuntimeError::new_err(format!("{:#?}", e)))?
                    .0)
            })
    }

    fn get_offline_features_async<'p>(
        &'p self,
        observation: &PyAny,
        feature_query: &PyList,
        output: &str,
        py: Python<'p>,
    ) -> PyResult<&'p PyAny> {
        let observation: ObservationSettings = observation.extract()?;
        let observation = observation.0;
        let mut queries: Vec<feathr_client::FeatureQuery> = vec![];
        for f in feature_query.into_iter() {
            let q = if let Ok(s) = f.extract::<String>() {
                feathr_client::FeatureQuery::by_name(&[&s])
            } else if let Ok(f) = f.extract::<FeatureQuery>() {
                f.0
            } else {
                return Err(PyValueError::new_err(format!(
                    "feature_query must be list of strings or FeatureQuery objects"
                )));
            };
            queries.push(q);
        }
        let queries: Vec<&feathr_client::FeatureQuery> = queries.iter().map(|q| q).collect();
        let request = self
            .0
            .feature_join_job(observation, &queries, output)
            .map_err(|e| PyRuntimeError::new_err(format!("{:#?}", e)))?
            .output_path(output)
            .build();
        let client = self.1 .0.clone();

        pyo3_asyncio::tokio::future_into_py(py, async move {
            Ok(client
                .submit_job(request)
                .await
                .map_err(|e| PyRuntimeError::new_err(format!("{:#?}", e)))?
                .0)
        })
    }

    #[args(step = "DateTimeResolution::Daily", sink = "None")]
    fn materialize_features(
        &self,
        features: &PyList,
        start: &PyDateTime,
        end: &PyDateTime,
        step: DateTimeResolution,
        sink: Option<RedisSink>,
    ) -> PyResult<Vec<u64>> {
        let mut feature_names: Vec<String> = vec![];
        for f in features.into_iter() {
            if let Ok(f) = f.extract::<AnchorFeature>() {
                feature_names.push(f.get_name());
            } else if let Ok(f) = f.extract::<DerivedFeature>() {
                feature_names.push(f.get_name());
            } else if let Ok(f) = f.extract::<String>() {
                feature_names.push(f);
            }
        }
        let start: pyo3_chrono::NaiveDateTime = start.extract()?;
        let start: DateTime<Utc> = Utc.from_utc_datetime(&start.0);
        let end: pyo3_chrono::NaiveDateTime = end.extract()?;
        let end: DateTime<Utc> = Utc.from_utc_datetime(&end.0);
        let sink = sink.map(|s| feathr_client::OutputSink::Redis(s.0));
        let mut builder = self
            .0
            .feature_gen_job(&feature_names, start, end, step.into())
            .map_err(|e| PyValueError::new_err(format!("{:#?}", e)))?;
        if let Some(sink) = sink {
            builder.sink(sink);
        }

        let request = builder
            .build()
            .map_err(|e| PyValueError::new_err(format!("{:#?}", e)))?;
        let client = self.1 .0.clone();

        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                let jobs_ids: Vec<u64> = client
                    .submit_jobs(request)
                    .await
                    .map_err(|e| PyRuntimeError::new_err(format!("{:#?}", e)))?
                    .into_iter()
                    .map(|job_id| job_id.0)
                    .collect();
                Ok(jobs_ids)
            })
    }

    #[args(step = "DateTimeResolution::Daily", sink = "None")]
    fn materialize_features_async<'p>(
        &'p self,
        features: &PyList,
        start: &PyDateTime,
        end: &PyDateTime,
        step: DateTimeResolution,
        sink: Option<RedisSink>,
        py: Python<'p>,
    ) -> PyResult<&'p PyAny> {
        let mut feature_names: Vec<String> = vec![];
        for f in features.into_iter() {
            if let Ok(f) = f.extract::<AnchorFeature>() {
                feature_names.push(f.get_name());
            } else if let Ok(f) = f.extract::<DerivedFeature>() {
                feature_names.push(f.get_name());
            } else if let Ok(f) = f.extract::<String>() {
                feature_names.push(f);
            }
        }
        let start: pyo3_chrono::NaiveDateTime = start.extract()?;
        let start: DateTime<Utc> = Utc.from_utc_datetime(&start.0);
        let end: pyo3_chrono::NaiveDateTime = end.extract()?;
        let end: DateTime<Utc> = Utc.from_utc_datetime(&end.0);
        let sink = sink.map(|s| feathr_client::OutputSink::Redis(s.0));
        let mut builder = self
            .0
            .feature_gen_job(&feature_names, start, end, step.into())
            .map_err(|e| PyValueError::new_err(format!("{:#?}", e)))?;
        if let Some(sink) = sink {
            builder.sink(sink);
        }

        let request = builder
            .build()
            .map_err(|e| PyValueError::new_err(format!("{:#?}", e)))?;
        let client = self.1 .0.clone();

        pyo3_asyncio::tokio::future_into_py(py, async move {
            let jobs_ids: Vec<u64> = client
                .submit_jobs(request)
                .await
                .map_err(|e| PyRuntimeError::new_err(format!("{:#?}", e)))?
                .into_iter()
                .map(|job_id| job_id.0)
                .collect();
            Ok(jobs_ids)
        })
    }

    #[allow(non_snake_case)]
    #[getter]
    pub fn INPUT_CONTEXT(&self) -> Source {
        self.0.INPUT_CONTEXT().into()
    }
}

impl Into<feathr_client::FeathrProject> for FeathrProject {
    fn into(self) -> feathr_client::FeathrProject {
        self.0
    }
}

#[pyclass]
#[derive(Clone)]
struct FeathrClient(feathr_client::FeathrClient);

#[pymethods]
impl FeathrClient {
    #[new]
    fn load(config_file: String) -> PyResult<Self> {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                feathr_client::FeathrClient::load(config_file)
                    .await
                    .map_err(|e| PyRuntimeError::new_err(format!("{:#?}", e)))
                    .map(|c| FeathrClient(c))
            })
    }

    #[staticmethod]
    fn load_async(config_file: String, py: Python<'_>) -> PyResult<&PyAny> {
        pyo3_asyncio::tokio::future_into_py(py, async move {
            feathr_client::FeathrClient::load(config_file)
                .await
                .map_err(|e| PyRuntimeError::new_err(format!("{:#?}", e)))
                .map(|c| FeathrClient(c))
        })
    }

    fn new_project(&self, name: &str) -> FeathrProject {
        FeathrProject(feathr_client::FeathrProject::new(name), self.clone())
    }

    #[args(timeout = "None")]
    fn wait_for_job(&self, job_id: u64, timeout: Option<i64>) -> PyResult<String> {
        let client = self.0.clone();
        let timeout = timeout.map(|s| Duration::seconds(s));
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                Ok(client
                    .wait_for_job(JobId(job_id), timeout)
                    .await
                    .map_err(|e| PyRuntimeError::new_err(format!("{:#?}", e)))?)
            })
    }

    #[args(timeout = "None")]
    fn wait_for_job_async<'p>(
        &'p self,
        id: u64,
        timeout: Option<i64>,
        py: Python<'p>,
    ) -> PyResult<&'p PyAny> {
        let client = self.0.clone();
        let timeout = timeout.map(|s| Duration::seconds(s));
        pyo3_asyncio::tokio::future_into_py(py, async move {
            Ok(client
                .wait_for_job(JobId(id), timeout)
                .await
                .map_err(|e| PyRuntimeError::new_err(format!("{:#?}", e)))?)
        })
    }

    #[args(timeout = "None")]
    fn wait_for_jobs(&self, job_id: Vec<u64>, timeout: Option<i64>) -> PyResult<Vec<String>> {
        let client = self.0.clone();
        let timeout = timeout.map(|s| Duration::seconds(s));
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                let jobs = job_id
                    .into_iter()
                    .map(|job_id| client.wait_for_job(JobId(job_id), timeout));
                let complete: Vec<String> = join_all(jobs)
                    .await
                    .into_iter()
                    .map(|r| r.unwrap_or_default())
                    .collect();
                Ok(complete)
            })
    }

    #[args(timeout = "None")]
    fn wait_for_jobs_async<'p>(
        &'p self,
        job_id: Vec<u64>,
        timeout: Option<i64>,
        py: Python<'p>,
    ) -> PyResult<&'p PyAny> {
        let client = self.0.clone();
        let timeout = timeout.map(|s| Duration::seconds(s));
        pyo3_asyncio::tokio::future_into_py(py, async move {
            let jobs = job_id
                .into_iter()
                .map(|job_id| client.wait_for_job(JobId(job_id), timeout));
            let complete: Vec<String> = join_all(jobs)
                .await
                .into_iter()
                .map(|r| r.unwrap_or_default())
                .collect();
            Ok(complete)
        })
    }

    pub fn get_job_status(&self, job_id: u64) -> PyResult<JobStatus> {
        let client = self.0.clone();
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                let status: JobStatus = client
                    .get_job_status(feathr_client::JobId(job_id))
                    .await
                    .map_err(|e| PyRuntimeError::new_err(format!("{:#?}", e)))?
                    .into();
                Ok(status)
            })
    }

    pub fn get_job_status_async<'p>(&'p self, job_id: u64, py: Python<'p>) -> PyResult<&'p PyAny> {
        let client = self.0.clone();
        pyo3_asyncio::tokio::future_into_py(py, async move {
            let status: JobStatus = client
                .get_job_status(feathr_client::JobId(job_id))
                .await
                .map_err(|e| PyRuntimeError::new_err(format!("{:#?}", e)))?
                .into();
            Ok(status)
        })
    }

    pub fn get_remote_url(&self, path: &str) -> String {
        self.0.get_remote_url(path)
    }
}

#[pyfunction]
fn load(config_file: String) -> PyResult<FeathrClient> {
    FeathrClient::load(config_file)
}

/// A Python module implemented in Rust.
#[pymodule]
fn feathrs(_py: Python, m: &PyModule) -> PyResult<()> {
    pyo3_log::init();
    m.add_class::<ValueType>()?;
    m.add_class::<VectorType>()?;
    m.add_class::<TensorCategory>()?;
    m.add_class::<FeatureType>()?;
    m.add_class::<TypedKey>()?;
    m.add_class::<Aggregation>()?;
    m.add_class::<Transformation>()?;
    m.add_class::<Source>()?;
    m.add_class::<JdbcSourceAuth>()?;
    m.add_class::<AnchorFeature>()?;
    m.add_class::<DerivedFeature>()?;
    m.add_class::<AnchorGroup>()?;
    m.add_class::<FeatureQuery>()?;
    m.add_class::<ObservationSettings>()?;
    m.add_class::<DateTimeResolution>()?;
    m.add_class::<RedisSink>()?;
    m.add_class::<JobStatus>()?;
    m.add_class::<FeathrProject>()?;
    m.add_class::<FeathrClient>()?;
    m.add_function(wrap_pyfunction!(load, m)?)?;
    Ok(())
}
