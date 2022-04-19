use std::sync::{Arc, RwLock};

use serde::{ser::SerializeStruct, Deserialize, Serialize};

use crate::{
    client::{FeathrClientImpl, FeathrClientModifier},
    Error,
};

#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(untagged)]
enum JdbcAuth {
    Userpass { user: String, password: String },
    Token { token: String },
    Anonymous,
}

impl Serialize for JdbcAuth {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match &self {
            JdbcAuth::Anonymous => {
                let mut state = serializer.serialize_struct("JdbcAuth", 2)?;
                state.serialize_field("type", "jdbc")?;
                state.serialize_field("anonymous", &true)?;
                state.end()
            }
            JdbcAuth::Userpass { user, password } => {
                let mut state = serializer.serialize_struct("JdbcAuth", 3)?;
                state.serialize_field("type", "jdbc")?;
                state.serialize_field("user", &user)?;
                state.serialize_field("password", &password)?;
                state.end()
            }
            JdbcAuth::Token { token } => {
                let mut state = serializer.serialize_struct("JdbcAuth", 4)?;
                state.serialize_field("type", "jdbc")?;
                state.serialize_field("token", &token)?;
                state.serialize_field("useToken", &true)?;
                state.end()
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
#[serde(rename_all = "camelCase")]
enum SourceLocation {
    Hdfs {
        path: String,
    },
    Jdbc {
        url: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        dbtable: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        query: Option<String>,
        #[serde(flatten)]
        auth: JdbcAuth,
    },
    InputContext,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TimeWindowParameters {
    timestamp_column: String,
    timestamp_column_format: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SourceImpl {
    #[serde(skip)]
    pub(crate) name: String,
    location: SourceLocation,
    #[serde(skip_serializing_if = "Option::is_none")]
    time_window_parameters: Option<TimeWindowParameters>,
}

impl SourceImpl {
    #[allow(non_snake_case)]
    pub(crate) fn INPUT_CONTEXT() -> SourceImpl {
        SourceImpl {
            name: "PASSTHROUGH".to_string(),
            location: SourceLocation::InputContext,
            time_window_parameters: None,
        }
    }

    pub(crate) fn get_secret_keys(&self) -> Vec<String> {
        match &self.location {
            SourceLocation::Jdbc { auth, .. } => match auth {
                JdbcAuth::Userpass { .. } => vec![
                    format!("{}_USER", self.name),
                    format!("{}_PASSWORD", self.name),
                ],
                JdbcAuth::Token { .. } => vec![
                    format!("{}_TOKEN", self.name),
                ],
                _ => vec![],
            },
            _ => vec![],
        }
    }
}

#[derive(Clone, Debug)]
pub struct Source {
    pub(crate) inner: Arc<SourceImpl>,
}

impl Source {
    pub fn get_name(&self) -> String {
        self.inner.name.clone()
    }

    pub fn get_secret_keys(&self) -> Vec<String> {
        self.inner.get_secret_keys()
    }
}

pub struct HdfsSourceBuilder {
    owner: Arc<RwLock<FeathrClientImpl>>,
    name: String,
    path: Option<String>,
    time_window_parameters: Option<TimeWindowParameters>,
}

impl HdfsSourceBuilder {
    pub(crate) fn new(owner: Arc<RwLock<FeathrClientImpl>>, name: &str) -> Self {
        Self {
            owner,
            name: name.to_string(),
            path: None,
            time_window_parameters: None,
        }
    }

    pub fn set_path(&mut self, path: &str) -> &mut Self {
        self.path = Some(path.to_string());
        self
    }

    pub fn set_time_window_parameters(
        &mut self,
        timestamp_column: &str,
        timestamp_column_format: &str,
    ) -> &mut Self {
        self.time_window_parameters = Some(TimeWindowParameters {
            timestamp_column: timestamp_column.to_string(),
            timestamp_column_format: timestamp_column_format.to_string(),
        });
        self
    }

    pub fn build(&self) -> Result<Source, Error> {
        let imp = SourceImpl {
            name: self.name.to_string(),
            location: SourceLocation::Hdfs {
                path: self.path.clone().ok_or(Error::MissingHdfsUrl(self.name.to_string()))?,
            },
            time_window_parameters: self.time_window_parameters.clone(),
        };
        self.owner.insert_source(imp)
    }
}
pub struct JdbcSourceBuilder {
    owner: Arc<RwLock<FeathrClientImpl>>,
    name: String,
    url: Option<String>,
    dbtable: Option<String>,
    query: Option<String>,
    auth: Option<JdbcAuth>,
    time_window_parameters: Option<TimeWindowParameters>,
}

#[derive(Clone, Copy, Debug)]
pub enum JdbcSourceAuth {
    Anonymous,
    Userpass,
    Token,
}

impl JdbcSourceBuilder {
    pub(crate) fn new(owner: Arc<RwLock<FeathrClientImpl>>, name: &str) -> Self {
        Self {
            owner,
            name: name.to_string(),
            url: None,
            dbtable: None,
            query: None,
            auth: None,
            time_window_parameters: None,
        }
    }

    pub fn set_url(&mut self, url: &str) -> &mut Self {
        self.url = Some(url.to_string());
        self
    }

    pub fn set_dbtable(&mut self, dbtable: &str) -> &mut Self {
        self.dbtable = Some(dbtable.to_string());
        self
    }

    pub fn set_query(&mut self, query: &str) -> &mut Self {
        self.query = Some(query.to_string());
        self
    }

    pub fn set_auth(&mut self, auth: JdbcSourceAuth) -> &mut Self {
        match auth {
            JdbcSourceAuth::Anonymous => self.auth = Some(JdbcAuth::Anonymous),
            JdbcSourceAuth::Userpass => {
                self.auth = Some(JdbcAuth::Userpass {
                    user: format!("${{{}_USER}}", self.name),
                    password: format!("${{{}_PASSWORD}}", self.name),
                })
            }
            JdbcSourceAuth::Token => {
                self.auth = Some(JdbcAuth::Token {
                    token: format!("${{{}_TOKEN}}", self.name),
                })
            }
        }
        self
    }

    pub fn set_time_window_parameters(
        &mut self,
        timestamp_column: &str,
        timestamp_column_format: &str,
    ) -> &mut Self {
        self.time_window_parameters = Some(TimeWindowParameters {
            timestamp_column: timestamp_column.to_string(),
            timestamp_column_format: timestamp_column_format.to_string(),
        });
        self
    }

    pub fn build(&self) -> Result<Source, Error> {
        let imp = SourceImpl {
            name: self.name.to_string(),
            location: SourceLocation::Jdbc {
                url: self.url.clone().ok_or(Error::MissingJdbcUrl(self.name.to_string()))?,
                dbtable: self.dbtable.to_owned(),
                query: self.query.to_owned(),
                auth: self.auth.clone().unwrap_or(JdbcAuth::Anonymous),
            },
            time_window_parameters: self.time_window_parameters.clone(),
        };
        self.owner.insert_source(imp)
    }
}
