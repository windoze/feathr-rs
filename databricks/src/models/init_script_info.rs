/*
 * Jobs API 2.1
 *
 * The Jobs API allows you to create, edit, and delete jobs.
 *
 * The version of the OpenAPI document: 2.1
 * 
 * Generated by: https://openapi-generator.tech
 */




#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct InitScriptInfo {
    #[serde(rename = "dbfs", skip_serializing_if = "Option::is_none")]
    pub dbfs: Option<Box<crate::models::DbfsStorageInfo>>,
    #[serde(rename = "file", skip_serializing_if = "Option::is_none")]
    pub file: Option<Box<crate::models::FileStorageInfo>>,
}

impl InitScriptInfo {
    pub fn new() -> InitScriptInfo {
        InitScriptInfo {
            dbfs: None,
            file: None,
        }
    }
}


