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
pub struct InlineObject1 {
    /// The canonical identifier of the job to update. This field is required.
    #[serde(rename = "job_id")]
    pub job_id: i64,
    #[serde(rename = "new_settings", skip_serializing_if = "Option::is_none")]
    pub new_settings: Option<Box<crate::models::JobSettings>>,
    /// Remove top-level fields in the job settings. Removing nested fields is not supported. This field is optional.
    #[serde(rename = "fields_to_remove", skip_serializing_if = "Option::is_none")]
    pub fields_to_remove: Option<Vec<String>>,
}

impl InlineObject1 {
    pub fn new(job_id: i64) -> InlineObject1 {
        InlineObject1 {
            job_id,
            new_settings: None,
            fields_to_remove: None,
        }
    }
}


