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
pub struct InlineObject2 {
    /// The canonical identifier of the job to delete. This field is required.
    #[serde(rename = "job_id")]
    pub job_id: i64,
}

impl InlineObject2 {
    pub fn new(job_id: i64) -> InlineObject2 {
        InlineObject2 {
            job_id,
        }
    }
}


