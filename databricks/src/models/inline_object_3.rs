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
pub struct InlineObject3 {
    /// This field is required.
    #[serde(rename = "run_id")]
    pub run_id: i64,
}

impl InlineObject3 {
    pub fn new(run_id: i64) -> InlineObject3 {
        InlineObject3 {
            run_id,
        }
    }
}

