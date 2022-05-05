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
pub struct InlineResponse2006 {
    /// The exported content in HTML format (one for every view item).
    #[serde(rename = "views", skip_serializing_if = "Option::is_none")]
    pub views: Option<Vec<crate::models::ViewItem>>,
}

impl InlineResponse2006 {
    pub fn new() -> InlineResponse2006 {
        InlineResponse2006 {
            views: None,
        }
    }
}


