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
pub struct NotebookOutput {
    /// The value passed to [dbutils.notebook.exit()](https://docs.microsoft.com/azure/databricks/notebooks/notebook-workflows#notebook-workflows-exit). Azure Databricks restricts this API to return the first 5 MB of the value. For a larger result, your job can store the results in a cloud storage service. This field is absent if `dbutils.notebook.exit()` was never called.
    #[serde(rename = "result", skip_serializing_if = "Option::is_none")]
    pub result: Option<String>,
    /// Whether or not the result was truncated.
    #[serde(rename = "truncated", skip_serializing_if = "Option::is_none")]
    pub truncated: Option<bool>,
}

impl NotebookOutput {
    pub fn new() -> NotebookOutput {
        NotebookOutput {
            result: None,
            truncated: None,
        }
    }
}

