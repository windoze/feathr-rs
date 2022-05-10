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
pub struct TerminationParameter {
    /// The username of the user who terminated the cluster.
    #[serde(rename = "username", skip_serializing_if = "Option::is_none")]
    pub username: Option<String>,
    /// The Azure provided error code describing why cluster nodes could not be provisioned. For reference, see: [https://docs.microsoft.com/azure/virtual-machines/windows/error-messages](https://docs.microsoft.com/azure/virtual-machines/windows/error-messages).
    #[serde(rename = "azure_error_code", skip_serializing_if = "Option::is_none")]
    pub azure_error_code: Option<String>,
    /// Human-readable context of various failures from Azure. This field is unstructured, and its exact format is subject to change.
    #[serde(rename = "azure_error_message", skip_serializing_if = "Option::is_none")]
    pub azure_error_message: Option<String>,
    /// Additional context that may explain the reason for cluster termination. This field is unstructured, and its exact format is subject to change.
    #[serde(rename = "databricks_error_message", skip_serializing_if = "Option::is_none")]
    pub databricks_error_message: Option<String>,
    /// An idle cluster was shut down after being inactive for this duration.
    #[serde(rename = "inactivity_duration_min", skip_serializing_if = "Option::is_none")]
    pub inactivity_duration_min: Option<String>,
    /// The ID of the instance that was hosting the Spark driver.
    #[serde(rename = "instance_id", skip_serializing_if = "Option::is_none")]
    pub instance_id: Option<String>,
    /// The ID of the instance pool the cluster is using.
    #[serde(rename = "instance_pool_id", skip_serializing_if = "Option::is_none")]
    pub instance_pool_id: Option<String>,
    /// The [error code](https://docs.microsoft.com/azure/databricks/dev-tools/api/latest/clusters#clusterterminationreasonpoolclusterterminationcode) for cluster failures specific to a pool.
    #[serde(rename = "instance_pool_error_code", skip_serializing_if = "Option::is_none")]
    pub instance_pool_error_code: Option<String>,
}

impl TerminationParameter {
    pub fn new() -> TerminationParameter {
        TerminationParameter {
            username: None,
            azure_error_code: None,
            azure_error_message: None,
            databricks_error_message: None,
            inactivity_duration_min: None,
            instance_id: None,
            instance_pool_id: None,
            instance_pool_error_code: None,
        }
    }
}

