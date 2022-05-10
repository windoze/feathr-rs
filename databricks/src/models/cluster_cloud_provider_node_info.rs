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
pub struct ClusterCloudProviderNodeInfo {
    #[serde(rename = "status", skip_serializing_if = "Option::is_none")]
    pub status: Option<crate::models::ClusterCloudProviderNodeStatus>,
    /// Available CPU core quota.
    #[serde(rename = "available_core_quota", skip_serializing_if = "Option::is_none")]
    pub available_core_quota: Option<i32>,
    /// Total CPU core quota.
    #[serde(rename = "total_core_quota", skip_serializing_if = "Option::is_none")]
    pub total_core_quota: Option<i32>,
}

impl ClusterCloudProviderNodeInfo {
    pub fn new() -> ClusterCloudProviderNodeInfo {
        ClusterCloudProviderNodeInfo {
            status: None,
            available_core_quota: None,
            total_core_quota: None,
        }
    }
}

