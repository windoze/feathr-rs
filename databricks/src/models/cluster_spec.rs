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
pub struct ClusterSpec {
    /// If existing_cluster_id, the ID of an existing cluster that is used for all runs of this job. When running jobs on an existing cluster, you may need to manually restart the cluster if it stops responding. We suggest running jobs on new clusters for greater reliability.
    #[serde(rename = "existing_cluster_id", skip_serializing_if = "Option::is_none")]
    pub existing_cluster_id: Option<String>,
    #[serde(rename = "new_cluster", skip_serializing_if = "Option::is_none")]
    pub new_cluster: Option<Box<crate::models::NewCluster>>,
    /// An optional list of libraries to be installed on the cluster that executes the job. The default value is an empty list.
    #[serde(rename = "libraries", skip_serializing_if = "Option::is_none")]
    pub libraries: Option<Vec<crate::models::Library>>,
}

impl ClusterSpec {
    pub fn new() -> ClusterSpec {
        ClusterSpec {
            existing_cluster_id: None,
            new_cluster: None,
            libraries: None,
        }
    }
}


