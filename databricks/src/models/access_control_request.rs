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
pub struct AccessControlRequest {
    /// Email address for the user.
    #[serde(rename = "user_name", skip_serializing_if = "Option::is_none")]
    pub user_name: Option<String>,
    #[serde(rename = "permission_level", skip_serializing_if = "Option::is_none")]
    pub permission_level: Option<Box<crate::models::PermissionLevel>>,
    /// Group name. There are two built-in groups: `users` for all users, and `admins` for administrators.
    #[serde(rename = "group_name", skip_serializing_if = "Option::is_none")]
    pub group_name: Option<String>,
    /// Name of an Azure service principal.
    #[serde(rename = "service_principal_name", skip_serializing_if = "Option::is_none")]
    pub service_principal_name: Option<String>,
}

impl AccessControlRequest {
    pub fn new() -> AccessControlRequest {
        AccessControlRequest {
            user_name: None,
            permission_level: None,
            group_name: None,
            service_principal_name: None,
        }
    }
}


