/*
 * Jobs API 2.1
 *
 * The Jobs API allows you to create, edit, and delete jobs.
 *
 * The version of the OpenAPI document: 2.1
 * 
 * Generated by: https://openapi-generator.tech
 */

/// LibraryInstallStatus : * `PENDING`: No action has yet been taken to install the library. This state should be very short lived. * `RESOLVING`: Metadata necessary to install the library is being retrieved from the provided repository. For Jar, Egg, and Whl libraries, this step is a no-op. * `INSTALLING`: The library is actively being installed, either by adding resources to Spark or executing system commands inside the Spark nodes. * `INSTALLED`: The library has been successfully instally. * `SKIPPED`: Installation on a Databricks Runtime 7.0 or above cluster was skipped due to Scala version incompatibility. * `FAILED`: Some step in installation failed. More information can be found in the messages field. * `UNINSTALL_ON_RESTART`: The library has been marked for removal. Libraries can be removed only when clusters are restarted, so libraries that enter this state remains until the cluster is restarted.

/// * `PENDING`: No action has yet been taken to install the library. This state should be very short lived. * `RESOLVING`: Metadata necessary to install the library is being retrieved from the provided repository. For Jar, Egg, and Whl libraries, this step is a no-op. * `INSTALLING`: The library is actively being installed, either by adding resources to Spark or executing system commands inside the Spark nodes. * `INSTALLED`: The library has been successfully instally. * `SKIPPED`: Installation on a Databricks Runtime 7.0 or above cluster was skipped due to Scala version incompatibility. * `FAILED`: Some step in installation failed. More information can be found in the messages field. * `UNINSTALL_ON_RESTART`: The library has been marked for removal. Libraries can be removed only when clusters are restarted, so libraries that enter this state remains until the cluster is restarted.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub enum LibraryInstallStatus {
    #[serde(rename = "PENDING")]
    PENDING,
    #[serde(rename = "RESOLVING")]
    RESOLVING,
    #[serde(rename = "INSTALLING")]
    INSTALLING,
    #[serde(rename = "INSTALLED")]
    INSTALLED,
    #[serde(rename = "SKIPPED")]
    SKIPPED,
    #[serde(rename = "FAILED")]
    FAILED,
    #[serde(rename = "UNINSTALL_ON_RESTART")]
    UNINSTALLONRESTART,

}

impl ToString for LibraryInstallStatus {
    fn to_string(&self) -> String {
        match self {
            Self::PENDING => String::from("PENDING"),
            Self::RESOLVING => String::from("RESOLVING"),
            Self::INSTALLING => String::from("INSTALLING"),
            Self::INSTALLED => String::from("INSTALLED"),
            Self::SKIPPED => String::from("SKIPPED"),
            Self::FAILED => String::from("FAILED"),
            Self::UNINSTALLONRESTART => String::from("UNINSTALL_ON_RESTART"),
        }
    }
}

impl Default for LibraryInstallStatus {
    fn default() -> LibraryInstallStatus {
        Self::PENDING
    }
}



