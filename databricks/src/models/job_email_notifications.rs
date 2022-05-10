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
pub struct JobEmailNotifications {
    /// A list of email addresses to be notified when a run begins. If not specified on job creation, reset, or update, the list is empty, and notifications are not sent.
    #[serde(rename = "on_start", skip_serializing_if = "Option::is_none")]
    pub on_start: Option<Vec<String>>,
    /// A list of email addresses to be notified when a run successfully completes. A run is considered to have completed successfully if it ends with a `TERMINATED` `life_cycle_state` and a `SUCCESSFUL` result_state. If not specified on job creation, reset, or update, the list is empty, and notifications are not sent.
    #[serde(rename = "on_success", skip_serializing_if = "Option::is_none")]
    pub on_success: Option<Vec<String>>,
    /// A list of email addresses to be notified when a run unsuccessfully completes. A run is considered to have completed unsuccessfully if it ends with an `INTERNAL_ERROR` `life_cycle_state` or a `SKIPPED`, `FAILED`, or `TIMED_OUT` result_state. If this is not specified on job creation, reset, or update the list is empty, and notifications are not sent.
    #[serde(rename = "on_failure", skip_serializing_if = "Option::is_none")]
    pub on_failure: Option<Vec<String>>,
    /// If true, do not send email to recipients specified in `on_failure` if the run is skipped.
    #[serde(rename = "no_alert_for_skipped_runs", skip_serializing_if = "Option::is_none")]
    pub no_alert_for_skipped_runs: Option<bool>,
}

impl JobEmailNotifications {
    pub fn new() -> JobEmailNotifications {
        JobEmailNotifications {
            on_start: None,
            on_success: None,
            on_failure: None,
            no_alert_for_skipped_runs: None,
        }
    }
}

