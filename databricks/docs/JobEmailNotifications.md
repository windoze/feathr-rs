# JobEmailNotifications

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**on_start** | Option<**Vec<String>**> | A list of email addresses to be notified when a run begins. If not specified on job creation, reset, or update, the list is empty, and notifications are not sent. | [optional]
**on_success** | Option<**Vec<String>**> | A list of email addresses to be notified when a run successfully completes. A run is considered to have completed successfully if it ends with a `TERMINATED` `life_cycle_state` and a `SUCCESSFUL` result_state. If not specified on job creation, reset, or update, the list is empty, and notifications are not sent. | [optional]
**on_failure** | Option<**Vec<String>**> | A list of email addresses to be notified when a run unsuccessfully completes. A run is considered to have completed unsuccessfully if it ends with an `INTERNAL_ERROR` `life_cycle_state` or a `SKIPPED`, `FAILED`, or `TIMED_OUT` result_state. If this is not specified on job creation, reset, or update the list is empty, and notifications are not sent. | [optional]
**no_alert_for_skipped_runs** | Option<**bool**> | If true, do not send email to recipients specified in `on_failure` if the run is skipped. | [optional]

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


