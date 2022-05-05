# InlineResponse2007

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**notebook_output** | Option<[**crate::models::NotebookOutput**](NotebookOutput.md)> |  | [optional]
**logs** | Option<**String**> | The output from tasks that write to cluster logs such as [SparkJarTask](https://docs.microsoft.com/azure/databricks/dev-tools/api/latest/jobs#/components/schemas/SparkJarTask) or [SparkPythonTask](https://docs.microsoft.com/azure/databricks/dev-tools/api/latest/jobs#/components/schemas/SparkPythonTask. Azure Databricks restricts this API to return the last 5 MB of these logs. To return a larger result, use the [ClusterLogConf](https://docs.microsoft.com/azure/databricks/dev-tools/api/latest/clusters#clusterlogconf) field to configure log storage for the job cluster. | [optional]
**logs_truncated** | Option<**bool**> | Whether the logs are truncated. | [optional]
**error** | Option<**String**> | An error message indicating why a task failed or why output is not available. The message is unstructured, and its exact format is subject to change. | [optional]
**error_trace** | Option<**String**> | If there was an error executing the run, this field contains any available stack traces. | [optional]
**metadata** | Option<[**crate::models::Run**](Run.md)> |  | [optional]

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


