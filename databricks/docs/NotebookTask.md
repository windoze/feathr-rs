# NotebookTask

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**notebook_path** | **String** | The absolute path of the notebook to be run in the Azure Databricks workspace. This path must begin with a slash. This field is required. | 
**base_parameters** | Option<[**::std::collections::HashMap<String, serde_json::Value>**](serde_json::Value.md)> | Base parameters to be used for each run of this job. If the run is initiated by a call to [`run-now`](https://docs.microsoft.com/azure/databricks/dev-tools/api/latest/jobs#operation/JobsRunNow) with parameters specified, the two parameters maps are merged. If the same key is specified in `base_parameters` and in `run-now`, the value from `run-now` is used.  Use [Task parameter variables](https://docs.microsoft.com/azure/databricks/jobs#parameter-variables) to set parameters containing information about job runs.  If the notebook takes a parameter that is not specified in the job’s `base_parameters` or the `run-now` override parameters, the default value from the notebook is used.  Retrieve these parameters in a notebook using [dbutils.widgets.get](https://docs.microsoft.com/azure/databricks/dev-tools/databricks-utils#dbutils-widgets). | [optional]

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


