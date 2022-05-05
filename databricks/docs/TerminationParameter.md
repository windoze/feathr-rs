# TerminationParameter

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**username** | Option<**String**> | The username of the user who terminated the cluster. | [optional]
**azure_error_code** | Option<**String**> | The Azure provided error code describing why cluster nodes could not be provisioned. For reference, see: [https://docs.microsoft.com/azure/virtual-machines/windows/error-messages](https://docs.microsoft.com/azure/virtual-machines/windows/error-messages). | [optional]
**azure_error_message** | Option<**String**> | Human-readable context of various failures from Azure. This field is unstructured, and its exact format is subject to change. | [optional]
**databricks_error_message** | Option<**String**> | Additional context that may explain the reason for cluster termination. This field is unstructured, and its exact format is subject to change. | [optional]
**inactivity_duration_min** | Option<**String**> | An idle cluster was shut down after being inactive for this duration. | [optional]
**instance_id** | Option<**String**> | The ID of the instance that was hosting the Spark driver. | [optional]
**instance_pool_id** | Option<**String**> | The ID of the instance pool the cluster is using. | [optional]
**instance_pool_error_code** | Option<**String**> | The [error code](https://docs.microsoft.com/azure/databricks/dev-tools/api/latest/clusters#clusterterminationreasonpoolclusterterminationcode) for cluster failures specific to a pool. | [optional]

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


