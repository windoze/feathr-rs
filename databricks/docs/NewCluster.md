# NewCluster

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**num_workers** | Option<**i32**> | If num_workers, number of worker nodes that this cluster must have. A cluster has one Spark driver and num_workers executors for a total of num_workers + 1 Spark nodes. When reading the properties of a cluster, this field reflects the desired number of workers rather than the actual current number of workers. For example, if a cluster is resized from 5 to 10 workers, this field immediately updates to reflect the target size of 10 workers, whereas the workers listed in `spark_info` gradually increase from 5 to 10 as the new nodes are provisioned. | [optional]
**autoscale** | Option<[**crate::models::AutoScale**](AutoScale.md)> |  | [optional]
**spark_version** | **String** | The Spark version of the cluster. A list of available Spark versions can be retrieved by using the [Runtime versions](https://docs.microsoft.com/azure/databricks/dev-tools/api/latest/clusters#runtime-versions) API call. This field is required. | 
**spark_conf** | Option<[**::std::collections::HashMap<String, serde_json::Value>**](serde_json::Value.md)> | An arbitrary object where the object key is a configuration propery name and the value is a configuration property value. | [optional]
**azure_attributes** | Option<[**crate::models::AzureAttributes**](AzureAttributes.md)> |  | [optional]
**node_type_id** | **String** | This field encodes, through a single value, the resources available to each of the Spark nodes in this cluster. For example, the Spark nodes can be provisioned and optimized for memory or compute intensive workloads A list of available node types can be retrieved by using the [List node types](https://docs.microsoft.com/azure/databricks/dev-tools/api/latest/clusters#list-node-types) API call. This field is required. | 
**driver_node_type_id** | Option<**String**> | The node type of the Spark driver. This field is optional; if unset, the driver node type is set as the same value as `node_type_id` defined above. | [optional]
**custom_tags** | Option<**::std::collections::HashMap<String, String>**> | An object with key value pairs. The key length must be between 1 and 127 UTF-8 characters, inclusive. The value length must be less than or equal to 255 UTF-8 characters. | [optional]
**cluster_log_conf** | Option<[**crate::models::ClusterLogConf**](ClusterLogConf.md)> |  | [optional]
**init_scripts** | Option<[**Vec<crate::models::InitScriptInfo>**](InitScriptInfo.md)> | The configuration for storing init scripts. Any number of scripts can be specified. The scripts are executed sequentially in the order provided. If `cluster_log_conf` is specified, init script logs are sent to `<destination>/<cluster-id>/init_scripts`. | [optional]
**spark_env_vars** | Option<[**::std::collections::HashMap<String, serde_json::Value>**](serde_json::Value.md)> | An arbitrary object where the object key is an environment variable name and the value is an environment variable value. | [optional]
**enable_elastic_disk** | Option<**bool**> | Autoscaling Local Storage: when enabled, this cluster dynamically acquires additional disk space when its Spark workers are running low on disk space. Refer to [Autoscaling local storage](https://docs.microsoft.com/azure/databricks/clusters/configure#autoscaling-local-storage-azure) for details. | [optional]
**instance_pool_id** | Option<**String**> | The optional ID of the instance pool to use for cluster nodes. If `driver_instance_pool_id` is present, `instance_pool_id` is used for worker nodes only. Otherwise, it is used for both the driver node and worker nodes. Refer to [Instance Pools API](https://docs.microsoft.com/azure/databricks/dev-tools/api/latest/instance-pools) for details. | [optional]
**policy_id** | Option<**String**> | A [cluster policy](https://docs.microsoft.com/azure/databricks/dev-tools/api/latest/policies) ID. | [optional]

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


