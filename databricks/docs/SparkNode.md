# SparkNode

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**private_ip** | Option<**String**> | Private IP address (typically a 10.x.x.x address) of the Spark node. This is different from the private IP address of the host instance. | [optional]
**public_dns** | Option<**String**> | Public DNS address of this node. This address can be used to access the Spark JDBC server on the driver node. | [optional]
**node_id** | Option<**String**> | Globally unique identifier for this node. | [optional]
**instance_id** | Option<**String**> | Globally unique identifier for the host instance from the cloud provider. | [optional]
**start_timestamp** | Option<**i64**> | The timestamp (in millisecond) when the Spark node is launched. | [optional]
**host_private_ip** | Option<**String**> | The private IP address of the host instance. | [optional]

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


