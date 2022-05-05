# AzureAttributes

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**first_on_demand** | Option<**i32**> | The first `first_on_demand` nodes of the cluster are placed on on-demand instances. This value must be greater than 0, or else cluster creation validation fails. If this value is greater than or equal to the current cluster size, all nodes are placed on on-demand instances. If this value is less than the current cluster size, `first_on_demand` nodes are placed on on-demand instances and the remainder are placed on availability instances. This value does not affect cluster size and cannot be mutated over the lifetime of a cluster. | [optional]
**availability** | Option<**String**> | Availability type used for all subsequent nodes past the `first_on_demand` ones.  `SPOT_AZURE`: use spot instances. `ON_DEMAND_AZURE`: use on demand instances. `SPOT_WITH_FALLBACK_AZURE`: preferably use spot instances, but fall back to on-demand instances if spot instances cannot be acquired (for example, if Azure spot prices are too high or out of quota). Does not apply to pool availability. | [optional]
**spot_bid_max_price** | Option<**f64**> | The max bid price used for Azure spot instances. You can set this to greater than or equal to the current spot price. You can also set this to -1 (the default), which specifies that the instance cannot be evicted on the basis of price. The price for the instance is the current price for spot instances or the price for a standard instance. You can view historical pricing and eviction rates in the Azure portal. | [optional]

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


