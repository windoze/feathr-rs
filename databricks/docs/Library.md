# Library

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**jar** | Option<**String**> | If jar, URI of the JAR to be installed. DBFS and ADLS (`abfss`) URIs are supported. For example: `{ \"jar\": \"dbfs:/mnt/databricks/library.jar\" }` or `{ \"jar\": \"abfss://my-bucket/library.jar\" }`. If ADLS is used, make sure the cluster has read access on the library. | [optional]
**egg** | Option<**String**> | If egg, URI of the egg to be installed. DBFS and ADLS URIs are supported. For example: `{ \"egg\": \"dbfs:/my/egg\" }` or `{ \"egg\": \"abfss://my-bucket/egg\" }`. | [optional]
**whl** | Option<**String**> | If whl, URI of the wheel or zipped wheels to be installed. DBFS and ADLS URIs are supported. For example: `{ \"whl\": \"dbfs:/my/whl\" }` or `{ \"whl\": \"abfss://my-bucket/whl\" }`. If ADLS is used, make sure the cluster has read access on the library. Also the wheel file name needs to use the [correct convention](https://www.python.org/dev/peps/pep-0427/#file-format). If zipped wheels are to be installed, the file name suffix should be `.wheelhouse.zip`. | [optional]
**pypi** | Option<[**crate::models::PythonPyPiLibrary**](PythonPyPiLibrary.md)> |  | [optional]
**maven** | Option<[**crate::models::MavenLibrary**](MavenLibrary.md)> |  | [optional]
**cran** | Option<[**crate::models::RCranLibrary**](RCranLibrary.md)> |  | [optional]

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


