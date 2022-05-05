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
pub struct SparkPythonTask {
    /// The URI of the Python file to be executed. DBFS paths are supported. This field is required.
    #[serde(rename = "python_file")]
    pub python_file: String,
    /// Command line parameters passed to the Python file.  Use [Task parameter variables](https://docs.microsoft.com/azure/databricks/jobs#parameter-variables) to set parameters containing information about job runs.
    #[serde(rename = "parameters", skip_serializing_if = "Option::is_none")]
    pub parameters: Option<Vec<String>>,
}

impl SparkPythonTask {
    pub fn new(python_file: String) -> SparkPythonTask {
        SparkPythonTask {
            python_file,
            parameters: None,
        }
    }
}


