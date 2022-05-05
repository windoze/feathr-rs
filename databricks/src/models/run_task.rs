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
pub struct RunTask {
    /// The ID of the task run.
    #[serde(rename = "run_id", skip_serializing_if = "Option::is_none")]
    pub run_id: Option<i64>,
    /// A unique name for the task. This field is used to refer to this task from other tasks. This field is required and must be unique within its parent job. On Update or Reset, this field is used to reference the tasks to be updated or reset. The maximum length is 100 characters.
    #[serde(rename = "task_key", skip_serializing_if = "Option::is_none")]
    pub task_key: Option<String>,
    /// An optional description for this task. The maximum length is 4096 bytes.
    #[serde(rename = "description", skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(rename = "state", skip_serializing_if = "Option::is_none")]
    pub state: Option<Box<crate::models::RunState>>,
    /// An optional array of objects specifying the dependency graph of the task. All tasks specified in this field must complete successfully before executing this task. The key is `task_key`, and the value is the name assigned to the dependent task. This field is required when a job consists of more than one task.
    #[serde(rename = "depends_on", skip_serializing_if = "Option::is_none")]
    pub depends_on: Option<Vec<serde_json::Value>>,
    /// If existing_cluster_id, the ID of an existing cluster that is used for all runs of this job. When running jobs on an existing cluster, you may need to manually restart the cluster if it stops responding. We suggest running jobs on new clusters for greater reliability.
    #[serde(rename = "existing_cluster_id", skip_serializing_if = "Option::is_none")]
    pub existing_cluster_id: Option<String>,
    #[serde(rename = "new_cluster", skip_serializing_if = "Option::is_none")]
    pub new_cluster: Option<Box<crate::models::NewCluster>>,
    /// An optional list of libraries to be installed on the cluster that executes the job. The default value is an empty list.
    #[serde(rename = "libraries", skip_serializing_if = "Option::is_none")]
    pub libraries: Option<Vec<crate::models::Library>>,
    #[serde(rename = "notebook_task", skip_serializing_if = "Option::is_none")]
    pub notebook_task: Option<Box<crate::models::NotebookTask>>,
    #[serde(rename = "spark_jar_task", skip_serializing_if = "Option::is_none")]
    pub spark_jar_task: Option<Box<crate::models::SparkJarTask>>,
    #[serde(rename = "spark_python_task", skip_serializing_if = "Option::is_none")]
    pub spark_python_task: Option<Box<crate::models::SparkPythonTask>>,
    #[serde(rename = "spark_submit_task", skip_serializing_if = "Option::is_none")]
    pub spark_submit_task: Option<Box<crate::models::SparkSubmitTask>>,
    #[serde(rename = "pipeline_task", skip_serializing_if = "Option::is_none")]
    pub pipeline_task: Option<Box<crate::models::PipelineTask>>,
    #[serde(rename = "python_wheel_task", skip_serializing_if = "Option::is_none")]
    pub python_wheel_task: Option<Box<crate::models::PythonWheelTask>>,
    /// The time at which this run was started in epoch milliseconds (milliseconds since 1/1/1970 UTC). This may not be the time when the job task starts executing, for example, if the job is scheduled to run on a new cluster, this is the time the cluster creation call is issued.
    #[serde(rename = "start_time", skip_serializing_if = "Option::is_none")]
    pub start_time: Option<i64>,
    /// The time it took to set up the cluster in milliseconds. For runs that run on new clusters this is the cluster creation time, for runs that run on existing clusters this time should be very short.
    #[serde(rename = "setup_duration", skip_serializing_if = "Option::is_none")]
    pub setup_duration: Option<i64>,
    /// The time in milliseconds it took to execute the commands in the JAR or notebook until they completed, failed, timed out, were cancelled, or encountered an unexpected error.
    #[serde(rename = "execution_duration", skip_serializing_if = "Option::is_none")]
    pub execution_duration: Option<i64>,
    /// The time in milliseconds it took to terminate the cluster and clean up any associated artifacts. The total duration of the run is the sum of the setup_duration, the execution_duration, and the cleanup_duration.
    #[serde(rename = "cleanup_duration", skip_serializing_if = "Option::is_none")]
    pub cleanup_duration: Option<i64>,
    /// The time at which this run ended in epoch milliseconds (milliseconds since 1/1/1970 UTC). This field is set to 0 if the job is still running.
    #[serde(rename = "end_time", skip_serializing_if = "Option::is_none")]
    pub end_time: Option<i64>,
    /// The sequence number of this run attempt for a triggered job run. The initial attempt of a run has an attempt_number of 0\\. If the initial run attempt fails, and the job has a retry policy (`max_retries` \\> 0), subsequent runs are created with an `original_attempt_run_id` of the original attempt’s ID and an incrementing `attempt_number`. Runs are retried only until they succeed, and the maximum `attempt_number` is the same as the `max_retries` value for the job.
    #[serde(rename = "attempt_number", skip_serializing_if = "Option::is_none")]
    pub attempt_number: Option<i32>,
    #[serde(rename = "cluster_instance", skip_serializing_if = "Option::is_none")]
    pub cluster_instance: Option<Box<crate::models::ClusterInstance>>,
}

impl RunTask {
    pub fn new() -> RunTask {
        RunTask {
            run_id: None,
            task_key: None,
            description: None,
            state: None,
            depends_on: None,
            existing_cluster_id: None,
            new_cluster: None,
            libraries: None,
            notebook_task: None,
            spark_jar_task: None,
            spark_python_task: None,
            spark_submit_task: None,
            pipeline_task: None,
            python_wheel_task: None,
            start_time: None,
            setup_duration: None,
            execution_duration: None,
            cleanup_duration: None,
            end_time: None,
            attempt_number: None,
            cluster_instance: None,
        }
    }
}


