use std::{path::Path, sync::Arc, time::Duration};

use log::debug;

use crate::{
    load_var_source, AzureSynapseClient, FeathrApiClient, FeathrProject, FeatureRegistry,
    JobClient, JobId, SubmitJobRequest, VarSource,
};

pub struct FeathrClient {
    job_client: AzureSynapseClient,
    registry_client: FeathrApiClient,
    var_source: Arc<dyn VarSource + Send + Sync>,
}

impl FeathrClient {
    pub async fn load<T>(conf_file: T) -> Result<Self, crate::Error>
    where
        T: AsRef<Path>,
    {
        let var_source = load_var_source(conf_file);
        Ok(Self {
            job_client: AzureSynapseClient::from_var_source(var_source.clone()).await?,
            registry_client: FeathrApiClient::from_var_source(var_source.clone()).await?,
            var_source,
        })
    }

    pub async fn load_project(&self, name: &str) -> Result<FeathrProject, crate::Error> {
        self.registry_client.load_project(name).await
    }

    pub async fn submit_job(&self, request: SubmitJobRequest) -> Result<JobId, crate::Error> {
        self.job_client
            .submit_job(self.var_source.clone(), request)
            .await
    }

    pub async fn wait_for_job(
        &self,
        job_id: JobId,
        timeout: Option<Duration>,
    ) -> Result<String, crate::Error> {
        let status = self.job_client.wait_for_job(job_id, timeout).await?;
        debug!("Job {} completed with status {}", job_id, status);
        self.job_client.get_job_log(job_id).await
    }
}

#[allow(dead_code)]
#[allow(unused_variables)]
#[cfg(test)]
mod tests {
    use dotenv;
    use std::sync::Once;
    use std::time::Duration;

    use crate::*;

    static INIT_ENV_LOGGER: Once = Once::new();

    async fn init() -> FeathrClient {
        dotenv::dotenv().ok();
        INIT_ENV_LOGGER.call_once(|| env_logger::init());
        FeathrClient::load("../test-script/feathr_config.yaml")
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn it_works() {
        let client = init().await;
        let proj = FeathrProject::new("p1");
        let batch_source = proj.hdfs_source("nycTaxiBatchSource", "wasbs://public@azurefeathrstorage.blob.core.windows.net/sample_data/green_tripdata_2020-04.csv")
            .time_window(
                "lpep_dropoff_datetime",
                "yyyy-MM-dd HH:mm:ss"
            )
            .build()
            .unwrap();

        let request_features = proj
            .anchor_group("request_features", batch_source.clone())
            .build()
            .unwrap();

        let f_trip_distance = request_features
            .anchor("f_trip_distance", FeatureType::FLOAT)
            .unwrap()
            .transform("trip_distance".into())
            .build()
            .unwrap();

        let f_trip_time_duration = request_features
            .anchor("f_trip_time_duration", FeatureType::INT32).unwrap()
            .transform("(to_unix_timestamp(lpep_dropoff_datetime) - to_unix_timestamp(lpep_pickup_datetime))/60".into())
            .build()
            .unwrap();

        let f_is_long_trip_distance = request_features
            .anchor("f_is_long_trip_distance", FeatureType::BOOLEAN)
            .unwrap()
            .transform("cast_float(trip_distance)>30".into())
            .build()
            .unwrap();

        let f_day_of_week = request_features
            .anchor("f_day_of_week", FeatureType::INT32)
            .unwrap()
            .transform("dayofweek(lpep_dropoff_datetime)".into())
            .build()
            .unwrap();

        let location_id = TypedKey::new("DOLocationID", ValueType::INT32)
            .full_name("nyc_taxi.location_id")
            .description("location id in NYC");

        let agg_features = proj
            .anchor_group("aggregationFeatures", batch_source)
            .build()
            .unwrap();

        let f_location_avg_fare = agg_features
            .anchor("f_location_avg_fare", FeatureType::FLOAT)
            .unwrap()
            .keys(&[location_id.clone()])
            .transform(
                Transformation::window_agg(
                    "cast_float(fare_amount)",
                    Aggregation::AVG,
                    Duration::from_days(90),
                )
                .unwrap(),
            )
            .build()
            .unwrap();

        let f_location_max_fare = agg_features
            .anchor("f_location_avg_fare", FeatureType::FLOAT)
            .unwrap()
            .keys(&[location_id.clone()])
            .transform(
                Transformation::window_agg(
                    "cast_float(fare_amount)",
                    Aggregation::MAX,
                    Duration::from_days(90),
                )
                .unwrap(),
            )
            .build()
            .unwrap();

        println!("features.conf:\n{}", proj.get_feature_config().unwrap());

        let output = "abfss://xchfeathrtest4fs@xchfeathrtest4sto.dfs.core.windows.net/output.bin";
        let q = FeatureQuery::new(&["f_location_avg_fare"], &[&location_id]);
        let ob = ObservationSettings::new("wasbs://public@azurefeathrstorage.blob.core.windows.net/sample_data/green_tripdata_2020-04.csv", "lpep_dropoff_datetime", "yyyy-MM-dd HH:mm:ss");

        println!(
            "features_join.conf:\n{}",
            proj.get_feature_join_config(ob, vec![q.clone()], output)
                .unwrap()
        );

        let req = proj
            .feature_join_job(ObservationSettings::new("wasbs://public@azurefeathrstorage.blob.core.windows.net/sample_data/green_tripdata_2020-04.csv", "lpep_dropoff_datetime", "yyyy-MM-dd HH:mm:ss"), vec![q], output)
            .unwrap()
            .output_path(output)
            .build();

        println!("Request: {:#?}", req);

        let id = client.submit_job(req).await.unwrap();

        let log = client.wait_for_job(id, None).await.unwrap();

        println!("Job output:\n{}", log);

        println!(
            "Job output URL: {}",
            client
                .job_client
                .get_job_output_url(id)
                .await
                .unwrap()
                .unwrap()
        );
    }
}
