use std::{path::Path, sync::Arc};

use chrono::Duration;
use log::debug;

use crate::{
    load_var_source, AzureSynapseClient, Error, FeathrApiClient, FeathrProject, FeatureRegistry,
    JobClient, JobId, JobStatus, SubmitJobRequest, VarSource,
};

pub struct FeathrClient {
    job_client: AzureSynapseClient,
    registry_client: FeathrApiClient,
    var_source: Arc<dyn VarSource + Send + Sync>,
}

impl FeathrClient {
    pub async fn load<T>(conf_file: T) -> Result<Self, Error>
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

    pub async fn load_project(&self, name: &str) -> Result<FeathrProject, Error> {
        self.registry_client.load_project(name).await
    }

    pub async fn submit_job(&self, request: SubmitJobRequest) -> Result<JobId, Error> {
        self.job_client
            .submit_job(self.var_source.clone(), request)
            .await
    }

    pub async fn submit_jobs(&self, requests: Vec<SubmitJobRequest>) -> Result<Vec<JobId>, Error> {
        let mut ret = vec![];
        for request in requests.into_iter() {
            ret.push(
                self.job_client
                    .submit_job(self.var_source.clone(), request)
                    .await?,
            )
        }
        Ok(ret)
    }

    pub async fn wait_for_job(
        &self,
        job_id: JobId,
        timeout: Option<Duration>,
    ) -> Result<String, Error> {
        let status = self.job_client.wait_for_job(job_id, timeout).await?;
        debug!("Job {} completed with status {}", job_id, status);
        self.job_client.get_job_log(job_id).await
    }

    pub async fn get_job_status(&self, job_id: JobId) -> Result<JobStatus, Error> {
        self.job_client.get_job_status(job_id).await
    }
}

#[cfg(test)]
mod tests {
    use chrono::{Duration, TimeZone, Utc};
    use dotenv;
    use std::sync::Once;

    use futures::future::join_all;

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
    async fn materialization_e2e_job() {
        let client = init().await;
        let proj = FeathrProject::new("p1");
        let batch_source = proj.hdfs_source("nycTaxiBatchSource", "wasbs://public@azurefeathrstorage.blob.core.windows.net/sample_data/green_tripdata_2020-04.csv")
            .time_window(
                "lpep_dropoff_datetime",
                "yyyy-MM-dd HH:mm:ss"
            )
            .build()
            .unwrap();

        let location_id = TypedKey::new("DOLocationID", ValueType::INT32)
            .full_name("nyc_taxi.location_id")
            .description("location id in NYC");

        let trans = Transformation::window_agg(
            "cast_float(fare_amount)",
            Aggregation::AVG,
            Duration::days(90),
        )
        .unwrap();

        let agg_features = proj
            .anchor_group("aggregationFeatures", batch_source)
            .build()
            .unwrap();

        let f_location_avg_fare = agg_features
            .anchor("f_location_avg_fare", FeatureType::FLOAT)
            .unwrap()
            .keys(&[&location_id])
            .transform(&trans)
            .build()
            .unwrap();

        let f_location_max_fare = agg_features
            .anchor("f_location_max_fare", FeatureType::FLOAT)
            .unwrap()
            .keys(&[&location_id])
            .transform(trans)
            .build()
            .unwrap();

        let start = Utc.ymd(2020, 5, 20).and_hms(0, 0, 0);
        let reqs = proj
            .feature_gen_job(start, start + Duration::days(1), DateTimeResolution::Daily)
            .unwrap()
            .sink(RedisSink::new("table1"))
            .feature(&f_location_avg_fare)
            .feature(&f_location_max_fare)
            .build()
            .unwrap();
        for r in reqs.iter() {
            println!("{}:\n{}", r.job_config_file_name, r.gen_job_config);
        }

        let job_ids = client.submit_jobs(reqs).await.unwrap();

        let finished = job_ids.iter().map(|&id| client.wait_for_job(id, None));
        let outputs: Vec<String> = join_all(finished)
            .await
            .into_iter()
            .map(|r| r.unwrap())
            .collect();
        println!("{:#?}", outputs);

        for id in job_ids.into_iter() {
            assert_eq!(client.get_job_status(id).await.unwrap(), JobStatus::Success);
        }
    }

    #[tokio::test]
    async fn join_e2e_job() {
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
            .anchor_group("request_features", proj.INPUT_CONTEXT())
            .build()
            .unwrap();

        let f_trip_distance = request_features
            .anchor("f_trip_distance", FeatureType::FLOAT)
            .unwrap()
            .transform("trip_distance")
            .build()
            .unwrap();

        let f_trip_time_duration = request_features
            .anchor("f_trip_time_duration", FeatureType::INT32).unwrap()
            .transform("(to_unix_timestamp(lpep_dropoff_datetime) - to_unix_timestamp(lpep_pickup_datetime))/60")
            .build()
            .unwrap();

        let f_is_long_trip_distance = request_features
            .anchor("f_is_long_trip_distance", FeatureType::BOOLEAN)
            .unwrap()
            .transform("cast_float(trip_distance)>30")
            .build()
            .unwrap();

        let f_day_of_week = request_features
            .anchor("f_day_of_week", FeatureType::INT32)
            .unwrap()
            .transform("dayofweek(lpep_dropoff_datetime)")
            .build()
            .unwrap();

        let location_id = TypedKey::new("DOLocationID", ValueType::INT32)
            .full_name("nyc_taxi.location_id")
            .description("location id in NYC");

        let agg_features = proj
            .anchor_group("aggregationFeatures", batch_source)
            .build()
            .unwrap();

        let trans = Transformation::window_agg(
            "cast_float(fare_amount)",
            Aggregation::AVG,
            Duration::days(90),
        )
        .unwrap();

        let f_location_avg_fare = agg_features
            .anchor("f_location_avg_fare", FeatureType::FLOAT)
            .unwrap()
            .keys(&[&location_id])
            .transform(&trans)
            .build()
            .unwrap();

        let f_location_max_fare = agg_features
            .anchor("f_location_avg_fare", FeatureType::FLOAT)
            .unwrap()
            .keys(&[&location_id])
            .transform(trans)
            .build()
            .unwrap();

        let f_trip_time_distance = proj
            .derived("f_trip_time_distance", FeatureType::FLOAT)
            .add_input(&f_trip_distance)
            .add_input(&f_trip_time_duration)
            .transform("f_trip_distance * f_trip_time_duration")
            .build()
            .unwrap();

        let f_trip_time_rounded = proj
            .derived("f_trip_time_rounded", FeatureType::INT32)
            .add_input(&f_trip_time_duration)
            .transform("f_trip_time_duration % 10")
            .build()
            .unwrap();

        println!("features.conf:\n{}", proj.get_feature_config().unwrap());

        let output = "abfss://xchfeathrtest4fs@xchfeathrtest4sto.dfs.core.windows.net/output.bin";
        let anchor_query = FeatureQuery::new(
            &[
                &f_trip_distance,
                &f_trip_time_duration,
                &f_is_long_trip_distance,
                &f_day_of_week,
                &f_location_avg_fare,
                &f_location_max_fare,
            ],
            &[&location_id],
        );
        let derived_query = FeatureQuery::new(
            &[&f_trip_time_distance, &f_trip_time_rounded],
            &[&location_id],
        );
        let ob = ObservationSettings::new("wasbs://public@azurefeathrstorage.blob.core.windows.net/sample_data/green_tripdata_2020-04.csv", "lpep_dropoff_datetime", "yyyy-MM-dd HH:mm:ss");

        println!(
            "features_join.conf:\n{}",
            proj.get_feature_join_config(&ob, &[&anchor_query, &derived_query], output)
                .unwrap()
        );

        let req = proj
            .feature_join_job(&ob, &[&anchor_query, &derived_query], output)
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

        assert_eq!(client.get_job_status(id).await.unwrap(), JobStatus::Success);
    }
}
