use std::path::Path;

use crate::{
    AzureSynapseClient, FeathrApiClient, FeathrProject, FeatureRegistry, JobClient, JobId,
    SubmitJobRequest, YamlSource,
};

pub struct FeathrClient {
    job_client: AzureSynapseClient,
    registry_client: FeathrApiClient,
    var_source: YamlSource,
}

impl FeathrClient {
    pub fn load<T>(config_path: T) -> Result<Self, crate::Error>
    where
        T: AsRef<Path>,
    {
        let var_source = YamlSource::load(config_path)?;
        Ok(Self {
            job_client: AzureSynapseClient::from_var_source(&var_source)?,
            registry_client: FeathrApiClient::from_var_source(&var_source)?,
            var_source,
        })
    }

    pub async fn load_project(&self, name: &str) -> Result<FeathrProject, crate::Error> {
        self.registry_client.load_project(name).await
    }

    pub async fn submit_job(&self, request: SubmitJobRequest) -> Result<JobId, crate::Error> {
        self.job_client.submit_job(&self.var_source, request).await
    }
}

#[cfg(test)]
mod tests {
    use dotenv;
    use std::sync::Once;

    use crate::utils::str_to_dur;
    use crate::*;

    static INIT_ENV_LOGGER: Once = Once::new();

    fn init() -> FeathrClient {
        dotenv::dotenv().ok();
        INIT_ENV_LOGGER.call_once(|| env_logger::init());
        FeathrClient::load("../test-script/feathr_config.yaml").unwrap()
    }

    #[tokio::test]
    #[allow(dead_code)]
    async fn it_works() {
        let client = init();
        let proj = FeathrProject::new("p1");
        let batch_source = proj.hdfs_source_builder("nycTaxiBatchSource")
            .set_path("wasbs://public@azurefeathrstorage.blob.core.windows.net/sample_data/green_tripdata_2020-04.csv")
            .set_time_window_parameters(
                "lpep_dropoff_datetime",
                "yyyy-MM-dd HH:mm:ss"
            )
            .build()
            .unwrap();

        let request_features = proj
            .group_builder("request_features")
            // .set_source(Source::INPUT_CONTEXT())
            .set_source(batch_source.clone())
            .build()
            .unwrap();

        let f_trip_distance = proj
            .anchor_builder("request_features", "f_trip_distance")
            .set_type(FeatureType::FLOAT)
            .set_transform("trip_distance".into())
            .build()
            .unwrap();

        let f_trip_time_duration = proj
            .anchor_builder("request_features", "f_trip_time_duration")
            .set_type(FeatureType::INT32)
            .set_transform("(to_unix_timestamp(lpep_dropoff_datetime) - to_unix_timestamp(lpep_pickup_datetime))/60".into())
            .build()
            .unwrap();

        let f_is_long_trip_distance = proj
            .anchor_builder("request_features", "f_is_long_trip_distance")
            .set_type(FeatureType::BOOLEAN)
            .set_transform("cast_float(trip_distance)>30".into())
            .build()
            .unwrap();

        let f_day_of_week = proj
            .anchor_builder("request_features", "f_day_of_week")
            .set_type(FeatureType::INT32)
            .set_transform("dayofweek(lpep_dropoff_datetime)".into())
            .build()
            .unwrap();

        let location_id = TypedKey::new("DOLocationID", ValueType::INT32)
            .full_name("nyc_taxi.location_id")
            .description("location id in NYC");

        let agg_features = proj
            .group_builder("aggregationFeatures")
            .set_source(batch_source)
            .build()
            .unwrap();

        let f_location_avg_fare = proj
            .anchor_builder("aggregationFeatures", "f_location_avg_fare")
            .set_type(FeatureType::FLOAT)
            .set_keys(&[location_id.clone()])
            .set_transform(
                Transformation::window_agg(
                    "cast_float(fare_amount)",
                    Aggregation::AVG,
                    str_to_dur("90d").unwrap(),
                )
                .unwrap(),
            )
            .build()
            .unwrap();

        let f_location_max_fare = proj
            .anchor_builder("aggregationFeatures", "f_location_avg_fare")
            .set_type(FeatureType::FLOAT)
            .set_keys(&[location_id.clone()])
            .set_transform(
                Transformation::window_agg(
                    "cast_float(fare_amount)",
                    Aggregation::MAX,
                    str_to_dur("90d").unwrap(),
                )
                .unwrap(),
            )
            .build()
            .unwrap();

        println!("features.conf:\n{}", proj.get_feature_config().unwrap());

        let output = "abfss://xchfeathrtest4fs@xchfeathrtest4sto.dfs.core.windows.net/output.bin";
        let q = FeatureQuery::new(&["f_location_avg_fare"], &[&location_id]);
        let ob = ObservationSettings::new("wasbs://public@azurefeathrstorage.blob.core.windows.net/sample_data/green_tripdata_2020-04.csv", "lpep_dropoff_datetime", "yyyy-MM-dd HH:mm:ss");


        println!("features_join.conf:\n{}", proj.get_feature_join_config(ob, vec![q.clone()], output).unwrap());

        let req = proj
            .feature_join_job(ObservationSettings::new("wasbs://public@azurefeathrstorage.blob.core.windows.net/sample_data/green_tripdata_2020-04.csv", "lpep_dropoff_datetime", "yyyy-MM-dd HH:mm:ss"), vec![q], output)
            .unwrap()
            .output_path(output)
            .build();
        
        println!("Request: {:#?}", req);

        let id = client.submit_job(req).await.unwrap();
    }
}
