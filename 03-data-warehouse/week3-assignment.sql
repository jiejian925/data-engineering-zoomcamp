CREATE OR REPLACE EXTERNAL TABLE `dtc-de-course-447715.airflow2025.external_2024_yellow_taxi_`
OPTIONS (
  format = 'parquet',
  uris = ['https://storage.cloud.google.com/dtc-de-course-447715-airflow/raw/*.parquet']
);

CREATE OR REPLACE TABLE `dtc-de-course-447715.airflow2025.2024_yellow_taxi`
AS SELECT * FROM `dtc-de-course-447715.airflow2025.external_2024_yellow_taxi_`;

select count(1) from `dtc-de-course-447715.airflow2025.external_2024_yellow_taxi_`; 

SELECT count(distinct PULocationID) FROM `dtc-de-course-447715.airflow2025.external_2024_yellow_taxi_`;
SELECT count(distinct PULocationID) FROM `dtc-de-course-447715.airflow2025.2024_yellow_taxi`;

SELECT PULocationID FROM `dtc-de-course-447715.airflow2025.2024_yellow_taxi`;
SELECT PULocationID, DOLocationID FROM `dtc-de-course-447715.airflow2025.2024_yellow_taxi`;

select count(1) from `dtc-de-course-447715.airflow2025.external_2024_yellow_taxi_`
where fare_amount = 0;

CREATE OR REPLACE TABLE `dtc-de-course-447715.airflow2025.2024_yellow_taxi_data_partition_cluster`
partition by date(tpep_dropoff_datetime)
CLUSTER BY PUlocationID
as
SELECT * 
FROM `dtc-de-course-447715.airflow2025.2024_yellow_taxi`;

SELECT DISTINCT(VendorID) FROM `dtc-de-course-447715.airflow2025.2024_yellow_taxi_data_partition_cluster`
WHERE DATE(tpep_dropoff_datetime) BETWEEN '2024-03-01' AND '2022-03-15'

SELECT DISTINCT(VendorID) FROM `dtc-de-course-447715.airflow2025.2024_yellow_taxi`
WHERE DATE(tpep_dropoff_datetime) BETWEEN '2024-03-01' AND '2022-03-15'

-- Question 9: 0 bytes since the count(*) can directly retrieve from metadata so bigquery does not need to scan table