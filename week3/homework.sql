-- Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `green_taxi_2022.external_green_tripdata`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://bigquery_zoomcamp_sathyaan/green_taxi_data_2022.parquet']
);

SELECT * FROM green_taxi_2022.external_green_tripdata limit 10;


CREATE OR REPLACE TABLE green_taxi_2022.green_tripdata_non_partitoned AS
SELECT * FROM green_taxi_2022.external_green_tripdata;

SELECT * FROM green_taxi_2022.green_tripdata_non_partitoned limit 10;

select count(*) from green_taxi_2022.green_tripdata_non_partitoned;

SELECT count(distinct PULocationID) from green_taxi_2022.green_tripdata_non_partitoned;

SELECT count(distinct PULocationID) from `green_taxi_2022.external_green_tripdata`;

SELECT count(*) FROM `green_taxi_2022.green_tripdata_non_partitoned` where fare_amount = 0;

-- Create a partitioned table from external table
CREATE OR REPLACE TABLE `green_taxi_2022.green_tripdata_partitoned`
PARTITION BY
  DATE(lpep_pickup_datetime) AS
SELECT * FROM `green_taxi_2022.external_green_tripdata`;


SELECT count(distinct PULocationID) from `green_taxi_2022.green_tripdata_non_partitoned` where DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30';

SELECT count(distinct PULocationID) from `green_taxi_2022.green_tripdata_partitoned` where DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30';



