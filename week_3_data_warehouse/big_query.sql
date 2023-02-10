  -- Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE
  `dtc-de-course-350819.dezoomcamp.external_yellow_tripdata` OPTIONS ( format = 'csv',
    uris = ['gs://prefect-de-zoomcamp-lol/data/raw/yellow/yellow_tripdata_20*.csv.gz'] );
  -- Query public available table
SELECT
  station_id,
  name
FROM
  bigquery-public-data.new_york_citibike.citibike_stations
LIMIT
  100;
  -- Check yello trip data
SELECT
  *
FROM
  dezoomcamp.external_yellow_tripdata
LIMIT
  10;
  -- Create a non partitioned table from external table
CREATE OR REPLACE TABLE
  dezoomcamp.yellow_tripdata_non_partitoned AS
SELECT
  *
FROM
  dezoomcamp.external_yellow_tripdata;
  -- Create a partitioned table from external table
CREATE OR REPLACE TABLE
  dezoomcamp.yellow_tripdata_partitoned
PARTITION BY
  DATE(tpep_pickup_datetime) AS
SELECT
  *
FROM
  dezoomcamp.external_yellow_tripdata;
  -- Impact of partition
  -- Scanning 1.6GB of data
SELECT
  DISTINCT(VendorID)
FROM
  dezoomcamp.yellow_tripdata_non_partitoned
WHERE
  DATE(tpep_pickup_datetime) BETWEEN '2019-06-01'
  AND '2019-06-30';
  -- Scanning ~106 MB of DATA
SELECT
  DISTINCT(VendorID)
FROM
  dezoomcamp.yellow_tripdata_partitoned
WHERE
  DATE(tpep_pickup_datetime) BETWEEN '2019-06-01'
  AND '2019-06-30';
  -- Let's look into the partitons
SELECT
  table_name,
  partition_id,
  total_rows
FROM
  `dezoomcamp.INFORMATION_SCHEMA.PARTITIONS`
WHERE
  table_name = 'yellow_tripdata_partitoned'
ORDER BY
  total_rows DESC;
  -- Creating a partition and cluster table
CREATE OR REPLACE TABLE
  dezoomcamp.yellow_tripdata_partitoned_clustered
PARTITION BY
  DATE(tpep_pickup_datetime)
CLUSTER BY
  VendorID AS
SELECT
  *
FROM
  dezoomcamp.external_yellow_tripdata;
  -- Query scans 1.1 GB
SELECT
  COUNT(*) AS trips
FROM
  dezoomcamp.yellow_tripdata_partitoned
WHERE
  DATE(tpep_pickup_datetime) BETWEEN '2019-06-01'
  AND '2020-12-31'
  AND VendorID=1;
  -- Query scans 864.5 MB
SELECT
  COUNT(*) AS trips
FROM
  dezoomcamp.yellow_tripdata_partitoned_clustered
WHERE
  DATE(tpep_pickup_datetime) BETWEEN '2019-06-01'
  AND '2020-12-31'
  AND VendorID=1;