CREATE OR REPLACE EXTERNAL TABLE `dezoomcamp.external_fhv_tripdata`
OPTIONS (
  format = 'CSV',
  uris = ['gs://prefect-de-zoomcamp-lol/data/raw/fhv/fhv*.csv.gz']
);

CREATE OR REPLACE TABLE `dezoomcamp.fhv_tripdata` as
select * from dezoomcamp.external_fhv_tripdata ;
-- Q1
SELECT count(*) FROM `dezoomcamp.fhv_tripdata`;
-- 43,244,696
----------------------
-- Q2
SELECT COUNT(DISTINCT(affiliated_base_number)) FROM `dezoomcamp.external_fhv_tripdata`;
-- 0 B
SELECT COUNT(DISTINCT(affiliated_base_number)) FROM `dezoomcamp.fhv_tripdata`;
-- 317.94MB
----------------
-- Q3
select count(*) FROM `dezoomcamp.fhv_tripdata` 
where PUlocationID is null  and  DOlocationID is null ; 
-- 717,748
---------------------------
-- Q4 
-- Question 4: What is the best strategy to make an optimized table in Big Query if your query will always filter by pickup_datetime and order by affiliated_base_number?
-- Partition by pickup_datetime Cluster on affiliated_base_number
------------------------------
-- Q5
CREATE OR REPLACE TABLE
  dezoomcamp.fhv_tripdata_partioned_clustered
PARTITION BY
  DATE(pickup_datetime)
CLUSTER BY
  affiliated_base_number AS
SELECT
  *
FROM
  dezoomcamp.fhv_tripdata;

  select distinct(affiliated_base_number) from   dezoomcamp.fhv_tripdata
  where   DATE(pickup_datetime) >='2019-03-01' and  DATE(pickup_datetime) <='2019-03-31' ;
-- 647.87 MB 

  select distinct(affiliated_base_number) from   dezoomcamp.fhv_tripdata_partioned_clustered
  where   DATE(pickup_datetime) >='2019-03-01' and  DATE(pickup_datetime) <='2019-03-31' ;
--  23.06 MB
------------------------------
-- Q6
-- Question 6: Where is the data stored in the External Table you created?
-- GCP Bucket
--------------------------------------------------------------
-- Q7
-- Question 7: It is best practice in Big Query to always cluster your data.
-- False
