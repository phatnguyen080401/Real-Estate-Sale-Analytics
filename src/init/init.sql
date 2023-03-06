-- Create virtual warehouse
USE ROLE ACCOUNTADMIN;

CREATE OR REPLACE WAREHOUSE nyc_wh
  WITH WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 120
  AUTO_RESUME = true
  INITIALLY_SUSPENDED = TRUE;

-- Create database
CREATE OR REPLACE DATABASE nyc_db;

-- Create schema
CREATE OR REPLACE SCHEMA nyc_lake;
CREATE OR REPLACE SCHEMA yellow_taxi_batch;
CREATE OR REPLACE SCHEMA yellow_taxi_speed;

-- Create tables
-- table: data_lake
CREATE OR REPLACE TABLE nyc_db.nyc_lake.data_lake (
	id BIGINT NOT NULL AUTOINCREMENT(1,1) PRIMARY KEY,
	vendor_id SMALLINT,
	tpep_pickup_datetime TEXT,
	tpep_dropoff_datetime TEXT,
	passenger_count FLOAT,
	trip_distance FLOAT,
	ratecode_id FLOAT,
	store_and_fwd_flag TEXT,
	pu_location_id SMALLINT,
	do_location_id SMALLINT,
	payment_type SMALLINT,
	fare_amount FLOAT,
	extra FLOAT,
	mta_tax FLOAT,
	tip_amount FLOAT,
	tolls_amount FLOAT,
	improvement_surcharge FLOAT,
	total_amount FLOAT,
	congestion_surcharge FLOAT,
	airport_fee FLOAT,
	created_at TIMESTAMP_NTZ(9)
);

-- schema: yellow_taxi_batch
-- table: total_trip_distance
CREATE OR REPLACE TABLE nyc_db.yellow_taxi_batch.total_trip_distance (
    total_trip_distance FLOAT,
    total_rides BIGINT,
    started_at TIMESTAMP_NTZ(9),
    ended_at TIMESTAMP_NTZ(9)
);

-- schema: yellow_taxi_batch
-- table: total_amount
CREATE OR REPLACE TABLE nyc_db.yellow_taxi_batch.total_amount (
    total_amount FLOAT,
    total_rides BIGINT,
    started_at TIMESTAMP_NTZ(9),
    ended_at TIMESTAMP_NTZ(9)
);

-- schema: yellow_taxi_batch
-- table: total_passenger
CREATE OR REPLACE TABLE nyc_db.yellow_taxi_batch.total_passenger (
    total_passenger FLOAT,
    total_rides BIGINT,
    started_at TIMESTAMP_NTZ(9),
    ended_at TIMESTAMP_NTZ(9)
);

-- schema: yellow_taxi_speed
-- table: pickup_dropoff
CREATE OR REPLACE TABLE nyc_db.yellow_taxi_speed.pickup_dropoff (
    vendor_id FLOAT,
    tpep_pickup_datetime TIMESTAMP_NTZ(9),
    tpep_dropoff_datetime TIMESTAMP_NTZ(9),
    pu_location_id FLOAT,
    do_location_id FLOAT,
    created_at TIMESTAMP_NTZ(9)
);

-- schema: yellow_taxi_speed
-- table: user_per_payment
CREATE OR REPLACE TABLE nyc_db.yellow_taxi_speed.user_per_payment (
    vendor_id FLOAT,
    payment_type FLOAT,
    created_at TIMESTAMP_NTZ(9),
    PRIMARY KEY (id)
);