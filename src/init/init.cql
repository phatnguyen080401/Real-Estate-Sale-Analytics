CREATE KEYSPACE IF NOT EXISTS nyc_taxi 
WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };

USE nyc_taxi; 

-- DROP TABLE IF EXISTS data_lake;
CREATE TABLE IF NOT EXISTS data_lake (
                            vendor_id int,
                            tpep_pickup_datetime text,
                            tpep_dropoff_datetime text,
                            passenger_count double,
                            trip_distance double,
                            ratecode_id double,
                            store_and_fwd_flag text,
                            pu_location_id double,
                            do_location_id double,
                            payment_type double, 
                            fare_amount double,
                            extra double,
                            mta_tax double,
                            tip_amount double,
                            tolls_amount double, 
                            improvement_surcharge double, 
                            total_amount double, 
                            congestion_surcharge double, 
                            airport_fee double, 
                            PRIMARY KEY (tpep_pickup_datetime, tpep_dropoff_datetime)
);

-- DROP TABLE IF EXISTS total_trip_distance_batch;
CREATE TABLE IF NOT EXISTS total_trip_distance_batch (
                            total_trip_distance double,
                            total_rides int,
                            started_at timestamp,
                            ended_at timestamp,
                            PRIMARY KEY ((total_trip_distance, total_rides), ended_at)
) WITH CLUSTERING ORDER BY (ended_at DESC);

-- DROP TABLE IF EXISTS total_trip_distance_speed;
CREATE TABLE IF NOT EXISTS total_trip_distance_speed (
                            tpep_pickup_datetime timestamp,
                            tpep_dropoff_datetime timestamp,
                            trip_distance double,
                            created_at timestamp,
                            PRIMARY KEY ((tpep_pickup_datetime, tpep_dropoff_datetime), created_at)
) WITH CLUSTERING ORDER BY (created_at DESC) 
    AND default_time_to_live = 604800;

-- DROP TABLE IF EXISTS total_amount_batch;
CREATE TABLE IF NOT EXISTS total_amount_batch (
                            total_amount double,
                            total_rides int,
                            started_at timestamp,
                            ended_at timestamp,
                            PRIMARY KEY ((total_amount, total_rides), ended_at)
) WITH CLUSTERING ORDER BY (ended_at DESC);

-- DROP TABLE IF EXISTS total_amount_speed;
CREATE TABLE IF NOT EXISTS total_amount_speed (
                            tpep_pickup_datetime timestamp,
                            tpep_dropoff_datetime timestamp,
                            total_amount double,
                            created_at timestamp,
                            PRIMARY KEY ((tpep_pickup_datetime, tpep_dropoff_datetime), created_at)
) WITH CLUSTERING ORDER BY (created_at DESC) 
    AND default_time_to_live = 604800;

-- DROP TABLE IF EXISTS total_passenger_batch;
CREATE TABLE IF NOT EXISTS total_passenger_batch (
                            total_passenger double,
                            total_rides int,
                            started_at timestamp,
                            ended_at timestamp,
                            PRIMARY KEY ((total_passenger, total_rides), ended_at)
) WITH CLUSTERING ORDER BY (ended_at DESC);

-- DROP TABLE IF EXISTS total_passenger_speed;
CREATE TABLE IF NOT EXISTS total_passenger_speed (
                            tpep_pickup_datetime timestamp,
                            tpep_dropoff_datetime timestamp,
                            passenger_count double,
                            created_at timestamp,
                            PRIMARY KEY ((tpep_pickup_datetime, tpep_dropoff_datetime), created_at)
) WITH CLUSTERING ORDER BY (created_at DESC) 
    AND default_time_to_live = 604800;

-- DROP TABLE IF EXISTS pickup_dropoff_districts_speed;
CREATE TABLE IF NOT EXISTS pickup_dropoff_districts_speed (
                            vendor_id double,
                            pu_location_id double,
                            do_location_id double,
                            created_at timestamp,
                            PRIMARY KEY ((pu_location_id, do_location_id), created_at)
) WITH CLUSTERING ORDER BY (created_at DESC)