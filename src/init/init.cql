CREATE KEYSPACE IF NOT EXISTS nyc_taxi 
WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };

USE nyc_taxi; 

-- DROP TABLE IF EXISTS data_lake;
CREATE TABLE IF NOT EXISTS data_lake (
                            vendor_id double,
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