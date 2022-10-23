CREATE KEYSPACE IF NOT EXISTS twitter 
WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };

USE twitter; 

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
                            PRIMARY KEY ((tpep_pickup_datetime, tpep_dropoff_datetime), passenger_count)
);

-- DROP TABLE IF EXISTS batch_layer;
CREATE TABLE IF NOT EXISTS batch_layer (
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
                            PRIMARY KEY ((tpep_pickup_datetime, tpep_dropoff_datetime), passenger_count)
);

-- DROP TABLE IF EXISTS speed_layer;
CREATE TABLE IF NOT EXISTS speed_layer (
                            vendor_id double,
                            tpep_pickup_datetime text,
                            tpep_dropoff_datetime text,
                            passenger_count double,
                            trip_distance double,
                            pu_location_id double,
                            do_location_id double,
                            fare_amount double,
                            tip_amount double,
                            total_amount double,  
                            PRIMARY KEY ((tpep_pickup_datetime, tpep_dropoff_datetime), passenger_count)
);