USE ROLE SECURITYADMIN;

-- Create role
CREATE OR REPLACE ROLE DBADMIN COMMENT='Database admin';

-- Grant role to use virtual warehouse and database
GRANT USAGE ON WAREHOUSE nyc_wh TO ROLE DBADMIN;
GRANT USAGE ON DATABASE nyc_db TO ROLE DBADMIN;

-- Grant role to use schema
GRANT USAGE ON SCHEMA nyc_db.nyc_lake TO ROLE DBADMIN;
GRANT USAGE ON SCHEMA nyc_db.yellow_taxi_batch TO ROLE DBADMIN;
GRANT USAGE ON SCHEMA nyc_db.yellow_taxi_speed TO ROLE DBADMIN;

-- Grant role to use all tables
GRANT SELECT ON ALL TABLES IN SCHEMA nyc_db.nyc_lake TO ROLE DBADMIN;
GRANT SELECT ON FUTURE TABLES IN SCHEMA nyc_db.nyc_lake TO ROLE DBADMIN;

GRANT SELECT,INSERT,UPDATE,DELETE ON ALL TABLES IN SCHEMA nyc_db.yellow_taxi_batch TO ROLE DBADMIN;
GRANT SELECT,INSERT,UPDATE,DELETE ON FUTURE TABLES IN SCHEMA nyc_db.yellow_taxi_batch TO ROLE DBADMIN;

GRANT SELECT,INSERT,UPDATE,DELETE ON ALL TABLES IN SCHEMA nyc_db.yellow_taxi_speed TO ROLE DBADMIN;
GRANT SELECT,INSERT,UPDATE,DELETE ON FUTURE TABLES IN SCHEMA nyc_db.yellow_taxi_speed TO ROLE DBADMIN;

-- Create new user
CREATE OR REPLACE USER dbadmin PASSWORD='123456' 
    DEFAULT_WAREHOUSE='nyc_wh' 
    DEFAULT_NAMESPACE='nyc_db' 
    MUST_CHANGE_PASSWORD=FALSE;
    
-- Grant role to user
GRANT ROLE DBADMIN TO USER dbadmin;
ALTER USER dbadmin SET DEFAULT_ROLE='DBADMIN';

-- View all role of custom role
SHOW GRANTS TO ROLE DBADMIN;

-- View list of users using this role
SHOW GRANTS OF ROLE DBADMIN;