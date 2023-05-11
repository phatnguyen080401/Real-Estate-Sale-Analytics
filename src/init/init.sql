-- Create virtual warehouse
USE ROLE ACCOUNTADMIN;

CREATE OR REPLACE WAREHOUSE real_estate_sales_wh
  WITH WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = true
  INITIALLY_SUSPENDED = TRUE;

-- Create database
CREATE OR REPLACE DATABASE sale_db;

-- Create schema
CREATE OR REPLACE SCHEMA sale_lake;
CREATE OR REPLACE SCHEMA sale_batch;
CREATE OR REPLACE SCHEMA sale_speed;

-- Create tables

-- schema: sale_lake
-- table: data_lake
CREATE OR REPLACE TABLE sale_db.sale_lake.data_lake (
	serial_number BIGINT,
	list_year SMALLINT,
	date_recorded DATE,
	town TEXT,
	address TEXT,
	assessed_value FLOAT,
	sale_amount FLOAT,
	sales_ratio FLOAT,
	property_type TEXT,
	residential_type TEXT,
	non_use_code TEXT,
	assessor_remarks TEXT,
	opm_remarks TEXT,
	location TEXT,
	created_at TIMESTAMP_NTZ(9)
);

-- schema: sale_batch
-- table: total_sale_amount_ratio
CREATE OR REPLACE TABLE sale_db.sale_batch.total_sale_amount_ratio (
    total_sales_ratio FLOAT,
    total_sale_amount FLOAT,
    total_customer BIGINT,
    started_at TIMESTAMP_NTZ(9),
    ended_at TIMESTAMP_NTZ(9)
);

-- schema: sale_batch
-- table: total_customer_by_property_type
CREATE OR REPLACE TABLE sale_db.sale_batch.total_customer_by_property_type (
    property_type TEXT,
    total_customer BIGINT,
    started_at TIMESTAMP_NTZ(9),
    ended_at TIMESTAMP_NTZ(9)
);

-- schema: sale_batch
-- table: total_customer_by_town
CREATE OR REPLACE TABLE sale_db.sale_batch.total_customer_by_town (
    town TEXT,
    total_customer BIGINT,
    started_at TIMESTAMP_NTZ(9),
    ended_at TIMESTAMP_NTZ(9)
);

-- schema: sale_speed
-- table: total_sale_amount_ratio
CREATE OR REPLACE TABLE sale_db.sale_speed.total_sale_amount_ratio (
    total_sales_ratio FLOAT,
    total_sale_amount FLOAT,
    total_customer BIGINT,
    created_at TIMESTAMP_NTZ(9)
) CLUSTER BY (created_at);

-- schema: sale_speed
-- table: total_customer_by_property_type
CREATE OR REPLACE TABLE sale_db.sale_speed.total_customer_by_property_type (
    property_type TEXT,
    total_customer BIGINT,
    created_at TIMESTAMP_NTZ(9)
) CLUSTER BY (created_at);

-- schema: sale_speed
-- table: total_customer_by_town
CREATE OR REPLACE TABLE sale_db.sale_speed.total_customer_by_town (
    town TEXT,
    total_customer BIGINT,
    created_at TIMESTAMP_NTZ(9)
) CLUSTER BY (created_at);