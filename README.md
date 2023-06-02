# Real Estate Sale 2001-2020 Analytics and Prediction
- [Real Estate Sale 2001-2020 Analytics and Prediction](#real-estate-sale-2001-2020-analytics-and-prediction )
  - [Introduction](#introduction)
  - [Project structure](#project-structure)
  - [Architecture](#lambda-architecture)
  - [Prerequisite](#prerequisite)
  - [Setup](#setup)
  - [Database SQL scripts](#database-sql-scripts)
  - [Airflow Webserver UI](#airflow-webserver-UI)
    - [Local](#local)
    - [Remote](#remote)
  - [Airflow DAGs](#airflow-dags)
    - [Batch Layer DAG](#batch-layer-dag)
    - [Speed Layer DAG](#speed-layer-dag)
    - [Kafka Jobs DAG](#kafka-jobs-dag)
    - [Fetch Data DAG](#fetch-data-dag)
  - [Configure Great Expectations (opational)](#configure-great-expectations-optional)

## Introduction
In this project, we are trying to build a data pipeline using Lambda architecture to handle massive quantities of data by 
taking advantage of both batch and stream processing methods. Besides, we also analyze, create real-time dashboard and apply some Machine Learning models to predict the data of the Real Estate Sale 2001-2020 records.

## Project structure
```bash
.
├── dags
│   ├── custom_functions
│   │   ├── __init__.py
│   │   └── functions.py
│   ├── batch_layer_dag.py
│   ├── fetch_data_dag.py
│   ├── kafka_jobs_dag.py
│   └── speed_layer_dag.py
├── deploy
│   ├── docker
│   │   └── volumes
│   │       ├── airflow
│   │       ├── kafka
│   │       ├── postgres
│   │       └── zookeeper
│   ├── apache-airflow.yml
│   └── docker-compose.yml
├── great_expectations
│   ├── checkpoints
│   │   ├── total_customer_by_property_type_checkpoint.yml
│   │   ├── total_customer_by_town_checkpoint.yml
│   │   └── total_sale_amount_ratio_checkpoint.yml
│   ├── expectations
│   │   ├── total_customer_by_property_type_suite.json
│   │   ├── total_customer_by_town_suite.json
│   │   └── total_sale_amount_ratio_suite.json
│   ├── plugins
│   ├── profilers
│   └── great_expectations.yml
├── images
│   └── architecture.png
├── scripts
│   └── setup.sh
├── src
│   ├── batch_layer
│   │   ├── total_customer_by_property_type_batch.py
│   │   ├── total_customer_by_town_batch.py
│   │   └── total_sale_amount_ratio_batch.py
│   ├── checkpoint
│   ├── config
│   │   ├── __init__.py
│   │   └── config.py
│   ├── dashboard
│   │   └── dashboard.py
│   ├── data
│   ├── data_source
│   ├── helper
│   │   ├── __init__.py
│   │   └── helper.py
│   ├── init
│   │   ├── init.sql
│   │   └── user_roles.sql
│   ├── kafka_jobs
│   │   ├── __init__.py
│   │   ├── consumer.py
│   │   └── producer.py
│   ├── logger
│   │   ├── __init__.py
│   │   └── logger.py
│   ├── logs
│   ├── speed_layer
│   │   ├── total_customer_by_property_type_speed.py
│   │   ├── total_customer_by_town_speed.py
│   │   └── total_sale_amount_ratio_speed.py
│   ├── test
│   │   ├── batch_validations
│   │   │   ├── total_customer_by_property_type_gx.py
│   │   │   ├── total_customer_by_town_gx.py
│   │   │   └── total_sale_amount_ratio_gx.py
│   │   └── utils
│   │       ├── __init__.py
│   │       └── utils.py
│   ├── tmp
│   ├── Dockerfile
│   ├── config.ini
│   ├── config.template.ini
│   └── requirements.txt
├── Makefile
├── README.md
```

## Architecture
![Lambda architecture](https://github.com/phatnguyen080401/NYC-Taxi-Analytics/blob/master/images/architecture.png)

## Prerequisite
* Python 3.8.*
* Apache Spark 3.2.*
* Scala 2.12.*
* Docker and Docker Compose
* Snowflake account
* Linux OS

## Setup
1. **Environment variables file**
   * Nevigate to `deploy` folder 
   * Change `.env-template` to `.env`
   * Adjust some basic values and credentials in `.env`
2. **Virtual environment**
   * Setup environment: `make setup`
3. **Create docker network**
   * Create network: `docker network create kafka-airflow`
4. **wwSnowflake credentials for great expectation**
   * Go to folder **great_expectations**: `cd great_expectations`
   * Modify **great_expectations.yml** file: `datasources > snowflake_db > execution_engine > connection_string`
   * Snowflake credentials:
      - With password: `snowflake://<USER_NAME>:<PASSWORD>@<ACCOUNT>.<REGION>/<DATABASE>/<SCHEMA>?warehouse=<WAREHOUSE>&role=<ROLE>`
      - No password: `snowflake://<USER_NAME>@<ACCOUNT>.<REGION>/<DATABASE>/<SCHEMA>?authenticator=externalbrowser&warehouse=<WAREHOUSE>&role=<ROLE>`
5. **Build docker image**
   * Build image: `make build-image`
6. **Run Kafka container**
   * Run command: `make start-kafka`
7. **Run Airflow container**
   * Run command: `make start-airflow`

## Database SQL scripts
The SQL scripts are located in `src/init/` folder. You need to run these scripts in Snowflake worksheet to create initial database for the project.

There are 2 scripts for creating database and user roles:
   * `init.sql`: This script sets up the data warehouse, database, schema and tables.
   * `user_roles.sql`: This script used to create user role **Database admin** who has ability to query and CRUD tables. 

## Airflow Webserver UI
### Local
Run and connect Airflow in the same machine with port 8080, the Airflow webserver address: https://localhost:8080
### Remote
Run Airflow in one machine and connect the Airflow webserver from other machine, follow below steps:
   1. Connect to server which hosts the Airflow and bind the port 8080 with local port in remote machine (for safe security): `ssh -o ServerAliveInterval=120 -o ServerAliveCountMax=2 -L 8080:localhost:8080 <username>@<ip-address> -p <port>`
   2. Setup firewall:
      * Enable **ufw** firewall: `sudo ufw enable`
      * Open port **8080**: `sudo ufw allow 8080/tcp`
      * View list of allowed ports: `sudo ufw status verbose`

## Airflow DAGs
All DAGs are located in `dags/` folder.

### Batch Layer DAG
![Batch Layer DAG](https://github.com/phatnguyen080401/NYC-Taxi-Analytics/blob/master/images/batch_layer_dag.png)

* `total_customer_by_property_type_batch`: Get the data from **`sale_lake.data_lake`** and calculate the total customer categorized by property type. Then store the result into **`sale_batch.total_customer_by_property_type`**.

* `total_customer_by_property_type_validation`: This task runs the checkpoint to validate data that has just landed in **`sale_batch.total_customer_by_property_type`**.

* `total_customer_by_town_batch`: Get the data from **`sale_lake.data_lake`** and calculate the total customer categorized by town. Then store the result into **`sale_batch.total_customer_by_town`**.

* `total_customer_by_town_validation`: After the previous task stores the result in Snowflake, this task will run another checkpoint to validate the data in **`sale_batch.total_customer_by_town`**.

* `total_sale_amount_ratio_batch`: Get the data from **`sale_lake.data_lake`** and calculate the total sale amount, total customer and total sale ratio. Then store the result into **`sale_batch.total_sale_amount_ratio`**.

* `total_sale_amount_ratio_validation`: After this task runs successfully and stores the result into table, it will run a checkpoint to validate data in **`sale_batch.total_sale_amount_ratio`**.

* `done`: It is a task which will be only triggered when all tasks run successfully.

### Speed Layer DAG
![Speed Layer DAG](https://github.com/phatnguyen080401/NYC-Taxi-Analytics/blob/master/images/speed_layer_dag.png)

* `total_customer_by_property_type_speed`: This task runs Spark Streaming to get the data which is stored in Kafka topic **`real_estate_sales`**, transform it and calculate the total customer grouped by property type. Then store the result into **`sale_speed.total_customer_by_property_type_speed`**.

* `total_customer_by_town_speed`: This task is simillar with the previous one except it stores the result about total customre from each town into **`sale_speed.total_customer_by_town_speed`**.

* `total_sale_amount_ratio_speed`: This task executes anothe Spark Streaming and calculate the total sale amount, total sale ratio and total customer. Then store them into **`sale_speed.total_sale_amount_ratio_speed`**.

* `done`: This task will be triggered if one of previous task runs failed. Otherwise, it will not be executed.

### Kafka Jobs DAG
![Kafka Jobs DAG](https://github.com/phatnguyen080401/NYC-Taxi-Analytics/blob/master/images/kafka_jobs_dag.png)

* `producer`: This task using Spark Streaming to ingest real-time data. Whenever a new parquet file is moved into `data/` folder, it will read this parquet file and store the data in Kafka topic **`real_estate_sales`**. 

* `consumer`: When new data is stored in Kafka topic, this task will read these new data immediately, adjust data type of some columns, fill null value to empty columns and store them into data lake: **`sale_lake.data_lake`**.

* `done`: This task will be triggered if one of previous task runs failed. Otherwise, it will not be executed.

### Fetch Data DAG
![Fetch Data DAG](https://github.com/phatnguyen080401/NYC-Taxi-Analytics/blob/master/images/fetch_data_dag.png)

* `folder_is_empty`: This task uses **BranchPythonOperator** to check whether or not `tmp/` folder is empty. There are two branches:
   - Folder is empty: to **download_file** branch.
   - Folder is not empty: to **move_file_to_folder_data** branch.

* `move_file_to_folder_data`: It will move a parquet file in `tmp/` folder to `data/` folder in a certain period of time.

* `point_to_next_file`: Each parquet file in `tmp/` folder has a suffix number: `Real_Estate_Sales-{x}.parquet` (x: 00,01,02,...). This task will rely on this number and point to the next file to move in next schedule.

* `download_file`: It will download `Real Estate Sales 2001-2020 GL` dataset from the website ([link download](https://data.ct.gov/api/views/5mzw-sjtu/rows.csv?accessType=DOWNLOAD)) in **csv** format and store in `data_source/` folder.

* `convert_to_parquet`: This task converts the **csv** file to **parquet** file.

* `adjust_dataframe`: After convert to **parquet** file, it will change the data type of all columns, rename columns and fill null value to empty cells. Then overwrite the old parquet file.

* `split_file`: When the **parquet** file is in correct format and data type, it is divided into smaller parquet files based on the number of partition. In this project, we set the partitions to 4000. You can modify the number of partition by opening the Airflow webserver, then go to `Admin > Variables` session and change the parameter **PARTITION**.

## Configure Great Expectations (opational)
**Note: The configuration in this section can be skipped since I have already done and created.** 

In this project, we will use Great Expectations version 3 API. Except for the very first step of creating a Data Context (the Great Expectations folder and associated files), all of the configuration for connecting to Datasources, creating Expectation Suites, creating Checkpoints and validating, will be accomplished using Python scripts in `src/test/` folder. 

This folder contains 2 sub-folders:
   * `batch_validations`: expectations, checkpoints and validations for batch layer.
   * `utils`: helps to run, config and create datasource, batch request and checkpoints.

We have already initialized the Data Context, validations and checkpoints. But if you wish to start from scratch, keep a copy of the **config_variables.yml** and **great_expectations.yml** files and delete the great_expectations folder.

1. Initialize the Data Context by running:
```
great_expectations init
```

2. Run Python script in the `test/batch_validations/` folder where batches of data can be sampled, expectation suites created, and Checkpoints can be configured.
   
   **Note: Before running any of the Python scripts, run the Airflow DAG at least once, so that all the data files and tables are moved to their respective locations. Only then wilFl you be able to run the scripts and test the Datasource connections locally.**

3. All checkpoints and expectation suites is located in `great_expectations/checkpoints/` and `great_expectations/expectations/` folders respectively.

4. Build data docs:
```
great_expectations docs build
```