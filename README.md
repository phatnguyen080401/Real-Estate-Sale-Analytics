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
1. **Config.ini file**
   * Change `config.template.ini` to `config.ini`
   * Adjust some basic values in `config.ini`
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

## Airflow Webserver UI
### Local
### Remote

## Airflow DAGs
### Batch Layer DAG
### Speed Layer DAG
### Kafka Jobs DAG
### Fetch Data DAG

## Configure Great Expectations (opational)