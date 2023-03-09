# New York City Yellow Taxi Trip Analytics and Prediction
In this project, we are trying to build a data pipeline using Lambda architecture to handle massive quantities of data by 
taking advantage of both batch and stream processing methods. Besides, we also analyze, create real-time dashboard and apply some Machine Learning models to predict the data of the New York City yellow taxi trip record.

# Architecture
![Lambda architecture](https://github.com/phatnguyen080401/NYC-Taxi-Analytics/blob/master/images/architecture.png)


# Prerequisite
* Python 3.8.*
* Apache Spark 3.2.*
* Scala 2.12.*
* Snowflake account

# Setup
1. **Config.ini file**
   * Change `config.template.ini` to `config.ini`
   * Adjust some basic value in `config.ini`
2. **Virtual environment**
   * Setup environment: `make setup-env`