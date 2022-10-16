# Lambda-architecture
In this project, we are trying to build data pipeline using Lambda architecture to handle massive quantities of data by 
taking advantage of both batch and stream processing methods. Besides, we also analyze Twitter's tweets.

## Prerequisite
* Python 3.*
* Apache Spark 3.2.*
* Account for Twitter API

## Setup
1. **Config.ini file**
   * Change `config.template.ini` to `config.ini`
   * Adjust some basic value in `config.ini`
2. **logs folder**
   * Grant full permission : `sudo chmod a+rwx src/logs`

## Usage
1. Clone repository

```
  git clone 
```

2. Run Docker containers

```
  make start-docker
```

3. Setup virtual env for project

```
  make setup-env
```

4. Run project 

```
   make start-all
```
5. Analyze

```
  Go to notebook for analyzing
```

## Common Error
1. If not find **twitter** keyspace, run container `cassandra-init-schema` again
