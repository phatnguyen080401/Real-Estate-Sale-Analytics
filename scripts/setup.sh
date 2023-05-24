#!/bin/bash -e

# Install package for virtual environment
pip install virtualenv

# Setup virtual env
cd ./src
virtualenv venv
source venv/bin/activate
pip install -r requirements.txt

# Get UID and GID of Airflow
cd ../deploy
# echo -e "AIRFLOW_UID=$(id -u)\nAIRFLOW_GID=0" > .env
echo -e "AIRFLOW_UID=0\nAIRFLOW_GID=0" > .env

# Create great_expectations 
cd ..
great_expectations -y init