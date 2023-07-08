#!/bin/bash -e

echo "--------------------------------------------"
# Install package for virtual environment
echo "Install Python virtual environment..."
pip install virtualenv
echo "Install successfully"

echo "--------------------------------------------"
# Setup virtual env
cd ./src
echo "In folder: src"

echo "Install packages..."
virtualenv venv
source venv/bin/activate
pip install -r requirements.txt
echo "Install successfully"

# echo "--------------------------------------------"
# # Get UID and GID of Airflow
# cd ../deploy
# echo "In folder: deploy"

# echo "Get Airflow UID and GID "
# # echo -e "AIRFLOW_UID=$(id -u)\nAIRFLOW_GID=0" > .env
# echo -e "AIRFLOW_UID=0\nAIRFLOW_GID=0" > .env

echo "--------------------------------------------"
# Create great_expectations 
cd ..
echo "In folder: ."

DIRECTORY="great_expectations"
if [ ! -d "$DIRECTORY" ]; then
  echo "Initialize Great Expectations..."
  great_expectations -y init
  echo "Initialize successfully"
else
    echo "$DIRECTORY  folder already exists"
fi
