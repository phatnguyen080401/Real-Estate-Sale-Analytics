#!/bin/bash -e

cd ./deploy
# echo -e "AIRFLOW_UID=$(id -u)\nAIRFLOW_GID=0" > .env
echo -e "AIRFLOW_UID=0\nAIRFLOW_GID=0" > .env