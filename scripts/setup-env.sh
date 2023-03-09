#!/bin/bash -e

cd ./src
virtualenv venv
source venv/bin/activate
pip install -r requirements.txt