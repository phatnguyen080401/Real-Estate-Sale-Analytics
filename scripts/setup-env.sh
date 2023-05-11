#!/bin/bash -e

pip install virtualenv

cd ./src
virtualenv venv
source venv/bin/activate
pip install -r requirements.txt