#!/bin/bash -e

cd ./src
virtualenv venv
source venv/bin/activate        # Linux
#venv/Scripts/activate          # Windows
pip install -r requirements.txt