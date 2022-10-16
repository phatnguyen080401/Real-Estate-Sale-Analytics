#!/bin/bash -e

cd ./src
source venv/bin/activate      # Linux
#venv/Scripts/activate        # Windows
python kafka_jobs/producer.py &
python kafka_jobs/consumer.py &
python speed_layer/speed.py &
python batch_layer/batch.py 