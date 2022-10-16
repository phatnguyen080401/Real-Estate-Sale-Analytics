import sys
sys.path.append(".")

import time
from helper import Helper

helper = Helper.getHelper()

year = 2017
month = 2
# while month < 4:
#   file_name = f"yellow_tripdata_{year}-{month if month > 9 else '0' + str(month)}"
#   url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{file_name}.parquet"

#   helper.download_from_url(file_name, url)

#   month += 1

file_name = f"yellow_tripdata_{year}-{month if month > 9 else '0' + str(month)}"
helper.split_file(file_name, partition=500)

# num = 1
# while num < 5:
#   file_name = f"part.{num}"
#   helper.move_file(file_name)
#   num += 1
#   time.sleep(5)
