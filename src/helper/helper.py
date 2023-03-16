import os
import re
import shutil
from urllib.error import HTTPError
from urllib import request
import dask.dataframe as dd

class Helper:

  __instance = None

  @staticmethod 
  def getHelper():
    if Helper.__instance == None:
      Helper()
    return Helper.__instance

  def __init__(self):
    if Helper.__instance:
      raise Exception("This class is a singleton!")
    else:
      Helper.__instance = self

  def join_path(self, path1, path2):
    return os.path.join(path1, path2)

  def get_files_from_folder(self, folder):
    return [os.path.join(dirpath, filename) \
            for (dirpath, dirnames, filenames) in os.walk(folder) \
            for filename in filenames if filenames]

  def remove_files_from_folder(self, folder):
    files = self.get_files_from_folder(folder)

    for file in files:
      os.remove(file)

  def get_str_between(self, str, str1, str2):
    return re.findall(r'{}(.*?){}'.format(str1, str2), str)

  def download_from_url(self, file_name, url):
    try:
      request.urlretrieve(url, f"./data_source/{file_name}.parquet")
    except HTTPError:
      print(f"File {file_name}.parquet not exist") 

  def split_file(self, file_name, schema, partition=100):
    name_function = lambda x: f"{file_name}-{x}.parquet"
    ddf = dd.read_parquet(f"./data_source/{file_name}.parquet")
    sorted_ddf = ddf.sort_values("tpep_pickup_datetime")
    sorted_ddf.repartition(partition).to_parquet("tmp/", name_function=name_function, schema=schema)

  def move_file(self, source, destination, file_name):
    shutil.move(f"./{source}/{file_name}.parquet", f"./{destination}/{file_name}.parquet")

  def main():
    pass