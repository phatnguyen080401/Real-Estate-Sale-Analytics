import logging
import os

LOG_DIR = os.getenv("LOG_DIR")
formatter = logging.Formatter('%(asctime)s- %(name)s - %(levelname)s - %(message)s')

class Logger:

  def __init__(self, name, level=logging.INFO):
    self.logger = logging.getLogger(name)
    self.logger.setLevel(level)

    handler = logging.FileHandler(f"{LOG_DIR}/{name}.log", 'w')
    handler.setFormatter(formatter)

    self.logger.addHandler(handler)

  def debug(self, msg):
    self.logger.debug(msg)

  def info(self, msg):
    self.logger.info(msg)

  def warning(self, msg):
    self.logger.warning(msg)

  def error(self, msg):
    self.logger.error(msg)