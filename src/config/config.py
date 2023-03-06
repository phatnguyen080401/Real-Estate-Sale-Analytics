import sys
sys.path.append(".")

import os
import configparser
from helper.helper import Helper

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

helper = Helper.getHelper()

parser = configparser.ConfigParser()
parser.read_file(open(helper.join_path(ROOT_DIR, "./config.ini")))

config = {
    "ROOT_DIR": ROOT_DIR,

    "SNOWFLAKE": {
        "URL": parser.get('SNOWFLAKE', 'URL'),
        "ACCOUNT": parser.get('SNOWFLAKE', 'ACCOUNT'),
        "USER": parser.get('SNOWFLAKE', 'USER'),
        "PASSWORD": parser.get('SNOWFLAKE', 'PASSWORD'),
        "DATABASE": parser.get('SNOWFLAKE', 'DATABASE'),
        "SCHEMA": parser.get('SNOWFLAKE', 'SCHEMA'),
        "WAREHOUSE": parser.get('SNOWFLAKE', 'WAREHOUSE'),
    },

    "KAFKA": {
        "KAFKA_ENDPOINT": parser.get('KAFKA', 'KAFKA_ENDPOINT'),
        "KAFKA_ENDPOINT_PORT": parser.getint('KAFKA', 'KAFKA_ENDPOINT_PORT'),
        "KAFKA_TOPIC": parser.get('KAFKA', 'KAFKA_TOPIC')
    },

    "LOG": {
        "LOG_DIR": helper.join_path(ROOT_DIR, f"./{parser.get('LOG', 'LOG_DIR')}")
    },
}
