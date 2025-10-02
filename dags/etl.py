import psycopg2  # database connection and operation
import requests  # for api interaction
import yaml
import logging
import json

# logging configuration
logging.basicConfig(filename='dags/script.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

start_date = date(2024, 1, 1)
BASE_URL = f"https://api.carbonintensity.org.uk/regional/intensity/{start_date}/pt24h"
