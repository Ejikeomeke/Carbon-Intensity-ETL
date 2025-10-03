from datetime import date, datetime
import pandas as pd
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


def extract_data(URL) -> list:
    try:
        headers = {
            'Accept': 'application/json',
            "User-Agent": "Mozilla/5.0"
        }
        response = requests.get(URL,
                                params={},
                                headers=headers)
        print(f"Response Code: {response.status_code}")
        data = response.json()["data"]
        logging.info(f"data extracted succesfully")
        return data
    except Exception as e:
        logging.error(f"{e}")
        raise
