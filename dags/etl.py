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


def transform_data(data) -> list:
    TRANSFORMED_data = []
    for entry in data:
        time = datetime.strptime(entry['from'], '%Y-%m-%dT%H:%MZ')
        date_rec = time.strftime('%Y-%m-%d')
        time_rec = time.strftime('%H:%M')
        day = time.strftime('%A') # day in word like Monday....
        month = time.strftime('%B')
        for region in entry['regions']:
            dnoregion = region['dnoregion']
            regionid = region['regionid']
            intensity_forecast = region['intensity']['forecast']
            intensity_index = region['intensity']['index']
            generation_mix_data = {}
            for fuel_data in region['generationmix']:
                fuel_type = fuel_data['fuel']
                percentage = fuel_data['perc']
                generation_mix_data[fuel_type] = percentage
            TRANSFORMED_data.append({
                'date': date_rec,
                'from': time_rec,
                'day_recorded': day,
                'month_recorded': month,
                'dnoregion': dnoregion,
                'regionid': regionid,
                'intensity_forecast': intensity_forecast,
                'intensity_index': intensity_index,
                **generation_mix_data
            })
    logging.info("Data Transformed Successfully")
    return TRANSFORMED_data
    
