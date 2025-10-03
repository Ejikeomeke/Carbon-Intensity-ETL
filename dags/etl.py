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


def connectDB() -> tuple:
    try:
        with open('dags/conn.yaml', 'r') as file:
            config = yaml.safe_load(file)

            host = config.get('host_linux')
            user = config.get('user')
            db = config.get('database')
            password = config.get('password')
            port = config.get('port')

            conn = psycopg2.connect(
                dbname=db,
                user=user,
                password=password,
                host=host,
                port=port
            )
            cur = conn.cursor()
            print('Connected succesfully!')
        return conn, cur
    except Exception as e:
        logging.error("connection failed {e}")

    

def create_tables():
    conn, curr = connectDB()
    querries = [
                """CREATE SCHEMA IF NOT EXISTS carbon;""", 
    
                """CREATE TABLE IF NOT EXISTS carbon.regions(
                    region_id SERIAL PRIMARY KEY,
                    short_name VARCHAR(50) NOT NULL
                    );""",
                                        
                """CREATE TABLE IF NOT EXISTS carbon.carbon_intensity(
                    id SERIAL PRIMARY KEY,
                    date DATE NOT NULL,
                    "from" TIME NOT NULL,
                    day_recorded VARCHAR(10) NOT NULL,
                    month_recorded VARCHAR(20) NOT NULL,
                    dnoregion VARCHAR(100) NOT NULL,
                    region_id INT NOT NULL,
                    intensity_forecast INT NOT NULL,
                    intensity_index VARCHAR(20) NOT NULL,
                    biomass DECIMAL(5, 2),
                    coal DECIMAL(5, 2),
                    imports DECIMAL(5,2),
                    gas DECIMAL(5, 2),
                    nuclear DECIMAL(5, 2),
                    other DECIMAL(5, 2),
                    hydro DECIMAL(5, 2),
                    solar DECIMAL(5, 2),
                    wind DECIMAL(5, 2),
                    FOREIGN KEY (region_id) REFERENCES carbon.regions(region_id)
                    );""",
                    
                """INSERT INTO carbon.regions (region_id, short_name) VALUES
                (1, 'North Scotland'),
                (2, 'South Scotland'),
                (3, 'North West England'),
                (4, 'North East England'),
                (5, 'Yorkshire'),
                (6, 'North Wales & Merseyside'),
                (7, 'South Wales'),
                (8, 'West Midlands'),
                (9, 'East Midleands'),
                (10, 'East England'),
                (11, 'South West England'),
                (12, 'South England'),
                (13, 'London'),
                (14, 'South East England'),
                (15, 'England'),
                (16, 'Scotland'),
                (17, 'Wales'),
                (18, 'GB') ON CONFLICT (region_id) DO NOTHING;"""
                                  

                    ]
    for querry in querries:
        curr.execute(querry)
    conn.commit()
    return None
