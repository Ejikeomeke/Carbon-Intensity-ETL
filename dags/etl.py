from datetime import date, datetime
import pandas as pd
import psycopg2  # database connection and operation
import requests  # for api interaction
import yaml
import logging

# logging configuration
logging.basicConfig(filename='script.log', level=logging.INFO,
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
            
            logging.info('Connected succesfully!')
        return conn
    except Exception as e:
        logging.error(f"connection failed {e}")
        raise

    

def create_tables():
    try:
        with connectDB() as conn:
            with conn.cursor() as cur:
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
                    cur.execute(querry)
                conn.commit()
                logging.info("tables created succesfully")
        return None
    except Exception as e:
        logging.error(f"schema creation failed {e}")
        raise
        
    

def load_data_db(data) -> None:
    try:
        with connectDB() as conn:
            with conn.cursor() as cur:
                data_count = 0
                for data_point in data:
                    # Extract values from the tranformed data
                    date_rec = data_point['date']
                    time_rec = data_point['from']
                    day_recorded = data_point['day_recorded']
                    month_recorded = data_point['month_recorded']
                    dnoregion = data_point['dnoregion']
                    regionid = data_point['regionid']
                    intensity_forecast = data_point['intensity_forecast']
                    intensity_index = data_point['intensity_index']
                    biomass = data_point.get('biomass')  # Use.get() to handle missing values
                    coal = data_point.get('coal')
                    imports = data_point.get('imports')
                    gas = data_point.get('gas')
                    nuclear = data_point.get('nuclear')
                    other = data_point.get('other')
                    hydro = data_point.get('hydro')
                    solar = data_point.get('solar')
                    wind = data_point.get('wind')
                    

                    # SQL query to insert data
                    insert_query = """
                        INSERT INTO carbon.carbon_intensity ("date", "from", day_recorded, month_recorded,
                        dnoregion, region_id, intensity_forecast, intensity_index,
                        biomass, coal, imports, gas, nuclear, other, hydro, solar, wind)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """

                    # Execute the query with the data
                    cur.execute(insert_query, (date_rec, time_rec, day_recorded, month_recorded,
                                            dnoregion, regionid, intensity_forecast,
                                            intensity_index, biomass, coal, imports, gas,
                                            nuclear, other, hydro, solar, wind))

                    # Count the number of records
                    data_count += 1

                # Commit the changes to the database
                conn.commit()
                logging.info(f"{data_count} records inserted successfully!")
    except Exception as e:
        logging.error(f"data loading failed {e}")
        raise


def load_data_csv(data, fname="carbon_intensity_data") -> None:
    df = pd.DataFrame(data)
    re_no = df.shape[0]
    df.to_csv(f"{fname}.csv", index=True, index_label="ID")
    print(f"{re_no} records successfully saved to csv.")


if __name__ == "__main__":
    data = extract_data(URL=BASE_URL)
    transformed_data = transform_data(data=data)
    create_tables()
    load_data_db(transformed_data)
    load_data_csv(transformed_data)
    
