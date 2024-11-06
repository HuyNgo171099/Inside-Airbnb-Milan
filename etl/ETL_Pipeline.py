'''
PART I: IMPORTING LIBRARIES
'''

# general libraries
import os
import requests
import gzip
import shutil
import csv
import logging
import pandas as pd
import numpy as np
import datetime
from datetime import timedelta

# librarires for PostgreSQL
import psycopg2

# libraries for Airflow
from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator

'''
PART II: AIRFLOW CONFIGURATION
'''

# set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# define the default arguments
default_args = {
    'owner': 'HuyNgo',
    'email': 'ngohuy171099@gmail.com',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# define the DAG
dag = DAG(
    'etl_pipeline_milan_inside_airbnb_data',
    default_args=default_args,
    description='An ETL pipeline that extracts data from Inside Airbnb, cleans it, and loads it into a PostgreSQL database',
    schedule_interval=timedelta(days=7),
    start_date=days_ago(0),               
    catchup=False,                        
    is_paused_upon_creation=False       
)

# create a directory called 'app/files' inside the container
data_dir = '/usr/local/airflow/files'
os.makedirs(data_dir, exist_ok=True)

'''
PART III: EXTRACT FUNCTIONS
'''

# define the function to download the data
def download_data():
    url = 'https://data.insideairbnb.com/italy/lombardy/milan/2024-06-22/data/listings.csv.gz'
    local_file = os.path.join(data_dir, 'listings.gz')
    logger.info(f'Downloading data from {url} to {local_file}')
    with requests.get(url, stream=True) as response:
        response.raise_for_status()
        with open(local_file, 'wb') as file:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    file.write(chunk)

# define the function to unzip the data
def unzip_data():
    zipped_file = os.path.join(data_dir, 'listings.gz')
    unzipped_file = os.path.join(data_dir, 'listings.csv')
    logger.info(f'Unzipping {zipped_file} to {unzipped_file}')
    with gzip.open(zipped_file, 'rb') as infile, open(unzipped_file, 'wb') as outfile:
        shutil.copyfileobj(infile, outfile)

# define the function to extract the data
def extract_data():
    original_file = os.path.join(data_dir, 'listings.csv')
    extracted_file = os.path.join(data_dir, 'extracted_listings.csv')
    logger.info(f'Extracting data from {original_file} to {extracted_file}')
    with open(original_file, 'r', encoding='utf-8') as infile, open(extracted_file, 'w', encoding='utf-8') as outfile:
        reader = csv.DictReader(infile)
        writer = csv.writer(outfile)
        writer.writerow(['id', 'host_id', 'neighbourhood_cleansed', 'room_type', 'minimum_nights', 'review_scores_rating'])
        for row in reader:
            writer.writerow([
                row['id'],
                row['host_id'],
                row['neighbourhood_cleansed'],
                row['room_type'],
                row['minimum_nights'],
                row['review_scores_rating']
            ])
            
'''
PART IV: TRANSFORM FUNCTION
'''

# define the function to transform the data
def transform_data():
    df = pd.read_csv(os.path.join(data_dir, 'extracted_listings.csv'))
    logger.info('Transforming data')
    df.rename(columns={'id': 'listing_id', 'neighbourhood_cleansed': 'neighbourhood_name'}, inplace=True)
    df.dropna(inplace=True)
    df.drop_duplicates(inplace=True)
    df = df[df['room_type'].isin(['Entire home/apt', 'Private room', 'Shared room', 'Hotel room'])]
    rows_with_invalid_data = df[(df['minimum_nights'] == 0) | (df['review_scores_rating'] < 1) | (df['review_scores_rating'] > 5)].index
    df.drop(rows_with_invalid_data, inplace=True)
    transformed_file = os.path.join(data_dir, 'transformed_listings.csv')
    df.to_csv(transformed_file, index=False)
    
'''
PART V: LOAD FUNCTION
'''

# define the function to load the data
def load_data():
    # read transformed data from CSV
    df = pd.read_csv(os.path.join(data_dir, 'transformed_listings.csv'))

    # define connection parameters
    dsn_hostname = 'db'
    dsn_user = 'postgres'
    dsn_pwd = os.getenv('DB_PASSWORD') 
    dsn_port = '5432'
    dsn_database = 'inside_airbnb_milan'

    try:
        # establish connection using psycopg2 directly
        conn = psycopg2.connect(
            host=dsn_hostname,
            user=dsn_user,
            password=dsn_pwd,
            port=dsn_port,
            database=dsn_database
        )
        cursor = conn.cursor()
        
        cursor.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto;")
        conn.commit()

        # create the listings table if it doesn't exist
        SQL_create_listings = """
        CREATE TABLE IF NOT EXISTS listings (
            listing_id BIGINT PRIMARY KEY,
            host_id BIGINT,
            neighbourhood_name VARCHAR(100),
            room_type VARCHAR(100),
            minimum_nights INT,
            review_scores_rating NUMERIC(4, 2)
        );
        """
        cursor.execute(SQL_create_listings)

        # insert listings with ON CONFLICT handling
        SQL_insert_listings = """
        INSERT INTO listings (listing_id, host_id, neighbourhood_name, room_type, minimum_nights, review_scores_rating)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (listing_id) 
        DO UPDATE SET 
            host_id = EXCLUDED.host_id,
            neighbourhood_name = EXCLUDED.neighbourhood_name,
            room_type = EXCLUDED.room_type,
            minimum_nights = EXCLUDED.minimum_nights,
            review_scores_rating = EXCLUDED.review_scores_rating;
        """
        
        # execute batch insertion
        insert_data = [tuple(row) for row in df.itertuples(index=False, name=None)]
        cursor.executemany(SQL_insert_listings, insert_data)

        # insert distinct neighborhoods
        SQL_insert_neigh = """
        INSERT INTO Neighbourhood (neighbourhood_name) 
        SELECT DISTINCT neighbourhood_name FROM listings 
        ON CONFLICT (neighbourhood_name) DO NOTHING;
        """
        cursor.execute(SQL_insert_neigh)

        # insert distinct hosts
        SQL_insert_real = """
        INSERT INTO Real_host (host_id) 
        SELECT DISTINCT host_id FROM listings 
        ON CONFLICT (host_id) DO NOTHING;
        """
        cursor.execute(SQL_insert_real)

        # insert mappings
        SQL_insert_mapping = """
        INSERT INTO Mapping_UUID (pseudo_host_id, host_id) 
        SELECT encode(gen_random_bytes(16), 'hex'), host_id FROM Real_host
        ON CONFLICT (host_id) DO NOTHING;
        """
        cursor.execute(SQL_insert_mapping)

        # insert pseudo hosts
        SQL_insert_pseudo = """
        INSERT INTO Pseudo_host (pseudo_host_id) 
        SELECT DISTINCT pseudo_host_id FROM Mapping_UUID
        ON CONFLICT (pseudo_host_id) DO NOTHING;
        """
        cursor.execute(SQL_insert_pseudo)

        # insert listings with foreign keys
        SQL_insert_listing = """
        INSERT INTO Listing (listing_id, pseudo_host_id, neighbourhood_id, room_type, minimum_nights, review_scores_rating) 
        SELECT ls.listing_id, h.pseudo_host_id, n.neighbourhood_id, ls.room_type, ls.minimum_nights, ls.review_scores_rating
        FROM listings AS ls
        INNER JOIN Neighbourhood AS n ON ls.neighbourhood_name = n.neighbourhood_name
        INNER JOIN Mapping_UUID AS h ON ls.host_id = h.host_id
        ON CONFLICT (listing_id)
        DO UPDATE SET 
            neighbourhood_id = EXCLUDED.neighbourhood_id,
            room_type = EXCLUDED.room_type,
            minimum_nights = EXCLUDED.minimum_nights,
            review_scores_rating = EXCLUDED.review_scores_rating;
        """
        cursor.execute(SQL_insert_listing)

        # commit the transaction
        conn.commit()
        
    except Exception as e:
        logger.error(f'An error occurred during loading: {e}')
        if conn:
            conn.rollback()
    
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

'''
PART VI: DEFINE TASKS
'''

# define the tasks
download_task = PythonOperator(
    task_id='download_data',
    python_callable=download_data,
    dag=dag,
)

unzip_task = PythonOperator(
    task_id='unzip_data',
    python_callable=unzip_data,
    dag=dag,
)

extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    dag=dag,
)

'''
PART VII: SET TASK DEPENDENCIES
'''

# define the task dependencies
download_task >> unzip_task >> extract_task >> transform_task >> load_task