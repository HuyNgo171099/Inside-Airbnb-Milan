import psycopg2
import os

# connection details
dsn_hostname = 'db'
dsn_user = 'postgres'
dsn_pwd = os.getenv('DB_PASSWORD')
dsn_port = '5432'
dsn_database = 'inside_airbnb_milan'

# establish the connection
conn = psycopg2.connect(
    host=dsn_hostname,
    user=dsn_user,
    password=dsn_pwd,
    port=dsn_port,
    database=dsn_database
)

# create a cursor object
cursor = conn.cursor()

# create the neighborhood table
SQL_neigh_table = """CREATE TABLE IF NOT EXISTS Neighbourhood (
    neighbourhood_id SERIAL PRIMARY KEY, 
    neighbourhood_name TEXT UNIQUE
    );"""
cursor.execute(SQL_neigh_table)

# create the host tables
SQL_real_host_table = """CREATE TABLE IF NOT EXISTS Real_host (
    host_id BIGINT PRIMARY KEY
    );"""
cursor.execute(SQL_real_host_table)

SQL_mapping_UUID_table = """CREATE TABLE IF NOT EXISTS Mapping_UUID (
    pseudo_host_id TEXT PRIMARY KEY,
    host_id BIGINT UNIQUE
    );"""
cursor.execute(SQL_mapping_UUID_table)

SQL_pseudo_host_table = """CREATE TABLE IF NOT EXISTS Pseudo_host (
    pseudo_host_id TEXT PRIMARY KEY
    );"""
cursor.execute(SQL_pseudo_host_table)

# create the listing table
SQL_listing_table = """CREATE TABLE IF NOT EXISTS Listing (
    listing_id BIGINT PRIMARY KEY,
    pseudo_host_id TEXT NOT NULL,
    neighbourhood_id INTEGER NOT NULL,
    room_type TEXT,
    minimum_nights INTEGER,
    review_scores_rating numeric(4, 2),
    FOREIGN KEY (pseudo_host_id) REFERENCES Pseudo_host (pseudo_host_id),
	FOREIGN KEY (neighbourhood_id) REFERENCES Neighbourhood (neighbourhood_id)
    );"""
cursor.execute(SQL_listing_table)

# commit the changes and close the connection
conn.commit()
conn.close()