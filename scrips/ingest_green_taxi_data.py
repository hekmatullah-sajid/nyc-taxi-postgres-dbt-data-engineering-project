#!/usr/bin/env python
# coding: utf-8

import pandas as pd
from sqlalchemy import create_engine
import time
import argparse
import os

def main(params):
    user = params.user
    password = params.password
    host = params.host 
    port = params.port 
    db = params.db
    table_name = params.table_name
    csv_file_url = params.csv_file_url

    tbl_schema = 'nyc_taxi_dbt'  # Specify the schema name

    if csv_file_url.endswith('.csv.gz'):
        csv_file = 'output.csv.gz'
    else:
        csv_file = 'output.csv'

    os.system(f"wget {csv_file_url} -O {csv_file}")


    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    df_iterator = pd.read_csv(csv_file, iterator=True, chunksize=1000000)

    df = next(df_iterator)

    df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
    df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

    df.head(n=0).to_sql(name=table_name, schema=tbl_schema, con=engine, if_exists='append')

    df.to_sql(name=table_name, schema=tbl_schema, con=engine, if_exists='append')

    while True:
        t_start = time.time()
        
        try:
            # Assuming df_iterator is a generator providing DataFrames
            df = next(df_iterator)
        except StopIteration:
            # Exit the loop when there are no more iterations
            break
        
        # Convert datetime columns to pandas datetime format
        df['lpep_pickup_datetime'] = pd.to_datetime(df['lpep_pickup_datetime'])
        df['lpep_dropoff_datetime'] = pd.to_datetime(df['lpep_dropoff_datetime'])
        
        # Append the DataFrame to a table named 'yellow_taxi_data' in the database
        df.to_sql(name=table_name, schema=tbl_schema, con=engine, if_exists='append', index=False)
        
        t_end = time.time()
        
        print(f"Inserted another chunk, took {t_end - t_start:.3f} seconds.")

if __name__ == '__main__':

    # user, password, host, port, database name, table name, CSV file url

    parser = argparse.ArgumentParser(description='Process and import data into PostgreSQL database.')

    parser.add_argument('--user', required=True, help='User name for connecting to the PostgreSQL database.')
    parser.add_argument('--password', required=True, help='Password for connecting to the PostgreSQL database.')
    parser.add_argument('--host', required=True, help='Host address of the PostgreSQL database server.')
    parser.add_argument('--port', required=True, help='Port number on which the PostgreSQL database server is listening.')
    parser.add_argument('--db', required=True, help='Name of the PostgreSQL database to connect to.')
    parser.add_argument('--table_name', required=True, help='Name of the table where the results will be written.')
    parser.add_argument('--csv_file_url', required=True, help='Path of the CSV file containing the data to be imported.')

    args = parser.parse_args()

    main(args)