#!/usr/bin/env python
# coding: utf-8

import pandas as pd
from sqlalchemy import create_engine
from time import time
import os

import argparse


def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url
   
    csv_name = 'output.csv'

    os.system(f'wget {url} -0 {csv_name}')

    engine = create_engine('postgresql://root:root@localhost:5432/ny_taxi')
    engine.connect()

    df = pd.read_csv('yellow_tripdata_2021-01.csv.gz', chunksize=100000, iterator=True)

    df_chunk = next(df)
    df_chunk['tpep_pickup_datetime'] = pd.to_datetime(df_chunk['tpep_pickup_datetime'])
    df_chunk['tpep_dropoff_datetime'] = pd.to_datetime(df_chunk['tpep_dropoff_datetime'])   

    df_chunk_header = pd.DataFrame(df_chunk).head(0)
    df_chunk_header

    print(
        pd.io.sql.get_schema(df_chunk_header, name='yellow_taxi_data', con=engine)
    )

    df_chunk_header.to_sql(name='yellow_taxi_data', con=engine, if_exists='replace', index=False)

    df = pd.read_csv('yellow_tripdata_2021-01.csv.gz', chunksize=100000, iterator=True)









    while True:
        t_start = time()

        df_chunk = next(df)
        
        df_chunk['tpep_pickup_datetime'] = pd.to_datetime(df_chunk['tpep_pickup_datetime'])
        df_chunk['tpep_dropoff_datetime'] = pd.to_datetime(df_chunk['tpep_dropoff_datetime'])  
        
        df_chunk.to_sql(name='yellow_taxi_data', con=engine, if_exists='append', index=False)
        
        t_end = time()
        
        print(f'Inserted another chunk.. %.3f seconds ' % (t_end - t_start))



if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    # user name for postgres
    parser.add_argument('user', help='user name for postgres')

    # password for postgres
    parser.add_argument('password', help='password for postgres')

    # host for postgres
    parser.add_argument('host', help='host for postgres')

    # port for postgres
    parser.add_argument('port', help='port for postgres')

    # database name for postgres
    parser.add_argument('db', help='database name for postgres')

    # table name of where ny taxi data will be loaded
    parser.add_argument('table-name', help='table name of where ny taxi data will be loaded')

    # url of the csv file
    parser.add_argument('url', help='url of the csv file')

    args = parser.parse_args()

    main(args)











