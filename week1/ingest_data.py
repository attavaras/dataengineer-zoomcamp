from time import time

import pandas as pd
from sqlalchemy import create_engine

import argparse
import os

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db_name = params.db
    table_name = params.table_name
    url = params.url
    csv_name = "green_tripdata_2019-09.csv"

    #os.system(f"wget {url} -O {csv_name}")

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db_name}')

    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=50000)
    df = next(df_iter)
    df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)
    df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
    df.head(n=0).to_sql(con=engine, name='green_taxi_data', if_exists='replace')
    while len(df) == 50000:
        t_start = time()

        df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)
        df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)

        df.to_sql(con=engine, name=table_name, if_exists='append')
        t_end = time()

        print("inserted another chunk ... %.3f seconds " % (t_end - t_start))
        
        df = next(df_iter)

    df.to_sql(con=engine, name=table_name, if_exists='append')



parser = argparse.ArgumentParser(description='Ingest CSV file to Postgres')

# user
# password 
# host
# port
# database name
# table name
# url of the csv


if __name__ == "__main__":

    parser.add_argument('--user', help='user name for postgres') 
    parser.add_argument('--password', help='password for postgres') 
    parser.add_argument('--host', help='hostname for postgres') 
    parser.add_argument('--port', help='port for postgres') 
    parser.add_argument('--db', help='dbname for postgres') 
    parser.add_argument('--table_name', help='table name where we will write the results in postgres') 
    parser.add_argument('--url', help='url of the csv file') 

    args = parser.parse_args()
    main(args)

