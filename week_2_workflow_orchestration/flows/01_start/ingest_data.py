#!/usr/bin/env python
# coding: utf-8

import os
import argparse

from time import time

import pandas as pd
from sqlalchemy import create_engine
from prefect import task,flow
from prefect.tasks import task_input_hash
from datetime import timedelta


from prefect_sqlalchemy import SqlAlchemyConnector

@task(log_prints=True,retries=2,cache_key_fn=task_input_hash,cache_expiration=timedelta(days=1))
def extract_data(url):
    if url.endswith('.csv.gz'):
        csv_name = 'output.csv.gz'
    else:
        csv_name = 'output.csv'
    os.system(f"wget {url} -O {csv_name}")

    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)

    df = next(df_iter)

    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
    return df
@task(log_prints=True, )
def transform_date(df):
    print(f"missing pass count {(df.passenger_count==0).sum()}")
    df = df.query("passenger_count !=0")
    print(f"missing pass count {(df.passenger_count==0).sum()}")
    return df


@task(log_prints=True,retries=2)    
def ingest_data(table_name,df):
    # user = params.get("user")
    # password = params.get("password")
    # host = params.get("host") 
    # port = params.get("port") 
    # db = params.get("db")
    # table_name = params.get("table_name")
    # df = params.get("data")

    
    # the backup files are gzipped, and it's important to keep the correct extension
    # for pandas to be able to open the file
   
    


    ny_taxi_db = SqlAlchemyConnector.load("post-ny-taxi")
    with ny_taxi_db.get_connection(begin=False) as  engine:
    # engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    
        df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

        df.to_sql(name=table_name, con=engine, if_exists='append')





@flow(name="ingest flow")
def main_flow(table_name):
    # args ={
    # "user" : "root",
    # "password" : "root",
    # "host" : "localhost" ,
    # "port" : "5432" ,
    # "db" : "ny_taxi",
    # "table_name" : "yellow_taxi_trips",
    # }
    url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz"
    raw_data = extract_data(url=url)
    data = transform_date(raw_data)
    # args['data']=data
    ingest_data(table_name=table_name,df=data)


if __name__ == '__main__':
  main_flow("yellow_taxi_trips")
