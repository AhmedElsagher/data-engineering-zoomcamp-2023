from pathlib import Path
import pandas as pd
from prefect import flow,task
from prefect_gcp import GcpCredentials,GcsBucket
import os 
from prefect.tasks import task_input_hash
from datetime import timedelta


@task(log_prints=True,retries=2,cache_key_fn=task_input_hash,cache_expiration=timedelta(days=1))
def extract_from_gcs(color,year,month)->Path:
    gcs_path = f"{color}/{color}_tripdata_{year}-{month:02}.parquet"
    bucket_block = GcsBucket.load("zoom-gcs")
    bucket_block.get_directory(
        from_path = f"data/{gcs_path}",
        local_path ="../../")
    

    return Path(f"../../data/{gcs_path}")

@task(log_prints=True, )
def transform(path:Path)->pd.DataFrame:
    df =pd.read_parquet(path)
    # print(df.columns.values)
    # print(f"count missing passenger_count {df.passenger_count.isnull().sum()}")
    # df.passenger_count = df.passenger_count.fillna(0)
    # print(f"count missing passenger_count {df.passenger_count.isnull().sum()}")
    return df

@task(log_prints=True, )
def write_to_bg(df:pd.DataFrame)->None:
    gcs_creds = GcpCredentials.load("zoom-gcp-cred")
    gcs_creds = gcs_creds.get_credentials_from_service_account()
    df.to_gbq(
        destination_table="dezoomcamp.yellow_trips_2019",
        project_id="dtc-de-course-350819",
        credentials=gcs_creds,
        chunksize=500_000,
        if_exists="append"
    )
    return None


@flow()
def etl_gcs_to_bg(color,year,month):


    path = extract_from_gcs(color=color,year=year,month=month)
    df = transform(path)
    write_to_bg(df)


@flow()
def etl_bg_parent_flow(
        months :list[int]=[2,3],
        year: int=2019,
        color:str="yellow"):
    for month in months:
        etl_gcs_to_bg(year=year,color=color,month=month)

if __name__ == '__main__':
    months =[2,3]
    year: int=2019
    color:str="yellow"
    etl_bg_parent_flow(months=months,year=year,color=color)

