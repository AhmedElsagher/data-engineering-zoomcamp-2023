from pathlib import Path
import pandas as pd
from prefect import flow,task
from prefect_gcp import GcsBucket
import os 
from prefect.tasks import task_input_hash
from datetime import timedelta


# its not your local
@task(log_prints=True,retries=2,cache_key_fn=task_input_hash,cache_expiration=timedelta(days=1))
def fetch(dataset_url : str) -> pd.DataFrame:
    # dataset_file = dataset_url.split("/")[-1]
    # local_data_path = f"/home/ahmedelsogheer/Documents/zoom/data-engineering-zoomcamp/week_2_workflow_orchestration/data/{dataset_file}"
    # os.system(f"wget {dataset_url} -O {local_data_path}")

    df = pd.read_csv(dataset_url)
    return df 

@task(log_prints=True)
def clean(df: pd.DataFrame) -> pd.DataFrame:


    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
    print(df.dtypes)
    print(f"rows: {df.shape[0]}")
    print(df.columns)
    return df
 
@task(log_prints=True)
def write_local(df :pd.DataFrame,color:str,dataset_file:str)-> Path:

    path=Path(f"data/{color}/{dataset_file}.parquet")
    print(os.getcwd())
    df.to_parquet(f"../../{path}",index=False,compression="gzip")
    print(path)
    return path

@task(log_prints=True)
def write_gcs(path:str)-> None:
    bucket_block = GcsBucket.load("zoom-gcs")
    bucket_block.upload_from_path(
        from_path = f"../../{path}",
        to_path=path)
   

@flow()
def etl_web_to_gcs(year:int,month:int,color:str)-> None:
    """the main ETL FUNC"""
    
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"
    df = fetch(dataset_url)
    df_clean = clean(df)
    path = write_local(df=df_clean,dataset_file=dataset_file,color=color)
    write_gcs(path)
@flow()
def etl_parent_flow(
        months :list[int]=[1,2],
        year: int=2021,
        color:str="yellow"):
    for month in months:
        etl_web_to_gcs(year=year,color=color,month=month)

if __name__ == '__main__':
    months =[1,3]
    year: int=2021
    color:str="yellow"
    etl_parent_flow(months=months,year=year,color=color)
