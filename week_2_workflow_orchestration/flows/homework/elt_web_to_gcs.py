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
def clean(df: pd.DataFrame,color:str) -> pd.DataFrame:

    print(df.columns)
    if color=="yellow":
        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
    elif color =="green":
        df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
        df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)
       
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
def etl_web_to_gcs()-> None:
    """the main ETL FUNC"""
    color = "green"
    year = 2020
    month = 1
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"
    df = fetch(dataset_url)
    df_clean = clean(df=df,color=color)
    path = write_local(df=df_clean,dataset_file=dataset_file,color=color)
    write_gcs(path)


if __name__ == '__main__':
  etl_web_to_gcs()
