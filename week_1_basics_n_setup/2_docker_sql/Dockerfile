FROM python:3.9.1

RUN apt-get install wget
RUN pip install pandas sqlalchemy psycopg2
RUN pip freeze

WORKDIR /app
COPY ingest_data.py ingest_data.py 
COPY "yellow_tripdata_2021-01.csv.gz" output.csv.gz

ENTRYPOINT [ "python", "ingest_data.py" ]