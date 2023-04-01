import os
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator, 
    DataprocSubmitJobOperator, 
    DataprocDeleteClusterOperator
)
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator
from airflow.models.baseoperator import chain
from pyarrow import csv as pv
from pyarrow import parquet as pq
import pandas as pd
from holidays import country_holidays

from metadata import LIQUOR_SCHEMA, POPULATION_SCHEMA

AIRFLOW_HOME = os.getenv("AIRFLOW_HOME")
DATA_DIRECTORY = f"{AIRFLOW_HOME}/data"
CODE_DIRECTORY = f"{AIRFLOW_HOME}/code"

# Small dataset (2019)
# IOWA_LIQUOR_FILE_URL = "https://data.iowa.gov/api/views/38x4-vs5h/rows.csv\?accessType\=DOWNLOAD"
# Full dataset (2012-2023)
IOWA_LIQUOR_FILE_URL = "https://data.iowa.gov/api/views/m3tr-qhgy/rows.csv\?accessType\=DOWNLOAD"
IOWA_LIQUOR_FILE_NAME = "iowa_liquor"

IOWA_POPULATION_FILE_URL = "https://data.iowa.gov/api/views/y8va-rhk9/rows.csv?accessType=DOWNLOAD"
IOWA_POPULATION_FILE_NAME = "iowa_population"

HOLIDAYS_FILE_NAME = "us_holidays"
START_YEAR = 2012
END_YEAR = "{{ ds.strftime('%Y') }}"
BLOCK_SIZE = 64 * 1024 * 1024

GCS_BUCKET = "dtc_data_lake_de-zoomcamp-375618"
GCS_BUCKET_PATH = "iowa_liquor"
GCP_REGION = "europe-west6"
GCP_PROJECT_ID = "de-zoomcamp-375618"

PYSPARK_URI = f"gs://{GCS_BUCKET}/{GCS_BUCKET_PATH}/code/pyspark_write_to_bq.py"
DATAPROC_CLUSTER = "de-zoomcamp-cluster"
DATAPROC_CLUSTER_CONFIG = {
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "n1-standard-2",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 512},
    },
    "worker_config": {
        "num_instances": 2,
        "machine_type_uri": "n1-standard-2",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 512},
    },
}
PYSPARK_JOB = {
    "reference": {"project_id": GCP_PROJECT_ID},
    "placement": {"cluster_name": DATAPROC_CLUSTER},
    "pyspark_job": {
        "main_python_file_uri": PYSPARK_URI,
        "jar_file_uris": ["gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar"]
    },                          
}

DATA_TO_GCS_LIST = [
    f"{DATA_DIRECTORY}/{IOWA_LIQUOR_FILE_NAME}.parquet",
    f"{DATA_DIRECTORY}/{IOWA_POPULATION_FILE_NAME}.parquet",
    f"{DATA_DIRECTORY}/{HOLIDAYS_FILE_NAME}.parquet",
]

CODE_TO_GCS_LIST = [
    f"{CODE_DIRECTORY}/pyspark_write_to_bq.py",
]

BQ_DATASET = "iowa_liquor"
BQ_TABLE_LIQUOR_SALE = "fact_liquor_sale"
BQ_TABLE_POPULATION = "dim_population"
BQ_TABLE_HOLIDAY = "dim_holiday"


def convert_csv_to_parquet(file_name, schema, timestamp_parsers):
    column_names = [field.name for field in schema]

    with pq.ParquetWriter(
        f"{file_name}.parquet", schema
    ) as pq_writer:
        chunks = pv.open_csv(
            f"{file_name}.csv",
            read_options=pv.ReadOptions(
                block_size=BLOCK_SIZE,
                column_names=column_names,
                skip_rows=1,
            ),
            convert_options=pv.ConvertOptions(
                column_types=schema,
                timestamp_parsers=timestamp_parsers,
            ),
        )

        for chunk in chunks:
            pq_writer.write_batch(chunk)


def create_holidays_parquet(start_year, end_year):
    data = []
    end_year = int(end_year) + 1
    
    for year in range(start_year, end_year):
        holidays = country_holidays("US", years=year, observed=False)
        for holiday in holidays.items():
            data.append(holiday)

    df = pd.DataFrame(data, columns=["holiday_date", "holiday_name"])
    df["holiday_date"] = pd.to_datetime(df["holiday_date"])
    df.to_parquet(f"{DATA_DIRECTORY}/{HOLIDAYS_FILE_NAME}.parquet")
    

def transform_population_csv():
    df = pd.read_csv(f"{DATA_DIRECTORY}/{IOWA_POPULATION_FILE_NAME}.csv")
    df["City"] = df["City"].str.replace(" (pt.)", "", regex=False)
    df["Year"] = df["Year"].str.slice(-4).astype("int")
    df["Estimate"] = df["Estimate"].fillna(0).astype("int")
    df.to_csv(f"{DATA_DIRECTORY}/{IOWA_POPULATION_FILE_NAME}.csv", index=False)
    

with DAG(
    "elt_iowa_liquor",
    start_date=datetime(2023, 3, 1),
    schedule_interval="0 0 3 * *",
) as dag:

    create_holidays_parquet_task = PythonOperator(
        task_id="create_holidays_parquet",
        python_callable=create_holidays_parquet,
        op_kwargs={
            "start_year": START_YEAR,
            "end_year": "{{ logical_date.strftime('%Y') }}",
        },
    )

    download_iowa_liquor_file_task = BashOperator(
        task_id="download_iowa_liquor_file",
        bash_command=f"curl -o {DATA_DIRECTORY}/{IOWA_LIQUOR_FILE_NAME}.csv {IOWA_LIQUOR_FILE_URL}",
    )
    
    download_iowa_population_file_task = BashOperator(
        task_id="download_iowa_population_file",
        bash_command=f"curl -o {DATA_DIRECTORY}/{IOWA_POPULATION_FILE_NAME}.csv {IOWA_POPULATION_FILE_URL}",
    )    
    
    transform_population_csv_task = PythonOperator(
        task_id="transform_population_csv",
        python_callable=transform_population_csv
    )

    convert_iowa_liquor_csv_to_parquet_task = PythonOperator(
        task_id="convert_iowa_liquor_csv_to_parquet",
        python_callable=convert_csv_to_parquet,
        op_kwargs={
            "file_name": f"{DATA_DIRECTORY}/{IOWA_LIQUOR_FILE_NAME}", 
            "schema": LIQUOR_SCHEMA, 
            "timestamp_parsers": ["%m/%d/%Y"]
        }
    )
    
    convert_iowa_population_csv_to_parquet_task = PythonOperator(
        task_id="convert_iowa_population_csv_to_parquet",
        python_callable=convert_csv_to_parquet,
        op_kwargs={
            "file_name": f"{DATA_DIRECTORY}/{IOWA_POPULATION_FILE_NAME}", 
            "schema": POPULATION_SCHEMA, 
            "timestamp_parsers": ["%B %d %Y"]
        }
    )    

    upload_data_to_gcs_task = LocalFilesystemToGCSOperator(
        task_id="upload_data_to_gcs",
        src=DATA_TO_GCS_LIST,
        dst=f"{GCS_BUCKET_PATH}/",
        bucket=GCS_BUCKET,
    )
    
    upload_code_to_gcs_task = LocalFilesystemToGCSOperator(
        task_id="upload_code_to_gcs",
        src=CODE_TO_GCS_LIST,
        dst=f"{GCS_BUCKET_PATH}/code/",
        bucket=GCS_BUCKET,
    )    
   
    create_bq_dataset_task = BigQueryCreateEmptyDatasetOperator(
        task_id="create_bq_dataset", 
        dataset_id=BQ_DATASET, 
        location=GCP_REGION
    )
    
    create_cluster_task = DataprocCreateClusterOperator(
        task_id="create_cluster",
        project_id=GCP_PROJECT_ID,
        cluster_config=DATAPROC_CLUSTER_CONFIG,
        region=GCP_REGION,
        cluster_name=DATAPROC_CLUSTER,
    )

    pyspark_write_to_bq_task = DataprocSubmitJobOperator(
        task_id="pyspark_write_to_bq", 
        job=PYSPARK_JOB, 
        region=GCP_REGION, 
        project_id=GCP_PROJECT_ID
    )    
    
    delete_cluster_task = DataprocDeleteClusterOperator(
        task_id="delete_cluster",
        project_id=GCP_PROJECT_ID,
        region=GCP_REGION,
        cluster_name=DATAPROC_CLUSTER,
    )   
    
    create_bq_table_dim_population_task = GCSToBigQueryOperator(
        task_id="create_bq_table_dim_population",
        bucket=GCS_BUCKET,
        source_objects=[f"{GCS_BUCKET_PATH}/{IOWA_POPULATION_FILE_NAME}.parquet"],
        source_format="PARQUET",
        destination_project_dataset_table=f"{BQ_DATASET}.{BQ_TABLE_POPULATION}",
        location=GCP_REGION,
        write_disposition="WRITE_TRUNCATE",
    )     

    create_bq_table_dim_holidays_task = GCSToBigQueryOperator(
        task_id="create_bq_table_dim_holidays",
        bucket=GCS_BUCKET,
        source_objects=[f"{GCS_BUCKET_PATH}/{HOLIDAYS_FILE_NAME}.parquet"],
        source_format="PARQUET",
        destination_project_dataset_table=f"{BQ_DATASET}.{BQ_TABLE_HOLIDAY}",
        location=GCP_REGION,
        write_disposition="WRITE_TRUNCATE",
    )

    delete_local_files_task = BashOperator(
        task_id="delete_local_files",
        bash_command=f"rm -f {DATA_DIRECTORY}/*",
    )

    chain(
        download_iowa_liquor_file_task, 
        convert_iowa_liquor_csv_to_parquet_task, 
        create_bq_dataset_task
        
    )
    
    chain(
        download_iowa_population_file_task, 
        transform_population_csv_task,
        convert_iowa_population_csv_to_parquet_task, 
        create_bq_dataset_task
    )    

    chain(
        create_holidays_parquet_task, 
        create_bq_dataset_task
    )
    
    chain(
        create_bq_dataset_task,
        [upload_code_to_gcs_task, upload_data_to_gcs_task],
        create_cluster_task, 
        [
            create_bq_table_dim_population_task, 
            create_bq_table_dim_holidays_task, 
            pyspark_write_to_bq_task
        ],
        delete_cluster_task,
        delete_local_files_task  
    )

    chain(
        delete_cluster_task,               
        delete_local_files_task
    )
