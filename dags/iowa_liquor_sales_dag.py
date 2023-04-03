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

from metadata import LIQUOR_SCHEMA, CITY_COUNTY_POPULATION_SCHEMA, CITY_POPULATION_SCHEMA, COUNTY_POPULATION_SCHEMA

AIRFLOW_HOME = os.getenv("AIRFLOW_HOME")
DATA_DIRECTORY = f"{AIRFLOW_HOME}/data"
CODE_DIRECTORY = f"{AIRFLOW_HOME}/code"

LIQUOR_SALE_NAME = "fact_liquor_sale"
CITY_COUNTY_POPULATION_NAME = "dim_city_county_population"
CITY_POPULATION_NAME = "dim_city_population"
COUNTY_POPULATION_NAME = "dim_county_population"
HOLIDAYS_NAME = "dim_holiday"
SOURCE_FILE_URLS = {
    # LIQUOR_SALE_NAME: "https://data.iowa.gov/api/views/38x4-vs5h/rows.csv\?accessType\=DOWNLOAD",
    LIQUOR_SALE_NAME: "https://data.iowa.gov/api/views/m3tr-qhgy/rows.csv\?accessType\=DOWNLOAD",
    CITY_COUNTY_POPULATION_NAME: "https://data.iowa.gov/api/views/y8va-rhk9/rows.csv?accessType=DOWNLOAD", 
    CITY_POPULATION_NAME: "https://data.iowa.gov/api/views/acem-thbp/rows.csv?accessType=DOWNLOAD",
    COUNTY_POPULATION_NAME: "https://data.iowa.gov/api/views/qtnr-zsrc/rows.csv?accessType=DOWNLOAD",
}

START_YEAR = 2012
END_YEAR = "{{ ds.strftime('%Y') }}"
BLOCK_SIZE = 64 * 1024 * 1024

GCS_BUCKET = "dtc_data_lake_de-zoomcamp-375618"
GCS_BUCKET_PATH = "iowa_liquor"
BQ_DATASET = "iowa_liquor"
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
    f"{DATA_DIRECTORY}/{LIQUOR_SALE_NAME}.parquet",
    f"{DATA_DIRECTORY}/{CITY_COUNTY_POPULATION_NAME}.parquet",
    f"{DATA_DIRECTORY}/{CITY_POPULATION_NAME}.parquet",
    f"{DATA_DIRECTORY}/{COUNTY_POPULATION_NAME}.parquet",
    f"{DATA_DIRECTORY}/{HOLIDAYS_NAME}.parquet",
]

CODE_TO_GCS_LIST = [
    f"{CODE_DIRECTORY}/pyspark_write_to_bq.py",
]


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
    df.to_parquet(f"{DATA_DIRECTORY}/{HOLIDAYS_NAME}.parquet")
    

def transform_population_csv(file_name):
    df = pd.read_csv(f"{DATA_DIRECTORY}/{file_name}.csv")
    if "City" in df.columns:
        df["City"] = df["City"].str.replace(" (pt.)", "", regex=False)
    if "County" in df.columns:
        df["County"] = df["County"].str.replace(" County", "", regex=False)
    df["Year"] = df["Year"].str.slice(-4).astype("int")
    if "Estimate" in df.columns:
        df["Estimate"] = df["Estimate"].fillna(0).astype("int")
    if "Population" in df.columns:
        df["Population"] = df["Population"].fillna(0).astype("int")    
    df.to_csv(f"{DATA_DIRECTORY}/{file_name}.csv", index=False)
    
    
with DAG(
    "iowa_liquor_sales_dag", 
    start_date=datetime(2023, 4, 1),
    schedule_interval="0 0 3 * *",
    catchup = False
) as dag:
    
    create_holidays_parquet_task = PythonOperator(
        task_id="create_holidays_parquet",
        python_callable=create_holidays_parquet,
        op_kwargs={
            "start_year": START_YEAR,
            "end_year": "{{ logical_date.strftime('%Y') }}",
        },
    )  
    
    download_liquor_sales_file_task = BashOperator(
        task_id="download_liquor_sales_file",
        bash_command=f"curl -o {DATA_DIRECTORY}/{LIQUOR_SALE_NAME}.csv {SOURCE_FILE_URLS[LIQUOR_SALE_NAME]}",
    )
    
    download_city_county_population_file_task = BashOperator(
        task_id="download_city_county_population_file_task",
        bash_command=f"curl -o {DATA_DIRECTORY}/{CITY_COUNTY_POPULATION_NAME}.csv {SOURCE_FILE_URLS[CITY_COUNTY_POPULATION_NAME]}",
    )
    
    download_city_population_file_task = BashOperator(
        task_id="download_city_population_file_task",
        bash_command=f"curl -o {DATA_DIRECTORY}/{CITY_POPULATION_NAME}.csv {SOURCE_FILE_URLS[CITY_POPULATION_NAME]}",
    )

    download_county_population_file_task = BashOperator(
        task_id="download_county_population_file_task",
        bash_command=f"curl -o {DATA_DIRECTORY}/{COUNTY_POPULATION_NAME}.csv {SOURCE_FILE_URLS[COUNTY_POPULATION_NAME]}",
    )
    
    transform_city_county_population_csv_task = PythonOperator(
        task_id="transform_city_county_population_csv",
        python_callable=transform_population_csv,
        op_kwargs={"file_name": CITY_COUNTY_POPULATION_NAME}
    )
    
    transform_city_population_csv_task = PythonOperator(
        task_id="transform_city_population_csv",
        python_callable=transform_population_csv,
        op_kwargs={"file_name": CITY_POPULATION_NAME}
    )

    transform_county_population_csv_task = PythonOperator(
        task_id="transform_county_population_csv_task",
        python_callable=transform_population_csv,
        op_kwargs={"file_name": COUNTY_POPULATION_NAME}
    )

    convert_liquor_sales_csv_to_parquet_task = PythonOperator(
        task_id="convert_liquor_sales_csv_to_parquet",
        python_callable=convert_csv_to_parquet,
        op_kwargs={
            "file_name": f"{DATA_DIRECTORY}/{LIQUOR_SALE_NAME}", 
            "schema": LIQUOR_SCHEMA, 
            "timestamp_parsers": ["%m/%d/%Y"]
        }
    )
    
    convert_city_county_population_csv_to_parquet_task = PythonOperator(
        task_id="convert_city_county_population_csv_to_parquet",
        python_callable=convert_csv_to_parquet,
        op_kwargs={
            "file_name": f"{DATA_DIRECTORY}/{CITY_COUNTY_POPULATION_NAME}", 
            "schema": CITY_COUNTY_POPULATION_SCHEMA, 
            "timestamp_parsers": ["%B %d %Y"]
        }
    )
    
    convert_city_population_csv_to_parquet_task = PythonOperator(
        task_id="convert_city_population_csv_to_parquet",
        python_callable=convert_csv_to_parquet,
        op_kwargs={
            "file_name": f"{DATA_DIRECTORY}/{CITY_POPULATION_NAME}", 
            "schema": CITY_POPULATION_SCHEMA, 
            "timestamp_parsers": ["%B %d %Y"]
        }
    )
    
    convert_county_population_csv_to_parquet_task = PythonOperator(
        task_id="convert_county_population_csv_to_parquet",
        python_callable=convert_csv_to_parquet,
        op_kwargs={
            "file_name": f"{DATA_DIRECTORY}/{COUNTY_POPULATION_NAME}", 
            "schema": COUNTY_POPULATION_SCHEMA, 
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
    
    create_bq_table_dim_city_county_population_task = GCSToBigQueryOperator(
        task_id="create_bq_table_dim_city_county_population",
        bucket=GCS_BUCKET,
        source_objects=[f"{GCS_BUCKET_PATH}/{CITY_COUNTY_POPULATION_NAME}.parquet"],
        source_format="PARQUET",
        destination_project_dataset_table=f"{BQ_DATASET}.{CITY_COUNTY_POPULATION_NAME}",
        location=GCP_REGION,
        write_disposition="WRITE_TRUNCATE",
    )   
    
    create_bq_table_dim_city_population_task = GCSToBigQueryOperator(
        task_id="create_bq_table_dim_city_population",
        bucket=GCS_BUCKET,
        source_objects=[f"{GCS_BUCKET_PATH}/{CITY_POPULATION_NAME}.parquet"],
        source_format="PARQUET",
        destination_project_dataset_table=f"{BQ_DATASET}.{CITY_POPULATION_NAME}",
        location=GCP_REGION,
        write_disposition="WRITE_TRUNCATE",
    )       
    
    create_bq_table_dim_county_population_task = GCSToBigQueryOperator(
        task_id="create_bq_table_dim_county_population",
        bucket=GCS_BUCKET,
        source_objects=[f"{GCS_BUCKET_PATH}/{COUNTY_POPULATION_NAME}.parquet"],
        source_format="PARQUET",
        destination_project_dataset_table=f"{BQ_DATASET}.{COUNTY_POPULATION_NAME}",
        location=GCP_REGION,
        write_disposition="WRITE_TRUNCATE",
    )         

    create_bq_table_dim_holidays_task = GCSToBigQueryOperator(
        task_id="create_bq_table_dim_holidays",
        bucket=GCS_BUCKET,
        source_objects=[f"{GCS_BUCKET_PATH}/{HOLIDAYS_NAME}.parquet"],
        source_format="PARQUET",
        destination_project_dataset_table=f"{BQ_DATASET}.{HOLIDAYS_NAME}",
        location=GCP_REGION,
        write_disposition="WRITE_TRUNCATE",
    )

    delete_local_files_task = BashOperator(
        task_id="delete_local_files",
        bash_command=f"rm -f {DATA_DIRECTORY}/*",
    )    
    
    chain(
        download_liquor_sales_file_task, 
        convert_liquor_sales_csv_to_parquet_task, 
        create_bq_dataset_task
        
    )
    
    chain(
        download_city_county_population_file_task, 
        transform_city_county_population_csv_task,
        convert_city_county_population_csv_to_parquet_task, 
        create_bq_dataset_task
    )    
    
    chain(
        download_city_population_file_task, 
        transform_city_population_csv_task,
        convert_city_population_csv_to_parquet_task, 
        create_bq_dataset_task
    )    
    
    chain(
        download_county_population_file_task, 
        transform_county_population_csv_task,
        convert_county_population_csv_to_parquet_task, 
        create_bq_dataset_task
    )            

    chain(
        create_holidays_parquet_task, 
        create_bq_dataset_task
    )
    
    chain(
        create_bq_dataset_task,
        [
            upload_code_to_gcs_task, 
            upload_data_to_gcs_task
        ],
        create_cluster_task, 
        [
            create_bq_table_dim_city_county_population_task, 
            create_bq_table_dim_city_population_task, 
            create_bq_table_dim_county_population_task, 
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
