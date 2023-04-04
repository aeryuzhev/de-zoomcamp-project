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
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator, 
    BigQueryInsertJobOperator
)
from airflow.models.baseoperator import chain
from pyarrow import csv as pv
from pyarrow import parquet as pq
import pandas as pd
from holidays import country_holidays

from iowa_liquor_sales_dag_config import *

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


def transform_population_csv(file_name):
    df = pd.read_csv(f"{DATA_DIRECTORY}/{file_name}.csv")
    df["County"] = df["County"].str.replace(" County", "", regex=False)
    df["Year"] = df["Year"].str.slice(-4).astype("int")
    df["Population"] = df["Population"].fillna(0).astype("int")    
    df.to_csv(f"{DATA_DIRECTORY}/{file_name}.csv", index=False)
    
    
with DAG(
    "iowa_liquor_sales_dag", 
    start_date=datetime(2023, 4, 1),
    schedule_interval="0 0 3 * *",
    catchup = False
) as dag:
    
    download_liquor_sales_file_task = BashOperator(
        task_id="download_liquor_sales_file",
        bash_command=f"curl -o {DATA_DIRECTORY}/{LIQUOR_SALE_NAME}.csv {SOURCE_FILE_URLS[LIQUOR_SALE_NAME]}",
    )
    
    download_county_population_file_task = BashOperator(
        task_id="download_county_population_file_task",
        bash_command=f"curl -o {DATA_DIRECTORY}/{COUNTY_POPULATION_NAME}.csv {SOURCE_FILE_URLS[COUNTY_POPULATION_NAME]}",
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
            "schema": LIQUOR_PYARROW_SCHEMA, 
            "timestamp_parsers": ["%m/%d/%Y"]
        }
    )
    
    convert_county_population_csv_to_parquet_task = PythonOperator(
        task_id="convert_county_population_csv_to_parquet",
        python_callable=convert_csv_to_parquet,
        op_kwargs={
            "file_name": f"{DATA_DIRECTORY}/{COUNTY_POPULATION_NAME}", 
            "schema": COUNTY_POPULATION_PYARROW_SCHEMA, 
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
        
    create_bq_table_dim_county_population_task = GCSToBigQueryOperator(
        task_id="create_bq_table_dim_county_population",
        bucket=GCS_BUCKET,
        source_objects=[f"{GCS_BUCKET_PATH}/{COUNTY_POPULATION_NAME}.parquet"],
        source_format="PARQUET",
        schema_fields=COUNTY_POPULATION_BQ_SCHEMA,
        destination_project_dataset_table=f"{BQ_DATASET}.{COUNTY_POPULATION_NAME}",
        location=GCP_REGION,
        write_disposition="WRITE_TRUNCATE",
    )         
    
    create_bq_table_dm_total_county_sale_task = BigQueryInsertJobOperator(
        task_id="create_bq_table_dm_total_county_sale",
        configuration={
            "query": {
                "query": DDL_DM_TOTAL_COUNTY_SALE,
                "useLegacySql": False,                
            }
        }
    )
    
    create_bq_table_dm_total_year_sale_task = BigQueryInsertJobOperator(
        task_id="create_bq_table_dm_total_year_sale",
        configuration={
            "query": {
                "query": DDL_DM_TOTAL_YEAR_SALE,
                "useLegacySql": False,
            }
        }
    )
    
    create_bq_table_dm_total_category_sale_task = BigQueryInsertJobOperator(
        task_id="create_bq_table_dm_total_category_sale",
        configuration={
            "query": {
                "query": DDL_DM_TOTAL_CATEGORY_SALE,
                "useLegacySql": False,
            }
        }
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
        download_county_population_file_task, 
        transform_county_population_csv_task,
        convert_county_population_csv_to_parquet_task, 
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
            create_bq_table_dim_county_population_task, 
            pyspark_write_to_bq_task,            
        ],
        delete_cluster_task,
        delete_local_files_task,
        [
            create_bq_table_dm_total_county_sale_task,  
            create_bq_table_dm_total_year_sale_task,
            create_bq_table_dm_total_category_sale_task,
        ]
    )
