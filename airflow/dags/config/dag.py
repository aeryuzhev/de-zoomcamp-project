import os

from gcp import GCP_PROJECT_ID, GCS_BUCKET, GCS_BUCKET_PATH

# Airflow pathes for data and scripts.
AIRFLOW_HOME = os.getenv("AIRFLOW_HOME")
DATA_DIRECTORY = f"{AIRFLOW_HOME}/data"
CODE_DIRECTORY = f"{AIRFLOW_HOME}/scripts"

# Names for source files and for sql tables.
LIQUOR_SALE_NAME = "fact_liquor_sale"
COUNTY_POPULATION_NAME = "dim_county_population"

# Source file urls.
SOURCE_FILE_URLS = {
    # LIQUOR_SALE_NAME: "https://data.iowa.gov/api/views/38x4-vs5h/rows.csv\?accessType\=DOWNLOAD",
    LIQUOR_SALE_NAME: "https://data.iowa.gov/api/views/m3tr-qhgy/rows.csv\?accessType\=DOWNLOAD",
    COUNTY_POPULATION_NAME: "https://data.iowa.gov/api/views/qtnr-zsrc/rows.csv?accessType=DOWNLOAD",
}

# Lists of files for uploading to GCS.
DATA_TO_GCS_LIST = [
    f"{DATA_DIRECTORY}/{LIQUOR_SALE_NAME}.parquet",
    f"{DATA_DIRECTORY}/{COUNTY_POPULATION_NAME}.parquet",
]
CODE_TO_GCS_LIST = [
    f"{CODE_DIRECTORY}/pyspark_write_to_bq.py",
]

# Pyspark job and cluster configuration.
PYSPARK_URI = f"gs://{GCS_BUCKET}/{GCS_BUCKET_PATH}/scripts/pyspark_write_to_bq.py"
DATAPROC_CLUSTER = "iowa-cluster"
DATAPROC_CLUSTER_CONFIG = {
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "n1-standard-2",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 64},
    },
    "worker_config": {
        "num_instances": 2,
        "machine_type_uri": "n1-standard-2",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 64},
    },
}
PYSPARK_JOB = {
    "reference": {"project_id": GCP_PROJECT_ID},
    "placement": {"cluster_name": DATAPROC_CLUSTER},
    "pyspark_job": {
        "main_python_file_uri": PYSPARK_URI,
        "args": [GCP_PROJECT_ID],
        "jar_file_uris": ["gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar"]
    },                          
}

# Block size when reading a large csv and convert it by batches to parquet.
READ_BLOCK_SIZE = 64 * 1024 * 1024