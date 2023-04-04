import os

import pyarrow as pa

AIRFLOW_HOME = os.getenv("AIRFLOW_HOME")
DATA_DIRECTORY = f"{AIRFLOW_HOME}/data"
CODE_DIRECTORY = f"{AIRFLOW_HOME}/code"

LIQUOR_SALE_NAME = "fact_liquor_sale"
COUNTY_POPULATION_NAME = "dim_county_population"
SOURCE_FILE_URLS = {
    # LIQUOR_SALE_NAME: "https://data.iowa.gov/api/views/38x4-vs5h/rows.csv\?accessType\=DOWNLOAD",
    LIQUOR_SALE_NAME: "https://data.iowa.gov/api/views/m3tr-qhgy/rows.csv\?accessType\=DOWNLOAD",
    COUNTY_POPULATION_NAME: "https://data.iowa.gov/api/views/qtnr-zsrc/rows.csv?accessType=DOWNLOAD",
}

BEGIN_SALES_YEAR = 2012
END_SALES_YEAR = 2021

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
    f"{DATA_DIRECTORY}/{COUNTY_POPULATION_NAME}.parquet",
]

CODE_TO_GCS_LIST = [
    f"{CODE_DIRECTORY}/pyspark_write_to_bq.py",
]

LIQUOR_PYARROW_SCHEMA = pa.schema(
    [
        ("invoice_item_number", pa.string()),
        ("sale_date", pa.timestamp("s")),
        ("store_number", pa.int64()),
        ("store_name", pa.string()),
        ("address", pa.string()),
        ("city", pa.string()),
        ("zip_code", pa.string()),
        ("store_location", pa.string()),
        ("county_number", pa.int64()),
        ("county", pa.string()),
        ("category", pa.int64()),
        ("category_name", pa.string()),
        ("vendor_number", pa.int64()),
        ("vendor_name", pa.string()),
        ("item_number", pa.string()),
        ("item_description", pa.string()),
        ("pack", pa.int64()),
        ("bottle_volume_ml", pa.int64()),
        ("state_bottle_cost", pa.float64()),
        ("state_bottle_retail", pa.float64()),
        ("bottles_sold", pa.int64()),
        ("sale_dollars", pa.float64()),
        ("volume_sold_liters", pa.float64()),
        ("volume_sold_gallons", pa.float64()),
    ]
)

COUNTY_POPULATION_PYARROW_SCHEMA = pa.schema(
    [
        ("fips", pa.int64()),
        ("county", pa.string()),
        ("year", pa.int64()),
        ("population", pa.int64()),
        ("primary_point", pa.string()), 
    ]
)

COUNTY_POPULATION_BQ_SCHEMA = [
    {"name": "fips", "type": "INT64"},
    {"name": "county", "type": "STRING"},
    {"name": "year", "type": "INT64"},
    {"name": "population", "type": "INT64"},
    {"name": "primary_point", "type": "GEOGRAPHY"}, 
]


DDL_DM_TOTAL_COUNTY_SALE= f"""
create or replace table {BQ_DATASET}.dm_total_county_sale as
    with
    cte_avg_county_population as (
        select
            p.county,
            p.fips,
            round(avg(p.population)) as avg_population,
            round(sum(p.population)) as sum_population
        from
            {BQ_DATASET}.dim_county_population p
        where
            p.`year` between {BEGIN_SALES_YEAR} and {END_SALES_YEAR}
        group by
            1, 2
    ),
    cte_total_enriched_with_population as (
        select
            s.county, 
            p.avg_population,
            p.sum_population,
            p.fips,
            round(sum(s.volume_sold_liters), 2) as total_sold_liters,
            round(sum(s.sale_dollars), 2) as total_sale_dollars
        from
            {BQ_DATASET}.fact_liquor_sale s 
            join cte_avg_county_population p on s.county = p.county
        where
            extract(year from s.sale_date) between {BEGIN_SALES_YEAR} and {END_SALES_YEAR}
        group by
            1, 2, 3, 4       
    ),
    cte_county_category_total as (
        select
            s.county,
            s.category_name,
            round(sum(s.volume_sold_liters)) as total_sold_liters,
            row_number() over(partition by s.county order by sum(s.volume_sold_liters) desc) as county_row_order_desc
        from    
            {BQ_DATASET}.fact_liquor_sale s
        group by
            1, 2
    )
    select
        s.county, 
        s.avg_population,
        s.fips,
        s.total_sold_liters,
        s.total_sale_dollars,
        round(s.total_sold_liters / s.sum_population, 2) as liters_per_person,
        t.category_name
    from
        cte_total_enriched_with_population s
        join cte_county_category_total t on t.county = s.county
            and county_row_order_desc = 1
"""

DDL_DM_TOTAL_YEAR_SALE = f"""
create or replace table {BQ_DATASET}.dm_total_year_sale as
    select
        extract(year from s.sale_date) as sale_year,
        round(sum(s.volume_sold_liters)) as total_sold_liters
    from
        {BQ_DATASET}.fact_liquor_sale s
    where
        extract(year from s.sale_date) between {BEGIN_SALES_YEAR} and {END_SALES_YEAR}
    group by
        1
"""

DDL_DM_TOTAL_CATEGORY_SALE = f"""
create or replace table {BQ_DATASET}.dm_total_category_sale as
    select
        s.category_name,
        round(sum(s.volume_sold_liters)) as total_sold_liters
    from
        {BQ_DATASET}.fact_liquor_sale s
    where
        extract(year from s.sale_date) between {BEGIN_SALES_YEAR} and {END_SALES_YEAR}
    group by
        1
"""