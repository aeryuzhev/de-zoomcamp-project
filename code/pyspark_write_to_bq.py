import string

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

GCS_TEMP_BUCKET = "dtc_data_lake_de-zoomcamp-375618" 
GCS_IOWA_LIQUOR_FILE = f"gs://{GCS_TEMP_BUCKET}/iowa_liquor/fact_liquor_sale.parquet"
BQ_LIQUOR_SALE_TABLE = "iowa_liquor.fact_liquor_sale"

map_county_dict = {
    "BUENA VIST": "Buena Vista",
    "CERRO GORD": "Cerro Gordo",
    "OBRIEN": "O'Brien",
    "POTTAWATTA": "Pottawattamie",
    "O'BRIEN": "O'Brien"
}

map_city_dict = {
    "DEWITT": "De Witt",
    "GRAND MOUNDS": "Grand Mound",
    "ARNOLD'S PARK": "Arnolds Park",
    "ST LUCAS": "St. Lucas", 
    "JEWELL": "Jewell Junction",
    "MT PLEASANT": "Mount Pleasant", 
    "LONETREE": "Lone Tree", 
    "ST CHARLES": "St. Charles", 
    "SAINT ANSGAR": "St. Ansgar", 
    "ST ANSGAR": "St. Ansgar", 
    "LEMARS": "Le Mars",
    "LECLAIRE": "Le Claire",
    "OTUMWA": "Ottumwa", 
    "CLEARLAKE": "Clear Lake",
    "MELCHER-DALLAS": "Melcher-Dallas",
    "FT. ATKINSON": "Fort Atkinson",
    "GUTTENBURG": "Guttenberg",
    "KELLOG": "Kellogg",
    "MCGREGOR": "McGregor",
    "MT VERNON": "Mount Vernon",
    "OTTUWMA": "Ottumwa",
}

map_city_county_dict = {
    ("BETTENDORF", "IOWA"): "Scott",
    ("CONRAD", "GUTHRIE"): "Grundy",
    ("CORNING", "CLAYTON"): "Adams",
    ("COUNCIL BLUFFS", "MILLS"): "Pottawattamie",
    ("DUBUQUE", "IOWA"): "Dubuque",
    ("FORT DODGE", "IOWA"): "Webster",
    ("FORT DODGE", "HARDIN"): "Webster",
    ("MARTENSDALE", "MARSHALL"): "Warren",
    ("NEWTON", "WEBSTER"): "Jasper",
    ("PERRY", "BOONE"): "Dallas",
    ("PERRY", "CASS"): "Dallas",
    ("WELLMAN", "WAPELLO"): "Washington",
    ("ACKLEY", "WEBSTER"): "Hardin",
    ("CAMBRIDGE", "POLK"): "Story",
    ("COLUMBUS JUNCTION", "BLACK HAWK"): "Louisa",
    ("DAVENPORT", "IOWA"): "Scott",
    ("EDGEWOOD", "IOWA"): "Delaware",
    ("GLADBROOK", "IOWA"): "Tama",
    ("NORTH LIBERTY", "IOWA"): "Johnson",
    ("OSKALOOSA", "BUCHANAN"): "Mahaska",
    ("SHELBY", "HARRISON"): "Shelby",
    ("STATE CENTER", "IOWA"): "Marshall",
    ("SWISHER", "POLK"): "Johnson",
    ("TRAER", "IOWA"): "Tama",
    ("WATERLOO", "IOWA"): "Black Hawk",
    ("WILTON", "IOWA"): "Muscatine",
}

spark = (
    SparkSession.builder
    .appName("load_to_bigquery")
    .getOrCreate()    
)

df = spark.read.parquet(GCS_IOWA_LIQUOR_FILE)


def get_city_county_whens(map_city_county_dict):
    whens = F
    for key, value in map_city_county_dict.items():
        old_city, old_county, new_county = key[0], key[1], value  
        whens = whens.when(
            (F.col("county") == old_county) & (F.col("city") == old_city), new_county
        )          
    whens = whens.otherwise(F.col("county"))
    
    return whens


@F.udf(returnType=StringType())
def udf_init_cap(value):  
    if value:   
        value = string.capwords(value)
        if "-" in value:
            value = string.capwords(value, sep="-")
        if value[1] == "'" or value[:2] == "Mc":
            value = value[:2] + value[2].upper() + value[3:]   
    return value


df_cleaned = df.filter((F.col("city") != "") & (F.col("county") != ""))

df_mapped = (
    df_cleaned
    .replace(to_replace=map_county_dict, subset="county")
    .replace(to_replace=map_city_dict, subset="city")
    .withColumn("county", get_city_county_whens(map_city_county_dict))     
)

df_transformed = (
    df_mapped
    .withColumn("county", udf_init_cap(F.col("county")))
    .withColumn("city", udf_init_cap(F.col("city")))
)

df_transformed.write \
    .format("bigquery") \
    .mode("overwrite") \
    .option("temporaryGcsBucket", GCS_TEMP_BUCKET) \
    .option("table", BQ_LIQUOR_SALE_TABLE) \
    .option("partitionField", "sale_date") \
    .option("partitionType", "MONTH") \
    .option("clusteredFields", "county,city") \
    .save()
