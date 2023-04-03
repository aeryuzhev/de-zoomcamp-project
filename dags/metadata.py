import pyarrow as pa

LIQUOR_SCHEMA = pa.schema(
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

CITY_COUNTY_POPULATION_SCHEMA = pa.schema(
    [
        ("fips", pa.int64()),
        ("county", pa.string()),
        ("city", pa.string()),
        ("year", pa.int64()),
        ("population", pa.int64()),
        ("primary_point", pa.string()), 
    ]
)

CITY_POPULATION_SCHEMA = pa.schema(
    [
        ("fips", pa.int64()),
        ("city", pa.string()),
        ("year", pa.int64()),
        ("population", pa.int64()),
        ("primary_point", pa.string()), 
    ]
)

COUNTY_POPULATION_SCHEMA = pa.schema(
    [
        ("fips", pa.int64()),
        ("county", pa.string()),
        ("year", pa.int64()),
        ("population", pa.int64()),
        ("primary_point", pa.string()), 
    ]
)