import dlt

@dlt.table(
    name="order_items_bronze",
    comment="Raw order items data ingested via Auto Loader with schema evolution"
)
def ingest_order_items():
    """
    Bronze layer: Ingest raw order items CSV using Auto Loader.
    
    Features:
    - Auto Loader for incremental file processing
    - Schema inference with type hints for numeric and timestamp columns
    - Schema evolution enabled (addNewColumns mode)
    - Rescued data column for malformed records
    """

    df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.schemaLocation", "/Volumes/second_data_engineering_project/pipeline_metadata/autoloader_metadata/schemas/bronze/order_items")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaHints", "order_item_id INT, shipping_limit_date TIMESTAMP, price DOUBLE, freight_value DOUBLE")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("rescuedDataColumn", "_rescued_data")
        .load("/Volumes/second_data_engineering_project/landing/raw_files/olist_order_items_dataset.csv")
    )

    return df