import dlt

@dlt.table(
    name="customers_bronze",
    comment="Raw customers data ingested via Auto Loader with schema evolution"
)
def ingest_customers():
    """
    Bronze layer: Ingest raw customers CSV using Auto Loader.
    
    Features:
    - Auto Loader for incremental file processing
    - Schema inference with type hints for customer_zip_code_prefix
    - Schema evolution enabled (addNewColumns mode)
    - Rescued data column for malformed records
    """

    df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.schemaLocation", "/Volumes/second_data_engineering_project/pipeline_metadata/autoloader_metadata/schemas/bronze/customers")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaHints", "customer_zip_code_prefix INT")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("rescuedDataColumn", "_rescued_data")
        .load("/Volumes/second_data_engineering_project/landing/raw_files/olist_customers_dataset.csv")
    )

    return df