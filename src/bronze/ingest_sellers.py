import dlt

@dlt.table(
    name="sellers_bronze",
    comment="Raw sellers data ingested via Auto Loader with schema evolution"
)
def ingest_sellers():
    """
    Bronze layer: Ingest raw sellers CSV using Auto Loader.
    
    Features:
    - Auto Loader for incremental file processing
    - Schema inference with type hints for seller_zip_code_prefix
    - Schema evolution enabled (addNewColumns mode)
    - Rescued data column for malformed records
    """

    df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.schemaLocation", "/Volumes/second_data_engineering_project/pipeline_metadata/autoloader_metadata/schemas/sellers")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaHints", "seller_zip_code_prefix INT")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("rescuedDataColumn", "_rescued_data")
        .load("/Volumes/second_data_engineering_project/landing/raw_files/")
    )

    return df