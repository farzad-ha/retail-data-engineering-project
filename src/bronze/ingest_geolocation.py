import dlt

@dlt.table(
    name="geolocation_bronze",
    comment="Raw geolocation data ingested via Auto Loader with schema evolution"
)
def ingest_geolocation():
    """
    Bronze layer: Ingest raw geolocation CSV using Auto Loader.
    
    Features:
    - Auto Loader for incremental file processing
    - Schema inference with type hints for zip code and coordinate columns
    - Schema evolution enabled (addNewColumns mode)
    - Rescued data column for malformed records
    """

    df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.schemaLocation", "/Volumes/second_data_engineering_project/pipeline_metadata/autoloader_metadata/schemas/bronze/geolocation")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaHints", "geolocation_zip_code_prefix INT, geolocation_lat DOUBLE, geolocation_lng DOUBLE")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("rescuedDataColumn", "_rescued_data")
        .load("/Volumes/second_data_engineering_project/landing/raw_files/olist_geolocation_dataset.csv")
    )

    return df