import dlt

@dlt.table(
    name="reviews_bronze",
    comment="Raw reviews data ingested via Auto Loader with schema evolution"
)
def ingest_reviews():
    """
    Bronze layer: Ingest raw reviews CSV using Auto Loader.
    
    Features:
    - Auto Loader for incremental file processing
    - Multiline CSV support for review text fields
    - Schema inference with type hints for review score and timestamp columns
    - Schema evolution enabled (addNewColumns mode)
    - Rescued data column for malformed records
    """

    df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("multiLine", "true")
        .option("cloudFiles.schemaLocation", "/Volumes/second_data_engineering_project/pipeline_metadata/autoloader_metadata/schemas/bronze/reviews")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaHints", "review_score INT, review_creation_date TIMESTAMP, review_answer_timestamp TIMESTAMP")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("rescuedDataColumn", "_rescued_data")
        .load("/Volumes/second_data_engineering_project/landing/raw_files/olist_order_reviews_dataset.csv")
    )

    return df