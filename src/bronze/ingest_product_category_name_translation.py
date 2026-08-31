import dlt

@dlt.table(
    name="product_category_name_translation_bronze",
    comment="Raw product category name translation data ingested via Auto Loader with schema evolution"
)
def ingest_product_category_name_translation():
    """
    Bronze layer: Ingest raw product category translation CSV using Auto Loader.
    
    Features:
    - Auto Loader for incremental file processing
    - Schema inference for all string columns
    - Schema evolution enabled (addNewColumns mode)
    - Rescued data column for malformed records
    """

    df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.schemaLocation", "/Volumes/second_data_engineering_project/pipeline_metadata/autoloader_metadata/schemas/bronze/product_category_translation")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("rescuedDataColumn", "_rescued_data")
        .load("/Volumes/second_data_engineering_project/landing/raw_files")
    )

    return df