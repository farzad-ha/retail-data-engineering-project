import dlt

@dlt.table(
    name="products_bronze",
    comment="Raw products data ingested via Auto Loader with schema evolution"
)
def ingest_products():
    """
    Bronze layer: Ingest raw products CSV using Auto Loader.
    
    Features:
    - Auto Loader for incremental file processing
    - Schema inference with type hints for product dimension columns
    - Schema evolution enabled (addNewColumns mode)
    - Rescued data column for malformed records
    """

    df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.schemaLocation", "/Volumes/second_data_engineering_project/pipeline_metadata/autoloader_metadata/schemas/bronze/products")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaHints", "product_name_lenght INT, product_description_lenght INT, product_photos_qty INT, product_weight_g INT, product_length_cm INT, product_height_cm INT, product_width_cm INT")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("rescuedDataColumn", "_rescued_data")
        .load("/Volumes/second_data_engineering_project/landing/raw_files/olist_products_dataset.csv")
    )

    return df