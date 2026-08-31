import dlt

@dlt.table(
    name="payments_bronze",
    comment="Raw payments data ingested via Auto Loader with schema evolution"
)
def ingest_payments():
    """
    Bronze layer: Ingest raw payments CSV using Auto Loader.
    
    Features:
    - Auto Loader for incremental file processing
    - Schema inference with type hints for payment columns
    - Schema evolution enabled (addNewColumns mode)
    - Rescued data column for malformed records
    """

    df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.schemaLocation", "/Volumes/second_data_engineering_project/pipeline_metadata/autoloader_metadata/schemas/bronze/payments")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaHints", "payment_sequential INT, payment_installments INT, payment_value DOUBLE")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("rescuedDataColumn", "_rescued_data")
        .load("/Volumes/second_data_engineering_project/landing/raw_files/olist_order_payments_dataset.csv")
    )

    return df