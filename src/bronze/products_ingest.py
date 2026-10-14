# Read the raw products CSV using Auto Loader
# with schema hints for non-string types, schema inference, and schema evolution enabled

df_products_bronze = spark.readStream \
    .format("cloudFiles") \
    .option("cloudFiles.format", "csv") \
    .option("header", "true") \
    .option("cloudFiles.schemaLocation", "/Volumes/second_data_engineering_project/pipeline_metadata/autoloader_metadata/schemas/bronze/products") \
    .option("cloudFiles.inferColumnTypes", "true") \
    .option("cloudFiles.schemaHints", "product_name_lenght INT, product_description_lenght INT, product_photos_qty INT, product_weight_g INT, product_length_cm INT, product_height_cm INT, product_width_cm INT") \
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns") \
    .option("rescuedDataColumn", "_rescued_data") \
    .load("/Volumes/second_data_engineering_project/landing/raw_files/olist_products_dataset")

# Write the streaming Bronze DataFrame as a Delta table
# Using Trigger.AvailableNow for batch-like processing with Auto Loader benefits
# mergeSchema allows the Delta table to accept new columns discovered by Auto Loader
# Checkpoint location enables incremental processing on subsequent runs

df_products_bronze.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("mergeSchema", "true") \
    .option("checkpointLocation", "/Volumes/second_data_engineering_project/pipeline_metadata/autoloader_metadata/checkpoints/bronze/products") \
    .trigger(availableNow=True) \
    .toTable("second_data_engineering_project.bronze.products")