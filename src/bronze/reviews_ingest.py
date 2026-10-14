# Read the raw reviews CSV using Auto Loader
# with schema hints for non-string types, schema inference, and schema evolution enabled

df_reviews_bronze = spark.readStream \
    .format("cloudFiles") \
    .option("cloudFiles.format", "csv") \
    .option("header", "true") \
    .option("multiLine", "true") \
    .option("cloudFiles.schemaLocation", "/Volumes/second_data_engineering_project/pipeline_metadata/autoloader_metadata/schemas/bronze/reviews") \
    .option("cloudFiles.inferColumnTypes", "true") \
    .option("cloudFiles.schemaHints", "review_score INT, review_creation_date TIMESTAMP, review_answer_timestamp TIMESTAMP") \
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns") \
    .option("rescuedDataColumn", "_rescued_data") \
    .load("/Volumes/second_data_engineering_project/landing/raw_files/olist_order_reviews_dataset/")

# Write the streaming Bronze DataFrame as a Delta table
# Using Trigger.AvailableNow for batch-like processing with Auto Loader benefits
# mergeSchema allows the Delta table to accept new columns discovered by Auto Loader
# Checkpoint location enables incremental processing on subsequent runs

df_reviews_bronze.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("mergeSchema", "true") \
    .option("checkpointLocation", "/Volumes/second_data_engineering_project/pipeline_metadata/autoloader_metadata/checkpoints/bronze/reviews") \
    .trigger(availableNow=True) \
    .toTable("second_data_engineering_project.bronze.reviews")