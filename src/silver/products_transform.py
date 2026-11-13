df_bronze = spark.readStream.table("second_data_engineering_project.bronze.products")

from pyspark.sql import functions as F

# Trim product_id and add data quality flag
# Check for null, empty, or zero values, and invalid format

df_with_flag = (
    df_bronze
    # Standard cleaning: trim and lowercase string columns
    # Correct spellings
    .withColumn("product_id", F.lower(F.trim(F.col("product_id"))))
    .withColumn("product_category_name", F.lower(F.trim(F.col("product_category_name"))))
    .withColumnRenamed("product_name_lenght", "product_name_length")
    .withColumnRenamed("product_description_lenght", "product_description_length")
    .withColumn(
        "data_quality_flag",
        F.when(
            # product_id checks
            F.col("product_id").isNull() | 
            (F.col("product_id") == "") |
            (F.col("product_id") == "0") |
            ~ F.col("product_id").rlike("^[0-9a-fA-F]{32}$") |
            # product_category_name checks
            F.col("product_category_name").isNull() |
            (F.col("product_category_name") == "") |
            # product_name_lenght checks
            F.col("product_name_length").isNull() |
            (F.col("product_name_length") <= 0) |
            # product_description_lenght checks
            F.col("product_description_length").isNull() |
            (F.col("product_description_length") <= 0) |
            # product_photos_qty checks
            F.col("product_photos_qty").isNull() |
            (F.col("product_photos_qty") < 0) |
            # product_weight_g checks
            F.col("product_weight_g").isNull() |
            (F.col("product_weight_g") <= 0) |
            # product_length_cm checks
            F.col("product_length_cm").isNull() |
            (F.col("product_length_cm") <= 0) |
            # product_height_cm checks
            F.col("product_height_cm").isNull() |
            (F.col("product_height_cm") <= 0) |
            # product_width_cm checks
            F.col("product_width_cm").isNull() |
            (F.col("product_width_cm") <= 0),
            F.lit("quarantine")
        )
        .otherwise(F.lit("valid"))
    )
)


# Split into valid and quarantine tables
# Apply deduplication and drop columns specifically for silver streaming micro-batches
df_silver_stream = (
    df_with_flag
    .filter(F.col("data_quality_flag") == "valid")
    .drop("data_quality_flag")
    .drop("_rescued_data")
)

df_quarantine_stream = (
    df_with_flag
    .filter(F.col("data_quality_flag") == "quarantine")
    .drop("data_quality_flag")
)

# Define the foreachBatch Merge function for Silver
def upsert_to_silver(micro_batch_df, batch_id):
    # Skip processing entirely if the incoming micro-batch has no new records
    if micro_batch_df.isEmpty():
        return
        
    # Deduplicate micro-batch records before executing MERGE to prevent ambiguous match errors
    deduplicated_batch = micro_batch_df.dropDuplicates(["product_id"])
    
    # Expose the micro-batch as a temporary view for Spark SQL syntax
    deduplicated_batch.createOrReplaceTempView("source_updates")
    
    # Execute the atomic Merge Statement
    micro_batch_df.sparkSession.sql("""
        MERGE INTO second_data_engineering_project.silver.products AS target
        USING source_updates AS source
        ON target.product_id = source.product_id
        WHEN MATCHED THEN
            UPDATE SET *
        WHEN NOT MATCHED THEN
            INSERT *
    """)

# Define the foreachBatch Append function for Quarantine
def append_to_quarantine(micro_batch_df, batch_id):
    # This prevents the stream from committing an empty transaction block to Delta
    if micro_batch_df.isEmpty():
        return
        
    # Standard append write converted to batch syntax
    (
        micro_batch_df.write
        .format("delta")
        .option("mergeSchema", "true")
        .mode("append")
        .saveAsTable("second_data_engineering_project.silver.products_quarantine")
    )

# Run the Streams
# Silver Stream (Using foreachBatch for the custom MERGE block)
silver_query = (
    df_silver_stream.writeStream \
    .foreachBatch(upsert_to_silver) \
    .option("checkpointLocation", "/Volumes/second_data_engineering_project/pipeline_metadata/autoloader_metadata/checkpoints/silver/products") \
    .trigger(availableNow=True) \
    .option("mergeSchema", "true") \
    .start()

# Runs via the function append_to_quarantine
quarantine_query = (
    df_quarantine_stream.writeStream \
    .foreachBatch(append_to_quarantine) \
    .option("checkpointLocation", "/Volumes/second_data_engineering_project/pipeline_metadata/autoloader_metadata/checkpoints/silver/products_quarantine") \
    .trigger(availableNow=True) \
    .option("mergeSchema", "true") \
    .start()

# Wait for terminations
silver_query.awaitTermination()
quarantine_query.awaitTermination()

