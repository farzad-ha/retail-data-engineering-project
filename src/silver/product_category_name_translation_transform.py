df_bronze = spark.readStream.table("second_data_engineering_project.bronze.product_category_name_translation")

from pyspark.sql import functions as F

# Trim and standardize category names, add data quality flag
df_with_flag = (
    df_bronze
    .withColumn("product_category_name", F.lower(F.trim(F.col("product_category_name"))))
    .withColumn("product_category_name_english", F.lower(F.trim(F.col("product_category_name_english"))))
    .withColumn(
        "data_quality_flag",
        F.when(
            # Both category names must be populated
            F.col("product_category_name").isNull() |
            (F.col("product_category_name") == "") |
            F.col("product_category_name_english").isNull() |
            (F.col("product_category_name_english") == ""),
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
    deduplicated_batch = micro_batch_df.dropDuplicates(["product_category_name"])
    
    # Expose the micro-batch as a temporary view for Spark SQL syntax
    deduplicated_batch.createOrReplaceTempView("source_updates")
    
    # Execute the atomic Merge Statement
    micro_batch_df.sparkSession.sql("""
        MERGE INTO second_data_engineering_project.silver.product_category_name_translation AS target
        USING source_updates AS source
        ON target.product_category_name = source.product_category_name
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
        .saveAsTable("second_data_engineering_project.silver.product_category_name_translation_quarantine")
    )

# Run the Streams
# Silver Stream (Using foreachBatch for the custom MERGE block)
silver_query = (
    df_silver_stream.writeStream \
    .foreachBatch(upsert_to_silver) \
    .option("checkpointLocation", "/Volumes/second_data_engineering_project/pipeline_metadata/autoloader_metadata/checkpoints/silver/product_category_name_translation") \
    .trigger(availableNow=True) \
    .option("mergeSchema", "true") \
    .start()
)

# Runs via the function append_to_quarantine
quarantine_query = (
    df_quarantine_stream.writeStream \
    .foreachBatch(append_to_quarantine) \
    .option("checkpointLocation", "/Volumes/second_data_engineering_project/pipeline_metadata/autoloader_metadata/checkpoints/silver/product_category_name_translation_quarantine") \
    .trigger(availableNow=True) \
    .option("mergeSchema", "true") \
    .start()
)

# Wait for terminations
silver_query.awaitTermination()
quarantine_query.awaitTermination()