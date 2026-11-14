# Read bronze sellers stream

df_bronze = (
    spark.readStream
    .table("second_data_engineering_project.bronze.sellers")
)

from pyspark.sql import functions as F

# Trim and standardize fields, add data quality flag
# Check for null, empty, invalid values

df_with_flag = (
    df_bronze
    # Standard cleaning: trim and lowercase/uppercase as appropriate
    .withColumn("seller_id", F.lower(F.trim(F.col("seller_id"))))
    .withColumn("seller_city", F.initcap(F.trim(F.col("seller_city"))))
    .withColumn("seller_state", F.upper(F.trim(F.col("seller_state"))))
    .withColumn(
        "data_quality_flag",
        F.when(
            # seller_id checks
            F.col("seller_id").isNull() |
            (F.col("seller_id") == "") |
            (F.col("seller_id") == "0") |
            ~ F.col("seller_id").rlike("^[0-9a-fA-F]{32}$") |
            # seller_zip_code_prefix checks
            F.col("seller_zip_code_prefix").isNull() |
            (F.col("seller_zip_code_prefix") <= 0) |
            # seller_city checks
            F.col("seller_city").isNull() |
            (F.col("seller_city") == "") |
            # seller_state checks (must be 2-char state code)
            F.col("seller_state").isNull() |
            (F.col("seller_state") == "") |
            (F.length(F.col("seller_state")) != 2),
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
    deduplicated_batch = micro_batch_df.dropDuplicates(["seller_id"])
    
    # Expose the micro-batch as a temporary view for Spark SQL syntax
    deduplicated_batch.createOrReplaceTempView("source_updates")
    
    # Execute the atomic Merge Statement
    micro_batch_df.sparkSession.sql("""
        MERGE INTO second_data_engineering_project.silver.sellers AS target
        USING source_updates AS source
        ON target.seller_id = source.seller_id
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
        .saveAsTable("second_data_engineering_project.silver.sellers_quarantine")
    )

# Run the Streams
# Silver Stream (Using foreachBatch for the custom MERGE block)
silver_query = (
    df_silver_stream.writeStream \
    .foreachBatch(upsert_to_silver) \
    .option("checkpointLocation", "/Volumes/second_data_engineering_project/pipeline_metadata/autoloader_metadata/checkpoints/silver/sellers") \
    .trigger(availableNow=True) \
    .option("mergeSchema", "true") \
    .start()
)

# Runs via the function append_to_quarantine
quarantine_query = (
    df_quarantine_stream.writeStream \
    .foreachBatch(append_to_quarantine) \
    .option("checkpointLocation", "/Volumes/second_data_engineering_project/pipeline_metadata/autoloader_metadata/checkpoints/silver/sellers_quarantine") \
    .trigger(availableNow=True) \
    .option("mergeSchema", "true") \
    .start()
)

# Wait for terminations
silver_query.awaitTermination()
quarantine_query.awaitTermination()