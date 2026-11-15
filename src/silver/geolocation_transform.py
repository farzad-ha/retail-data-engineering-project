df = spark.readStream.table("second_data_engineering_project.bronze.geolocation")

from pyspark.sql import functions as F

# Clean and standardise string columns, then add quality flag
df_with_flag = (
    df
    # Standard cleaning: trim and format string columns
    .withColumn("geolocation_city", F.initcap(F.trim(F.col("geolocation_city"))))
    .withColumn("geolocation_state", F.upper(F.trim(F.col("geolocation_state"))))
    # Add quality flag for all validation rules
    .withColumn(
        "data_quality_flag",
        F.when(
            # geolocation_zip_code_prefix checks
            F.col("geolocation_zip_code_prefix").isNull() |
            (F.col("geolocation_zip_code_prefix") <= 0) |
            # geolocation_lat checks (must be between -90 and 90)
            F.col("geolocation_lat").isNull() |
            (F.col("geolocation_lat") < -90) |
            (F.col("geolocation_lat") > 90) |
            # geolocation_lng checks (must be between -180 and 180)
            F.col("geolocation_lng").isNull() |
            (F.col("geolocation_lng") < -180) |
            (F.col("geolocation_lng") > 180) |
            # geolocation_city checks
            F.col("geolocation_city").isNull() |
            (F.col("geolocation_city") == "") |
            # geolocation_state checks
            F.col("geolocation_state").isNull() |
            (F.col("geolocation_state") == "") |
            (F.length(F.col("geolocation_state")) != 2),
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
    deduplicated_batch = micro_batch_df.dropDuplicates([
        "geolocation_zip_code_prefix",
        "geolocation_lat",
        "geolocation_lng",
        "geolocation_city",
        "geolocation_state"
    ])
    
    target_table = "second_data_engineering_project.silver.geolocation"
    
    # Automatically create the Silver table on the very first run if it does not exist yet
    if not micro_batch_df.sparkSession.catalog.tableExists(target_table):
        (
            deduplicated_batch.write
            .format("delta")
            .mode("overwrite")
            .saveAsTable(target_table)
        )
        return

    # Expose the micro-batch as a temporary view for Spark SQL syntax
    deduplicated_batch.createOrReplaceTempView("source_updates")
    
    # Execute the atomic Merge Statement for all subsequent runs
    micro_batch_df.sparkSession.sql(f"""
        MERGE INTO {target_table} AS target
        USING source_updates AS source
        ON target.geolocation_zip_code_prefix = source.geolocation_zip_code_prefix
           AND target.geolocation_lat = source.geolocation_lat
           AND target.geolocation_lng = source.geolocation_lng
           AND target.geolocation_city = source.geolocation_city
           AND target.geolocation_state = source.geolocation_state
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
        .saveAsTable("second_data_engineering_project.silver.geolocation_quarantine")
    )

# Run the Streams
# Silver Stream (Using foreachBatch for the custom MERGE block)
silver_query = (
    df_silver_stream.writeStream
    .foreachBatch(upsert_to_silver)
    .option("checkpointLocation", "/Volumes/second_data_engineering_project/pipeline_metadata/autoloader_metadata/checkpoints/silver/geolocation_silver") \
    .trigger(availableNow=True) \
    .option("mergeSchema", "true") \
    .start()
)

# Runs via the function append_to_quarantine
quarantine_query = (
    df_quarantine_stream.writeStream
    .foreachBatch(append_to_quarantine)
    .option("checkpointLocation", "/Volumes/second_data_engineering_project/pipeline_metadata/autoloader_metadata/checkpoints/silver/geolocation_quarantine") \
    .trigger(availableNow=True) \
    .option("mergeSchema", "true") \
    .start()
)

# Wait for terminations
silver_query.awaitTermination()
quarantine_query.awaitTermination()