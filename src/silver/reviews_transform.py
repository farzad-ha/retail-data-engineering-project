df_bronze = spark.readStream.table("second_data_engineering_project.bronze.reviews")

from pyspark.sql import functions as F

# Trim and standardize fields, add data quality flag
# If review_comment_message is null, use review_comment_title as replacement
# If review_comment_message is null and review_comment_title is null
# or if it has any special caracters in the list [".", "?", "!", ":", ";", ",", ""]
# use "No comments provided"
df_with_flag = (
    df_bronze
    .withColumn("review_id", F.lower(F.trim(F.col("review_id"))))
    .withColumn("order_id", F.lower(F.trim(F.col("order_id"))))
    .withColumn("review_comment_title", F.trim(F.col("review_comment_title")))
    .withColumn("review_comment_message", F.coalesce(F.col("review_comment_message"), F.col("review_comment_title")))
    .withColumn(
    "review_comment_message",
    F.when(F.col("review_comment_message").isNull(), F.lit("No comments provided"))
    .otherwise(F.col("review_comment_message"))
)
    .withColumn(
    "review_comment_message",
    F.when(
    F.trim(F.col("review_comment_message")).isin([".", "?", "!", ":", ";", ",", ""]),
    F.lit("No comments provided")).otherwise(F.trim(F.col("review_comment_message")))
)     
    .withColumn(
        "data_quality_flag",
        F.when(
            # review_id checks
            F.col("review_id").isNull() |
            (F.col("review_id") == "") |
            ~F.col("review_id").rlike("^[0-9a-fA-F]{32}$") |
            # order_id checks
            F.col("order_id").isNull() |
            (F.col("order_id") == "") |
            ~F.col("order_id").rlike("^[0-9a-fA-F]{32}$") |
            # review_score checks (must be 1-5)
            F.col("review_score").isNull() |
            (F.col("review_score") < 1) |
            (F.col("review_score") > 5) |
            # review_creation_date checks
            F.col("review_creation_date").isNull(),
            F.lit("quarantine")
        )
        .otherwise(F.lit("valid"))
    )
)


# Split into valid and quarantine tables
# Drop _rescued_data and review_comment_title columns specifically for silver streaming micro-batches
df_silver_stream = (
    df_with_flag
    .filter(F.col("data_quality_flag") == "valid")
    .drop("data_quality_flag")
    .drop("_rescued_data")
    .drop("review_comment_title")
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
    deduplicated_batch = micro_batch_df.dropDuplicates(["review_id"])

    target_table = "second_data_engineering_project.silver.reviews"
    
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
    
    # Execute the atomic Merge Statement
    micro_batch_df.sparkSession.sql(f"""
        MERGE INTO {target_table} AS target
        USING source_updates AS source
        ON target.review_id = source.review_id
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
        .saveAsTable("second_data_engineering_project.silver.reviews_quarantine")
    )


# Run the Streams
# Silver Stream (Using foreachBatch for the custom MERGE block)
silver_query = (
    df_silver_stream.writeStream \
    .foreachBatch(upsert_to_silver) \
    .option("checkpointLocation", "/Volumes/second_data_engineering_project/pipeline_metadata/autoloader_metadata/checkpoints/silver/reviews") \
    .trigger(availableNow=True) \
    .option("mergeSchema", "true") \
    .start()
)

# Runs via the function append_to_quarantine
quarantine_query = (
    df_quarantine_stream.writeStream \
    .foreachBatch(append_to_quarantine) \
    .option("checkpointLocation", "/Volumes/second_data_engineering_project/pipeline_metadata/autoloader_metadata/checkpoints/silver/reviews_quarantine") \
    .trigger(availableNow=True) \
    .option("mergeSchema", "true") \
    .start()
)

# Wait for terminations
silver_query.awaitTermination()
quarantine_query.awaitTermination()
