df_bronze = spark.readStream.table("second_data_engineering_project.bronze.payments")

from pyspark.sql import functions as F

# Trim order_id and payment_type, add data quality flag
# Check for null, empty, invalid values

df_with_flag = (
    df_bronze
    # Standard cleaning: trim and lowercase string columns
    .withColumn("order_id", F.lower(F.trim(F.col("order_id"))))
    .withColumn("payment_type", F.lower(F.trim(F.col("payment_type"))))
    .withColumn(
        "data_quality_flag",
        F.when(
            # order_id checks
            F.col("order_id").isNull() |
            (F.col("order_id") == "") |
            (F.col("order_id") == "0") |
            ~ F.col("order_id").rlike("^[0-9a-fA-F]{32}$") |
            # payment_type checks (must be one of the 5 valid types)
            F.col("payment_type").isNull() |
            (F.col("payment_type") == "") |
            ~ F.col("payment_type").isin(["credit_card", "boleto", "voucher", "debit_card", "not_defined"]) |
            # payment_sequential checks
            F.col("payment_sequential").isNull() |
            (F.col("payment_sequential") <= 0) |
            # payment_installments checks (must be > 0)
            F.col("payment_installments").isNull() |
            (F.col("payment_installments") <= 0) |
            # payment_value checks (allow 0 for vouchers and not_defined)
            F.col("payment_value").isNull() |
            (F.col("payment_value") < 0),
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
    deduplicated_batch = micro_batch_df.dropDuplicates(["order_id", "payment_sequential"])
    
    # Expose the micro-batch as a temporary view for Spark SQL syntax
    deduplicated_batch.createOrReplaceTempView("source_updates")
    
    # Execute the atomic Merge Statement
    micro_batch_df.sparkSession.sql("""
        MERGE INTO second_data_engineering_project.silver.payments AS target
        USING source_updates AS source
        ON target.order_id = source.order_id AND target.payment_sequential = source.payment_sequential
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
        .saveAsTable("second_data_engineering_project.silver.payments_quarantine")
    )



# Run the Streams
# Silver Stream (Using foreachBatch for the custom MERGE block)
silver_query = (
    df_silver_stream.writeStream \
    .foreachBatch(upsert_to_silver) \
    .option("checkpointLocation", "/Volumes/second_data_engineering_project/pipeline_metadata/autoloader_metadata/checkpoints/silver/payments") \
    .trigger(availableNow=True) \
    .option("mergeSchema", "true") \
    .start()
)


# Run the Streams
# Runs via the function append_to_quarantine
quarantine_query = (
    df_quarantine_stream.writeStream \
    .foreachBatch(append_to_quarantine) \
    .option("checkpointLocation", "/Volumes/second_data_engineering_project/pipeline_metadata/autoloader_metadata/checkpoints/silver/payments_quarantine") \
    .trigger(availableNow=True) \
    .option("mergeSchema", "true") \
    .start()
)

# Wait for terminations
silver_query.awaitTermination()
quarantine_query.awaitTermination()