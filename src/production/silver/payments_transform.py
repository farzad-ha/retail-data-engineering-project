from pyspark.sql import functions as F

catalog = "second_data_engineering_project"

silver_base_path = "abfss://silver@engineerolistproject.dfs.core.windows.net"

metadata_base_path = f"{silver_base_path}/pipeline_metadata/checkpoints"

df_bronze = spark.readStream.table(f"{catalog}.bronze.payments")

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
    
    target_table = "second_data_engineering_project.silver.payments"
    
    # Automatically create the Silver table on the very first run if it does not exist yet
    if not micro_batch_df.sparkSession.catalog.tableExists(target_table):
        (
            deduplicated_batch.write
            .format("delta")
            .mode("overwrite")
            .option("path", f"{silver_base_path}/tables/payments")
            .saveAsTable(target_table)
        )
        return
    
    # Expose the micro-batch as a temporary view for Spark SQL syntax
    deduplicated_batch.createOrReplaceTempView("source_updates")
    
    # Execute the atomic Merge Statement
    micro_batch_df.sparkSession.sql(f"""
        MERGE INTO {target_table} AS target
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
        .option("path", f"{silver_base_path}/tables/payments_quarantine")
        .saveAsTable(f"{catalog}.silver.payments_quarantine")
    )



# Run the Streams
# Silver Stream (Using foreachBatch for the custom MERGE block)
silver_query = (
    df_silver_stream.writeStream \
    .foreachBatch(upsert_to_silver) \
    .option("checkpointLocation", f"{metadata_base_path}/payments") \
    .trigger(availableNow=True) \
    .option("mergeSchema", "true") \
    .start()
)


# Run the Streams
# Runs via the function append_to_quarantine
quarantine_query = (
    df_quarantine_stream.writeStream \
    .foreachBatch(append_to_quarantine) \
    .option("checkpointLocation", f"{metadata_base_path}/payments_quarantine") \
    .trigger(availableNow=True) \
    .option("mergeSchema", "true") \
    .start()
)

# Wait for terminations
silver_query.awaitTermination()
quarantine_query.awaitTermination()