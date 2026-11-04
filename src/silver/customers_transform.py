df_bronze = spark.readStream.table("second_data_engineering_project.bronze.customers")

from pyspark.sql import functions as F

# Trim customer_id and add data quality flag
# Check for null, empty, or zero values, and invalid format

df_with_flag = (
    df_bronze
    # Standard cleaning: trim and lowercase all string columns
    .withColumn("customer_id", F.lower(F.trim(F.col("customer_id"))))
    .withColumn("customer_unique_id", F.lower(F.trim(F.col("customer_unique_id"))))
    .withColumn("customer_city", F.initcap(F.trim(F.col("customer_city"))))
    .withColumn("customer_state", F.upper(F.trim(F.col("customer_state"))))
    .withColumn(
        "data_quality_flag",
        F.when(
            # customer_id checks
            F.col("customer_id").isNull() | 
            (F.col("customer_id") == "") |
            (F.col("customer_id") == "0") |
            ~ F.col("customer_id").rlike("^[0-9a-fA-F]{32}$") |
            # customer_unique_id checks
            F.col("customer_unique_id").isNull() | 
            (F.col("customer_unique_id") == "") |
            (F.col("customer_unique_id") == "0") |
            ~ F.col("customer_unique_id").rlike("^[0-9a-fA-F]{32}$") |
            # customer_city checks
            F.col("customer_city").isNull() |
            (F.col("customer_city") == "") |
            # customer_state checks
            F.col("customer_state").isNull() |
            (F.col("customer_state") == "") |
            (F.length(F.col("customer_state")) != 2) |
            # customer_zip_code_prefix checks
            F.col("customer_zip_code_prefix").isNull() |
            (F.col("customer_zip_code_prefix") <= 0),
            F.lit("quarantine")
        )
        .otherwise(F.lit("valid"))
    )
)

# Define the Insert-Only Merge function for the Silver table
def merge_into_silver(microBatchDF, batchId):
    # Safety optimization to instantly skip empty micro-batches
    if not microBatchDF.take(1):
        return

    # Filter for valid rows, clean up tracking columns, and drop internal batch duplicates
    valid_df = (
        microBatchDF
        .filter(F.col("data_quality_flag") == "valid")
        .drop("data_quality_flag", "_rescued_data")
        .dropDuplicates(["customer_id"])
    )

    # If no valid data exists in this specific slice, skip running the SQL query
    if not valid_df.take(1):
        return

    # Register the dataframe to the micro-batch local session view
    valid_df.createOrReplaceTempView("incoming_valid_customers")

    # Execute the insert-only merge statement (No UPDATE or DELETE clause to protect historic data)
    microBatchDF.sparkSession.sql("""
        MERGE INTO second_data_engineering_project.silver.customers AS target
        USING incoming_valid_customers AS source
        ON (target.customer_id = source.customer_id)
        WHEN NOT MATCHED THEN
            INSERT *
    """)

 # Define the Append function for the Quarantine table
def append_into_quarantine(microBatchDF, batchId):
    # Safety optimization to instantly skip empty micro-batches
    if not microBatchDF.take(1):
        return

    # Filter for quarantined rows and drop the tracking flag
    quarantine_df = (
        microBatchDF
        .filter(F.col("data_quality_flag") == "quarantine")
        .drop("data_quality_flag")
    )

    # If no quarantine data exists in this specific slice, skip writing
    if not quarantine_df.take(1):
        return

    # Append standard delta format rows directly into the quarantine table
    (quarantine_df.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable("second_data_engineering_project.silver.customers_quarantine")
    )

# Write valid records to silver table
query_silver = df_with_flag.writeStream \
    .foreachBatch(merge_into_silver) \
    .option("checkpointLocation", "/Volumes/second_data_engineering_project/pipeline_metadata/autoloader_metadata/checkpoints/silver/customers") \
    .trigger(availableNow=True) \
    .start()

# Write quarantine records to quarantine table
query_quarantine = df_with_flag.writeStream \
    .foreachBatch(append_into_quarantine) \
    .option("checkpointLocation", "/Volumes/second_data_engineering_project/pipeline_metadata/autoloader_metadata/checkpoints/silver/customers_quarantine") \
    .trigger(availableNow=True) \
    .start()


query_silver.awaitTermination()
query_quarantine.awaitTermination()
