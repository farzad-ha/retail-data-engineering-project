df_bronze = spark.readStream.table("second_data_engineering_project.bronze.orders")

from pyspark.sql import functions as F

# Trim and standardize fields, add data quality flag
# Check for null, empty, invalid values, and logical timestamp order

df_with_flag = (
    df_bronze
    # Standard cleaning: trim and lowercase string columns
    .withColumn("order_id", F.lower(F.trim(F.col("order_id"))))
    .withColumn("customer_id", F.lower(F.trim(F.col("customer_id"))))
    .withColumn("order_status", F.lower(F.trim(F.col("order_status"))))
    .withColumn(
        "data_quality_flag",
        F.when(
            # order_id checks
            F.col("order_id").isNull() |
            (F.col("order_id") == "") |
            (F.col("order_id") == "0") |
            ~ F.col("order_id").rlike("^[0-9a-fA-F]{32}$") |
            # customer_id checks
            F.col("customer_id").isNull() |
            (F.col("customer_id") == "") |
            (F.col("customer_id") == "0") |
            ~ F.col("customer_id").rlike("^[0-9a-fA-F]{32}$") |
            # order_status checks (must be one of 8 valid statuses)
            F.col("order_status").isNull() |
            (F.col("order_status") == "") |
            ~ F.col("order_status").isin(["delivered", "shipped", "canceled", "unavailable", "invoiced", "processing", "created", "approved"]) |
            # order_purchase_timestamp checks (required)
            F.col("order_purchase_timestamp").isNull() |
            # order_estimated_delivery_date checks (required)
            F.col("order_estimated_delivery_date").isNull() |
            # Logical timestamp order checks
            # approved_at must be >= purchase_timestamp (when not NULL)
            (F.col("order_approved_at").isNotNull() & (F.col("order_approved_at") < F.col("order_purchase_timestamp"))) |
            # carrier_date must be >= approved_at (when both not NULL)
            (F.col("order_delivered_carrier_date").isNotNull() & F.col("order_approved_at").isNotNull() & 
             (F.col("order_delivered_carrier_date") < F.col("order_approved_at"))) |
            # customer_date must be >= carrier_date (when both not NULL)
            (F.col("order_delivered_customer_date").isNotNull() & F.col("order_delivered_carrier_date").isNotNull() & 
             (F.col("order_delivered_customer_date") < F.col("order_delivered_carrier_date"))),
            F.lit("quarantine")
        )
        .otherwise(F.lit("valid"))
    )
)


# Add exception flag for business clarification cases
# Delivered orders missing customer delivery date
# Delivered order with customer delivery date present but carrier delivery date missing
df_with_flags = (
    df_with_flag
    .withColumn(
        "exception_flag",
        F.when(
            (F.col("order_status") == "delivered") & 
            F.col("order_delivered_customer_date").isNull(),
            F.lit("missing_order_delivered_customer_date")
        )
        .when(
            (F.col("order_status") == 'delivered') &
            F.col("order_delivered_carrier_date").isNull() &
            F.col("order_delivered_customer_date").isNotNull(),
            F.lit("missing_delivered_carrier_date")
        )
        .otherwise(F.lit(None))
    )
)

# Three-way split: valid, quarantine, and exceptions
# drop _rescued_data for df_exceptions & df_silver
df_silver = df_with_flags.filter(
    (F.col("data_quality_flag") == "valid") & 
    F.col("exception_flag").isNull()
).drop("data_quality_flag", "exception_flag").drop("_rescued_data").dropDuplicates(["order_id"])

df_quarantine = df_with_flags.filter(
    F.col("data_quality_flag") == "quarantine"
).drop("data_quality_flag", "exception_flag")

df_exceptions = df_with_flags.filter(
    (F.col("data_quality_flag") == "valid") & 
    F.col("exception_flag").isNotNull()
).drop("data_quality_flag").drop("_rescued_data")

# Write valid records to silver table
df_silver.writeStream \
    .option("checkpointLocation", "/Volumes/second_data_engineering_project/pipeline_metadata/autoloader_metadata/checkpoints/silver/orders") \
    .trigger(availableNow=True) \
    .option("mergeSchema", "true") \
    .table("second_data_engineering_project.silver.orders")

# Write quarantine records to quarantine table
df_quarantine.writeStream \
    .option("checkpointLocation", "/Volumes/second_data_engineering_project/pipeline_metadata/autoloader_metadata/checkpoints/silver/orders_quarantine") \
    .trigger(availableNow=True) \
    .option("mergeSchema", "true") \
    .table("second_data_engineering_project.silver.orders_quarantine")

# Write exception records to exceptions table (business clarification needed)
df_exceptions.writeStream \
    .option("checkpointLocation", "/Volumes/second_data_engineering_project/pipeline_metadata/autoloader_metadata/checkpoints/silver/orders_exceptions") \
    .trigger(availableNow=True) \
    .option("mergeSchema", "true") \
    .table("second_data_engineering_project.silver.orders_exceptions")

