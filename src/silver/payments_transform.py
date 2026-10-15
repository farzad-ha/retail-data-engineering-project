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
df_silver = df_with_flag.filter(F.col("data_quality_flag") == "valid").drop("data_quality_flag").dropDuplicates(["order_id", "payment_sequential"]).drop("_rescued_data")
df_quarantine = df_with_flag.filter(F.col("data_quality_flag") == "quarantine").drop("data_quality_flag")

# Write valid records to silver table
df_silver.writeStream \
    .option("checkpointLocation", "/Volumes/second_data_engineering_project/pipeline_metadata/autoloader_metadata/checkpoints/silver/payments") \
    .trigger(availableNow=True) \
    .option("mergeSchema", "true") \
    .table("second_data_engineering_project.silver.payments")

# Write quarantine records to quarantine table
df_quarantine.writeStream \
    .option("checkpointLocation", "/Volumes/second_data_engineering_project/pipeline_metadata/autoloader_metadata/checkpoints/silver/payments_quarantine") \
    .trigger(availableNow=True) \
    .option("mergeSchema", "true") \
    .table("second_data_engineering_project.silver.payments_quarantine")