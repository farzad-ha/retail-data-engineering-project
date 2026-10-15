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

# Split into valid and quarantine tables
df_silver = df_with_flag.filter(F.col("data_quality_flag") == "valid").drop("data_quality_flag")
df_quarantine = df_with_flag.filter(F.col("data_quality_flag") == "quarantine").drop("data_quality_flag")

# Drop _rescued_data column
# Drop duplicates
df_silver = df_silver.drop("_rescued_data").dropDuplicates(["customer_id"])

# Write valid records to silver table
df_silver.writeStream \
    .option("checkpointLocation", "/Volumes/second_data_engineering_project/pipeline_metadata/autoloader_metadata/checkpoints/silver/customers") \
    .trigger(availableNow=True) \
    .option("mergeSchema", "true") \
    .table("second_data_engineering_project.silver.customers")

# Write quarantine records to quarantine table
df_quarantine.writeStream \
    .option("checkpointLocation", "/Volumes/second_data_engineering_project/pipeline_metadata/autoloader_metadata/checkpoints/silver/customers_quarantine") \
    .trigger(availableNow=True) \
    .option("mergeSchema", "true") \
    .table("second_data_engineering_project.silver.customers_quarantine")


