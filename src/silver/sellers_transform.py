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
df_silver = df_with_flag.filter(F.col("data_quality_flag") == "valid").drop("data_quality_flag").drop("_rescued_data").dropDuplicates(["seller_id"])
df_quarantine = df_with_flag.filter(F.col("data_quality_flag") == "quarantine").drop("data_quality_flag")

# Write valid records to silver table
df_silver.writeStream \
    .option("checkpointLocation", "/Volumes/second_data_engineering_project/pipeline_metadata/autoloader_metadata/checkpoints/silver/sellers") \
    .trigger(availableNow=True) \
    .option("mergeSchema", "true") \
    .table("second_data_engineering_project.silver.sellers")

# Write quarantine records to quarantine table
df_quarantine.writeStream \
    .option("checkpointLocation", "/Volumes/second_data_engineering_project/pipeline_metadata/autoloader_metadata/checkpoints/silver/sellers_quarantine") \
    .trigger(availableNow=True) \
    .option("mergeSchema", "true") \
    .table("second_data_engineering_project.silver.sellers_quarantine")