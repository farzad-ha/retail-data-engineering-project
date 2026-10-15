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

# Split into valid and quarantine DataFrames
df_silver = df_with_flag.filter(F.col("data_quality_flag") == "valid").drop("data_quality_flag").drop("_rescued_data").dropDuplicates([
    "geolocation_zip_code_prefix",
    "geolocation_lat",
    "geolocation_lng",
    "geolocation_city",
    "geolocation_state"
])
df_quarantine = df_with_flag.filter(F.col("data_quality_flag") == "quarantine").drop("data_quality_flag")

# Write valid records to silver table
df_silver.writeStream \
    .option("checkpointLocation", "/Volumes/second_data_engineering_project/pipeline_metadata/autoloader_metadata/checkpoints/silver/geolocation_silver") \
    .trigger(availableNow=True) \
    .option("mergeSchema", "true") \
    .table("second_data_engineering_project.silver.geolocation")

# Write quarantine records to quarantine table
df_quarantine.writeStream \
    .option("checkpointLocation", "/Volumes/second_data_engineering_project/pipeline_metadata/autoloader_metadata/checkpoints/silver/geolocation_quarantine") \
    .trigger(availableNow=True) \
    .option("mergeSchema", "true") \
    .table("second_data_engineering_project.silver.geolocation_quarantine")