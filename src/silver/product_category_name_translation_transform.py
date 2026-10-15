df_bronze = spark.readStream.table("second_data_engineering_project.bronze.product_category_name_translation")

from pyspark.sql import functions as F

# Trim and standardize category names, add data quality flag
df_with_flag = (
    df_bronze
    .withColumn("product_category_name", F.lower(F.trim(F.col("product_category_name"))))
    .withColumn("product_category_name_english", F.lower(F.trim(F.col("product_category_name_english"))))
    .withColumn(
        "data_quality_flag",
        F.when(
            # Both category names must be populated
            F.col("product_category_name").isNull() |
            (F.col("product_category_name") == "") |
            F.col("product_category_name_english").isNull() |
            (F.col("product_category_name_english") == ""),
            F.lit("quarantine")
        )
        .otherwise(F.lit("valid"))
    )
)

# Split into valid and quarantine tables
# Drop duplicates only from df_silver
# Drop _rescued_data column only from df_silver
df_silver = df_with_flag.filter(F.col("data_quality_flag") == "valid").drop("data_quality_flag").dropDuplicates(["product_category_name"]).drop("_rescued_data")
df_quarantine = df_with_flag.filter(F.col("data_quality_flag") == "quarantine").drop("data_quality_flag")

# Write valid records to silver table
df_silver.writeStream \
    .option("checkpointLocation", "/Volumes/second_data_engineering_project/pipeline_metadata/autoloader_metadata/checkpoints/silver/product_category_name_translation") \
    .trigger(availableNow=True) \
    .option("mergeSchema", "true") \
    .table("second_data_engineering_project.silver.product_category_name_translation")

# Write quarantine records to quarantine table
df_quarantine.writeStream \
    .option("checkpointLocation", "/Volumes/second_data_engineering_project/pipeline_metadata/autoloader_metadata/checkpoints/silver/product_category_name_translation_quarantine") \
    .trigger(availableNow=True) \
    .option("mergeSchema", "true") \
    .table("second_data_engineering_project.silver.product_category_name_translation_quarantine")
