df_bronze = spark.readStream.table("second_data_engineering_project.bronze.products")

from pyspark.sql import functions as F

# Trim product_id and add data quality flag
# Check for null, empty, or zero values, and invalid format

df_with_flag = (
    df_bronze
    # Standard cleaning: trim and lowercase string columns
    # Correct spellings
    .withColumn("product_id", F.lower(F.trim(F.col("product_id"))))
    .withColumn("product_category_name", F.lower(F.trim(F.col("product_category_name"))))
    .withColumnRenamed("product_name_lenght", "product_name_length")
    .withColumnRenamed("product_description_lenght", "product_description_length")
    .withColumn(
        "data_quality_flag",
        F.when(
            # product_id checks
            F.col("product_id").isNull() | 
            (F.col("product_id") == "") |
            (F.col("product_id") == "0") |
            ~ F.col("product_id").rlike("^[0-9a-fA-F]{32}$") |
            # product_category_name checks
            F.col("product_category_name").isNull() |
            (F.col("product_category_name") == "") |
            # product_name_lenght checks
            F.col("product_name_length").isNull() |
            (F.col("product_name_length") <= 0) |
            # product_description_lenght checks
            F.col("product_description_length").isNull() |
            (F.col("product_description_length") <= 0) |
            # product_photos_qty checks
            F.col("product_photos_qty").isNull() |
            (F.col("product_photos_qty") < 0) |
            # product_weight_g checks
            F.col("product_weight_g").isNull() |
            (F.col("product_weight_g") <= 0) |
            # product_length_cm checks
            F.col("product_length_cm").isNull() |
            (F.col("product_length_cm") <= 0) |
            # product_height_cm checks
            F.col("product_height_cm").isNull() |
            (F.col("product_height_cm") <= 0) |
            # product_width_cm checks
            F.col("product_width_cm").isNull() |
            (F.col("product_width_cm") <= 0),
            F.lit("quarantine")
        )
        .otherwise(F.lit("valid"))
    )
)

# Split into valid and quarantine tables
df_silver = df_with_flag.filter(F.col("data_quality_flag") == "valid").drop("data_quality_flag").dropDuplicates(["product_id"]).drop("_rescued_data")

df_quarantine = df_with_flag.filter(F.col("data_quality_flag") == "quarantine").drop("data_quality_flag")

# Write valid records to silver table
df_silver.writeStream \
    .option("checkpointLocation", "/Volumes/second_data_engineering_project/pipeline_metadata/autoloader_metadata/checkpoints/silver/products") \
    .trigger(availableNow=True) \
    .option("mergeSchema", "true") \
    .table("second_data_engineering_project.silver.products")

# Write quarantine records to quarantine table
df_quarantine.writeStream \
    .option("checkpointLocation", "/Volumes/second_data_engineering_project/pipeline_metadata/autoloader_metadata/checkpoints/silver/products_quarantine") \
    .trigger(availableNow=True) \
    .option("mergeSchema", "true") \
    .table("second_data_engineering_project.silver.products_quarantine")