df_bronze = spark.readStream.table("second_data_engineering_project.bronze.reviews")

from pyspark.sql import functions as F

# Trim and standardize fields, add data quality flag
# If review_comment_message is null, use review_comment_title as replacement
# If review_comment_message is null and review_comment_title is null
# or if it has any special caracters in the list [".", "?", "!", ":", ";", ",", ""]
# use "No comments provided"
df_with_flag = (
    df_bronze
    .withColumn("review_id", F.lower(F.trim(F.col("review_id"))))
    .withColumn("order_id", F.lower(F.trim(F.col("order_id"))))
    .withColumn("review_comment_title", F.trim(F.col("review_comment_title")))
    .withColumn("review_comment_message", F.coalesce(F.col("review_comment_message"), F.col("review_comment_title")))
    .withColumn(
    "review_comment_message",
    F.when(F.col("review_comment_message").isNull(), F.lit("No comments provided"))
    .otherwise(F.col("review_comment_message"))
)
    .withColumn(
    "review_comment_message",
    F.when(
    F.trim(F.col("review_comment_message")).isin([".", "?", "!", ":", ";", ",", ""]),
    F.lit("No comments provided")).otherwise(F.trim(F.col("review_comment_message")))
)     
    .withColumn(
        "data_quality_flag",
        F.when(
            # review_id checks
            F.col("review_id").isNull() |
            (F.col("review_id") == "") |
            ~F.col("review_id").rlike("^[0-9a-fA-F]{32}$") |
            # order_id checks
            F.col("order_id").isNull() |
            (F.col("order_id") == "") |
            ~F.col("order_id").rlike("^[0-9a-fA-F]{32}$") |
            # review_score checks (must be 1-5)
            F.col("review_score").isNull() |
            (F.col("review_score") < 1) |
            (F.col("review_score") > 5) |
            # review_creation_date checks
            F.col("review_creation_date").isNull(),
            F.lit("quarantine")
        )
        .otherwise(F.lit("valid"))
    )
)

# Split into valid and quarantine tables
# drop _rescued_data column from df_silver only
# drop duplicates from df_silver only
# drop review_comment_title from df_silver only

df_silver = df_with_flag.filter(F.col("data_quality_flag") == "valid").drop("data_quality_flag").dropDuplicates(["review_id"]).drop("_rescued_data").drop("review_comment_title")
df_quarantine = df_with_flag.filter(F.col("data_quality_flag") == "quarantine").drop("data_quality_flag")

# Write valid records to silver table
df_silver.writeStream \
    .option("checkpointLocation", "/Volumes/second_data_engineering_project/pipeline_metadata/autoloader_metadata/checkpoints/silver/reviews") \
    .trigger(availableNow=True) \
    .option("mergeSchema", "true") \
    .table("second_data_engineering_project.silver.reviews")

# Write quarantine records to quarantine table
df_quarantine.writeStream \
    .option("checkpointLocation", "/Volumes/second_data_engineering_project/pipeline_metadata/autoloader_metadata/checkpoints/silver/reviews_quarantine") \
    .trigger(availableNow=True) \
    .option("mergeSchema", "true") \
    .table("second_data_engineering_project.silver.reviews_quarantine")

