df = spark.readStream.table("second_data_engineering_project.bronze.order_items")

# Load reference data for relationship validation (as batch DataFrames)
valid_orders = spark.table("second_data_engineering_project.bronze.orders").select("order_id").distinct()
valid_products = spark.table("second_data_engineering_project.bronze.products").select("product_id").distinct()
valid_sellers = spark.table("second_data_engineering_project.bronze.sellers").select("seller_id").distinct()

from pyspark.sql import functions as F

# Add marker columns to reference tables for clean left-join existence checks
valid_orders_marked = valid_orders.withColumn("_order_exists", F.lit(True))
valid_products_marked = valid_products.withColumn("_product_exists", F.lit(True))
valid_sellers_marked = valid_sellers.withColumn("_seller_exists", F.lit(True))

# Clean and standardize string columns, then add quality flags
df_with_flag = (
    df
    # Standard cleaning: trim and lowercase ID columns
    .withColumn("order_id", F.lower(F.trim(F.col("order_id"))))
    .withColumn("product_id", F.lower(F.trim(F.col("product_id"))))
    .withColumn("seller_id", F.lower(F.trim(F.col("seller_id"))))
    # Left join with reference tables to check relationships
    .join(F.broadcast(valid_orders_marked), on="order_id", how="left")
    .join(F.broadcast(valid_products_marked), on="product_id", how="left")
    .join(F.broadcast(valid_sellers_marked), on="seller_id", how="left")
    # Add quality flag for all validation rules
    .withColumn(
        "data_quality_flag",
        F.when(
            # order_id checks
            F.col("order_id").isNull() |
            (F.col("order_id") == "") |
            (F.col("order_id") == "0") |
            ~ F.col("order_id").rlike("^[0-9a-fA-F]{32}$") |
            # order_item_id checks
            F.col("order_item_id").isNull() |
            (F.col("order_item_id") <= 0) |
            # product_id checks
            F.col("product_id").isNull() |
            (F.col("product_id") == "") |
            (F.col("product_id") == "0") |
            ~ F.col("product_id").rlike("^[0-9a-fA-F]{32}$") |
            # seller_id checks
            F.col("seller_id").isNull() |
            (F.col("seller_id") == "") |
            (F.col("seller_id") == "0") |
            ~ F.col("seller_id").rlike("^[0-9a-fA-F]{32}$") |
            # price checks (must be positive)
            F.col("price").isNull() |
            (F.col("price") < 0) |
            # freight_value checks (must be non-negative)
            F.col("freight_value").isNull() |
            (F.col("freight_value") < 0) |
            # shipping_limit_date checks
            F.col("shipping_limit_date").isNull() |
            # Relationship checks: marker column is NULL when left join found no match
            # order_id not found in orders table → quarantine
            F.col("_order_exists").isNull() |
            # product_id not found in products table → quarantine
            F.col("_product_exists").isNull() |
            # seller_id not found in sellers table → quarantine
            F.col("_seller_exists").isNull(),
            F.lit("quarantine")
        )
        .otherwise(F.lit("valid"))
    )
    # Drop the marker columns from joins
    .drop("_order_exists", "_product_exists", "_seller_exists")
)

# Split into valid and quarantine DataFrames
# Drop duplicates only from df_silver
# Drop _rescued_data column only from df_silver
df_silver = df_with_flag.filter(F.col("data_quality_flag") == "valid").drop("data_quality_flag").drop("_rescued_data").dropDuplicates(["order_id", "order_item_id"])
df_quarantine = df_with_flag.filter(F.col("data_quality_flag") == "quarantine").drop("data_quality_flag")

# Write valid records to silver table
df_silver.writeStream \
    .option("checkpointLocation", "/Volumes/second_data_engineering_project/pipeline_metadata/autoloader_metadata/checkpoints/silver/order_items") \
    .trigger(availableNow=True) \
    .option("mergeSchema", "true") \
    .table("second_data_engineering_project.silver.order_items")

# Write quarantine records to quarantine table
df_quarantine.writeStream \
    .option("checkpointLocation", "/Volumes/second_data_engineering_project/pipeline_metadata/autoloader_metadata/checkpoints/silver/order_items_quarantine") \
    .trigger(availableNow=True) \
    .option("mergeSchema", "true") \
    .table("second_data_engineering_project.silver.order_items_quarantine")