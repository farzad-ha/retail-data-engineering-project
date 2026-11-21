# Top-level configuration variables
catalog = "second_data_engineering_project"
target_schema = "bronze"

bronze_base_path = "abfss://bronze@engineerolistproject.dfs.core.windows.net"

landing_base_path = f"{bronze_base_path}/landing/raw_files"

metadata_base_path = f"{bronze_base_path}/pipeline_metadata/autoloader_metadata"

# Table ingestion configurations
# Note: schema_subpath and checkpoint_subpath preserve your exact existing Volume paths
bronze_configs = [
    {
        "table_name": "sellers",
        "source_folder": "olist_sellers_dataset",
        "schema_subpath": "schemas/sellers",
        "checkpoint_subpath": "checkpoints/bronze/sellers",
        "schema_hints": "seller_zip_code_prefix INT"
    },
    {
        "table_name": "reviews",
        "source_folder": "olist_order_reviews_dataset",
        "schema_subpath": "schemas/bronze/reviews",
        "checkpoint_subpath": "checkpoints/bronze/reviews",
        "schema_hints": "review_score INT, review_creation_date TIMESTAMP, review_answer_timestamp TIMESTAMP",
        "extra_options": {"multiLine": "true"}
    },
    {
        "table_name": "products",
        "source_folder": "olist_products_dataset",
        "schema_subpath": "schemas/bronze/products",
        "checkpoint_subpath": "checkpoints/bronze/products",
        "schema_hints": "product_name_lenght INT, product_description_lenght INT, product_photos_qty INT, product_weight_g INT, product_length_cm INT, product_height_cm INT, product_width_cm INT"
    },
    {
        "table_name": "product_category_name_translation",
        "source_folder": "product_category_name_translation",
        "schema_subpath": "schemas/bronze/product_category_translation",
        "checkpoint_subpath": "checkpoints/bronze/product_category_translation"
    },
    {
        "table_name": "payments",
        "source_folder": "olist_order_payments_dataset",
        "schema_subpath": "schemas/bronze/payments",
        "checkpoint_subpath": "checkpoints/bronze/payments",
        "schema_hints": "payment_sequential INT, payment_installments INT, payment_value DOUBLE"
    },
    {
        "table_name": "orders",
        "source_folder": "olist_orders_dataset",
        "schema_subpath": "schemas/bronze/orders",
        "checkpoint_subpath": "checkpoints/bronze/orders",
        "schema_hints": "order_purchase_timestamp TIMESTAMP, order_approved_at TIMESTAMP, order_delivered_carrier_date TIMESTAMP, order_delivered_customer_date TIMESTAMP, order_estimated_delivery_date TIMESTAMP"
    },
    {
        "table_name": "order_items",
        "source_folder": "olist_order_items_dataset",
        "schema_subpath": "schemas/bronze/order_items",
        "checkpoint_subpath": "checkpoints/bronze/order_items",
        "schema_hints": "order_item_id INT, shipping_limit_date TIMESTAMP, price DOUBLE, freight_value DOUBLE"
    },
    {
        "table_name": "geolocation",
        "source_folder": "olist_geolocation_dataset",
        "schema_subpath": "schemas/bronze/geolocation",
        "checkpoint_subpath": "checkpoints/bronze/geolocation",
        "schema_hints": "geolocation_zip_code_prefix INT, geolocation_lat DOUBLE, geolocation_lng DOUBLE"
    },
    {
        "table_name": "customers",
        "source_folder": "olist_customers_dataset",
        "schema_subpath": "schemas/bronze/customers",
        "checkpoint_subpath": "checkpoints/bronze/customers",
        "schema_hints": "customer_zip_code_prefix INT"
    }
]

# Start all Auto Loader streams
active_queries = []

for config in bronze_configs:
    table_name = config["table_name"]
    source_path = f"{landing_base_path}/{config['source_folder']}"
    schema_location = f"{metadata_base_path}/{config['schema_subpath']}"
    checkpoint_location = f"{metadata_base_path}/{config['checkpoint_subpath']}"
    target_table = f"{catalog}.{target_schema}.{table_name}"

    # Configure Auto Loader reader
    reader = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.schemaLocation", schema_location)
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("rescuedDataColumn", "_rescued_data")
    )

    # Apply optional schema hints if defined for this table
    if "schema_hints" in config:
        reader = reader.option("cloudFiles.schemaHints", config["schema_hints"])

    # Apply any table-specific reader options (e.g., multiLine for reviews)
    if "extra_options" in config:
        reader = reader.options(**config["extra_options"])

    df_bronze = reader.load(source_path)

    # Start streaming write with Trigger.AvailableNow
    query = (
        df_bronze.writeStream
        .format("delta")
        .outputMode("append")
        .option("mergeSchema", "true")
        .option("checkpointLocation", checkpoint_location)
        .trigger(availableNow=True)
        .toTable(target_table)
    )
    active_queries.append(query)

# Wait for all streaming micro-batches to finish before completing the task
for query in active_queries:
    query.awaitTermination()
