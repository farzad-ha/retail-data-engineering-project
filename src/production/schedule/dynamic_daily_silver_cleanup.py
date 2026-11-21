# Top-level configuration variables
catalog = "second_data_engineering_project"
source_schema = "bronze"
target_schema = "silver"

# Table-to-join-condition mapping
cleanup_configs = [
    {
        "table_name": "customers",
        "join_condition": "LOWER(TRIM(source.customer_id)) = target.customer_id"
    },
    {
        "table_name": "sellers",
        "join_condition": "LOWER(TRIM(source.seller_id)) = target.seller_id"
    },
    {
        "table_name": "products",
        "join_condition": "LOWER(TRIM(source.product_id)) = target.product_id"
    },
    {
        "table_name": "product_category_name_translation",
        "join_condition": "LOWER(TRIM(source.product_category_name)) = target.product_category_name"
    },
    {
        "table_name": "orders",
        "join_condition": "LOWER(TRIM(source.order_id)) = target.order_id"
    },
    {
        "table_name": "reviews",
        "join_condition": "LOWER(TRIM(source.review_id)) = target.review_id"
    },
    {
        "table_name": "order_items",
        "join_condition": "LOWER(TRIM(source.order_id)) = target.order_id AND source.order_item_id = target.order_item_id"
    },
    {
        "table_name": "payments",
        "join_condition": "LOWER(TRIM(source.order_id)) = target.order_id AND source.payment_sequential = target.payment_sequential"
    },
    {
        "table_name": "geolocation",
        "join_condition": """
            source.geolocation_zip_code_prefix = target.geolocation_zip_code_prefix
            AND source.geolocation_lat = target.geolocation_lat
            AND source.geolocation_lng = target.geolocation_lng
            AND INITCAP(TRIM(source.geolocation_city)) = target.geolocation_city
            AND UPPER(TRIM(source.geolocation_state)) = target.geolocation_state
        """
    }
]

# Execution loop accessing keys by explicit name
# Skip cleanup if the target Silver table has not been created yet
for config in cleanup_configs:
    table_name = config["table_name"]
    join_condition = config["join_condition"]
    
    source_table = f"{catalog}.{source_schema}.{table_name}"
    target_table = f"{catalog}.{target_schema}.{table_name}"
    
    if not spark.catalog.tableExists(target_table):
        continue

    delete_query = f"""
        DELETE FROM {target_table} AS target
        WHERE NOT EXISTS (
            SELECT 1
            FROM {source_table} AS source
            WHERE {join_condition}
        )
    """
    
    spark.sql(delete_query)