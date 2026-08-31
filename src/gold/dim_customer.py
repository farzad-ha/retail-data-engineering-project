import dlt
from pyspark.sql import functions as F

@dlt.table(
    name="dim_customer",
    comment="Gold layer: Customer dimension table for analytics"
)
def dim_customer():
    """
    Gold layer: Customer dimension table.
    
    Business logic:
    - Create surrogate key for customer dimension
    - Add metadata columns (load_date, is_current)
    - Prepare for Type 2 SCD if needed in future
    
    Future enhancements:
    - Add customer segmentation
    - Calculate customer lifetime metrics
    - Add geographic enrichment
    """

    df = (
        dlt.read("second_data_engineering_project.silver.customers_silver")
        .select(
            F.col("customer_id"),
            F.col("customer_unique_id"),
            F.col("customer_zip_code_prefix"),
            F.col("customer_city"),
            F.col("customer_state"),
            F.current_timestamp().alias("load_date"),
            F.lit(True).alias("is_current")
        )
    )

    return df