import dlt
from pyspark.sql import functions as F

@dlt.table(
    name="customers_silver",
    comment="Silver layer: Clean customers data with data quality filters applied"
)
def transform_customers():
    """
    Silver layer: Transform and clean customers data.
    
    Data quality filters:
    - Remove records with NULL customer_id
    
    Future enhancements:
    - Standardize customer_state codes
    - Validate zip code formats
    - Deduplicate customer records
    """

    df = (
        dlt.read("second_data_engineering_project.bronze.customers_bronze")
        .where(F.col("customer_id").isNotNull())
    )

    return df