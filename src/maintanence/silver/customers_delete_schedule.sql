-- Scheduled daily cleaning task at midnight for the silver table to remove any records that are no longer in the bronze table
DELETE FROM second_data_engineering_project.silver.customers AS target
WHERE NOT EXISTS (
    SELECT 1
    FROM second_data_engineering_project.bronze.customers AS source
    WHERE LOWER(TRIM(source.customer_id)) = target.customer_id
);
