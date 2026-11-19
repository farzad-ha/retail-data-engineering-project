-- Scheduled daily cleaning task at midnight for the silver table to remove any records that are no longer in the bronze table
DELETE FROM second_data_engineering_project.silver.products AS target
WHERE NOT EXISTS (
    SELECT 1
    FROM second_data_engineering_project.bronze.products AS source
    WHERE LOWER(TRIM(source.product_id)) = target.product_id
);