-- Scheduled daily cleaning task at midnight for the silver table to remove any records that are no longer in the bronze table
DELETE FROM second_data_engineering_project.silver.product_category_name_translation AS target
WHERE NOT EXISTS (
    SELECT 1
    FROM second_data_engineering_project.bronze.product_category_name_translation AS source
    WHERE LOWER(TRIM(source.product_category_name)) = target.product_category_name
);