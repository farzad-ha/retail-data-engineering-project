-- Scheduled daily cleaning task at midnight for the silver table to remove any records that are no longer in the bronze table
DELETE FROM second_data_engineering_project.silver.order_items AS target
WHERE NOT EXISTS (
    SELECT 1
    FROM second_data_engineering_project.bronze.order_items AS source
    WHERE LOWER(TRIM(source.order_id)) = target.order_id
      AND source.order_item_id = target.order_item_id
);