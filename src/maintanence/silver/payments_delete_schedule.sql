-- Scheduled daily cleaning task at midnight for the silver table to remove any records that are no longer in the bronze table
DELETE FROM second_data_engineering_project.silver.payments AS target
WHERE NOT EXISTS (
    SELECT 1
    FROM second_data_engineering_project.bronze.payments AS source
    WHERE LOWER(TRIM(source.order_id)) = target.order_id
      AND source.payment_sequential = target.payment_sequential
);