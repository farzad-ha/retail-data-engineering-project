-- Scheduled daily cleaning task at midnight for the silver table to remove any records that are no longer in the bronze table
DELETE FROM second_data_engineering_project.silver.customers
WHERE customer_id NOT IN (
    SELECT DISTINCT customer_id 
    FROM second_data_engineering_project.bronze.customers
);
