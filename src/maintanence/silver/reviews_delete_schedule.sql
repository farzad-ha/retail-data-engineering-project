-- Scheduled daily cleaning task at midnight for the silver table to remove any records that are no longer in the bronze table
DELETE FROM second_data_engineering_project.silver.reviews AS target
WHERE NOT EXISTS (
    SELECT 1
    FROM second_data_engineering_project.bronze.reviews AS source
    WHERE LOWER(TRIM(source.review_id)) = target.review_id
);