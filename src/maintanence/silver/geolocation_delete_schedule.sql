-- Scheduled daily cleaning task at midnight for the silver table to remove any records that are no longer in the bronze table
DELETE FROM second_data_engineering_project.silver.geolocation AS target
WHERE NOT EXISTS (
    SELECT 1
    FROM second_data_engineering_project.bronze.geolocation AS source
    WHERE source.geolocation_zip_code_prefix = target.geolocation_zip_code_prefix
      AND source.geolocation_lat = target.geolocation_lat
      AND source.geolocation_lng = target.geolocation_lng
      AND INITCAP(TRIM(source.geolocation_city)) = target.geolocation_city
      AND UPPER(TRIM(source.geolocation_state)) = target.geolocation_state
);
