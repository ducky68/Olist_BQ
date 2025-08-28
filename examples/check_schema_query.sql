-- Check Column Existence and Data Quality Summary
-- Run this first to verify which tables have the had_duplicates column

-- Check stg_customers schema (updated)
SELECT 
  column_name,
  data_type
FROM `project-olist-470307.dbt_olist_olist_stg.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = 'stg_customers'
ORDER BY ordinal_position;

-- Quick test of had_duplicates column
-- SELECT had_duplicates, COUNT(*) as record_count 
-- FROM `project-olist-470307.dbt_olist_olist_stg.stg_customers` 
-- GROUP BY had_duplicates;
