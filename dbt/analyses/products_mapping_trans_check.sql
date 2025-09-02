-- Data Quality Check: Product Category Name Matching
-- Check matches and mismatches between products and translation tables

WITH 

-- Get distinct categories from products table
products_categories AS (
  SELECT DISTINCT 
    product_category_name,
    COUNT(*) as product_count
  FROM `project-olist-470307.dbt_olist_stg.stg_products`
  WHERE product_category_name IS NOT NULL
  GROUP BY product_category_name
),

-- Get distinct categories from translation table  
translation_categories AS (
  SELECT DISTINCT 
    product_category_name,
    product_category_name_english
  FROM `project-olist-470307.dbt_olist_stg.stg_product_category_name_translation`
  WHERE product_category_name IS NOT NULL
),

-- Full outer join to find matches and mismatches
match_analysis AS (
  SELECT 
    COALESCE(p.product_category_name, t.product_category_name) AS category_name,
    p.product_count,
    t.product_category_name_english,
    CASE 
      WHEN p.product_category_name IS NOT NULL AND t.product_category_name IS NOT NULL THEN 'MATCHED'
      WHEN p.product_category_name IS NOT NULL AND t.product_category_name IS NULL THEN 'MISSING_TRANSLATION'
      WHEN p.product_category_name IS NULL AND t.product_category_name IS NOT NULL THEN 'TRANSLATION_ORPHAN'
      ELSE 'UNKNOWN'
    END AS match_status
  FROM products_categories p
  FULL OUTER JOIN translation_categories t
    ON p.product_category_name = t.product_category_name
),

-- Summary counts
summary_stats AS (
  SELECT 
    COUNT(*) as total_categories_analyzed,
    COUNTIF(match_status = 'MATCHED') as matched_categories,
    COUNTIF(match_status = 'MISSING_TRANSLATION') as missing_translations,
    COUNTIF(match_status = 'TRANSLATION_ORPHAN') as translation_orphans,
    SUM(CASE WHEN match_status = 'MATCHED' THEN product_count ELSE 0 END) as matched_products,
    SUM(CASE WHEN match_status = 'MISSING_TRANSLATION' THEN product_count ELSE 0 END) as unmatched_products
  FROM match_analysis
)

-- Final Results: Summary + Detailed Mismatches
SELECT 
  '📊 SUMMARY' as section,
  CAST(NULL AS STRING) as category_name,
  CAST(NULL AS INT64) as product_count,
  CAST(NULL AS STRING) as english_translation,
  CAST(NULL AS STRING) as match_status,
  CONCAT(
    'Total Categories: ', total_categories_analyzed, ' | ',
    'Matches: ', matched_categories, ' | ',
    'Missing Translations: ', missing_translations, ' | ',
    'Translation Orphans: ', translation_orphans, ' | ',
    'Products with Translation: ', matched_products, ' | ',
    'Products without Translation: ', unmatched_products
  ) as details
FROM summary_stats

UNION ALL

SELECT 
  '🔍 MATCHED CATEGORIES' as section,
  category_name,
  product_count,
  product_category_name_english as english_translation,
  match_status,
  CONCAT('✅ ', product_count, ' products have English translation') as details
FROM match_analysis
WHERE match_status = 'MATCHED'

UNION ALL

SELECT 
  '⚠️ MISSING TRANSLATIONS' as section,
  category_name,
  product_count,
  CAST(NULL AS STRING) as english_translation,
  match_status,
  CONCAT('❌ ', product_count, ' products missing English translation') as details
FROM match_analysis
WHERE match_status = 'MISSING_TRANSLATION'

UNION ALL

SELECT 
  '🔧 TRANSLATION ORPHANS' as section,
  category_name,
  CAST(NULL AS INT64) as product_count,
  product_category_name_english as english_translation,
  match_status,
  '⚠️ Translation exists but no products use this category' as details
FROM match_analysis
WHERE match_status = 'TRANSLATION_ORPHAN'

ORDER BY 
  CASE section 
    WHEN '📊 SUMMARY' THEN 1
    WHEN '🔍 MATCHED CATEGORIES' THEN 2  
    WHEN '⚠️ MISSING TRANSLATIONS' THEN 3
    WHEN '🔧 TRANSLATION ORPHANS' THEN 4
  END,
  product_count DESC NULLS LAST,
  category_name;