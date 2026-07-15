-- ============================================================================
-- CTAS: Sales Registrations by Product (Pre-computed table for fast queries)
-- ============================================================================
-- Refresh: Run daily via Lambda (DROP + CREATE)
-- ============================================================================

DROP TABLE IF EXISTS arrive_home.ctas_sales_registrations;

CREATE TABLE arrive_home.ctas_sales_registrations
WITH (
    format = 'PARQUET',
    external_location = 's3://arrivehome-bi-prod/athena-ctas/ctas_sales_registrations/',
    parquet_compression = 'SNAPPY'
) AS
SELECT 
    YEAR(CAST(s.registered_at AS TIMESTAMP)) AS registration_year,
    MONTH(CAST(s.registered_at AS TIMESTAMP)) AS registration_month,
    DATE_FORMAT(CAST(s.registered_at AS TIMESTAMP), '%Y-%m') AS year_month,
    COALESCE(l.product_type, 'Unknown') AS product_type,
    COUNT(*) AS registration_count,
    COUNT(DISTINCT l.correspondent_id) AS correspondent_count,
    MIN(CAST(s.registered_at AS TIMESTAMP)) AS first_registration,
    MAX(CAST(s.registered_at AS TIMESTAMP)) AS last_registration,
    CURRENT_TIMESTAMP AS _refreshed_at
FROM arrive_home.fact_loan_status s
JOIN arrive_home.dim_loan l ON s.loan_id = l.loan_id
WHERE s.registered_at IS NOT NULL
GROUP BY 
    YEAR(CAST(s.registered_at AS TIMESTAMP)),
    MONTH(CAST(s.registered_at AS TIMESTAMP)),
    DATE_FORMAT(CAST(s.registered_at AS TIMESTAMP), '%Y-%m'),
    COALESCE(l.product_type, 'Unknown');
