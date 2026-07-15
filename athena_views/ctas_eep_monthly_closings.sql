-- ============================================================================
-- CTAS: EEP Monthly Closings (Pre-computed table for fast queries)
-- ============================================================================
-- Refresh: Run daily via Lambda (DROP + CREATE)
-- ============================================================================

DROP TABLE IF EXISTS arrive_home.ctas_eep_monthly_closings;

CREATE TABLE arrive_home.ctas_eep_monthly_closings
WITH (
    format = 'PARQUET',
    external_location = 's3://arrivehome-bi-prod/athena-ctas/ctas_eep_monthly_closings/',
    parquet_compression = 'SNAPPY'
) AS
SELECT 
    YEAR(CAST(l.closing_date AS DATE)) AS closing_year,
    MONTH(CAST(l.closing_date AS DATE)) AS closing_month,
    DATE_FORMAT(CAST(l.closing_date AS DATE), '%Y-%m') AS year_month,
    COUNT(*) AS closing_count,
    SUM(COALESCE(m.dpa_amount, 0)) AS total_dpa_amount,
    AVG(COALESCE(m.dpa_amount, 0)) AS avg_dpa_amount,
    COUNT(DISTINCT l.correspondent_id) AS correspondent_count,
    MIN(CAST(l.closing_date AS DATE)) AS first_closing,
    MAX(CAST(l.closing_date AS DATE)) AS last_closing,
    CURRENT_TIMESTAMP AS _refreshed_at
FROM arrive_home.dim_loan l
LEFT JOIN arrive_home.fact_loan_metrics m ON l.loan_id = m.loan_id
WHERE UPPER(COALESCE(l.product_type, '')) = 'EEP'
  AND l.closing_date IS NOT NULL
GROUP BY 
    YEAR(CAST(l.closing_date AS DATE)),
    MONTH(CAST(l.closing_date AS DATE)),
    DATE_FORMAT(CAST(l.closing_date AS DATE), '%Y-%m');
