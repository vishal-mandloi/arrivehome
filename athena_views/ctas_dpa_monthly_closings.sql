-- ============================================================================
-- CTAS: DPA Monthly Closings with Buyer (Pre-computed table for fast queries)
-- ============================================================================
-- Refresh: Run daily via Lambda (DROP + CREATE)
-- ============================================================================

DROP TABLE IF EXISTS arrive_home.ctas_dpa_monthly_closings;

CREATE TABLE arrive_home.ctas_dpa_monthly_closings
WITH (
    format = 'PARQUET',
    external_location = 's3://arrivehome-bi-prod/athena-ctas/ctas_dpa_monthly_closings/',
    parquet_compression = 'SNAPPY'
) AS
SELECT 
    YEAR(CAST(l.closing_date AS DATE)) AS closing_year,
    MONTH(CAST(l.closing_date AS DATE)) AS closing_month,
    DATE_FORMAT(CAST(l.closing_date AS DATE), '%Y-%m') AS year_month,
    CASE 
        WHEN UPPER(COALESCE(s.first_mortgage_ownership_status, '')) LIKE '%USF%' THEN 'USF'
        WHEN l.usf_loan_number IS NOT NULL AND TRIM(l.usf_loan_number) != '' THEN 'USF'
        ELSE 'MWF'
    END AS buyer,
    COUNT(*) AS closing_count,
    SUM(COALESCE(m.dpa_amount, 0)) AS total_dpa_amount,
    AVG(COALESCE(m.dpa_amount, 0)) AS avg_dpa_amount,
    COUNT(DISTINCT l.correspondent_id) AS correspondent_count,
    CURRENT_TIMESTAMP AS _refreshed_at
FROM arrive_home.dim_loan l
LEFT JOIN arrive_home.fact_loan_status s ON l.loan_id = s.loan_id
LEFT JOIN arrive_home.fact_loan_metrics m ON l.loan_id = m.loan_id
WHERE UPPER(COALESCE(l.product_type, '')) = 'DPA'
  AND l.closing_date IS NOT NULL
GROUP BY 
    YEAR(CAST(l.closing_date AS DATE)),
    MONTH(CAST(l.closing_date AS DATE)),
    DATE_FORMAT(CAST(l.closing_date AS DATE), '%Y-%m'),
    CASE 
        WHEN UPPER(COALESCE(s.first_mortgage_ownership_status, '')) LIKE '%USF%' THEN 'USF'
        WHEN l.usf_loan_number IS NOT NULL AND TRIM(l.usf_loan_number) != '' THEN 'USF'
        ELSE 'MWF'
    END;
