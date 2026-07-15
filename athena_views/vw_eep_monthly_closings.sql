-- ============================================================================
-- VIEW: EEP Monthly Closings
-- Report: How many EEP closings in a month
-- ============================================================================
-- Usage: SELECT * FROM arrive_home.vw_eep_monthly_closings WHERE closing_year = 2026;
-- ============================================================================

CREATE OR REPLACE VIEW arrive_home.vw_eep_monthly_closings AS
SELECT 
    YEAR(CAST(l.closing_date AS DATE)) AS closing_year,
    MONTH(CAST(l.closing_date AS DATE)) AS closing_month,
    DATE_FORMAT(CAST(l.closing_date AS DATE), '%Y-%m') AS year_month,
    COUNT(*) AS closing_count,
    SUM(COALESCE(m.dpa_amount, 0)) AS total_dpa_amount,
    AVG(COALESCE(m.dpa_amount, 0)) AS avg_dpa_amount,
    COUNT(DISTINCT l.correspondent_id) AS correspondent_count,
    MIN(CAST(l.closing_date AS DATE)) AS first_closing,
    MAX(CAST(l.closing_date AS DATE)) AS last_closing
FROM arrive_home.dim_loan l
LEFT JOIN arrive_home.fact_loan_metrics m ON l.loan_id = m.loan_id
WHERE UPPER(COALESCE(l.product_type, '')) = 'EEP'
  AND l.closing_date IS NOT NULL
GROUP BY 
    YEAR(CAST(l.closing_date AS DATE)),
    MONTH(CAST(l.closing_date AS DATE)),
    DATE_FORMAT(CAST(l.closing_date AS DATE), '%Y-%m')
ORDER BY closing_year DESC, closing_month DESC;
