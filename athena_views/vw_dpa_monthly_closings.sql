-- ============================================================================
-- VIEW: DPA Monthly Closings with Buyer
-- Report: How many DPA closings for the month by buyer
-- ============================================================================
-- Usage: SELECT * FROM arrive_home.vw_dpa_monthly_closings WHERE closing_year = 2026;
-- ============================================================================

CREATE OR REPLACE VIEW arrive_home.vw_dpa_monthly_closings AS
SELECT 
    YEAR(CAST(l.closing_date AS DATE)) AS closing_year,
    MONTH(CAST(l.closing_date AS DATE)) AS closing_month,
    DATE_FORMAT(CAST(l.closing_date AS DATE), '%Y-%m') AS year_month,
    s.first_mortgage_ownership_status AS buyer,
    COUNT(*) AS closing_count,
    SUM(COALESCE(m.dpa_amount, 0)) AS total_dpa_amount,
    AVG(COALESCE(m.dpa_amount, 0)) AS avg_dpa_amount,
    COUNT(DISTINCT l.correspondent_id) AS correspondent_count
FROM arrive_home.dim_loan l
LEFT JOIN arrive_home.fact_loan_status s ON l.loan_id = s.loan_id
LEFT JOIN arrive_home.fact_loan_metrics m ON l.loan_id = m.loan_id
WHERE UPPER(COALESCE(l.product_type, '')) = 'DPA'
  AND l.closing_date IS NOT NULL
GROUP BY 
    YEAR(CAST(l.closing_date AS DATE)),
    MONTH(CAST(l.closing_date AS DATE)),
    DATE_FORMAT(CAST(l.closing_date AS DATE), '%Y-%m'),
    s.first_mortgage_ownership_status
ORDER BY closing_year DESC, closing_month DESC, buyer;
