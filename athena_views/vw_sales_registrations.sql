-- ============================================================================
-- VIEW: Sales - Registrations by Month and Product
-- Report: Number of registrations for the month by product
-- ============================================================================
-- Usage: SELECT * FROM arrive_home.vw_sales_registrations WHERE registration_year = 2026;
-- ============================================================================

CREATE OR REPLACE VIEW arrive_home.vw_sales_registrations AS
SELECT 
    YEAR(CAST(s.registered_at AS TIMESTAMP)) AS registration_year,
    MONTH(CAST(s.registered_at AS TIMESTAMP)) AS registration_month,
    DATE_FORMAT(CAST(s.registered_at AS TIMESTAMP), '%Y-%m') AS year_month,
    COALESCE(l.product_type, 'Unknown') AS product_type,
    COUNT(*) AS registration_count,
    COUNT(DISTINCT l.correspondent_id) AS correspondent_count,
    MIN(CAST(s.registered_at AS TIMESTAMP)) AS first_registration,
    MAX(CAST(s.registered_at AS TIMESTAMP)) AS last_registration
FROM arrive_home.fact_loan_status s
JOIN arrive_home.dim_loan l ON s.loan_id = l.loan_id
WHERE s.registered_at IS NOT NULL
GROUP BY 
    YEAR(CAST(s.registered_at AS TIMESTAMP)),
    MONTH(CAST(s.registered_at AS TIMESTAMP)),
    DATE_FORMAT(CAST(s.registered_at AS TIMESTAMP), '%Y-%m'),
    COALESCE(l.product_type, 'Unknown')
ORDER BY registration_year DESC, registration_month DESC, product_type;
