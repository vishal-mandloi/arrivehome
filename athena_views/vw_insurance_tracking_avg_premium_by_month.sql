-- ============================================================================
-- VIEW: Avg Premium Worked vs Closed by Month (loan grain)
-- Matches Power BI: "Avg Premium - Worked vs Closed by Loan"
-- Database: insurance_tracking_sharepoint
-- ============================================================================
-- Grain: one row per month_label
-- worked  = avg of max(premium) per loan_key_v3 in that request month
-- closed  = avg of max(premium) per loan_key_v3 closed in that close month
-- ============================================================================

CREATE OR REPLACE VIEW insurance_tracking_sharepoint.vw_insurance_tracking_avg_premium_by_month AS
WITH loan_worked AS (
    SELECT
        loan_key_v3,
        CAST(request_month_start AS DATE) AS month_start,
        MAX(premium) AS premium
    FROM insurance_tracking_sharepoint.vw_insurance_tracking_procurement_analysis
    WHERE loan_key_v3 IS NOT NULL
      AND TRIM(loan_key_v3) <> ''
      AND request_month_start IS NOT NULL
      AND premium IS NOT NULL
    GROUP BY loan_key_v3, CAST(request_month_start AS DATE)
),
loan_closed AS (
    SELECT
        loan_key_v3,
        CAST(close_month_start AS DATE) AS month_start,
        MAX(premium) AS premium
    FROM insurance_tracking_sharepoint.vw_insurance_tracking_procurement_analysis
    WHERE is_closed_loan_row = 1
      AND loan_key_v3 IS NOT NULL
      AND TRIM(loan_key_v3) <> ''
      AND close_month_start IS NOT NULL
      AND premium IS NOT NULL
    GROUP BY loan_key_v3, CAST(close_month_start AS DATE)
),
all_months AS (
    SELECT month_start FROM loan_worked
    UNION
    SELECT month_start FROM loan_closed
)
SELECT
    date_format(m.month_start, '%b %Y') AS month_label,
    year(m.month_start) * 100 + month(m.month_start) AS year_month_sort,
    m.month_start,
    AVG(w.premium) AS avg_premium_worked,
    AVG(c.premium) AS avg_premium_closed
FROM all_months m
LEFT JOIN loan_worked w ON m.month_start = w.month_start
LEFT JOIN loan_closed c ON m.month_start = c.month_start
GROUP BY m.month_start
ORDER BY year_month_sort;
