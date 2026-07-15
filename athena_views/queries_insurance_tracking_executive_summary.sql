-- ============================================================================
-- Insurance Tracking — Executive Summary • Performance
-- Validation queries (Power BI parity checks)
-- Database: insurance_tracking_sharepoint
-- ============================================================================
-- Run these in Athena after creating vw_insurance_tracking_procurement_analysis.
-- Replace filter values to match your QuickSight slicers.
-- ============================================================================

-- ---------------------------------------------------------------------------
-- 0) Quick row-count sanity check
-- ---------------------------------------------------------------------------
SELECT COUNT(*) AS submission_rows
FROM insurance_tracking_sharepoint.vw_insurance_tracking_procurement_analysis;


-- ---------------------------------------------------------------------------
-- 1) KPI SCORECARD (top row)
--    Matches: Worked Submissions, Closed Submissions, Worked Loans, Closed Loans,
--             Loans Close Rate, Loans Flood Required, Age buckets
-- ---------------------------------------------------------------------------
SELECT
    COUNT(*) AS worked_submissions,
    SUM(is_closed_submission) AS closed_submissions,
    COUNT(DISTINCT loan_key_v3) AS worked_loans,
    COUNT(DISTINCT CASE WHEN is_closed_loan_row = 1 THEN loan_key_v3 END) AS closed_loans,
    ROUND(
        100.0 * COUNT(DISTINCT CASE WHEN is_closed_loan_row = 1 THEN loan_key_v3 END)
        / NULLIF(COUNT(DISTINCT loan_key_v3), 0),
        2
    ) AS loans_close_rate_pct,
    COUNT(DISTINCT CASE
        WHEN flood_required_inferred_code = 'REQ' THEN loan_key_v3
    END) AS loans_flood_required,
    COUNT(DISTINCT CASE
        WHEN age_bucket = '1978 or older' THEN loan_key_v3
    END) AS worked_loans_1978_or_older,
    COUNT(DISTINCT CASE
        WHEN age_bucket = 'Newer than 1978' THEN loan_key_v3
    END) AS worked_loans_newer_than_1978
FROM insurance_tracking_sharepoint.vw_insurance_tracking_procurement_analysis;
-- Optional filters (example):
-- WHERE source = 'External'
--   AND state = 'CA'
--   AND status IN ('Pending', 'Approved')
--   AND policy_type = 'HO3'
--   AND month_label = 'Nov 2024';


-- ---------------------------------------------------------------------------
-- 2) LINE CHART — Worked Loans vs Closed Loans vs Worked Submissions by month
--    Power BI uses one MonthLabel axis; this query pivots both date roles onto dim_month.
-- ---------------------------------------------------------------------------
WITH base AS (
    SELECT *
    FROM insurance_tracking_sharepoint.vw_insurance_tracking_procurement_analysis
    -- WHERE source = 'External'  -- apply same filters as dashboard
)
SELECT
    dm.month_label,
    dm.year_month_sort,
    COUNT(DISTINCT CASE WHEN b.request_month_start = dm.month_start THEN b.loan_key_v3 END)
        AS worked_loans,
    COUNT(DISTINCT CASE
        WHEN b.close_month_start = dm.month_start AND b.is_closed_loan_row = 1
        THEN b.loan_key_v3
    END) AS closed_loans,
    COUNT(CASE WHEN b.request_month_start = dm.month_start THEN 1 END)
        AS worked_submissions
FROM insurance_tracking_sharepoint.dim_month dm
LEFT JOIN base b
    ON b.request_month_start = dm.month_start
    OR (b.close_month_start = dm.month_start AND b.is_closed_loan_row = 1)
GROUP BY dm.month_label, dm.year_month_sort
ORDER BY dm.year_month_sort;


-- ---------------------------------------------------------------------------
-- 3) LINE CHART — Avg Coverage (Dwelling) Worked vs Closed by month (loan grain)
-- ---------------------------------------------------------------------------
WITH loan_worked AS (
    SELECT
        loan_key_v3,
        request_month_start AS month_start,
        MAX(dwelling_amount) AS dwelling_amount
    FROM insurance_tracking_sharepoint.vw_insurance_tracking_procurement_analysis
    WHERE loan_key_v3 IS NOT NULL
      AND request_month_start IS NOT NULL
    GROUP BY loan_key_v3, request_month_start
),
loan_closed AS (
    SELECT
        loan_key_v3,
        close_month_start AS month_start,
        MAX(dwelling_amount) AS dwelling_amount
    FROM insurance_tracking_sharepoint.vw_insurance_tracking_procurement_analysis
    WHERE is_closed_loan_row = 1
      AND close_month_start IS NOT NULL
    GROUP BY loan_key_v3, close_month_start
)
SELECT
    dm.month_label,
    dm.year_month_sort,
    AVG(w.dwelling_amount) AS avg_coverage_worked,
    AVG(c.dwelling_amount) AS avg_coverage_closed
FROM insurance_tracking_sharepoint.dim_month dm
LEFT JOIN loan_worked w ON dm.month_start = w.month_start
LEFT JOIN loan_closed c ON dm.month_start = c.month_start
GROUP BY dm.month_label, dm.year_month_sort
ORDER BY dm.year_month_sort;


-- ---------------------------------------------------------------------------
-- 4) LINE CHART — Avg Premium Worked vs Closed by month (loan grain)
-- ---------------------------------------------------------------------------
WITH loan_worked AS (
    SELECT
        loan_key_v3,
        request_month_start AS month_start,
        MAX(premium) AS premium
    FROM insurance_tracking_sharepoint.vw_insurance_tracking_procurement_analysis
    WHERE loan_key_v3 IS NOT NULL
      AND request_month_start IS NOT NULL
    GROUP BY loan_key_v3, request_month_start
),
loan_closed AS (
    SELECT
        loan_key_v3,
        close_month_start AS month_start,
        MAX(premium) AS premium
    FROM insurance_tracking_sharepoint.vw_insurance_tracking_procurement_analysis
    WHERE is_closed_loan_row = 1
      AND close_month_start IS NOT NULL
    GROUP BY loan_key_v3, close_month_start
)
SELECT
    dm.month_label,
    dm.year_month_sort,
    AVG(w.premium) AS avg_premium_worked,
    AVG(c.premium) AS avg_premium_closed
FROM insurance_tracking_sharepoint.dim_month dm
LEFT JOIN loan_worked w ON dm.month_start = w.month_start
LEFT JOIN loan_closed c ON dm.month_start = c.month_start
GROUP BY dm.month_label, dm.year_month_sort
ORDER BY dm.year_month_sort;


-- ---------------------------------------------------------------------------
-- 5) DONUT — Worked Loans Flood Required (matches screenshot labels)
-- ---------------------------------------------------------------------------
SELECT
    flood_required_bucket,
    COUNT(DISTINCT loan_key_v3) AS worked_loans
FROM insurance_tracking_sharepoint.vw_insurance_tracking_procurement_analysis
GROUP BY flood_required_bucket
ORDER BY worked_loans DESC;


-- ---------------------------------------------------------------------------
-- 6) DONUT — Closed Loans by Source
-- ---------------------------------------------------------------------------
SELECT
    source,
    COUNT(DISTINCT CASE WHEN is_closed_loan_row = 1 THEN loan_key_v3 END) AS closed_loans
FROM insurance_tracking_sharepoint.vw_insurance_tracking_procurement_analysis
GROUP BY source
ORDER BY closed_loans DESC;


-- ---------------------------------------------------------------------------
-- 7) FILTER LISTS (for QuickSight filter controls)
-- ---------------------------------------------------------------------------
SELECT DISTINCT source FROM insurance_tracking_sharepoint.vw_insurance_tracking_procurement_analysis ORDER BY 1;
SELECT DISTINCT state FROM insurance_tracking_sharepoint.vw_insurance_tracking_procurement_analysis ORDER BY 1;
SELECT DISTINCT status FROM insurance_tracking_sharepoint.vw_insurance_tracking_procurement_analysis ORDER BY 1;
SELECT DISTINCT policy_type FROM insurance_tracking_sharepoint.vw_insurance_tracking_procurement_analysis ORDER BY 1;
SELECT DISTINCT month_label, request_year_month_sort
FROM insurance_tracking_sharepoint.vw_insurance_tracking_procurement_analysis
WHERE month_label IS NOT NULL
ORDER BY request_year_month_sort;
