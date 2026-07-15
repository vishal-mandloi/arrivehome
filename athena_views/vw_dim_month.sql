-- ============================================================================
-- VIEW: dim_month (Athena) — mirrors Power BI DimMonth DAX
-- ============================================================================
-- DimMonth =
--   DISTINCT(UNION(RequestMonthStart, CloseMonthStart))
--   + MonthLabel = FORMAT(MonthStart, "MMM yyyy")
--   + Year / MonthNum / YearMonthSort
-- ============================================================================
-- Use as a QuickSight dimension (or keep embedded labels on the fact view).
-- ============================================================================

CREATE OR REPLACE VIEW insurance_tracking_sharepoint.vw_dim_month AS
SELECT DISTINCT
    CAST(month_start AS DATE) AS month_start,
    date_format(CAST(month_start AS DATE), '%b %Y') AS month_label,
    year(CAST(month_start AS DATE)) AS year,
    month(CAST(month_start AS DATE)) AS month_num,
    year(CAST(month_start AS DATE)) * 100 + month(CAST(month_start AS DATE)) AS year_month_sort
FROM (
    SELECT request_month_start AS month_start
    FROM insurance_tracking_sharepoint.fact_procurement
    WHERE request_month_start IS NOT NULL

    UNION

    SELECT close_month_start AS month_start
    FROM insurance_tracking_sharepoint.fact_procurement
    WHERE close_month_start IS NOT NULL
) m
WHERE month_start IS NOT NULL

UNION ALL

-- Power BI Month slicer shows (Blank); include so QuickSight filter can select it
SELECT
    CAST(NULL AS DATE) AS month_start,
    'Blank' AS month_label,
    CAST(NULL AS INTEGER) AS year,
    CAST(NULL AS INTEGER) AS month_num,
    0 AS year_month_sort;
