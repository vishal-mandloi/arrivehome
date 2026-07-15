-- ============================================================================
-- ATHENA QUERIES AND VIEWS FOR ARRIVEHOME BI REPORTS
-- ============================================================================
-- Note: Athena doesn't support materialized views, but you can:
-- 1. Create regular VIEWS (computed at query time)
-- 2. Create tables using CTAS (pre-computed, stored in S3)
-- 3. Use Glue jobs to create report tables (recommended)
-- ============================================================================

-- ============================================================================
-- PART 1: CREATE DATABASE (if not exists)
-- ============================================================================
CREATE DATABASE IF NOT EXISTS arrive_home_reports;

-- ============================================================================
-- PART 2: CREATE VIEWS FOR REPORTS
-- ============================================================================

-- ----------------------------------------------------------------------------
-- VIEW 1: EEP Monthly Closings
-- How many EEP closings in a month
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW arrive_home.vw_eep_monthly_closings AS
SELECT 
    YEAR(closingdate) AS closing_year,
    MONTH(closingdate) AS closing_month,
    DATE_FORMAT(closingdate, '%Y-%m') AS year_month,
    COUNT(*) AS closing_count,
    SUM(COALESCE(dpaamount, 0)) AS total_dpa_amount,
    COUNT(DISTINCT correspondent) AS correspondent_count
FROM arrive_home.dim_loan
WHERE UPPER(producttype) = 'EEP'
  AND closingdate IS NOT NULL
GROUP BY 
    YEAR(closingdate),
    MONTH(closingdate),
    DATE_FORMAT(closingdate, '%Y-%m')
ORDER BY closing_year DESC, closing_month DESC;

-- ----------------------------------------------------------------------------
-- VIEW 2: DPA Monthly Closings with Buyer (USF vs MWF)
-- How many DPA closings for the month, who is buying (USF or MWF)
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW arrive_home.vw_dpa_monthly_closings AS
SELECT 
    YEAR(closingdate) AS closing_year,
    MONTH(closingdate) AS closing_month,
    DATE_FORMAT(closingdate, '%Y-%m') AS year_month,
    CASE 
        WHEN UPPER(firstmortgageownershipstatus) LIKE '%USF%' THEN 'USF'
        WHEN usfloannumber IS NOT NULL AND usfloannumber != '' THEN 'USF'
        WHEN mountainwestcontractdate IS NOT NULL THEN 'MWF'
        ELSE 'Unknown'
    END AS buyer,
    COUNT(*) AS closing_count,
    SUM(COALESCE(dpaamount, 0)) AS total_dpa_amount,
    AVG(COALESCE(dpaamount, 0)) AS avg_dpa_amount
FROM arrive_home.dim_loan
WHERE UPPER(producttype) = 'DPA'
  AND closingdate IS NOT NULL
GROUP BY 
    YEAR(closingdate),
    MONTH(closingdate),
    DATE_FORMAT(closingdate, '%Y-%m'),
    CASE 
        WHEN UPPER(firstmortgageownershipstatus) LIKE '%USF%' THEN 'USF'
        WHEN usfloannumber IS NOT NULL AND usfloannumber != '' THEN 'USF'
        WHEN mountainwestcontractdate IS NOT NULL THEN 'MWF'
        ELSE 'Unknown'
    END
ORDER BY closing_year DESC, closing_month DESC, buyer;

-- ----------------------------------------------------------------------------
-- VIEW 3: Sales - Registrations by Month and Product
-- Number of registrations for the month by product
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW arrive_home.vw_sales_registrations AS
SELECT 
    YEAR(registeredat) AS registration_year,
    MONTH(registeredat) AS registration_month,
    DATE_FORMAT(registeredat, '%Y-%m') AS year_month,
    COALESCE(producttype, 'Unknown') AS product_type,
    COUNT(*) AS registration_count,
    COUNT(DISTINCT correspondent) AS correspondent_count
FROM arrive_home.dim_loan
WHERE registeredat IS NOT NULL
GROUP BY 
    YEAR(registeredat),
    MONTH(registeredat),
    DATE_FORMAT(registeredat, '%Y-%m'),
    COALESCE(producttype, 'Unknown')
ORDER BY registration_year DESC, registration_month DESC, product_type;

-- ============================================================================
-- PART 3: SAMPLE QUERIES FOR YOUR 3 REPORTS
-- ============================================================================

-- ----------------------------------------------------------------------------
-- REPORT 1: EEP Monthly Closings
-- "How many EEP closings in a month"
-- ----------------------------------------------------------------------------
-- Using the view:
SELECT * FROM arrive_home.vw_eep_monthly_closings
WHERE closing_year = 2026
ORDER BY closing_month;

-- Or direct query:
SELECT 
    DATE_FORMAT(closingdate, '%Y-%m') AS month,
    COUNT(*) AS eep_closings,
    SUM(dpaamount) AS total_dpa_amount
FROM arrive_home.dim_loan
WHERE producttype = 'EEP'
  AND closingdate IS NOT NULL
  AND YEAR(closingdate) = 2026
GROUP BY DATE_FORMAT(closingdate, '%Y-%m')
ORDER BY month;

-- ----------------------------------------------------------------------------
-- REPORT 2: DPA Monthly Closings by Buyer (USF vs MWF)
-- "How many DPA closings for that month. Who is buying - Village USF or MWF"
-- ----------------------------------------------------------------------------
-- Using the view:
SELECT * FROM arrive_home.vw_dpa_monthly_closings
WHERE closing_year = 2026
ORDER BY closing_month, buyer;

-- Pivot format (USF and MWF as columns):
SELECT 
    year_month,
    SUM(CASE WHEN buyer = 'USF' THEN closing_count ELSE 0 END) AS usf_closings,
    SUM(CASE WHEN buyer = 'MWF' THEN closing_count ELSE 0 END) AS mwf_closings,
    SUM(CASE WHEN buyer = 'Unknown' THEN closing_count ELSE 0 END) AS unknown_closings,
    SUM(closing_count) AS total_closings
FROM arrive_home.vw_dpa_monthly_closings
WHERE closing_year = 2026
GROUP BY year_month
ORDER BY year_month;

-- ----------------------------------------------------------------------------
-- REPORT 3: Sales - Registrations by Product
-- "Number of registrations for the month by product"
-- ----------------------------------------------------------------------------
-- Using the view:
SELECT * FROM arrive_home.vw_sales_registrations
WHERE registration_year = 2026
ORDER BY registration_month, product_type;

-- Pivot format (products as columns):
SELECT 
    year_month,
    SUM(CASE WHEN product_type = 'DPA' THEN registration_count ELSE 0 END) AS dpa_registrations,
    SUM(CASE WHEN product_type = 'EEP' THEN registration_count ELSE 0 END) AS eep_registrations,
    SUM(CASE WHEN product_type = 'White Label' THEN registration_count ELSE 0 END) AS whitelabel_registrations,
    SUM(CASE WHEN product_type = 'Solar Program' THEN registration_count ELSE 0 END) AS solar_registrations,
    SUM(registration_count) AS total_registrations
FROM arrive_home.vw_sales_registrations
WHERE registration_year = 2026
GROUP BY year_month
ORDER BY year_month;

-- ============================================================================
-- PART 4: CTAS - CREATE PRE-COMPUTED TABLES (Alternative to Materialized Views)
-- ============================================================================
-- These create actual tables stored in S3 (faster queries, but static data)
-- Re-run these queries daily to refresh the data

-- ----------------------------------------------------------------------------
-- CTAS: EEP Report Table
-- ----------------------------------------------------------------------------
CREATE TABLE arrive_home_reports.report_eep_closings
WITH (
    format = 'PARQUET',
    external_location = 's3://arrivehome-bi-prod/athena-ctas/report_eep_closings/',
    parquet_compression = 'SNAPPY'
) AS
SELECT 
    YEAR(closingdate) AS closing_year,
    MONTH(closingdate) AS closing_month,
    DATE_FORMAT(closingdate, '%Y-%m') AS year_month,
    COUNT(*) AS closing_count,
    SUM(COALESCE(dpaamount, 0)) AS total_dpa_amount,
    COUNT(DISTINCT correspondent) AS correspondent_count,
    CURRENT_TIMESTAMP AS report_generated_at
FROM arrive_home.dim_loan
WHERE UPPER(producttype) = 'EEP'
  AND closingdate IS NOT NULL
GROUP BY 
    YEAR(closingdate),
    MONTH(closingdate),
    DATE_FORMAT(closingdate, '%Y-%m');

-- ----------------------------------------------------------------------------
-- CTAS: DPA Report Table
-- ----------------------------------------------------------------------------
CREATE TABLE arrive_home_reports.report_dpa_closings
WITH (
    format = 'PARQUET',
    external_location = 's3://arrivehome-bi-prod/athena-ctas/report_dpa_closings/',
    parquet_compression = 'SNAPPY'
) AS
SELECT 
    YEAR(closingdate) AS closing_year,
    MONTH(closingdate) AS closing_month,
    DATE_FORMAT(closingdate, '%Y-%m') AS year_month,
    CASE 
        WHEN UPPER(firstmortgageownershipstatus) LIKE '%USF%' THEN 'USF'
        WHEN usfloannumber IS NOT NULL AND usfloannumber != '' THEN 'USF'
        WHEN mountainwestcontractdate IS NOT NULL THEN 'MWF'
        ELSE 'Unknown'
    END AS buyer,
    COUNT(*) AS closing_count,
    SUM(COALESCE(dpaamount, 0)) AS total_dpa_amount,
    CURRENT_TIMESTAMP AS report_generated_at
FROM arrive_home.dim_loan
WHERE UPPER(producttype) = 'DPA'
  AND closingdate IS NOT NULL
GROUP BY 
    YEAR(closingdate),
    MONTH(closingdate),
    DATE_FORMAT(closingdate, '%Y-%m'),
    CASE 
        WHEN UPPER(firstmortgageownershipstatus) LIKE '%USF%' THEN 'USF'
        WHEN usfloannumber IS NOT NULL AND usfloannumber != '' THEN 'USF'
        WHEN mountainwestcontractdate IS NOT NULL THEN 'MWF'
        ELSE 'Unknown'
    END;

-- ----------------------------------------------------------------------------
-- CTAS: Sales Registrations Table
-- ----------------------------------------------------------------------------
CREATE TABLE arrive_home_reports.report_sales_registrations
WITH (
    format = 'PARQUET',
    external_location = 's3://arrivehome-bi-prod/athena-ctas/report_sales_registrations/',
    parquet_compression = 'SNAPPY'
) AS
SELECT 
    YEAR(registeredat) AS registration_year,
    MONTH(registeredat) AS registration_month,
    DATE_FORMAT(registeredat, '%Y-%m') AS year_month,
    COALESCE(producttype, 'Unknown') AS product_type,
    COUNT(*) AS registration_count,
    COUNT(DISTINCT correspondent) AS correspondent_count,
    CURRENT_TIMESTAMP AS report_generated_at
FROM arrive_home.dim_loan
WHERE registeredat IS NOT NULL
GROUP BY 
    YEAR(registeredat),
    MONTH(registeredat),
    DATE_FORMAT(registeredat, '%Y-%m'),
    COALESCE(producttype, 'Unknown');

-- ============================================================================
-- PART 5: REFRESH CTAS TABLES (Run daily)
-- ============================================================================
-- To refresh CTAS tables, you need to:
-- 1. DROP the old table
-- 2. DELETE the S3 data
-- 3. Re-run the CTAS query

-- Example for EEP report:
DROP TABLE IF EXISTS arrive_home_reports.report_eep_closings;
-- Then run the CTAS query above again

-- ============================================================================
-- PART 6: DASHBOARD QUERIES (for QuickSight)
-- ============================================================================

-- Current Month Overview
SELECT 
    producttype AS product,
    COUNT(*) AS loans,
    SUM(CASE WHEN closingdate IS NOT NULL THEN 1 ELSE 0 END) AS closings,
    SUM(CASE WHEN registeredat IS NOT NULL THEN 1 ELSE 0 END) AS registrations
FROM arrive_home.dim_loan
WHERE (
    (closingdate >= DATE_ADD('month', -1, CURRENT_DATE)) OR
    (registeredat >= DATE_ADD('month', -1, CURRENT_DATE))
)
GROUP BY producttype;

-- Year-to-Date Summary
SELECT 
    producttype,
    DATE_FORMAT(closingdate, '%Y-%m') AS month,
    COUNT(*) AS closings,
    SUM(dpaamount) AS dpa_volume
FROM arrive_home.dim_loan
WHERE closingdate >= DATE_TRUNC('year', CURRENT_DATE)
  AND closingdate IS NOT NULL
GROUP BY producttype, DATE_FORMAT(closingdate, '%Y-%m')
ORDER BY month, producttype;

-- ============================================================================
-- END OF QUERIES
-- ============================================================================
