-- ============================================================================
-- One-row helper for QuickSight parameter dynamic default = current month
-- Database: insurance_tracking_sharepoint
-- ============================================================================
-- QuickSight → Datasets → New Athena dataset → this view
-- Then: Parameters → Dynamic default → map to current_month_label
-- ============================================================================

CREATE OR REPLACE VIEW insurance_tracking_sharepoint.vw_current_month_default AS
SELECT
    date_format(current_date, '%b %Y') AS current_month_label,
    year(current_date) * 100 + month(current_date) AS year_month_sort,
    current_date AS as_of_date;
