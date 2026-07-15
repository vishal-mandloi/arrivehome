-- ============================================================================
-- VIEW: Insurance Tracking — Procurement Analysis (QuickSight dataset grain)
-- Database: insurance_tracking_sharepoint (change if your Glue DB differs)
-- Grain: one row per submission (fact_procurement row / policy request)
-- ============================================================================
-- Month labels are derived INLINE (same as DimMonth DAX FORMAT([MonthStart], "MMM yyyy")).
-- They do NOT depend on joining dim_month — that join often fails when
-- request_month_start is NULL, or when date vs timestamp types don't match.
-- ============================================================================
-- month_label (slicer): COALESCE(request month, close month)
--   Power BI DimMonth is the UNION of both; on the fact row we surface request first.
-- ============================================================================

CREATE OR REPLACE VIEW insurance_tracking_sharepoint.vw_insurance_tracking_procurement_analysis AS
SELECT
    -- Keys
    f.loan_key,
    f.loan_key_v2,
    f.loan_key_v3,
    f.loan_number_norm,
    f.policy_number_norm,
    f.arrive_home_loan_number_norm,

    -- Source / status (filter fields)
    CASE
        WHEN UPPER(COALESCE(f.source_type, '')) LIKE '%INTERNAL%'
          OR UPPER(COALESCE(f.source_type, '')) LIKE '%UNIFICATION%'
        THEN 'Internal Procured'
        ELSE 'External'
    END AS source,
    f.source_type AS source_type_raw,
    f.state,
    COALESCE(NULLIF(TRIM(f.status_normalized_v2), ''), 'Blank') AS status,
    f.status_normalized,
    f.status_normalized_v2,
    COALESCE(NULLIF(TRIM(f.policy_type_raw), ''), 'Blank') AS policy_type,
    f.policy_type_raw,

    -- Flood / age
    f.flood_required_flag,
    f.flood_required_code,
    f.flood_required_inferred_code,
    CASE
        WHEN f.flood_required_inferred_code = 'REQ' THEN 'Flood Required'
        WHEN f.flood_required_inferred_code = 'NOT_REQ' THEN 'No Flood Required'
        ELSE 'Unknown'
    END AS flood_required_bucket,
    f.age_bucket,
    f.year_built,
    f.property_condition,

    -- Measures
    f.dwelling_amount,
    f.premium,
    f.closed_flag,
    f.days_to_close,

    -- Dates (normalize to DATE so QuickSight + joins behave)
    CAST(f.request_date AS DATE) AS request_date,
    CAST(f.close_bind_date AS DATE) AS close_bind_date,
    CAST(f.request_month_start AS DATE) AS request_month_start,
    CAST(f.close_month_start AS DATE) AS close_month_start,

    -- Request month attributes (DimMonth FORMAT logic; null -> 'Blank' for filter)
    COALESCE(
        CASE
            WHEN f.request_month_start IS NOT NULL
            THEN date_format(CAST(f.request_month_start AS DATE), '%b %Y')
            ELSE NULL
        END,
        'Blank'
    ) AS request_month_label,
    CASE
        WHEN f.request_month_start IS NOT NULL
        THEN year(CAST(f.request_month_start AS DATE)) * 100
             + month(CAST(f.request_month_start AS DATE))
        ELSE 0
    END AS request_year_month_sort,

    -- Close month attributes
    COALESCE(
        CASE
            WHEN f.close_month_start IS NOT NULL
            THEN date_format(CAST(f.close_month_start AS DATE), '%b %Y')
            ELSE NULL
        END,
        'Blank'
    ) AS close_month_label,
    CASE
        WHEN f.close_month_start IS NOT NULL
        THEN year(CAST(f.close_month_start AS DATE)) * 100
             + month(CAST(f.close_month_start AS DATE))
        ELSE 0
    END AS close_year_month_sort,

    -- Month filter helper: request month first, else close month, else 'Blank'
    -- (matches Power BI Month slicer showing (Blank) for null MonthLabel)
    COALESCE(
        CASE
            WHEN f.request_month_start IS NOT NULL
            THEN date_format(CAST(f.request_month_start AS DATE), '%b %Y')
            ELSE NULL
        END,
        CASE
            WHEN f.close_month_start IS NOT NULL
            THEN date_format(CAST(f.close_month_start AS DATE), '%b %Y')
            ELSE NULL
        END,
        'Blank'
    ) AS month_label,
    COALESCE(
        CASE
            WHEN f.request_month_start IS NOT NULL
            THEN year(CAST(f.request_month_start AS DATE)) * 100
                 + month(CAST(f.request_month_start AS DATE))
            ELSE NULL
        END,
        CASE
            WHEN f.close_month_start IS NOT NULL
            THEN year(CAST(f.close_month_start AS DATE)) * 100
                 + month(CAST(f.close_month_start AS DATE))
            ELSE NULL
        END,
        0
    ) AS year_month_sort,

    -- Current calendar month helpers (QuickSight default filter = current month)
    date_format(current_date, '%b %Y') AS current_month_label,
    CASE
        WHEN COALESCE(
            CASE
                WHEN f.request_month_start IS NOT NULL
                THEN date_format(CAST(f.request_month_start AS DATE), '%b %Y')
                ELSE NULL
            END,
            CASE
                WHEN f.close_month_start IS NOT NULL
                THEN date_format(CAST(f.close_month_start AS DATE), '%b %Y')
                ELSE NULL
            END,
            'Blank'
        ) = date_format(current_date, '%b %Y')
        THEN 1 ELSE 0
    END AS is_current_month,

    -- Flags for QuickSight calculated fields
    1 AS is_worked_submission,
    CASE WHEN f.closed_flag = 1 THEN 1 ELSE 0 END AS is_closed_submission,
    CASE WHEN f.loan_key_v3 IS NOT NULL AND TRIM(f.loan_key_v3) <> '' THEN 1 ELSE 0 END AS is_worked_loan_row,
    CASE
        WHEN f.closed_flag = 1
         AND f.loan_key_v3 IS NOT NULL
         AND TRIM(f.loan_key_v3) <> ''
        THEN 1 ELSE 0
    END AS is_closed_loan_row,

    -- Lender / location
    f.lender_name,
    f.lender_canonical,
    f.lender_group,
    f.address,
    f.city,
    f.zip,
    f.county,
    f.borrower_name,

    -- ETL metadata
    f.dt,
    f._etl_loaded_at

FROM insurance_tracking_sharepoint.fact_procurement f
WHERE f.loan_key IS NOT NULL
  AND TRIM(f.loan_key) <> '';
