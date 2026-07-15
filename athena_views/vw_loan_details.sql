-- ============================================================================
-- VIEW: Loan Details (All Loans - No Filter)
-- Report: Individual loan details for drill-down reports
-- ============================================================================
-- Usage: SELECT * FROM arrive_home.vw_loan_details WHERE product_type = 'DPA';
-- ============================================================================

CREATE OR REPLACE VIEW arrive_home.vw_loan_details AS
SELECT 
    -- Time dimensions
    YEAR(CAST(l.closing_date AS DATE)) AS closing_year,
    MONTH(CAST(l.closing_date AS DATE)) AS closing_month,
    DATE_FORMAT(CAST(l.closing_date AS DATE), '%Y-%m') AS year_month,
    
    -- Buyer / Ownership Status (actual value from data)
    s.first_mortgage_ownership_status AS buyer,
    
    -- Loan identifiers
    l.loan_id,
    l.ah_loan_number AS loan_number,
    l.lender_loan_number,
    l.usf_loan_number,
    
    -- Borrower info (primary borrower)
    b.first_name AS borrower_first_name,
    b.last_name AS borrower_last_name,
    CONCAT(COALESCE(b.first_name, ''), ' ', COALESCE(b.last_name, '')) AS borrower_name,
    
    -- Product info
    l.product_type,
    l.dpa_repayment_type AS dpa_program_type,
    l.workflow_type,
    l.first_mortgage_type,
    
    -- Financial amounts
    m.first_mortgage_total_amount AS loan_amount,
    m.first_mortgage_base_amount,
    m.dpa_amount,
    m.dpa_percent,
    m.purchase_price,
    m.appraised_value,
    
    -- Correspondent / Lender
    l.correspondent_id,
    c.correspondent_name AS lender_correspondent,
    
    -- Loan officer
    l.loan_officer_name,
    l.loan_officer_email,
    
    -- Property info
    l.property_address,
    l.property_city,
    l.property_state,
    l.property_zip,
    l.property_county,
    l.property_type,
    
    -- Key dates
    CAST(l.created_at AS DATE) AS application_received_date,
    CAST(s.registered_at AS DATE) AS registration_date,
    CAST(s.locked_at AS DATE) AS lock_date,
    CAST(s.clear_to_close_at AS DATE) AS eligibility_review_date,
    CAST(s.approved_for_purchase_at AS DATE) AS approval_date,
    CAST(l.closing_date AS DATE) AS closing_date,
    CAST(s.closed_at AS DATE) AS funding_date,
    CAST(s.purchased_at AS DATE) AS purchase_date,
    CAST(s.securitized_at AS DATE) AS recording_date,
    
    -- Status
    s.current_status AS loan_status,
    s.health_status,
    s.first_mortgage_ownership_status AS servicing_member

FROM arrive_home.dim_loan l
LEFT JOIN arrive_home.fact_loan_status s ON l.loan_id = s.loan_id
LEFT JOIN arrive_home.fact_loan_metrics m ON l.loan_id = m.loan_id
LEFT JOIN arrive_home.dim_correspondent c ON l.correspondent_id = c.correspondent_id
LEFT JOIN arrive_home.dim_borrower b ON l.loan_id = b.loan_id AND b.is_primary = true

ORDER BY l.closing_date DESC;
