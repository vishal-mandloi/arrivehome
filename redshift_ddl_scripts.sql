-- ============================================================================
-- REDSHIFT DDL SCRIPTS: ArriveHome Data Warehouse
-- ============================================================================
-- Purpose: Create dimension and fact tables for star schema
-- Run this in Redshift Query Editor or via psql
-- ============================================================================

-- Create schema for data warehouse
--CREATE SCHEMA IF NOT EXISTS dw;

-- ============================================================================
-- DIMENSION TABLES
-- ============================================================================

-- ----------------------------------------------------------------------------
-- dim_date: Date Dimension
-- ----------------------------------------------------------------------------
DROP TABLE IF EXISTS public.dim_date CASCADE;
CREATE TABLE public.dim_date (
    date_key        DATE            NOT NULL    SORTKEY,
    date_id         INTEGER         NOT NULL    DISTKEY,
    year            SMALLINT        NOT NULL,
    quarter         SMALLINT        NOT NULL,
    month           SMALLINT        NOT NULL,
    day             SMALLINT        NOT NULL,
    day_of_week     SMALLINT        NOT NULL,
    week_of_year    SMALLINT        NOT NULL,
    day_name        VARCHAR(10)     NOT NULL,
    month_name      VARCHAR(10)     NOT NULL,
    year_month      VARCHAR(7)      NOT NULL,
    is_weekend      BOOLEAN         NOT NULL,
    _etl_loaded_at  TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (date_id)
);

-- ----------------------------------------------------------------------------
-- dim_product: Product Dimension
-- ----------------------------------------------------------------------------
DROP TABLE IF EXISTS public.dim_product CASCADE;
CREATE TABLE public.dim_product (
    product_id          INTEGER         NOT NULL    DISTKEY SORTKEY,
    product_code        VARCHAR(50)     NOT NULL,
    product_name        VARCHAR(100)    NOT NULL,
    product_category    VARCHAR(50),
    _etl_loaded_at      TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (product_id)
);

-- ----------------------------------------------------------------------------
-- dim_correspondent: Correspondent/Lender Dimension
-- ----------------------------------------------------------------------------
DROP TABLE IF EXISTS public.dim_correspondent CASCADE;
CREATE TABLE public.dim_correspondent (
    correspondent_id    VARCHAR(50)     NOT NULL    DISTKEY SORTKEY,
    correspondent_name  VARCHAR(255),
    nmls_number         VARCHAR(20),
    address             VARCHAR(500),
    city                VARCHAR(100),
    state               VARCHAR(2),
    zip_code            VARCHAR(10),
    created_at          TIMESTAMP,
    _etl_loaded_at      TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (correspondent_id)
);

-- ----------------------------------------------------------------------------
-- dim_loan: Loan Dimension
-- ----------------------------------------------------------------------------
DROP TABLE IF EXISTS public.dim_loan CASCADE;
CREATE TABLE public.dim_loan (
    loan_id                 VARCHAR(50)     NOT NULL    DISTKEY SORTKEY,
    ah_loan_number          VARCHAR(50),
    lender_loan_number      VARCHAR(50),
    usf_loan_number         VARCHAR(50),
    bsi_loan_number         VARCHAR(50),
    essex_loan_number       VARCHAR(50),
    bluewater_id            INTEGER,
    
    -- Product info
    workflow_type           VARCHAR(50),
    product_type            VARCHAR(50),
    dpa_repayment_type      VARCHAR(50),
    first_mortgage_type     VARCHAR(10),
    
    -- Property info
    property_address        VARCHAR(500),
    property_city           VARCHAR(100),
    property_state          VARCHAR(2),
    property_zip            VARCHAR(10),
    property_county         VARCHAR(100),
    property_type           VARCHAR(50),
    number_of_units         SMALLINT,
    
    -- Loan officer info
    loan_officer_name       VARCHAR(255),
    loan_officer_email      VARCHAR(255),
    loan_officer_nmls       VARCHAR(20),
    
    -- Foreign keys
    correspondent_id        VARCHAR(50),
    created_by_user_id      VARCHAR(50),
    
    -- Dates
    created_at              TIMESTAMP,
    closing_date            DATE,
    closed_at               TIMESTAMP,
    
    _etl_loaded_at          TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (loan_id)
);

-- ----------------------------------------------------------------------------
-- dim_borrower: Borrower Dimension
-- ----------------------------------------------------------------------------
DROP TABLE IF EXISTS public.dim_borrower CASCADE;
CREATE TABLE public.dim_borrower (
    borrower_id         BIGINT          NOT NULL    DISTKEY SORTKEY,
    loan_id             VARCHAR(50)     NOT NULL,
    borrower_position   SMALLINT,
    first_name          VARCHAR(100),
    middle_name         VARCHAR(100),
    last_name           VARCHAR(100),
    suffix              VARCHAR(20),
    email               VARCHAR(255),
    primary_phone       VARCHAR(20),
    home_phone          VARCHAR(20),
    work_phone          VARCHAR(20),
    ssn_masked          VARCHAR(15),
    date_of_birth       DATE,
    credit_score        SMALLINT,
    dti                 DECIMAL(10,4),
    monthly_income      DECIMAL(18,2),
    marital_status      VARCHAR(50),
    ethnicity           VARCHAR(100),
    race                VARCHAR(100),
    sex                 VARCHAR(20),
    occupancy_type      VARCHAR(50),
    current_address     VARCHAR(500),
    current_city        VARCHAR(100),
    current_state       VARCHAR(2),
    current_zip         VARCHAR(10),
    employer_name       VARCHAR(255),
    years_worked        DECIMAL(5,2),
    borrower_type       VARCHAR(50),
    is_primary          BOOLEAN,
    _etl_loaded_at      TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (borrower_id)
);

-- ----------------------------------------------------------------------------
-- dim_user: User Dimension (optional - add if needed)
-- ----------------------------------------------------------------------------
DROP TABLE IF EXISTS public.dim_user CASCADE;
CREATE TABLE public.dim_user (
    user_id             VARCHAR(50)     NOT NULL    DISTKEY SORTKEY,
    user_type           VARCHAR(50),
    first_name          VARCHAR(100),
    last_name           VARCHAR(100),
    email               VARCHAR(255),
    correspondent_id    VARCHAR(50),
    internal_roles      VARCHAR(500),
    correspondent_roles VARCHAR(500),
    _etl_loaded_at      TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (user_id)
);

-- ============================================================================
-- FACT TABLES
-- ============================================================================

-- ----------------------------------------------------------------------------
-- fact_loan_status: Loan Status Tracking Fact
-- ----------------------------------------------------------------------------
DROP TABLE IF EXISTS public.fact_loan_status CASCADE;
CREATE TABLE public.fact_loan_status (
    loan_id                         VARCHAR(50)     NOT NULL    DISTKEY,
    correspondent_id                VARCHAR(50),
    
    -- Status
    current_status                  VARCHAR(50),
    health_status                   VARCHAR(20),
    health_reason                   VARCHAR(100),
    first_mortgage_ownership_status VARCHAR(50),
    
    -- Date keys (for joining with dim_date)
    closing_date_key                INTEGER         SORTKEY,
    created_date_key                INTEGER,
    purchased_date_key              INTEGER,
    
    -- Status timestamps
    registered_at                   TIMESTAMP,
    locked_at                       TIMESTAMP,
    closed_at                       TIMESTAMP,
    purchased_at                    TIMESTAMP,
    securitized_at                  TIMESTAMP,
    cancelled_at                    TIMESTAMP,
    denied_at                       TIMESTAMP,
    
    -- Processing timestamps
    clear_to_close_at               TIMESTAMP,
    approved_for_purchase_at        TIMESTAMP,
    all_conditions_cleared_at       TIMESTAMP,
    
    _etl_loaded_at                  TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (loan_id)
);

-- ----------------------------------------------------------------------------
-- fact_loan_metrics: Loan Financial Metrics Fact
-- ----------------------------------------------------------------------------
DROP TABLE IF EXISTS public.fact_loan_metrics CASCADE;
CREATE TABLE public.fact_loan_metrics (
    loan_id                         VARCHAR(50)     NOT NULL    DISTKEY,
    correspondent_id                VARCHAR(50),
    product_type                    VARCHAR(50),
    workflow_type                   VARCHAR(50),
    
    -- Date key
    closing_date_key                INTEGER         SORTKEY,
    
    -- First Mortgage metrics
    first_mortgage_base_amount      DECIMAL(18,2),
    first_mortgage_total_amount     DECIMAL(18,2),
    first_mortgage_interest_rate    DECIMAL(10,6),
    first_mortgage_ltv              DECIMAL(10,6),
    first_mortgage_term_months      SMALLINT,
    
    -- Second Mortgage / DPA metrics
    dpa_amount                      DECIMAL(18,2),
    dpa_percent                     DECIMAL(10,6),
    second_mortgage_upb             DECIMAL(18,2),
    second_mortgage_interest_rate   DECIMAL(10,6),
    second_mortgage_term_months     SMALLINT,
    
    -- Property values
    purchase_price                  DECIMAL(18,2),
    appraised_value                 DECIMAL(18,2),
    combined_ltv                    DECIMAL(10,6),
    
    -- Borrower metrics
    total_income                    DECIMAL(18,2),
    total_assets                    DECIMAL(18,2),
    total_liabilities               DECIMAL(18,2),
    backend_dti                     DECIMAL(10,6),
    frontend_dti                    DECIMAL(10,6),
    credit_score                    SMALLINT,
    
    -- Payment metrics
    monthly_payment                 DECIMAL(18,2),
    escrow_payment                  DECIMAL(18,2),
    pmi_payment                     DECIMAL(18,2),
    
    -- Pricing
    base_price                      DECIMAL(10,6),
    total_price                     DECIMAL(10,6),
    
    _etl_loaded_at                  TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (loan_id)
);

-- ----------------------------------------------------------------------------
-- fact_conditions: Loan Conditions Fact (optional)
-- ----------------------------------------------------------------------------
DROP TABLE IF EXISTS public.fact_conditions CASCADE;
CREATE TABLE public.fact_conditions (
    condition_id            BIGINT          NOT NULL    DISTKEY SORTKEY,
    loan_id                 VARCHAR(50)     NOT NULL,
    condition_name          VARCHAR(255),
    condition_type          VARCHAR(50),
    condition_status        VARCHAR(50),
    assigned_to_user_id     VARCHAR(50),
    created_at              TIMESTAMP,
    cleared_at              TIMESTAMP,
    due_date                DATE,
    days_to_clear           INTEGER,
    _etl_loaded_at          TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (condition_id)
);

-- ----------------------------------------------------------------------------
-- fact_reconciliation: Reconciliation Fact (optional)
-- ----------------------------------------------------------------------------
DROP TABLE IF EXISTS public.fact_reconciliation CASCADE;
CREATE TABLE public.fact_reconciliation (
    reconciliation_id       BIGINT          NOT NULL    DISTKEY SORTKEY,
    loan_id                 VARCHAR(50)     NOT NULL,
    correspondent_id        VARCHAR(50),
    reconciliation_status   VARCHAR(50),
    reconciliation_date     DATE,
    invoice_amount          DECIMAL(18,2),
    payment_amount          DECIMAL(18,2),
    variance_amount         DECIMAL(18,2),
    wire_received_at        TIMESTAMP,
    wire_sent_at            TIMESTAMP,
    _etl_loaded_at          TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (reconciliation_id)
);

-- ============================================================================
-- MATERIALIZED VIEWS (Pre-computed reports for dashboards)
-- ============================================================================

-- ----------------------------------------------------------------------------
-- mv_corr_dashboard: Correspondent Dashboard Metrics
-- ----------------------------------------------------------------------------
DROP MATERIALIZED VIEW IF EXISTS public.mv_corr_dashboard;
CREATE MATERIALIZED VIEW public.mv_corr_dashboard AS
SELECT 
    c.correspondent_id,
    c.correspondent_name,
    d.year,
    d.month,
    d.year_month,
    COUNT(DISTINCT l.loan_id) as total_loans,
    COUNT(DISTINCT CASE WHEN s.current_status = 'Purchased' THEN l.loan_id END) as purchased_loans,
    COUNT(DISTINCT CASE WHEN s.current_status = 'Cancelled' THEN l.loan_id END) as cancelled_loans,
    SUM(m.first_mortgage_total_amount) as total_loan_volume,
    AVG(m.first_mortgage_interest_rate) as avg_interest_rate,
    AVG(m.credit_score) as avg_credit_score,
    SUM(m.dpa_amount) as total_dpa_amount
FROM public.dim_loan l
LEFT JOIN public.dim_correspondent c ON l.correspondent_id = c.correspondent_id
LEFT JOIN public.fact_loan_status s ON l.loan_id = s.loan_id
LEFT JOIN public.fact_loan_metrics m ON l.loan_id = m.loan_id
LEFT JOIN public.dim_date d ON s.closing_date_key = d.date_id
WHERE d.date_key IS NOT NULL
GROUP BY 1, 2, 3, 4, 5;

-- ----------------------------------------------------------------------------
-- mv_monthly_metrics: Monthly KPI Metrics
-- ----------------------------------------------------------------------------
DROP MATERIALIZED VIEW IF EXISTS public.mv_monthly_metrics;
CREATE MATERIALIZED VIEW public.mv_monthly_metrics AS
SELECT 
    d.year,
    d.month,
    d.year_month,
    l.product_type,
    l.workflow_type,
    COUNT(DISTINCT l.loan_id) as loan_count,
    SUM(m.first_mortgage_total_amount) as total_volume,
    AVG(m.first_mortgage_total_amount) as avg_loan_amount,
    SUM(m.dpa_amount) as total_dpa_volume,
    AVG(m.dpa_amount) as avg_dpa_amount,
    AVG(m.credit_score) as avg_credit_score,
    AVG(m.backend_dti) as avg_dti,
    AVG(m.combined_ltv) as avg_ltv
FROM public.dim_loan l
LEFT JOIN public.fact_loan_metrics m ON l.loan_id = m.loan_id
LEFT JOIN public.dim_date d ON m.closing_date_key = d.date_id
WHERE d.date_key IS NOT NULL
GROUP BY 1, 2, 3, 4, 5;

-- ----------------------------------------------------------------------------
-- mv_document_sla: Document SLA Tracking (placeholder)
-- ----------------------------------------------------------------------------
-- Add when document tracking data is available

-- ============================================================================
-- INDEXES for better query performance
-- ============================================================================

-- Indexes on dimension tables
CREATE INDEX idx_dim_loan_correspondent ON public.dim_loan(correspondent_id);
CREATE INDEX idx_dim_loan_product_type ON public.dim_loan(product_type);
CREATE INDEX idx_dim_loan_closing_date ON public.dim_loan(closing_date);
CREATE INDEX idx_dim_borrower_loan ON public.dim_borrower(loan_id);

-- Indexes on fact tables
CREATE INDEX idx_fact_status_correspondent ON public.fact_loan_status(correspondent_id);
CREATE INDEX idx_fact_status_status ON public.fact_loan_status(current_status);
CREATE INDEX idx_fact_metrics_correspondent ON public.fact_loan_metrics(correspondent_id);
CREATE INDEX idx_fact_metrics_product ON public.fact_loan_metrics(product_type);

-- ============================================================================
-- GRANT PERMISSIONS (adjust roles as needed)
-- ============================================================================
-- GRANT USAGE ON SCHEMA dw TO bi_users;
-- GRANT SELECT ON ALL TABLES IN SCHEMA dw TO bi_users;
-- GRANT SELECT ON ALL TABLES IN SCHEMA dw TO redshift_data_api;

-- ============================================================================
-- SAMPLE QUERIES
-- ============================================================================

/*
-- Monthly loan volume by product type
SELECT 
    year_month,
    product_type,
    loan_count,
    total_volume
FROM public.mv_monthly_metrics
ORDER BY year_month DESC, product_type;

-- Correspondent performance dashboard
SELECT 
    correspondent_name,
    year_month,
    total_loans,
    purchased_loans,
    total_loan_volume,
    avg_credit_score
FROM public.mv_corr_dashboard
WHERE year = 2026
ORDER BY total_loan_volume DESC;

-- DPA analysis by month
SELECT 
    d.year_month,
    COUNT(*) as dpa_loans,
    SUM(m.dpa_amount) as total_dpa,
    AVG(m.dpa_percent) as avg_dpa_percent
FROM public.fact_loan_metrics m
JOIN public.dim_date d ON m.closing_date_key = d.date_id
WHERE m.product_type = 'DPA'
GROUP BY 1
ORDER BY 1 DESC;
*/

-- ============================================================================
-- END OF DDL SCRIPTS
-- ============================================================================
