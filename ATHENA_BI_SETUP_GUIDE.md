# ArriveHome BI Setup Guide - Athena Architecture

## Overview

This guide covers the complete setup for MongoDB → S3 → Athena → QuickSight BI pipeline.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                            DAILY DATA PIPELINE                                       │
├─────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                      │
│  ┌─────────────┐    Job 1       ┌─────────────┐    Job 2      ┌─────────────────┐   │
│  │  MongoDB    │ ─────────────► │  S3 Raw     │ ────────────► │  S3 Staging     │   │
│  │  (Source)   │   Extract      │  Zone       │   Transform   │  Zone           │   │
│  │             │                │  (Parquet)  │               │  (Star Schema)  │   │
│  │  • loans    │                │             │               │                 │   │
│  │  • users    │                │  /raw-zone/ │               │  /staging-zone/ │   │
│  │  • corr.    │                │   • loans/  │               │   • dim_loan/   │   │
│  │  • cond.    │                │   • users/  │               │   • dim_user/   │   │
│  │  • invest.  │                │   • etc.    │               │   • fact_*/     │   │
│  └─────────────┘                └─────────────┘               └────────┬────────┘   │
│                                                                        │            │
│                                                                   Crawler           │
│                                                                        │            │
│                                                                        ▼            │
│  ┌─────────────┐    Job 3       ┌─────────────────┐           ┌───────────────┐    │
│  │  QuickSight │ ◄───────────── │  S3 Reports     │ ◄──────── │  Glue Data    │    │
│  │  Dashboards │   Query        │  (Pre-computed) │   Job 3   │  Catalog      │    │
│  └─────────────┘                │                 │           │  (Tables)     │    │
│         │                       │  /reports/      │           └───────────────┘    │
│         │                       │   • report_eep/ │                   │            │
│         │                       │   • report_dpa/ │                   │            │
│         │                       │   • report_sales│                   ▼            │
│         │                       └─────────────────┘           ┌───────────────┐    │
│         │                                                     │  Amazon       │    │
│         └────────────────────────────────────────────────────►│  Athena       │    │
│                              Query                            │  (SQL)        │    │
│                                                               └───────────────┘    │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

---

## S3 Bucket Structure

```
s3://arrivehome-bi-prod/
├── raw-zone/                    ← MongoDB extracted data
│   ├── loans/
│   ├── correspondents/
│   ├── users/
│   ├── conditions/
│   └── investors/
│
├── staging-zone/                ← Transformed star schema
│   ├── dimensions/
│   │   ├── dim_date/
│   │   ├── dim_loan/
│   │   ├── dim_borrower/
│   │   ├── dim_correspondent/
│   │   └── dim_product/
│   └── facts/
│       ├── fact_loan_status/
│       └── fact_loan_metrics/
│
├── reports/                     ← Pre-computed report tables
│   ├── report_eep_monthly_closings/
│   ├── report_dpa_monthly_closings/
│   ├── report_sales_registrations/
│   └── report_monthly_summary/
│
├── athena-results/              ← Athena query results
│
└── athena-ctas/                 ← CTAS table storage (if using)
```

---

## Jobs Overview

### Job 1: Extract All Collections
**File:** `glue_job_extract_all_collections.py`
**Schedule:** Daily 2:00 AM
**Parameters:**
- `--EXTRACTION_MODE`: `full` or `incremental`
- `--DAYS_BACK`: Days to look back for incremental (default: 1)

**Collections extracted:**
| Collection | MongoDB Collection | S3 Output |
|------------|-------------------|-----------|
| loans | loans | /raw-zone/loans/ |
| correspondents | correspondents | /raw-zone/correspondents/ |
| users | users | /raw-zone/users/ |
| conditions | conditions | /raw-zone/conditions/ |
| investors | investors | /raw-zone/investors/ |

### Job 2: Transform to Staging
**File:** `glue_job_transform_to_staging.py`
**Schedule:** Daily 3:00 AM (after Job 1)

**Creates:**
- Dimension tables (dim_*)
- Fact tables (fact_*)

### Job 3: Create Report Tables
**File:** `glue_job_create_report_tables.py`
**Schedule:** Daily 4:00 AM (after Job 2)

**Creates:**
- `report_eep_monthly_closings`
- `report_dpa_monthly_closings`
- `report_sales_registrations`
- `report_monthly_summary`

---

## Incremental vs Full Refresh

### Option 1: Full Refresh (Recommended for Now)
- Extracts ALL data every day
- Simpler, no data consistency issues
- Best for datasets < 1 million records
- Set `--EXTRACTION_MODE=full`

### Option 2: Incremental Updates
- Only extracts new/updated records
- Uses `updatedAt` or `createdAt` timestamp
- Requires merge logic in transform job
- Set `--EXTRACTION_MODE=incremental --DAYS_BACK=1`

**Recommendation:** Start with Full Refresh, switch to Incremental when data grows large.

---

## Daily Schedule (AWS EventBridge / Step Functions)

```
02:00 AM  →  Job 1: Extract All Collections (Full Refresh)
            │
            ├── Extract loans
            ├── Extract correspondents
            ├── Extract users
            ├── Extract conditions
            └── Extract investors
            
03:00 AM  →  Job 2: Transform to Staging
            │
            ├── Create dim_* tables
            └── Create fact_* tables
            
03:30 AM  →  Glue Crawler: Crawl staging-zone/
            │
            └── Update Glue Data Catalog
            
04:00 AM  →  Job 3: Create Report Tables
            │
            ├── report_eep_monthly_closings
            ├── report_dpa_monthly_closings
            ├── report_sales_registrations
            └── report_monthly_summary
            
04:30 AM  →  Glue Crawler: Crawl reports/
            │
            └── Update report tables in catalog
            
05:00 AM  →  Data Ready for Athena/QuickSight!
```

---

## Glue Crawler Setup

### Crawler 1: Staging Zone Crawler
- **Name:** `staging-zone-crawler`
- **Data source:** `s3://arrivehome-bi-prod/staging-zone/`
- **Database:** `arrive_home`
- **Schedule:** Daily after Transform job

### Crawler 2: Reports Crawler
- **Name:** `reports-crawler`
- **Data source:** `s3://arrivehome-bi-prod/reports/`
- **Database:** `arrive_home` (or `arrive_home_reports`)
- **Schedule:** Daily after Report job

---

## Your 3 Reports

### Report 1: EEP Monthly Closings
**Question:** "How many EEP closings in a month?"

```sql
SELECT 
    year_month,
    closing_count,
    total_dpa_amount,
    correspondent_count
FROM arrive_home.report_eep_monthly_closings
WHERE closing_year = 2026
ORDER BY closing_month;
```

### Report 2: DPA Monthly Closings (USF vs MWF)
**Question:** "How many DPA closings for that month. Who is buying - Village USF or MWF?"

```sql
SELECT 
    year_month,
    buyer,
    closing_count,
    total_dpa_amount,
    buyer_percentage
FROM arrive_home.report_dpa_monthly_closings
WHERE closing_year = 2026
ORDER BY closing_month, buyer;
```

**Pivot View:**
```sql
SELECT 
    year_month,
    SUM(CASE WHEN buyer = 'USF' THEN closing_count ELSE 0 END) AS usf_closings,
    SUM(CASE WHEN buyer = 'MWF' THEN closing_count ELSE 0 END) AS mwf_closings,
    SUM(closing_count) AS total_closings
FROM arrive_home.report_dpa_monthly_closings
WHERE closing_year = 2026
GROUP BY year_month
ORDER BY year_month;
```

### Report 3: Sales - Registrations by Product
**Question:** "Number of registrations for the month by product"

```sql
SELECT 
    year_month,
    product_type,
    registration_count,
    product_percentage
FROM arrive_home.report_sales_registrations
WHERE registration_year = 2026
ORDER BY registration_month, product_type;
```

**Pivot View:**
```sql
SELECT 
    year_month,
    SUM(CASE WHEN product_type = 'DPA' THEN registration_count ELSE 0 END) AS dpa,
    SUM(CASE WHEN product_type = 'EEP' THEN registration_count ELSE 0 END) AS eep,
    SUM(CASE WHEN product_type = 'White Label' THEN registration_count ELSE 0 END) AS white_label,
    SUM(registration_count) AS total
FROM arrive_home.report_sales_registrations
WHERE registration_year = 2026
GROUP BY year_month
ORDER BY year_month;
```

---

## Materialized Views Alternative

Since Athena doesn't support true materialized views, we use:

### Option A: Glue Job Creates Report Tables (Recommended)
- `glue_job_create_report_tables.py` runs daily
- Creates pre-computed Parquet tables in S3
- Crawler updates Athena catalog
- Fastest query performance

### Option B: Athena CTAS (Create Table As Select)
- Run CTAS queries in Athena
- Creates tables stored in S3
- Must DROP and recreate to refresh
- Good for ad-hoc analysis

### Option C: Athena Views
- Regular SQL views
- Computed at query time
- No storage, always fresh
- Slower for complex queries

---

## QuickSight Connection

1. **Add Data Source:**
   - Type: Amazon Athena
   - Workgroup: `primary`
   - Database: `arrive_home`

2. **Create Datasets:**
   - `report_eep_monthly_closings`
   - `report_dpa_monthly_closings`
   - `report_sales_registrations`

3. **Build Dashboards:**
   - EEP Closings Dashboard
   - DPA Buyer Analysis
   - Sales Registration Trends

---

## Cost Estimate (Monthly)

| Service | Usage | Cost |
|---------|-------|------|
| Glue Jobs | ~2 hours/day | $50-80 |
| Glue Crawlers | 3 crawlers daily | $5-10 |
| S3 Storage | ~50 GB | $12 |
| Athena Queries | ~100 GB scanned | $25-50 |
| QuickSight | 5 authors + 20 readers | $220 |
| **Total** | | **~$350/month** |

---

## Troubleshooting

### No data in Athena?
1. Check if Crawler ran successfully
2. Verify S3 paths have data
3. Check Glue Data Catalog tables

### Stale data in reports?
1. Check if daily jobs ran
2. Verify job logs for errors
3. Re-run report job manually

### Slow queries?
1. Use report tables instead of raw data
2. Add partition columns (year, month)
3. Use columnar format (Parquet)

---

## Files Reference

| File | Purpose |
|------|---------|
| `glue_job_extract_all_collections.py` | Extract 5 collections from MongoDB |
| `glue_job_transform_to_staging.py` | Transform to star schema |
| `glue_job_create_report_tables.py` | Create pre-computed reports |
| `athena_queries_and_views.sql` | SQL queries and views |
| `ATHENA_BI_SETUP_GUIDE.md` | This guide |

---

*Last Updated: January 28, 2026*
