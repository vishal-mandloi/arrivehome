# Hybrid Architecture: Views + CTAS Setup Guide

## Overview

This architecture separates:
- **Views**: Flexible, always-fresh queries (for ad-hoc analysis)
- **CTAS Tables**: Pre-computed tables (for fast dashboard queries)

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                                                                              │
│   S3 Staging Zone                                                           │
│   (dim_loan, dim_*, fact_*)                                                 │
│           │                                                                  │
│           ├──────────────────┬──────────────────────────────┐               │
│           │                  │                              │               │
│           ▼                  ▼                              ▼               │
│   ┌───────────────┐  ┌───────────────┐              ┌───────────────┐       │
│   │ Athena Views  │  │ Lambda        │              │ QuickSight    │       │
│   │ (vw_*)        │  │ CTAS Refresh  │              │ Dashboards    │       │
│   │               │  │               │              │               │       │
│   │ • Ad-hoc      │  │ Daily @ 5 AM  │              │ Uses CTAS     │       │
│   │ • Always fresh│  │               │              │ tables for    │       │
│   │ • Flexible    │  │               │              │ speed         │       │
│   └───────────────┘  └───────┬───────┘              └───────────────┘       │
│           │                  │                              ▲               │
│           │                  ▼                              │               │
│           │          ┌───────────────┐                      │               │
│           │          │ CTAS Tables   │──────────────────────┘               │
│           │          │ (ctas_*)      │                                      │
│           │          │               │                                      │
│           │          │ • Pre-computed│                                      │
│           │          │ • Fast queries│                                      │
│           │          │ • Refreshed   │                                      │
│           │          │   daily       │                                      │
│           │          └───────────────┘                                      │
│           │                                                                  │
│           ▼                                                                  │
│   ┌───────────────┐                                                         │
│   │ Analysts      │                                                         │
│   │ (Ad-hoc SQL)  │                                                         │
│   └───────────────┘                                                         │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## File Structure

```
athena_views/
├── vw_eep_monthly_closings.sql        ← View (ad-hoc queries)
├── vw_dpa_monthly_closings.sql        ← View (ad-hoc queries)
├── vw_sales_registrations.sql         ← View (ad-hoc queries)
├── ctas_eep_monthly_closings.sql      ← CTAS (dashboards)
├── ctas_dpa_monthly_closings.sql      ← CTAS (dashboards)
├── ctas_sales_registrations.sql       ← CTAS (dashboards)
└── config_ctas_tables.json            ← Configuration

lambda_refresh_ctas.py                  ← Lambda function
```

---

## Setup Steps

### Step 1: Create Athena Views

Run each view SQL in Athena Query Editor:

```sql
-- 1. EEP Monthly Closings View
-- Copy from: athena_views/vw_eep_monthly_closings.sql

-- 2. DPA Monthly Closings View  
-- Copy from: athena_views/vw_dpa_monthly_closings.sql

-- 3. Sales Registrations View
-- Copy from: athena_views/vw_sales_registrations.sql
```

### Step 2: Create Initial CTAS Tables

Run each CTAS SQL in Athena Query Editor:

```sql
-- 1. EEP CTAS
-- Copy from: athena_views/ctas_eep_monthly_closings.sql

-- 2. DPA CTAS
-- Copy from: athena_views/ctas_dpa_monthly_closings.sql

-- 3. Sales CTAS
-- Copy from: athena_views/ctas_sales_registrations.sql
```

### Step 3: Deploy Lambda Function

1. **Go to AWS Lambda Console** → Create function
2. **Function name:** `refresh-athena-ctas`
3. **Runtime:** Python 3.11
4. **Architecture:** x86_64
5. **Paste code from:** `lambda_refresh_ctas.py`

### Step 4: Configure Lambda IAM Role

Add this policy to the Lambda execution role:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "athena:StartQueryExecution",
                "athena:GetQueryExecution",
                "athena:GetQueryResults"
            ],
            "Resource": "*"
        },
        {
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:PutObject",
                "s3:DeleteObject",
                "s3:ListBucket"
            ],
            "Resource": [
                "arn:aws:s3:::arrivehome-bi-prod",
                "arn:aws:s3:::arrivehome-bi-prod/*"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "glue:GetTable",
                "glue:DeleteTable",
                "glue:CreateTable",
                "glue:GetDatabase"
            ],
            "Resource": "*"
        }
    ]
}
```

### Step 5: Set Lambda Timeout

- **Timeout:** 5 minutes (300 seconds)
- **Memory:** 256 MB

### Step 6: Create EventBridge Schedule

1. **Go to EventBridge** → Schedules → Create schedule
2. **Name:** `daily-ctas-refresh`
3. **Schedule:** Cron `0 5 * * ? *` (5:00 AM UTC daily)
4. **Target:** Lambda function `refresh-athena-ctas`

---

## How to Query

### Option 1: Views (Ad-hoc, Always Fresh)
```sql
-- Slower but always current data
SELECT * FROM arrive_home.vw_eep_monthly_closings
WHERE closing_year = 2026;
```

### Option 2: CTAS Tables (Fast, Refreshed Daily)
```sql
-- Faster, refreshed daily at 5 AM
SELECT * FROM arrive_home.ctas_eep_monthly_closings
WHERE closing_year = 2026;
```

### For QuickSight Dashboards
Use CTAS tables (`ctas_*`) for better performance.

---

## How to Add a New Report

### Step 1: Create View SQL File
```sql
-- athena_views/vw_new_report.sql
CREATE OR REPLACE VIEW arrive_home.vw_new_report AS
SELECT ...
FROM arrive_home.dim_loan
WHERE ...
GROUP BY ...;
```

### Step 2: Create CTAS SQL File
```sql
-- athena_views/ctas_new_report.sql
DROP TABLE IF EXISTS arrive_home.ctas_new_report;

CREATE TABLE arrive_home.ctas_new_report
WITH (
    format = 'PARQUET',
    external_location = 's3://arrivehome-bi-prod/athena-ctas/ctas_new_report/',
    parquet_compression = 'SNAPPY'
) AS
SELECT ...
FROM arrive_home.dim_loan
WHERE ...
GROUP BY ...;
```

### Step 3: Run SQL in Athena
Execute both SQL files in Athena Query Editor.

### Step 4: Update Lambda (for auto-refresh)

Add to `CTAS_TABLES` list in `lambda_refresh_ctas.py`:
```python
{
    "name": "ctas_new_report",
    "s3_location": f"{S3_CTAS_BASE}/ctas_new_report/",
    "description": "New Report Description"
}
```

Add to `CTAS_QUERIES` dict:
```python
"ctas_new_report": """
    CREATE TABLE {database}.ctas_new_report
    WITH (...)
    AS SELECT ...
"""
```

### Step 5: Deploy Updated Lambda
Update the Lambda function with new code.

---

## Daily Schedule

```
02:00 AM  →  Extract MongoDB → S3 Raw Zone
03:00 AM  →  Transform → S3 Staging Zone
04:00 AM  →  Crawler updates Data Catalog
05:00 AM  →  Lambda refreshes CTAS tables  ← NEW
06:00 AM  →  Data ready for dashboards!
```

---

## Comparison: Views vs CTAS

| Aspect | Views (vw_*) | CTAS Tables (ctas_*) |
|--------|--------------|---------------------|
| **Data Freshness** | Real-time | Daily refresh |
| **Query Speed** | Slower | Fast |
| **Use Case** | Ad-hoc analysis | Dashboards |
| **Storage Cost** | None | S3 storage |
| **Maintenance** | None | Lambda refresh |

---

## Troubleshooting

### CTAS refresh failed?
1. Check Lambda CloudWatch logs
2. Verify S3 permissions
3. Check Athena query errors

### View returns error?
1. Check if source tables exist
2. Verify column names match

### Data is stale?
1. Check Lambda schedule
2. Verify ETL jobs ran
3. Check CTAS `_refreshed_at` column

---

## Files Reference

| File | Purpose |
|------|---------|
| `athena_views/vw_*.sql` | View definitions (ad-hoc queries) |
| `athena_views/ctas_*.sql` | CTAS definitions (dashboard tables) |
| `athena_views/config_ctas_tables.json` | Configuration |
| `lambda_refresh_ctas.py` | Lambda to refresh CTAS daily |
| `HYBRID_VIEWS_CTAS_SETUP.md` | This guide |

---

*Last Updated: January 28, 2026*
