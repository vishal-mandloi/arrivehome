# DPA Monthly Closings Report - Complete Pipeline Guide

## Architecture Overview

Following the Arrive Home BI ETL Pipeline architecture:

```
┌─────────────────┐     ┌─────────────────────────┐     ┌─────────────────┐
│    MongoDB      │     │    AWS Glue ETL Jobs    │     │   S3 Storage    │
│  (Production)   │     │                         │     │                 │
├─────────────────┤     ├─────────────────────────┤     ├─────────────────┤
│                 │     │                         │     │                 │
│  loans          │────►│  Job 0: FLATTEN &       │────►│  raw-zone/      │
│  collection     │     │         EXTRACT         │     │  loans/         │
│                 │     │                         │     │                 │
└─────────────────┘     └───────────┬─────────────┘     └────────┬────────┘
                                    │                            │
                                    │                            │
                        ┌───────────▼─────────────┐              │
                        │                         │              │
                        │  Job 1: DPA REPORT      │◄─────────────┘
                        │  (Transform/Aggregate)  │
                        │                         │
                        └───────────┬─────────────┘
                                    │
                                    ▼
                        ┌─────────────────────────┐
                        │  S3 Staging/Reports     │
                        │  reports/dpa-monthly-   │
                        │  closings/              │
                        └─────────────────────────┘
```

---

## Step-by-Step Setup

### Prerequisites

1. **MongoDB Connection**: `ArriveHome` (already created)
2. **Glue Data Catalog**: 
   - Database: `arrive_home`
   - Table: `arrivehome_loans`
3. **S3 Bucket**: `arrivehome-bi-prod`

---

## Job 0: FLATTEN & EXTRACT

**Purpose**: Extract loan data from MongoDB to S3 Raw Zone (Parquet)

### Create Job 0

1. **Upload script** to S3:
   ```
   s3://arrivehome-bi-prod/glue-scripts/glue_job_0_extract_mongodb_to_s3.py
   ```

2. **Create Glue Job**:
   - Name: `job-0-extract-mongodb-to-s3`
   - Script path: `s3://arrivehome-bi-prod/glue-scripts/glue_job_0_extract_mongodb_to_s3.py`
   - **Connection**: `ArriveHome` (REQUIRED!)

3. **Job Parameters**:
   | Key | Value |
   |-----|-------|
   | `--JOB_NAME` | `job-0-extract-mongodb-to-s3` |
   | `--OUTPUT_S3_PATH` | `s3://arrivehome-bi-prod/raw-zone/loans/` |

4. **Worker Configuration**:
   - Worker type: `G.1X`
   - Number of workers: `2`
   - Job timeout: `60` minutes

5. **Schedule**: Daily at 2:00 AM (recommended)

### Run Job 0

1. Click **Run**
2. Wait for completion (~5-10 minutes)
3. Verify output in S3: `s3://arrivehome-bi-prod/raw-zone/loans/`

---

## Job 1: DPA MONTHLY CLOSINGS REPORT

**Purpose**: Transform and aggregate DPA closings by month and buyer

### Create Job 1

1. **Upload script** to S3:
   ```
   s3://arrivehome-bi-prod/glue-scripts/glue_job_dpa_monthly_closings.py
   ```

2. **Create Glue Job**:
   - Name: `job-1-dpa-monthly-closings-report`
   - Script path: `s3://arrivehome-bi-prod/glue-scripts/glue_job_dpa_monthly_closings.py`
   - **Connection**: None needed (reads from S3)

3. **Job Parameters**:
   | Key | Value |
   |-----|-------|
   | `--JOB_NAME` | `job-1-dpa-monthly-closings-report` |
   | `--INPUT_S3_PATH` | `s3://arrivehome-bi-prod/raw-zone/loans/` |
   | `--OUTPUT_S3_PATH` | `s3://arrivehome-bi-prod/reports/dpa-monthly-closings` |
   | `--MONTH` | `01` (or specific month) |
   | `--YEAR` | `2026` (or specific year) |

4. **Worker Configuration**:
   - Worker type: `G.1X`
   - Number of workers: `2`
   - Job timeout: `30` minutes

### Run Job 1

1. Ensure **Job 0 has completed** first
2. Click **Run**
3. Wait for completion (~2-5 minutes)
4. Verify output in S3

---

## Output Structure

After both jobs complete:

```
s3://arrivehome-bi-prod/
├── raw-zone/
│   └── loans/
│       └── part-00000-xxx.snappy.parquet    ← Job 0 output
│
└── reports/
    └── dpa-monthly-closings/
        ├── year=2026/month=01/
        │   └── part-00000-xxx.snappy.parquet    ← Aggregated report
        └── detailed/year=2026/month=01/
            └── buyer=USF/
            └── buyer=MWF/                        ← Loan-level detail
```

---

## Report Output Schema

### Aggregated Report

| Column | Type | Description |
|--------|------|-------------|
| closing_year | INT | Year of closing |
| closing_month | INT | Month of closing |
| closing_year_month | STRING | YYYY-MM format |
| buyer | STRING | USF, MWF, or Unknown |
| closing_count | BIGINT | Number of closings |
| total_closings_month | BIGINT | Total closings for month |
| buyer_percentage | DECIMAL | Percentage by buyer |

### Sample Output

| closing_year_month | buyer | closing_count | total_closings_month | buyer_percentage |
|-------------------|-------|---------------|---------------------|-----------------|
| 2026-01 | USF | 45 | 78 | 57.69 |
| 2026-01 | MWF | 33 | 78 | 42.31 |

---

## Querying Results with Athena

After running the jobs, you can query results with Athena:

```sql
-- Create table for aggregated report
CREATE EXTERNAL TABLE dpa_monthly_report (
  closing_year INT,
  closing_month INT,
  closing_year_month STRING,
  buyer STRING,
  closing_count BIGINT,
  total_closings_month BIGINT,
  buyer_percentage DECIMAL(5,2),
  first_closing_date TIMESTAMP,
  last_closing_date TIMESTAMP
)
STORED AS PARQUET
LOCATION 's3://arrivehome-bi-prod/reports/dpa-monthly-closings/';

-- Query the report
SELECT 
  closing_year_month,
  buyer,
  closing_count,
  buyer_percentage
FROM dpa_monthly_report
WHERE closing_year = 2026
ORDER BY closing_year_month, buyer;
```

---

## Scheduling (Optional)

### Using EventBridge Scheduler

1. Create schedule for **Job 0**: Daily at 2:00 AM
2. Create schedule for **Job 1**: Daily at 3:00 AM (after Job 0)

### Using Glue Workflow

1. Create workflow: `dpa-report-workflow`
2. Add trigger: Schedule (daily)
3. Add Job 0 as first job
4. Add Job 1 as dependent on Job 0 success

---

## Troubleshooting

### Job 0 Fails: "Entity Not Found"
- Verify connection `ArriveHome` is added to job
- Check database name: `arrive_home`
- Check table name: `arrivehome_loans`

### Job 1 Fails: "Path does not exist"
- Run Job 0 first to populate raw zone
- Verify `INPUT_S3_PATH` matches Job 0 output

### No DPA Loans Found
- Check `productType` field values in MongoDB
- May need to adjust filter logic for case sensitivity

### All Buyers Show "Unknown"
- Check field names match schema (camelCase vs lowercase)
- Verify MongoDB has populated buyer indicator fields

---

## Cost Estimate

| Component | Estimated Cost |
|-----------|---------------|
| Job 0 (G.1X, 2 workers, 10 min) | ~$0.15/run |
| Job 1 (G.1X, 2 workers, 5 min) | ~$0.08/run |
| S3 Storage (1GB) | ~$0.023/month |
| **Daily Total** | ~$0.23/day |
| **Monthly Total** | ~$7/month |

---

## Support

For issues:
1. Check CloudWatch Logs: `/aws-glue/jobs/output`
2. Review job metrics in Glue console
3. Verify S3 paths and permissions
