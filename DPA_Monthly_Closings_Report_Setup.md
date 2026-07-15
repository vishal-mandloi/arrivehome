# DPA Monthly Closings Report - AWS Glue ETL Job Setup Guide

## Overview
This AWS Glue ETL job generates a monthly report showing:
- **How many DPA loans closed in a given month**
- **Who bought the loans: Village USF or MWF (Mountain West Finance)**

## Prerequisites

### 1. MongoDB Crawler Must Be Run First
- Ensure the MongoDB crawler has successfully created tables in Glue Data Catalog
- Database name: `arrivehome_raw`
- Table name: `mongo_loan` (or as configured in crawler)

### 2. IAM Permissions for Glue Job Role
The Glue job execution role needs:
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "glue:GetTable",
        "glue:GetDatabase",
        "glue:GetTables"
      ],
      "Resource": [
        "arn:aws:glue:*:*:catalog",
        "arn:aws:glue:*:*:database/arrivehome_raw",
        "arn:aws:glue:*:*:table/arrivehome_raw/*"
      ]
    },
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject"
      ],
      "Resource": [
        "arn:aws:s3:::your-bi-bucket/raw/*",
        "arn:aws:s3:::your-bi-bucket/staging/*",
        "arn:aws:s3:::your-bi-bucket/reports/*"
      ]
    },
    {
      "Effect": "Allow",
      "Action": [
        "logs:CreateLogGroup",
        "logs:CreateLogStream",
        "logs:PutLogEvents"
      ],
      "Resource": "arn:aws:logs:*:*:*"
    }
  ]
}
```

### 3. S3 Bucket Structure
Create the following S3 paths:
```
s3://your-bi-bucket/reports/dpa-monthly-closings/
  ├── aggregated/
  │   └── year=YYYY/month=MM/
  └── detailed/
      └── year=YYYY/month=MM/buyer=BUYER/
```

## Step-by-Step Setup

### Step 1: Create AWS Glue Job

1. **Navigate to AWS Glue Console**
   - Go to: AWS Glue → ETL jobs → Visual ETL (or Scripts)
   - Click "Create job"

2. **Job Configuration**
   - **Name**: `dpa-monthly-closings-report`
   - **IAM Role**: Select the role with permissions above
   - **Type**: Spark
   - **Glue version**: 4.0 (or latest)
   - **Language**: Python 3
   - **Script path**: Upload `glue_job_dpa_monthly_closings.py`

3. **Job Parameters** (in "Job details" tab)
   ```
   --JOB_NAME=dpa-monthly-closings-report
   --MONTH=01
   --YEAR=2026
   --OUTPUT_S3_PATH=s3://your-bi-bucket/reports/dpa-monthly-closings
   ```

   **Note**: For monthly scheduled runs, you can use:
   - `--MONTH` and `--YEAR`: Specific month/year
   - Omit both: Process all DPA closings (for historical reports)

### Step 2: Configure Job Settings

**Advanced Properties:**
- **Worker type**: G.1X (for small-medium data) or G.2X (for large datasets)
- **Number of workers**: Auto (or set based on data volume)
- **Job timeout**: 60 minutes (adjust based on data size)
- **Max retries**: 2
- **Max concurrency**: 1 (to avoid conflicts)

**Connections:**
- Add the MongoDB connection created earlier (if reading directly from MongoDB)
- Or rely on Glue Data Catalog (recommended)

### Step 3: Test Run

1. **Run the job manually** with test parameters:
   ```
   --MONTH=01
   --YEAR=2026
   --OUTPUT_S3_PATH=s3://your-bi-bucket/reports/dpa-monthly-closings
   ```

2. **Check CloudWatch Logs** for any errors

3. **Verify S3 Output**:
   - Check that Parquet files are created in the output path
   - Verify data structure matches expected schema

### Step 4: Schedule the Job

**Option A: Using EventBridge Scheduler (Recommended)**

1. **Create EventBridge Rule**:
   - Schedule: `cron(0 2 1 * ? *)` (Runs at 2 AM on 1st of each month)
   - Target: AWS Glue job
   - Job name: `dpa-monthly-closings-report`

2. **Job Parameters for Scheduled Run**:
   ```
   --JOB_NAME=dpa-monthly-closings-report
   --MONTH={aws.scheduler.month}
   --YEAR={aws.scheduler.year}
   --OUTPUT_S3_PATH=s3://your-bi-bucket/reports/dpa-monthly-closings
   ```

**Option B: Using Glue Workflow**

1. Create a Glue Workflow: `dpa-reporting-workflow`
2. Add trigger: Schedule (monthly on 1st at 2 AM)
3. Add job: `dpa-monthly-closings-report`
4. Configure job parameters dynamically using Python expressions

**Option C: Lambda Function (For Dynamic Parameters)**

Create a Lambda that:
- Calculates previous month/year
- Invokes Glue job with correct parameters
- Triggered by EventBridge monthly

## Output Schema

### Aggregated Report (monthly_closings_df)
| Column | Type | Description |
|--------|------|-------------|
| closing_year | INT | Year of closing |
| closing_month | INT | Month of closing (1-12) |
| closing_year_month | STRING | Format: "YYYY-MM" |
| buyer | STRING | "USF", "MWF", or "Unknown" |
| closing_count | BIGINT | Number of closings for this buyer/month |
| total_closings_month | BIGINT | Total closings for the month (all buyers) |
| buyer_percentage | DECIMAL(5,2) | Percentage of month's closings |
| first_closing_date | TIMESTAMP | Earliest closing in this group |
| last_closing_date | TIMESTAMP | Latest closing in this group |

### Detailed Report (detailed_df)
Loan-level data with all relevant fields for drill-down analysis.

## Querying Results

### Using AWS Athena

1. **Create Athena Table** (one-time setup):
```sql
CREATE EXTERNAL TABLE dpa_monthly_closings_report (
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
PARTITIONED BY (
  closing_year INT,
  closing_month INT
)
STORED AS PARQUET
LOCATION 's3://your-bi-bucket/reports/dpa-monthly-closings/aggregated/'
TBLPROPERTIES ('projection.enabled'='true',
               'projection.closing_year.type'='integer',
               'projection.closing_year.range'='2020,2030',
               'projection.closing_month.type'='integer',
               'projection.closing_month.range'='1,12');
```

2. **Query Examples**:
```sql
-- Get closings for specific month
SELECT 
  closing_year_month,
  buyer,
  closing_count,
  total_closings_month,
  buyer_percentage
FROM dpa_monthly_closings_report
WHERE closing_year = 2026 
  AND closing_month = 1
ORDER BY buyer;

-- Compare USF vs MWF over time
SELECT 
  closing_year_month,
  SUM(CASE WHEN buyer = 'USF' THEN closing_count ELSE 0 END) as usf_closings,
  SUM(CASE WHEN buyer = 'MWF' THEN closing_count ELSE 0 END) as mwf_closings,
  SUM(closing_count) as total_closings
FROM dpa_monthly_closings_report
WHERE closing_year >= 2025
GROUP BY closing_year_month
ORDER BY closing_year_month;

-- Year-over-year comparison
SELECT 
  closing_year,
  SUM(CASE WHEN buyer = 'USF' THEN closing_count ELSE 0 END) as usf_total,
  SUM(CASE WHEN buyer = 'MWF' THEN closing_count ELSE 0 END) as mwf_total,
  SUM(closing_count) as grand_total
FROM dpa_monthly_closings_report
GROUP BY closing_year
ORDER BY closing_year;
```

### Using Redshift (After Loading)

If you load the Parquet files into Redshift:
```sql
-- Create table
CREATE TABLE dpa_monthly_closings_report (
  closing_year INT,
  closing_month INT,
  closing_year_month VARCHAR(7),
  buyer VARCHAR(10),
  closing_count BIGINT,
  total_closings_month BIGINT,
  buyer_percentage DECIMAL(5,2),
  first_closing_date TIMESTAMP,
  last_closing_date TIMESTAMP
)
DISTSTYLE EVEN
SORTKEY (closing_year, closing_month, buyer);

-- Load from S3
COPY dpa_monthly_closings_report
FROM 's3://your-bi-bucket/reports/dpa-monthly-closings/aggregated/'
IAM_ROLE 'arn:aws:iam::ACCOUNT:role/RedshiftLoadRole'
FORMAT AS PARQUET;
```

## Buyer Determination Logic

The job determines buyer using this logic:

### USF Indicators:
- `firstMortgageOwnershipStatus` contains "USF" or "USF Securitized"
- `usfLoanNumber` field is populated

### MWF (Mountain West Finance) Indicators:
- `mountainWestContractDate` is populated
- `purchaseAdviceFirstPaymentToMountainWestDate` is populated
- `mountainWestGoLiveDate` is populated

### Priority:
- If both USF and MWF indicators exist, USF takes priority (adjust based on business rules)

## Troubleshooting

### Common Issues:

1. **No DPA loans found**
   - Check: `productType` field values in MongoDB (case sensitivity)
   - Verify: Crawler correctly cataloged the field

2. **All buyers show as "Unknown"**
   - Check: Field names match schema (camelCase vs lowercase)
   - Verify: MongoDB fields are populated
   - Review: Buyer determination logic in script

3. **Job fails with "Table not found"**
   - Verify: Crawler has run successfully
   - Check: Database and table names match
   - Ensure: IAM role has Glue catalog permissions

4. **S3 write permissions error**
   - Verify: IAM role has S3 write permissions
   - Check: S3 bucket policy allows Glue service role

## Best Practices

1. **Run crawler before job**: Always ensure MongoDB schema is up-to-date
2. **Partition by date**: Use year/month partitions for efficient querying
3. **Compress output**: Parquet with Snappy compression (already configured)
4. **Monitor costs**: Use appropriate worker types and auto-scaling
5. **Version control**: Store script in S3 or Git, reference in job
6. **Error handling**: Add try-catch blocks for production
7. **Data validation**: Add checks for null dates, invalid buyers
8. **Alerting**: Set up CloudWatch alarms for job failures

## Next Steps

1. **Create QuickSight Dashboard**:
   - Connect to Athena or Redshift
   - Build visualizations: bar charts, time series
   - Add filters for month/year, buyer

2. **Automate Reporting**:
   - Email reports via SNS/SES
   - Generate Excel/CSV exports
   - Schedule weekly/monthly summaries

3. **Enhance Job**:
   - Add more buyer categories if needed
   - Include loan amounts, correspondent breakdown
   - Add data quality checks

## Support

For issues or questions:
- Check CloudWatch Logs: `/aws-glue/jobs/output`
- Review Glue job metrics in console
- Validate MongoDB data directly
