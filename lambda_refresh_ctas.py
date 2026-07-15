"""
AWS Lambda Function: Refresh Athena CTAS Tables
Purpose: Daily refresh of pre-computed report tables for fast dashboard queries
Trigger: EventBridge Schedule (daily at 5:00 AM after ETL completes)

Reports refreshed:
1. ctas_eep_monthly_closings
2. ctas_dpa_monthly_closings
3. ctas_sales_registrations

IAM Permissions Required:
- athena:StartQueryExecution
- athena:GetQueryExecution
- athena:GetQueryResults
- s3:GetObject, s3:PutObject, s3:DeleteObject on arrivehome-bi-prod bucket
- glue:GetTable, glue:DeleteTable, glue:CreateTable
"""

import boto3
import time
import json
from datetime import datetime

# Configuration
ATHENA_DATABASE = "arrive_home"
ATHENA_WORKGROUP = "primary"
ATHENA_OUTPUT_LOCATION = "s3://arrivehome-bi-prod/athena-results/"
S3_CTAS_BASE = "s3://arrivehome-bi-prod/athena-ctas"

# CTAS table definitions
# Each entry: { "table_name": ..., "s3_location": ..., "query": ... }
CTAS_TABLES = [
    {
        "name": "ctas_eep_monthly_closings",
        "s3_location": f"{S3_CTAS_BASE}/ctas_eep_monthly_closings/",
        "description": "EEP Monthly Closings Report"
    },
    {
        "name": "ctas_dpa_monthly_closings",
        "s3_location": f"{S3_CTAS_BASE}/ctas_dpa_monthly_closings/",
        "description": "DPA Monthly Closings with Buyer (USF/MWF)"
    },
    {
        "name": "ctas_sales_registrations",
        "s3_location": f"{S3_CTAS_BASE}/ctas_sales_registrations/",
        "description": "Sales Registrations by Product"
    }
]

# CTAS Queries - Using CAST for date/timestamp columns stored as strings
CTAS_QUERIES = {
    "ctas_eep_monthly_closings": """
        CREATE TABLE {database}.ctas_eep_monthly_closings
        WITH (
            format = 'PARQUET',
            external_location = '{s3_location}',
            parquet_compression = 'SNAPPY'
        ) AS
        SELECT 
            YEAR(CAST(l.closing_date AS DATE)) AS closing_year,
            MONTH(CAST(l.closing_date AS DATE)) AS closing_month,
            DATE_FORMAT(CAST(l.closing_date AS DATE), '%Y-%m') AS year_month,
            COUNT(*) AS closing_count,
            SUM(COALESCE(m.dpa_amount, 0)) AS total_dpa_amount,
            AVG(COALESCE(m.dpa_amount, 0)) AS avg_dpa_amount,
            COUNT(DISTINCT l.correspondent_id) AS correspondent_count,
            MIN(CAST(l.closing_date AS DATE)) AS first_closing,
            MAX(CAST(l.closing_date AS DATE)) AS last_closing,
            CURRENT_TIMESTAMP AS _refreshed_at
        FROM {database}.dim_loan l
        LEFT JOIN {database}.fact_loan_metrics m ON l.loan_id = m.loan_id
        WHERE UPPER(COALESCE(l.product_type, '')) = 'EEP'
          AND l.closing_date IS NOT NULL
        GROUP BY 
            YEAR(CAST(l.closing_date AS DATE)),
            MONTH(CAST(l.closing_date AS DATE)),
            DATE_FORMAT(CAST(l.closing_date AS DATE), '%Y-%m')
    """,
    
    "ctas_dpa_monthly_closings": """
        CREATE TABLE {database}.ctas_dpa_monthly_closings
        WITH (
            format = 'PARQUET',
            external_location = '{s3_location}',
            parquet_compression = 'SNAPPY'
        ) AS
        SELECT 
            YEAR(CAST(l.closing_date AS DATE)) AS closing_year,
            MONTH(CAST(l.closing_date AS DATE)) AS closing_month,
            DATE_FORMAT(CAST(l.closing_date AS DATE), '%Y-%m') AS year_month,
            CASE 
                WHEN UPPER(COALESCE(s.first_mortgage_ownership_status, '')) LIKE '%USF%' THEN 'USF'
                WHEN l.usf_loan_number IS NOT NULL AND TRIM(l.usf_loan_number) != '' THEN 'USF'
                ELSE 'MWF'
            END AS buyer,
            COUNT(*) AS closing_count,
            SUM(COALESCE(m.dpa_amount, 0)) AS total_dpa_amount,
            AVG(COALESCE(m.dpa_amount, 0)) AS avg_dpa_amount,
            COUNT(DISTINCT l.correspondent_id) AS correspondent_count,
            CURRENT_TIMESTAMP AS _refreshed_at
        FROM {database}.dim_loan l
        LEFT JOIN {database}.fact_loan_status s ON l.loan_id = s.loan_id
        LEFT JOIN {database}.fact_loan_metrics m ON l.loan_id = m.loan_id
        WHERE UPPER(COALESCE(l.product_type, '')) = 'DPA'
          AND l.closing_date IS NOT NULL
        GROUP BY 
            YEAR(CAST(l.closing_date AS DATE)),
            MONTH(CAST(l.closing_date AS DATE)),
            DATE_FORMAT(CAST(l.closing_date AS DATE), '%Y-%m'),
            CASE 
                WHEN UPPER(COALESCE(s.first_mortgage_ownership_status, '')) LIKE '%USF%' THEN 'USF'
                WHEN l.usf_loan_number IS NOT NULL AND TRIM(l.usf_loan_number) != '' THEN 'USF'
                ELSE 'MWF'
            END
    """,
    
    "ctas_sales_registrations": """
        CREATE TABLE {database}.ctas_sales_registrations
        WITH (
            format = 'PARQUET',
            external_location = '{s3_location}',
            parquet_compression = 'SNAPPY'
        ) AS
        SELECT 
            YEAR(CAST(s.registered_at AS TIMESTAMP)) AS registration_year,
            MONTH(CAST(s.registered_at AS TIMESTAMP)) AS registration_month,
            DATE_FORMAT(CAST(s.registered_at AS TIMESTAMP), '%Y-%m') AS year_month,
            COALESCE(l.product_type, 'Unknown') AS product_type,
            COUNT(*) AS registration_count,
            COUNT(DISTINCT l.correspondent_id) AS correspondent_count,
            MIN(CAST(s.registered_at AS TIMESTAMP)) AS first_registration,
            MAX(CAST(s.registered_at AS TIMESTAMP)) AS last_registration,
            CURRENT_TIMESTAMP AS _refreshed_at
        FROM {database}.fact_loan_status s
        JOIN {database}.dim_loan l ON s.loan_id = l.loan_id
        WHERE s.registered_at IS NOT NULL
        GROUP BY 
            YEAR(CAST(s.registered_at AS TIMESTAMP)),
            MONTH(CAST(s.registered_at AS TIMESTAMP)),
            DATE_FORMAT(CAST(s.registered_at AS TIMESTAMP), '%Y-%m'),
            COALESCE(l.product_type, 'Unknown')
    """
}

# Initialize clients
athena_client = boto3.client('athena')
s3_client = boto3.client('s3')


def run_athena_query(query, description=""):
    """Execute Athena query and wait for completion."""
    print(f"Executing: {description}")
    print(f"Query: {query[:100]}...")
    
    response = athena_client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': ATHENA_DATABASE},
        WorkGroup=ATHENA_WORKGROUP,
        ResultConfiguration={'OutputLocation': ATHENA_OUTPUT_LOCATION}
    )
    
    query_execution_id = response['QueryExecutionId']
    print(f"Query ID: {query_execution_id}")
    
    # Wait for query to complete
    max_attempts = 60  # 5 minutes max
    attempt = 0
    
    while attempt < max_attempts:
        response = athena_client.get_query_execution(
            QueryExecutionId=query_execution_id
        )
        state = response['QueryExecution']['Status']['State']
        
        if state == 'SUCCEEDED':
            print(f"Query succeeded!")
            return {"status": "success", "query_id": query_execution_id}
        elif state in ['FAILED', 'CANCELLED']:
            reason = response['QueryExecution']['Status'].get('StateChangeReason', 'Unknown')
            print(f"Query {state}: {reason}")
            return {"status": "failed", "error": reason, "query_id": query_execution_id}
        
        time.sleep(5)
        attempt += 1
    
    return {"status": "timeout", "query_id": query_execution_id}


def delete_s3_prefix(bucket, prefix):
    """Delete all objects under an S3 prefix."""
    print(f"Deleting S3 objects: s3://{bucket}/{prefix}")
    
    try:
        paginator = s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
        
        delete_keys = []
        for page in pages:
            if 'Contents' in page:
                for obj in page['Contents']:
                    delete_keys.append({'Key': obj['Key']})
        
        if delete_keys:
            # Delete in batches of 1000
            for i in range(0, len(delete_keys), 1000):
                batch = delete_keys[i:i+1000]
                s3_client.delete_objects(
                    Bucket=bucket,
                    Delete={'Objects': batch}
                )
            print(f"Deleted {len(delete_keys)} objects")
        else:
            print("No objects to delete")
            
        return True
    except Exception as e:
        print(f"Error deleting S3 objects: {e}")
        return False


def refresh_ctas_table(table_config):
    """Refresh a single CTAS table: DROP → DELETE S3 → CREATE."""
    table_name = table_config['name']
    s3_location = table_config['s3_location']
    description = table_config['description']
    
    print(f"\n{'='*60}")
    print(f"Refreshing: {table_name}")
    print(f"Description: {description}")
    print(f"{'='*60}")
    
    results = {"table": table_name, "steps": []}
    
    # Step 1: DROP existing table
    drop_query = f"DROP TABLE IF EXISTS {ATHENA_DATABASE}.{table_name}"
    drop_result = run_athena_query(drop_query, f"DROP {table_name}")
    results["steps"].append({"step": "drop", "result": drop_result})
    
    if drop_result["status"] != "success":
        print(f"Warning: DROP failed, continuing anyway...")
    
    # Step 2: Delete S3 data
    # Parse bucket and prefix from s3_location
    s3_parts = s3_location.replace("s3://", "").rstrip("/")
    bucket = s3_parts.split("/")[0]
    prefix = "/".join(s3_parts.split("/")[1:]) + "/"
    
    delete_result = delete_s3_prefix(bucket, prefix)
    results["steps"].append({"step": "delete_s3", "result": delete_result})
    
    # Step 3: CREATE new table
    create_query = CTAS_QUERIES[table_name].format(
        database=ATHENA_DATABASE,
        s3_location=s3_location
    )
    create_result = run_athena_query(create_query, f"CREATE {table_name}")
    results["steps"].append({"step": "create", "result": create_result})
    
    if create_result["status"] == "success":
        results["status"] = "success"
        print(f"✓ {table_name} refreshed successfully!")
    else:
        results["status"] = "failed"
        print(f"✗ {table_name} refresh failed!")
    
    return results


def lambda_handler(event, context):
    """
    Main Lambda handler.
    Triggered by EventBridge schedule or manual invocation.
    """
    print("=" * 80)
    print("CTAS TABLE REFRESH - Starting")
    print(f"Time: {datetime.now().isoformat()}")
    print("=" * 80)
    
    # Check if specific table requested (for manual runs)
    specific_table = event.get('table_name') if event else None
    
    if specific_table:
        print(f"Refreshing specific table: {specific_table}")
        tables_to_refresh = [t for t in CTAS_TABLES if t['name'] == specific_table]
    else:
        print(f"Refreshing all {len(CTAS_TABLES)} tables")
        tables_to_refresh = CTAS_TABLES
    
    # Refresh each table
    all_results = []
    success_count = 0
    
    for table_config in tables_to_refresh:
        result = refresh_ctas_table(table_config)
        all_results.append(result)
        if result["status"] == "success":
            success_count += 1
    
    # Summary
    print("\n" + "=" * 80)
    print("CTAS TABLE REFRESH - Complete")
    print("=" * 80)
    print(f"Tables refreshed: {success_count}/{len(tables_to_refresh)}")
    
    for result in all_results:
        status_icon = "✓" if result["status"] == "success" else "✗"
        print(f"  {status_icon} {result['table']}: {result['status']}")
    
    response = {
        "statusCode": 200 if success_count == len(tables_to_refresh) else 500,
        "body": {
            "message": f"Refreshed {success_count}/{len(tables_to_refresh)} tables",
            "timestamp": datetime.now().isoformat(),
            "results": all_results
        }
    }
    
    print(f"\nResponse: {json.dumps(response, default=str)}")
    return response


# For local testing
if __name__ == "__main__":
    # Simulate Lambda invocation
    test_event = {}  # Empty event = refresh all tables
    # test_event = {"table_name": "ctas_eep_monthly_closings"}  # Specific table
    
    result = lambda_handler(test_event, None)
    print(json.dumps(result, indent=2, default=str))
