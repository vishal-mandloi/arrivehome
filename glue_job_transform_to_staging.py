"""
AWS Glue ETL Job: Transform Raw Zone → Staging Zone (Star Schema)
Purpose: Transform raw data into dimension and fact tables for Athena
Input: s3://arrivehome-bi-prod/raw-zone/{collection}/
Output: s3://arrivehome-bi-prod/staging-zone/

Architecture:
    S3 Raw Zone (Parquet) → [This Job] → S3 Staging Zone (Star Schema)
                                              ↓
                                         Athena Views

Star Schema Tables Created:
    Dimensions: 
        - dim_date (generated date dimension 2020-2030)
        - dim_product (loan product types)
        - dim_correspondent (correspondent/lender info)
        - dim_investor (second mortgage / DPA investors)
        - dim_loan (loan details - main dimension)
        - dim_borrower (borrower information)
        - dim_eep_processing_entry (EEP processing status history from loans)
        - dim_loancondition (loan conditions)
        - dim_loanconditionevent (condition events/history)
        - dim_loanexception (loan underwriting exceptions)
        - dim_loandocument (loan documents metadata)
        - dim_holiday (company holidays)
    Facts: 
        - fact_loan_status (loan status snapshots)
        - fact_loan_metrics (financial metrics)
"""

import sys
import boto3
from awsglue.transforms import *
from awsglue.dynamicframe import DynamicFrame
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window
from datetime import datetime, date

# Get job parameters
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Initialize Glue context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Spark 3.x raises WRITE_ANCIENT_DATETIME for dates before 1582-10-15.
# Use CORRECTED mode to allow writing while applying Proleptic Gregorian calendar.
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
spark.conf.set("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")

# ============================================================================
# CONFIGURATION
# ============================================================================
# Raw zone inputs (produced by glue_job_extract_all_collections.py)
S3_LOANS_RAW_PATH = "s3://arrivehome-bi-prod/raw-zone/loans/"
S3_CORRESPONDENTS_RAW_PATH = "s3://arrivehome-bi-prod/raw-zone/correspondents/"
S3_USERS_RAW_PATH = "s3://arrivehome-bi-prod/raw-zone/users/"
S3_INVESTORS_RAW_PATH = "s3://arrivehome-bi-prod/raw-zone/investors/"

# Staging zone output
S3_STAGING_PATH = "s3://arrivehome-bi-prod/staging-zone/"
GLUE_DATABASE = "arrive_home"

print("=" * 80)
print("TRANSFORM Raw Zone → Staging Zone (Star Schema)")
print("=" * 80)
print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"Input (loans): {S3_LOANS_RAW_PATH}")
print(f"Input (correspondents): {S3_CORRESPONDENTS_RAW_PATH}")
print(f"Input (users): {S3_USERS_RAW_PATH}")
print(f"Input (investors): {S3_INVESTORS_RAW_PATH}")
print(f"Output: {S3_STAGING_PATH}")

# ============================================================================
# STEP 1: Read from S3 Raw Zone (Loans + Correspondents)
# ============================================================================
print("\n" + "-" * 40)
print("STEP 1: Reading from S3 Raw Zone (Loans)...")
print("-" * 40)

raw_df = spark.read.option("mergeSchema", "true").parquet(S3_LOANS_RAW_PATH)
loan_rows_read = raw_df.count()
print(f"Total loan rows in raw Parquet (all files): {loan_rows_read:,}")
print(f"Loan columns: {len(raw_df.columns)}")
print(
    "NOTE: This count is S3 raw-zone loans only, not live MongoDB. "
    "If it is below Mongo countDocuments, run extract with EXTRACTION_MODE=full "
    "or fix incremental filters; see extract job logs."
)

# One row per loan: incremental extract may append the same _id across runs; keep latest batch.
dedupe_key = F.col("_id").cast("string")
if "_etl_extracted_at" in raw_df.columns:
    w_loan_dedupe = Window.partitionBy(dedupe_key).orderBy(F.desc("_etl_extracted_at"))
    raw_df = (
        raw_df.withColumn("_dedup_rn", F.row_number().over(w_loan_dedupe))
        .filter(F.col("_dedup_rn") == 1)
        .drop("_dedup_rn")
    )
else:
    raw_df = raw_df.dropDuplicates(["_id"])

total_records = raw_df.count()
if extra := loan_rows_read - total_records:
    print(f"De-duplicated by _id (latest _etl_extracted_at): removed {extra:,} extra row(s); "
          f"distinct loans: {total_records:,}")
else:
    print(f"Distinct loans after de-dupe: {total_records:,}")

# Cache the raw loans data for multiple transformations
raw_df.cache()

print("\n" + "-" * 40)
print("STEP 1b: Reading from S3 Raw Zone (Correspondents)...")
print("-" * 40)

try:
    raw_correspondent_df = spark.read.option("mergeSchema", "true").parquet(S3_CORRESPONDENTS_RAW_PATH)
    total_corr = raw_correspondent_df.count()
    print(f"Total correspondent records read: {total_corr:,}")
    print(f"Correspondent columns: {len(raw_correspondent_df.columns)}")
    raw_correspondent_df.cache()
except Exception as e:
    print(f"WARNING: Could not read correspondents raw data: {e}")
    raw_correspondent_df = None

print("\n" + "-" * 40)
print("STEP 1c: Reading from S3 Raw Zone (Users)...")
print("-" * 40)

try:
    raw_user_df = spark.read.option("mergeSchema", "true").parquet(S3_USERS_RAW_PATH)
    total_users = raw_user_df.count()
    print(f"Total user records read: {total_users:,}")
    print(f"User columns: {len(raw_user_df.columns)}")
    raw_user_df.cache()
except Exception as e:
    print(f"WARNING: Could not read users raw data: {e}")
    raw_user_df = None

print("\n" + "-" * 40)
print("STEP 1d: Reading from S3 Raw Zone (Investors)...")
print("-" * 40)

try:
    raw_investor_df = spark.read.option("mergeSchema", "true").parquet(S3_INVESTORS_RAW_PATH)
    total_inv = raw_investor_df.count()
    print(f"Total investor records read: {total_inv:,}")
    print(f"Investor columns: {len(raw_investor_df.columns)}")
    raw_investor_df.cache()
except Exception as e:
    print(f"WARNING: Could not read investors raw data: {e}")
    raw_investor_df = None

# ============================================================================
# STEP 2: Create dim_date (Date Dimension)
# ============================================================================
print("\n" + "-" * 40)
print("STEP 2: Creating dim_date...")
print("-" * 40)

# Generate date range (2020 to 2030)
start_date = date(2020, 1, 1)
end_date = date(2030, 12, 31)

date_df = spark.sql(f"""
    SELECT explode(sequence(
        to_date('{start_date}'), 
        to_date('{end_date}'), 
        interval 1 day
    )) as date_key
""")

dim_date_df = date_df.select(
    F.col("date_key"),
    F.date_format("date_key", "yyyyMMdd").cast("int").alias("date_id"),
    F.year("date_key").alias("year"),
    F.quarter("date_key").alias("quarter"),
    F.month("date_key").alias("month"),
    F.dayofmonth("date_key").alias("day"),
    F.dayofweek("date_key").alias("day_of_week"),
    F.weekofyear("date_key").alias("week_of_year"),
    F.date_format("date_key", "EEEE").alias("day_name"),
    F.date_format("date_key", "MMMM").alias("month_name"),
    F.date_format("date_key", "yyyy-MM").alias("year_month"),
    F.when(F.dayofweek("date_key").isin([1, 7]), True).otherwise(False).alias("is_weekend"),
    F.lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S")).alias("_etl_loaded_at")
)

print(f"dim_date records: {dim_date_df.count():,}")

# ============================================================================
# STEP 3: Create dim_product (Product Dimension)
# ============================================================================
print("\n" + "-" * 40)
print("STEP 3: Creating dim_product...")
print("-" * 40)

# Static product dimension
products = [
    (1, "DPA", "Down Payment Assistance", "Assistance"),
    (2, "EEP", "Energy Efficient Program", "Efficiency"),
    (3, "White Label", "White Label Program", "White Label"),
    (4, "Solar Program", "Solar Program", "Solar"),
    (5, "CSPortal", "CS Portal", "Portal"),
    (6, "Loan Depot", "Loan Depot", "Partner"),
]

dim_product_df = spark.createDataFrame(products, [
    "product_id", "product_code", "product_name", "product_category"
]).withColumn("_etl_loaded_at", F.lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))

print(f"dim_product records: {dim_product_df.count()}")


def _account_executive_id_expr(available_columns, prefix="", field_types=None):
    """Map accountExecutiveId or accountExecutiveIds to a single string FK."""
    field_types = field_types or {}
    p = f"{prefix}." if prefix else ""

    def _col(field: str):
        return F.col(f"{p}{field}")

    singular = None
    if "accountExecutiveId" in available_columns:
        singular = _col("accountExecutiveId").cast("string")

    plural_first = None
    if "accountExecutiveIds" in available_columns:
        ids_col = _col("accountExecutiveIds")
        ids_type = field_types.get("accountExecutiveIds")
        if isinstance(ids_type, ArrayType):
            first_elt = F.element_at(ids_col, 1)
            if isinstance(ids_type.elementType, StructType):
                plural_first = F.coalesce(
                    first_elt.getField("_id").cast("string"),
                    first_elt.cast("string"),
                )
            else:
                plural_first = first_elt.cast("string")
        elif isinstance(ids_type, StringType):
            plural_first = F.regexp_extract(ids_col, r'^\s*\[\s*"?([^"\],\]]+)', 1)
            plural_first = F.when(plural_first != "", plural_first).otherwise(F.lit(None))
        else:
            plural_first = ids_col.cast("string")

    if singular is not None and plural_first is not None:
        return F.coalesce(singular, plural_first)
    if plural_first is not None:
        return plural_first
    if singular is not None:
        return singular
    return F.lit(None).cast("string")


# ============================================================================
# STEP 4: Create dim_correspondent (Correspondent Dimension)
# ============================================================================
print("\n" + "-" * 40)
print("STEP 4: Creating dim_correspondent...")
print("-" * 40)

if raw_correspondent_df is not None:
    # Build correspondent dimension from dedicated correspondents collection
    print("Building dim_correspondent from raw correspondents collection...")
    # Handle zip/zipCode variations safely
    corr_cols = set(raw_correspondent_df.columns)
    if "zip" in corr_cols and "zipCode" in corr_cols:
        zip_expr = F.coalesce(F.col("zip"), F.col("zipCode"))
    elif "zipCode" in corr_cols:
        zip_expr = F.col("zipCode")
    elif "zip" in corr_cols:
        zip_expr = F.col("zip")
    else:
        zip_expr = F.lit(None).cast("string")

    ae_expr = _account_executive_id_expr(
        corr_cols,
        field_types={f.name: f.dataType for f in raw_correspondent_df.schema.fields},
    )

    dim_correspondent_df = raw_correspondent_df.select(
        F.col("_id").alias("correspondent_id"),
        F.col("name").alias("correspondent_name"),
        F.col("nmlsNumber").alias("nmls_number"),
        F.col("address").alias("address"),
        F.col("city").alias("city"),
        F.col("state").alias("state"),
        zip_expr.alias("zip_code"),
        F.col("createdAt").alias("created_at"),
        ae_expr.alias("account_executive_id")
    ).distinct().filter(F.col("correspondent_id").isNotNull())
else:
    print("No correspondents raw data available – falling back to loans.corrrespondent field if present...")
    # Fallback: Extract correspondent data from nested structure in loans if exists
    if "correspondent" in raw_df.columns:
        correspondent_type = str(raw_df.schema["correspondent"].dataType)
        
        if "StructType" in correspondent_type:
            corr_struct = raw_df.schema["correspondent"].dataType
            corr_field_names = {f.name for f in corr_struct.fields}
            ae_expr = _account_executive_id_expr(
                corr_field_names,
                prefix="correspondent",
                field_types={f.name: f.dataType for f in corr_struct.fields},
            )
            dim_correspondent_df = raw_df.select(
                F.col("correspondent._id").alias("correspondent_id"),
                F.col("correspondent.name").alias("correspondent_name"),
                F.col("correspondent.nmlsNumber").alias("nmls_number"),
                F.col("correspondent.address").alias("address"),
                F.col("correspondent.city").alias("city"),
                F.col("correspondent.state").alias("state"),
                F.col("correspondent.zip").alias("zip_code"),
                F.col("correspondent.createdAt").alias("created_at"),
                ae_expr.alias("account_executive_id")
            ).distinct().filter(F.col("correspondent_id").isNotNull())
        else:
            # correspondent is a reference, create from unique values
            dim_correspondent_df = raw_df.select(
                F.col("correspondent").alias("correspondent_id")
            ).distinct().filter(F.col("correspondent_id").isNotNull()).withColumn(
                "correspondent_name", F.lit(None).cast("string")
            ).withColumn("nmls_number", F.lit(None).cast("string")
            ).withColumn("address", F.lit(None).cast("string")
            ).withColumn("city", F.lit(None).cast("string")
            ).withColumn("state", F.lit(None).cast("string")
            ).withColumn("zip_code", F.lit(None).cast("string")
            ).withColumn("created_at", F.lit(None).cast("timestamp")
            ).withColumn("account_executive_id", F.lit(None).cast("string"))
    else:
        # Create empty correspondent dimension
        dim_correspondent_df = spark.createDataFrame([], StructType([
            StructField("correspondent_id", StringType(), True),
            StructField("correspondent_name", StringType(), True),
            StructField("nmls_number", StringType(), True),
            StructField("address", StringType(), True),
            StructField("city", StringType(), True),
            StructField("state", StringType(), True),
            StructField("zip_code", StringType(), True),
            StructField("created_at", TimestampType(), True),
            StructField("account_executive_id", StringType(), True)
        ]))

dim_correspondent_df = dim_correspondent_df.withColumn(
    "_etl_loaded_at", F.lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
)

print(f"dim_correspondent records: {dim_correspondent_df.count():,}")

# ============================================================================
# STEP 4b: Create dim_user (User Dimension from users collection)
# ============================================================================
print("\n" + "-" * 40)
print("STEP 4b: Creating dim_user...")
print("-" * 40)

def safe_col_user(df, *candidates):
    """Return first existing column among candidates (e.g. __postgresId vs postgresId)."""
    for c in candidates:
        if c in df.columns:
            return F.col(c)
    return F.lit(None).cast("string")

if raw_user_df is not None:
    dim_user_df = raw_user_df.select(
        F.col("_id").alias("user_id"),
        F.col("name").alias("user_name"),
        F.col("email").alias("email"),
        safe_col_user(raw_user_df, "disableLogin", "disable_login").alias("disable_login"),
        safe_col_user(raw_user_df, "__postgresId", "postgresId").alias("postgres_id"),
        safe_col_user(raw_user_df, "__companyId", "companyId").alias("company_id"),
        safe_col_user(raw_user_df, "__userType", "userType").alias("user_type"),
        safe_col_user(raw_user_df, "__correspondentId", "correspondentId").alias("correspondent_id"),
        F.col("lastActiveAt").alias("last_active_at"),
        F.lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S")).alias("_etl_loaded_at")
    ).distinct().filter(F.col("user_id").isNotNull())
    print(f"dim_user records: {dim_user_df.count():,}")
else:
    dim_user_df = spark.createDataFrame([], StructType([
        StructField("user_id", StringType(), True),
        StructField("user_name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("disable_login", BooleanType(), True),
        StructField("postgres_id", StringType(), True),
        StructField("company_id", StringType(), True),
        StructField("user_type", StringType(), True),
        StructField("correspondent_id", StringType(), True),
        StructField("last_active_at", TimestampType(), True),
        StructField("_etl_loaded_at", StringType(), True)
    ]))
    print("dim_user: no raw users data, empty dimension")

# ============================================================================
# STEP 4c: Create dim_investor (Investor dimension from investors collection)
# ============================================================================
print("\n" + "-" * 40)
print("STEP 4c: Creating dim_investor...")
print("-" * 40)

if raw_investor_df is not None:
    dim_investor_df = raw_investor_df.select(
        F.col("_id").cast("string").alias("investor_id"),
        F.col("name").alias("investor_name"),
        F.lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S")).alias("_etl_loaded_at")
    ).distinct().filter(F.col("investor_id").isNotNull())
    print(f"dim_investor records: {dim_investor_df.count():,}")
else:
    dim_investor_df = spark.createDataFrame([], StructType([
        StructField("investor_id", StringType(), True),
        StructField("investor_name", StringType(), True),
        StructField("_etl_loaded_at", StringType(), True)
    ]))
    print("dim_investor: no raw investors data, empty dimension")

# ============================================================================
# STEP 5: Create dim_loan (Loan Dimension)
# ============================================================================
print("\n" + "-" * 40)
print("STEP 5: Creating dim_loan...")
print("-" * 40)

# Helper to safely get columns -- always casts to the target type so that
# string-encoded values (produced by resolveChoice in the extract job) are
# properly converted to their intended types (decimal, timestamp, etc.).
def safe_col(df, col_name, default_type="string"):
    if col_name in df.columns:
        return F.col(col_name).cast(default_type)
    else:
        return F.lit(None).cast(default_type)


def safe_money_decimal(df, col_name, precision="decimal(18,2)"):
    """Money from raw Parquet: may be numeric, string, or '{value=...}' after extract."""
    if col_name not in df.columns:
        return F.lit(None).cast(precision)
    s = F.col(col_name).cast("string")
    cleaned = F.when(
        s.rlike(r'^\{value=.*\}$'),
        F.regexp_extract(s, r'^\{value=(.*)\}$', 1),
    ).otherwise(s)
    cleaned = F.when(
        cleaned.isNull() | (F.length(F.trim(cleaned)) == 0),
        F.lit(None),
    ).otherwise(cleaned)
    return cleaned.cast(precision)


def safe_clean_string(df, col_name):
    """String cleanup: unwrap '{value=...}', trim, empty->null."""
    if col_name not in df.columns:
        return F.lit(None).cast("string")
    s = F.col(col_name).cast("string")
    unwrapped = F.when(
        s.rlike(r'^\{value=.*\}$'),
        F.regexp_extract(s, r'^\{value=(.*)\}$', 1),
    ).otherwise(s)
    trimmed = F.trim(unwrapped)
    return F.when(trimmed.isNull() | (F.length(trimmed) == 0), F.lit(None)).otherwise(trimmed)


def safe_bool(df, col_name):
    """Boolean cleanup: tolerate booleans and common string encodings."""
    if col_name not in df.columns:
        return F.lit(None).cast("boolean")
    # Handle both native booleans and string-encoded booleans
    s = safe_clean_string(df, col_name).cast("string")
    v = F.lower(F.trim(s))
    return (
        F.when(v.isNull(), F.lit(None).cast("boolean"))
         .when(v.isin("true", "t", "1", "yes", "y"), F.lit(True))
         .when(v.isin("false", "f", "0", "no", "n"), F.lit(False))
         .otherwise(F.lit(None).cast("boolean"))
    )


def second_mortgage_settled_status_expr(df):
    """Normalize secondMortgageSettledStatus -> 'Settled'/'Unsettled'/null."""
    v = F.lower(safe_clean_string(df, "secondMortgageSettledStatus"))
    return (
        F.when(v.isNull(), F.lit(None).cast("string"))
         .when(v.isin("settled", "unsettled"), F.initcap(v))
         .otherwise(F.lit(None).cast("string"))
    )


def second_mortgage_delinquency_status_expr(df):
    """Normalize secondMortgageDelinquencyStatus -> '0-29','30-59','60-89','90+'/null."""
    s = safe_clean_string(df, "secondMortgageDelinquencyStatus")
    v = F.regexp_replace(F.lower(s), r"\s+", "")
    mapped = (
        F.when(v.isin("0-29", "0to29", "0_29"), F.lit("0-29"))
         .when(v.isin("30-59", "30to59", "30_59"), F.lit("30-59"))
         .when(v.isin("60-89", "60to89", "60_89"), F.lit("60-89"))
         .when(v.isin("90+", "90plus", "90"), F.lit("90+"))
         .otherwise(F.lit(None).cast("string"))
    )
    return mapped


def _empty_dim_borrower_df():
    """Empty dim_borrower with the same schema reports/Athena expect."""
    return spark.createDataFrame([], StructType([
        StructField("borrower_id", LongType(), True),
        StructField("loan_id", StringType(), True),
        StructField("borrower_position", IntegerType(), True),
        StructField("first_name", StringType(), True),
        StructField("middle_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("suffix", StringType(), True),
        StructField("email", StringType(), True),
        StructField("primary_phone", StringType(), True),
        StructField("home_phone", StringType(), True),
        StructField("work_phone", StringType(), True),
        StructField("ssn_masked", StringType(), True),
        StructField("date_of_birth", DateType(), True),
        StructField("credit_score", IntegerType(), True),
        StructField("dti", DoubleType(), True),
        StructField("monthly_income", DoubleType(), True),
        StructField("marital_status", StringType(), True),
        StructField("ethnicity", StringType(), True),
        StructField("race", StringType(), True),
        StructField("sex", StringType(), True),
        StructField("occupancy_type", StringType(), True),
        StructField("current_address", StringType(), True),
        StructField("current_city", StringType(), True),
        StructField("current_state", StringType(), True),
        StructField("current_zip", StringType(), True),
        StructField("employer_name", StringType(), True),
        StructField("years_worked", DoubleType(), True),
        StructField("borrower_type", StringType(), True),
        StructField("is_primary", BooleanType(), True),
        StructField("_etl_loaded_at", StringType(), True),
    ]))


def _empty_dim_eep_processing_entry_df():
    """Empty dim_eep_processing_entry with a stable Athena-friendly schema."""
    return spark.createDataFrame([], StructType([
        StructField("eep_processing_entry_id", LongType(), True),
        StructField("loan_id", StringType(), True),
        StructField("array_index", IntegerType(), True),
        StructField("entry_key", StringType(), True),
        StructField("entry_position", IntegerType(), True),
        StructField("processed_at", TimestampType(), True),
        StructField("user_id", StringType(), True),
        StructField("new_processing_status", StringType(), True),
        StructField("_etl_loaded_at", StringType(), True),
    ]))


# Special helper for correspondent foreign key – handle both 'correspondent' and 'correspondentId'
def correspondent_fk(df):
    if "correspondent" in df.columns:
        return F.col("correspondent")
    elif "correspondentId" in df.columns:
        return F.col("correspondentId")
    else:
        return F.lit(None).cast("string")

# Helper for user FK columns – handle Id vs struct (e.g. purchaseClearingAssignedToId vs purchaseClearingAssignedTo)
def user_fk(df, base_name):
    id_col = f"{base_name}Id"
    ref_col = base_name  # struct might be present
    if id_col in df.columns:
        return F.col(id_col)
    if ref_col in df.columns:
        ref_type = str(df.schema[ref_col].dataType)
        if "StructType" in ref_type and "id" in [f.name for f in df.schema[ref_col].dataType.fields]:
            return F.col(f"{ref_col}.id")
        return F.col(ref_col)
    return F.lit(None).cast("string")


# Generic helper for reference fields that may be stored as {base}Id or as struct in {base}
def ref_fk(df, base_name):
    id_col = f"{base_name}Id"
    ref_col = base_name
    if id_col in df.columns:
        return F.col(id_col).cast("string")
    if ref_col in df.columns:
        ref_type = str(df.schema[ref_col].dataType)
        if "StructType" in ref_type:
            ref_fields = [f.name for f in df.schema[ref_col].dataType.fields]
            if "_id" in ref_fields:
                return F.col(f"{ref_col}._id").cast("string")
            if "id" in ref_fields:
                return F.col(f"{ref_col}.id").cast("string")
        return F.col(ref_col).cast("string")
    return F.lit(None).cast("string")

# Loan snapshot fields from MongoDB; join dim_investor to fill investor name when investorName is null.
dim_loan_base = raw_df.select(
    # Keys
    safe_col(raw_df, "_id").alias("loan_id"),
    safe_col(raw_df, "ahLoanNumber").alias("ah_loan_number"),
    safe_col(raw_df, "lenderLoanNumber").alias("lender_loan_number"),
    safe_col(raw_df, "usfLoanNumber").alias("usf_loan_number"),
    safe_col(raw_df, "bsiLoanNumber").alias("bsi_loan_number"),
    safe_col(raw_df, "essexLoanNumber").alias("essex_loan_number"),
    safe_col(raw_df, "blueWaterId", "integer").alias("bluewater_id"),
    
    # Product info
    safe_col(raw_df, "workflowType").alias("workflow_type"),
    safe_col(raw_df, "productType").alias("product_type"),
    safe_col(raw_df, "dpaRepaymentType").alias("dpa_repayment_type"),
    safe_col(raw_df, "firstMortgageType").alias("first_mortgage_type"),
    
    # Property info
    safe_col(raw_df, "propertyAddress").alias("property_address"),
    safe_col(raw_df, "propertyCity").alias("property_city"),
    safe_col(raw_df, "propertyState").alias("property_state"),
    safe_col(raw_df, "propertyZip").alias("property_zip"),
    safe_col(raw_df, "propertyCounty").alias("property_county"),
    safe_col(raw_df, "propertyType").alias("property_type"),
    safe_col(raw_df, "numberOfUnits", "integer").alias("number_of_units"),
    
    # Loan officer info
    safe_col(raw_df, "loanOfficerName").alias("loan_officer_name"),
    safe_col(raw_df, "loanOfficerEmail").alias("loan_officer_email"),
    safe_col(raw_df, "loanOfficerNmlsNumber").alias("loan_officer_nmls"),
    
    # Foreign keys
    correspondent_fk(raw_df).alias("correspondent_id"),
    safe_col(raw_df, "createdBy").alias("created_by_user_id"),
    user_fk(raw_df, "purchaseClearingAssignedTo").alias("purchase_clearing_assigned_to_id"),
    user_fk(raw_df, "purchaseClearingAssistantAssignedTo").alias("purchase_clearing_assistant_assigned_to_id"),
    user_fk(raw_df, "eepProcessor").alias("eep_processor_id"),
    user_fk(raw_df, "eepUnderwriter").alias("eep_underwriter_id"),
    user_fk(raw_df, "eepCloser").alias("eep_closer_id"),
    
    # Second mortgage / DPA investor (loan denormalized + FK to dim_investor)
    safe_col(raw_df, "secondMortgageInvestorId").alias("second_mortgage_investor_id"),
    safe_col(raw_df, "investorOrgId").alias("investor_org_id"),
    safe_col(raw_df, "investorName").alias("investor_name"),
    safe_clean_string(raw_df, "secondMortgageMinNumber").alias("second_mortgage_min_number"),
    
    # Dates
    safe_col(raw_df, "createdAt", "timestamp").alias("created_at"),
    safe_col(raw_df, "closingDate", "date").alias("closing_date"),
    safe_col(raw_df, "fundingDate", "date").alias("funding_date"),
    safe_col(raw_df, "closedAt", "timestamp").alias("closed_at"),
    
    # ETL metadata
    F.lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S")).alias("_etl_loaded_at")
)

dim_inv_for_join = dim_investor_df.select(
    F.col("investor_id").alias("_inv_join_id"),
    F.col("investor_name").alias("_investor_dim_name")
)

dim_loan_df = dim_loan_base.join(
    dim_inv_for_join,
    F.col("second_mortgage_investor_id") == F.col("_inv_join_id"),
    "left"
).withColumn(
    "second_mortgage_investor_name",
    F.coalesce(F.col("investor_name"), F.col("_investor_dim_name"))
).drop("_inv_join_id", "_investor_dim_name")

print(f"dim_loan records: {dim_loan_df.count():,}")

# ============================================================================
# STEP 6: Create dim_borrower (Borrower Dimension)
# ============================================================================
print("\n" + "-" * 40)
print("STEP 6: Creating dim_borrower...")
print("-" * 40)

if "borrowers" in raw_df.columns:
    # PyMongo extract: borrowers JSON string. Legacy Glue extract: array<struct>.
    # Parse on a branch DataFrame only — dim_borrower output columns unchanged for reports.
    borrowers_type_str = raw_df.schema["borrowers"].dataType.simpleString()
    print(f"  borrowers column type: {borrowers_type_str}")

    borrowers_map_schema = ArrayType(MapType(StringType(), StringType(), True))
    if borrowers_type_str == "string" or borrowers_type_str.startswith("string"):
        loans_borrowers_df = raw_df.withColumn(
            "borrowers",
            F.when(
                F.col("borrowers").isNull() | (F.length(F.trim(F.col("borrowers"))) == 0),
                F.array().cast(borrowers_map_schema),
            ).otherwise(
                F.from_json(F.col("borrowers"), borrowers_map_schema)
            ),
        )
        borrowers_use_map = True
    elif borrowers_type_str.startswith("array<struct"):
        loans_borrowers_df = raw_df
        borrowers_use_map = False
    elif borrowers_type_str.startswith("array<map"):
        loans_borrowers_df = raw_df
        borrowers_use_map = True
    else:
        print(f"  Warning: unsupported borrowers type '{borrowers_type_str}' — empty dim_borrower")
        loans_borrowers_df = None
        borrowers_use_map = False

    def borrower_field(name):
        if borrowers_use_map:
            return F.col("borrower").getItem(name)
        return F.col(f"borrower.{name}")

    if loans_borrowers_df is not None:
        # Explode borrowers array (must be ARRAY/MAP, not STRING)
        borrowers_exploded = loans_borrowers_df.select(
            F.col("_id").alias("loan_id"),
            F.posexplode_outer("borrowers").alias("borrower_position", "borrower")
        ).filter(F.col("borrower").isNotNull())

        # Note: Field names based on actual MongoDB schema:
        # fico (not creditScore), no citizenship field
        dim_borrower_df = borrowers_exploded.select(
            F.monotonically_increasing_id().alias("borrower_id"),
            F.col("loan_id"),
            F.col("borrower_position"),
            borrower_field("firstName").alias("first_name"),
            borrower_field("middleName").alias("middle_name"),
            borrower_field("lastName").alias("last_name"),
            borrower_field("suffix").alias("suffix"),
            borrower_field("email").alias("email"),
            borrower_field("primaryPhoneNumber").alias("primary_phone"),
            borrower_field("homePhoneNumber").alias("home_phone"),
            borrower_field("workPhoneNumber").alias("work_phone"),
            borrower_field("ssn").alias("ssn"),
            # Mask SSN - only keep last 4 digits
            F.when(
                borrower_field("ssn").isNotNull(),
                F.concat(F.lit("***-**-"), F.substring(borrower_field("ssn"), -4, 4))
            ).alias("ssn_masked"),
            borrower_field("dateOfBirth").cast("date").alias("date_of_birth"),
            borrower_field("fico").cast("integer").alias("credit_score"),  # fico is the credit score field
            borrower_field("dti").cast("double").alias("dti"),
            borrower_field("monthlyIncome").cast("double").alias("monthly_income"),
            borrower_field("maritalStatus").alias("marital_status"),
            borrower_field("ethnicity").alias("ethnicity"),
            borrower_field("race").alias("race"),
            borrower_field("sex").alias("sex"),
            borrower_field("occupancyType").alias("occupancy_type"),
            # Current address
            borrower_field("currentAddress").alias("current_address"),
            borrower_field("currentCity").alias("current_city"),
            borrower_field("currentState").alias("current_state"),
            borrower_field("currentZip").alias("current_zip"),
            # Employer info
            borrower_field("employerName").alias("employer_name"),
            borrower_field("yearsWorked").cast("double").alias("years_worked"),
            # Position and primary flag
            borrower_field("position").alias("borrower_type"),  # e.g., "Primary", "Co-Borrower"
            F.when(F.col("borrower_position") == 0, True).otherwise(False).alias("is_primary"),
            F.lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S")).alias("_etl_loaded_at")
        )
    else:
        dim_borrower_df = _empty_dim_borrower_df()
else:
    dim_borrower_df = _empty_dim_borrower_df()

print(f"dim_borrower records: {dim_borrower_df.count():,}")

# ============================================================================
# STEP 6b: Create dim_eep_processing_entry (EEP processing history from loans)
# ============================================================================
print("\n" + "-" * 40)
print("STEP 6b: Creating dim_eep_processing_entry...")
print("-" * 40)

if "eepProcessingEntries" in raw_df.columns:
    eep_type_str = raw_df.schema["eepProcessingEntries"].dataType.simpleString()
    print(f"  eepProcessingEntries column type: {eep_type_str}")

    eep_map_schema = ArrayType(MapType(StringType(), StringType(), True))
    if eep_type_str == "string" or eep_type_str.startswith("string"):
        loans_eep_df = raw_df.withColumn(
            "eepProcessingEntries",
            F.when(
                F.col("eepProcessingEntries").isNull()
                | (F.length(F.trim(F.col("eepProcessingEntries"))) == 0),
                F.array().cast(eep_map_schema),
            ).otherwise(
                F.from_json(F.col("eepProcessingEntries"), eep_map_schema)
            ),
        )
        eep_use_map = True
    elif eep_type_str.startswith("array<struct"):
        loans_eep_df = raw_df
        eep_use_map = False
    elif eep_type_str.startswith("array<map"):
        loans_eep_df = raw_df
        eep_use_map = True
    else:
        print(f"  Warning: unsupported eepProcessingEntries type '{eep_type_str}' — empty table")
        loans_eep_df = None
        eep_use_map = False

    def eep_entry_field(name):
        if eep_use_map:
            return F.col("eep_entry").getItem(name)
        return F.col(f"eep_entry.{name}")

    def eep_entry_user_id():
        if eep_use_map:
            return F.coalesce(
                F.col("eep_entry").getItem("userId"),
                F.col("eep_entry").getItem("user"),
            ).cast("string")

        element_type = raw_df.schema["eepProcessingEntries"].dataType.elementType
        field_names = [f.name for f in element_type.fields]
        if "userId" in field_names:
            return F.col("eep_entry.userId").cast("string")
        if "user" in field_names:
            user_field = element_type["user"].dataType
            if isinstance(user_field, StructType):
                user_fields = [f.name for f in user_field.fields]
                if "_id" in user_fields:
                    return F.col("eep_entry.user._id").cast("string")
                if "id" in user_fields:
                    return F.col("eep_entry.user.id").cast("string")
            return F.col("eep_entry.user").cast("string")
        return F.lit(None).cast("string")

    if loans_eep_df is not None:
        eep_exploded = loans_eep_df.select(
            F.col("_id").alias("loan_id"),
            F.posexplode_outer("eepProcessingEntries").alias("array_index", "eep_entry"),
        ).filter(F.col("eep_entry").isNotNull())

        dim_eep_processing_entry_df = eep_exploded.select(
            F.monotonically_increasing_id().alias("eep_processing_entry_id"),
            F.col("loan_id"),
            F.col("array_index"),
            eep_entry_field("_key").alias("entry_key"),
            eep_entry_field("_position").cast("integer").alias("entry_position"),
            eep_entry_field("at").cast("timestamp").alias("processed_at"),
            eep_entry_user_id().alias("user_id"),
            eep_entry_field("newProcessingStatus").alias("new_processing_status"),
            F.lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S")).alias("_etl_loaded_at"),
        )
    else:
        dim_eep_processing_entry_df = _empty_dim_eep_processing_entry_df()
else:
    print("  eepProcessingEntries column not found in raw loans — empty table")
    dim_eep_processing_entry_df = _empty_dim_eep_processing_entry_df()

print(f"dim_eep_processing_entry records: {dim_eep_processing_entry_df.count():,}")

# ============================================================================
# STEP 7: Create fact_loan_status (Loan Status Fact)
# ============================================================================
print("\n" + "-" * 40)
print("STEP 7: Creating fact_loan_status...")
print("-" * 40)

fact_loan_status_df = raw_df.select(
    # Keys
    safe_col(raw_df, "_id").alias("loan_id"),
    safe_col(raw_df, "correspondent").alias("correspondent_id"),
    
    # Status
    safe_col(raw_df, "status").alias("current_status"),
    safe_col(raw_df, "health").alias("health_status"),
    safe_col(raw_df, "healthReason").alias("health_reason"),
    safe_col(raw_df, "secondMortgageOwnershipStatus").alias("second_mortgage_ownership_status"),
    second_mortgage_settled_status_expr(raw_df).alias("second_mortgage_settled_status"),
    second_mortgage_delinquency_status_expr(raw_df).alias("second_mortgage_delinquency_status"),
    safe_bool(raw_df, "isEmployeeLoan").alias("is_employee_loan"),
    safe_col(raw_df, "secondMortgageSoldTo", "string").alias("second_mortgage_sold_to"),
    safe_col(raw_df, "secondMortgageSoldDate", "timestamp").alias("second_mortgage_sold_date"),
    safe_col(raw_df, "secondMortgageInvoiceTradeNumber", "string").alias("second_mortgage_invoice_trade_number"),
    safe_clean_string(raw_df, "secondMortgageMinNumber").alias("second_mortgage_min_number"),
    user_fk(raw_df, "eepProcessor").alias("eep_processor_id"),
    user_fk(raw_df, "eepUnderwriter").alias("eep_underwriter_id"),
    user_fk(raw_df, "eepCloser").alias("eep_closer_id"),
    
    # Ownership status
    safe_col(raw_df, "firstMortgageOwnershipStatus").alias("first_mortgage_ownership_status"),
    
    # Date keys (for joining with dim_date)
    F.date_format(safe_col(raw_df, "closingDate", "date"), "yyyyMMdd").cast("int").alias("closing_date_key"),
    F.date_format(safe_col(raw_df, "fundingDate", "date"), "yyyyMMdd").cast("int").alias("funding_date_key"),
    F.date_format(safe_col(raw_df, "createdAt", "timestamp"), "yyyyMMdd").cast("int").alias("created_date_key"),
    F.date_format(safe_col(raw_df, "purchasedAt", "timestamp"), "yyyyMMdd").cast("int").alias("purchased_date_key"),
    F.date_format(safe_col(raw_df, "secondMortgageNextPaymentDueDate", "timestamp"), "yyyyMMdd").cast("int").alias("second_mortgage_next_payment_due_date_key"),
    F.date_format(safe_col(raw_df, "secondMortgageLastPaymentMadeDate", "timestamp"), "yyyyMMdd").cast("int").alias("second_mortgage_last_payment_made_date_key"),
    F.date_format(safe_col(raw_df, "purchaseClearingIntakeAuditReadyDate", "timestamp"), "yyyyMMdd").cast("int").alias("purchase_clearing_intake_audit_ready_date_key"),
    F.date_format(safe_col(raw_df, "purchaseClearingIntakeAuditCompletionDate", "timestamp"), "yyyyMMdd").cast("int").alias("purchase_clearing_intake_audit_completion_date_key"),
    F.date_format(safe_col(raw_df, "secondMortgageFirstPaymentDueDate", "timestamp"), "yyyyMMdd").cast("int").alias("second_mortgage_first_payment_due_date_key"),
    
    # Status timestamps
    
    safe_col(raw_df, "registeredAt", "timestamp").alias("registered_at"),
    safe_col(raw_df, "lockedAt", "timestamp").alias("locked_at"),
    safe_col(raw_df, "closedAt", "timestamp").alias("closed_at"),
    safe_col(raw_df, "purchasedAt", "timestamp").alias("purchased_at"),
    safe_col(raw_df, "securitizedAt", "timestamp").alias("securitized_at"),
    safe_col(raw_df, "cancelledAt", "timestamp").alias("cancelled_at"),
    safe_col(raw_df, "deniedAt", "timestamp").alias("denied_at"),
    safe_col(raw_df, "secondMortgageNextPaymentDueDate", "timestamp").alias("second_mortgage_next_payment_due_date"),
    safe_col(raw_df, "secondMortgageLastPaymentMadeDate", "timestamp").alias("second_mortgage_last_payment_made_date"),
    safe_col(raw_df, "purchaseClearingIntakeAuditReadyDate", "timestamp").alias("purchase_clearing_intake_audit_ready_date"),
    safe_col(raw_df, "purchaseClearingIntakeAuditCompletionDate", "timestamp").alias("purchase_clearing_intake_audit_completion_date"),
    safe_col(raw_df, "secondMortgageFirstPaymentDueDate", "timestamp").alias("second_mortgage_first_payment_due_date"),
    safe_col(raw_df, "fundingDate", "date").alias("funding_date"),
    
    # Processing timestamps
    safe_col(raw_df, "clearToCloseAt", "timestamp").alias("clear_to_close_at"),
    safe_col(raw_df, "approvedForPurchaseAt", "timestamp").alias("approved_for_purchase_at"),
    safe_col(raw_df, "allConditionsClearedAt", "timestamp").alias("all_conditions_cleared_at"),
    safe_col(raw_df, "closedLoanPackageReceivedAt", "timestamp").alias("closed_loan_package_received_at"),
    safe_col(raw_df, "closedLoanPackageIndexedAt", "timestamp").alias("closed_loan_package_indexed_at"),
    safe_col(raw_df, "closedLoanPackageBoardedAt", "timestamp").alias("closed_loan_package_boarded_at"),
    safe_col(raw_df, "collateralReceivedAt", "timestamp").alias("collateral_received_at"),
    safe_col(raw_df, "reimbursedAt", "timestamp").alias("reimbursed_at"),
   
    # ETL metadata
    F.lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S")).alias("_etl_loaded_at")
)

print(f"fact_loan_status records: {fact_loan_status_df.count():,}")

# ============================================================================
# STEP 8: Create fact_loan_metrics (Loan Financial Metrics Fact)
# ============================================================================
print("\n" + "-" * 40)
print("STEP 8: Creating fact_loan_metrics...")
print("-" * 40)

fact_loan_metrics_df = raw_df.select(
    # Keys
    safe_col(raw_df, "_id").alias("loan_id"),
    correspondent_fk(raw_df).alias("correspondent_id"),
    safe_col(raw_df, "productType").alias("product_type"),
    safe_col(raw_df, "workflowType").alias("workflow_type"),
    
    # Date keys
    F.date_format(safe_col(raw_df, "closingDate", "date"), "yyyyMMdd").cast("int").alias("closing_date_key"),
    
    # Financial metrics - First Mortgage
    safe_col(raw_df, "firstMortgageBaseLoanAmount", "decimal(18,2)").alias("first_mortgage_base_amount"),
    safe_col(raw_df, "firstMortgageTotalLoanAmount", "decimal(18,2)").alias("first_mortgage_total_amount"),
    safe_col(raw_df, "firstMortgageInterestRate", "decimal(10,6)").alias("first_mortgage_interest_rate"),
    safe_col(raw_df, "firstMortgageLoanToValueRatio", "decimal(10,6)").alias("first_mortgage_ltv"),
    safe_col(raw_df, "firstMortgageTerm", "integer").alias("first_mortgage_term_months"),
    
    # Financial metrics - Second Mortgage / DPA
    safe_col(raw_df, "dpaAmount", "decimal(18,2)").alias("dpa_amount"),
    safe_col(raw_df, "dpaPercent", "decimal(10,6)").alias("dpa_percent"),
    safe_money_decimal(raw_df, "secondMortgageUpb").alias("second_mortgage_upb"),
    safe_col(raw_df, "secondMortgageInterestRate", "decimal(10,6)").alias("second_mortgage_interest_rate"),
    safe_col(raw_df, "secondMortgageTerm", "integer").alias("second_mortgage_term_months"),
    
    # Property values
    safe_col(raw_df, "purchasePrice", "decimal(18,2)").alias("purchase_price"),
    safe_col(raw_df, "appraisedValue", "decimal(18,2)").alias("appraised_value"),
    safe_col(raw_df, "combinedLoanToValueRatio", "decimal(10,6)").alias("combined_ltv"),
    
    # Borrower metrics
    safe_col(raw_df, "totalIncome", "decimal(18,2)").alias("total_income"),
    safe_col(raw_df, "totalAssetsBalance", "decimal(18,2)").alias("total_assets"),
    safe_col(raw_df, "totalLiabilitiesBalance", "decimal(18,2)").alias("total_liabilities"),
    safe_col(raw_df, "totalBackEndDebtToIncomeRatio", "decimal(10,6)").alias("backend_dti"),
    safe_col(raw_df, "totalFrontEndDebtToIncomeRatio", "decimal(10,6)").alias("frontend_dti"),
    safe_col(raw_df, "finalQualifyingCreditScore", "integer").alias("credit_score"),
    
    # Payment metrics
    safe_col(raw_df, "totalMonthlyLoanPayment", "decimal(18,2)").alias("monthly_payment"),
    safe_col(raw_df, "escrowMonthlyPayment", "decimal(18,2)").alias("escrow_payment"),
    safe_col(raw_df, "pmiMonthlyPayment", "decimal(18,2)").alias("pmi_payment"),
    
    # Pricing
    safe_col(raw_df, "basePrice", "decimal(10,6)").alias("base_price"),
    safe_col(raw_df, "totalPrice", "decimal(10,6)").alias("total_price"),
    
    # ETL metadata
    F.lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S")).alias("_etl_loaded_at")
)

print(f"fact_loan_metrics records: {fact_loan_metrics_df.count():,}")

# ============================================================================
# STEP 9: Create dim_loancondition
# ============================================================================
print("\n" + "-" * 40)
print("STEP 9: Creating dim_loancondition...")
print("-" * 40)

S3_RAW_LOANCONDITIONS = "s3://arrivehome-bi-prod/raw-zone/loanconditions/"

try:
    loanconditions_df = spark.read.option("mergeSchema", "true").parquet(S3_RAW_LOANCONDITIONS)
    print(f"Loan conditions raw records: {loanconditions_df.count():,}")
    
    dim_loancondition_df = loanconditions_df.select(
        safe_col(loanconditions_df, "_id").alias("loancondition_id"),
        safe_col(loanconditions_df, "loanId").alias("loan_id"),  # loanId not loan
        safe_col(loanconditions_df, "conditionSource").alias("condition_source"),
        safe_col(loanconditions_df, "conditionType").alias("condition_type"),
        safe_col(loanconditions_df, "documentName").alias("document_name"),
        safe_col(loanconditions_df, "documentType").alias("document_type"),
        safe_col(loanconditions_df, "fieldName").alias("field_name"),
        safe_col(loanconditions_df, "blueWaterFieldValueId").alias("bluewater_field_value_id"),
        safe_col(loanconditions_df, "unconfirmedFieldValue").alias("unconfirmed_field_value"),
        safe_col(loanconditions_df, "confirmedFieldValue").alias("confirmed_field_value"),
        safe_col(loanconditions_df, "description").alias("description"),
        safe_col(loanconditions_df, "createdAt", "timestamp").alias("created_at"),
        safe_col(loanconditions_df, "createdById").alias("created_by_id"),
        safe_col(loanconditions_df, "submittedAt", "timestamp").alias("submitted_at"),
        safe_col(loanconditions_df, "submittedById").alias("submitted_by_id"),  # submittedById
        safe_col(loanconditions_df, "insufficientAt", "timestamp").alias("insufficient_at"),
        safe_col(loanconditions_df, "insufficientById").alias("insufficient_by_id"),  # insufficientById
        safe_col(loanconditions_df, "clearedAt", "timestamp").alias("cleared_at"),
        safe_col(loanconditions_df, "clearedById").alias("cleared_by_id"),
        # Derived status
        F.when(F.col("clearedAt").isNotNull(), "Cleared")
         .when(F.col("insufficientAt").isNotNull(), "Insufficient")
         .when(F.col("submittedAt").isNotNull(), "Submitted")
         .otherwise("Created").alias("status"),
        F.lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S")).alias("_etl_loaded_at")
    )
    print(f"dim_loancondition records: {dim_loancondition_df.count():,}")

    # Explode documentTypes array into a separate, narrow dimension table
    if "documentTypes" in loanconditions_df.columns:
        loancondition_documenttypes_exploded = loanconditions_df.select(
            safe_col(loanconditions_df, "_id").alias("loancondition_id"),
            safe_col(loanconditions_df, "loanId").alias("loan_id"),
            F.posexplode_outer("documentTypes").alias("document_type_position", "document_type")
        ).filter(F.col("document_type").isNotNull())

        dim_loancondition_documenttype_df = loancondition_documenttypes_exploded.select(
            F.col("loancondition_id"),
            F.col("loan_id"),
            F.col("document_type_position"),
            F.col("document_type"),
            F.lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S")).alias("_etl_loaded_at")
        )
        print(f"dim_loancondition_documenttype records: {dim_loancondition_documenttype_df.count():,}")
    else:
        dim_loancondition_documenttype_df = None
except Exception as e:
    print(f"Warning: Could not read loanconditions - {str(e)}")
    dim_loancondition_df = None
    dim_loancondition_documenttype_df = None

# ============================================================================
# STEP 10: Create dim_loanconditionevent
# ============================================================================
print("\n" + "-" * 40)
print("STEP 10: Creating dim_loanconditionevent...")
print("-" * 40)

S3_RAW_LOANCONDITIONEVENTS = "s3://arrivehome-bi-prod/raw-zone/loanconditionevents/"

try:
    loanconditionevents_df = spark.read.option("mergeSchema", "true").parquet(S3_RAW_LOANCONDITIONEVENTS)
    print(f"Loan condition events raw records: {loanconditionevents_df.count():,}")
    
    dim_loanconditionevent_df = loanconditionevents_df.select(
        safe_col(loanconditionevents_df, "_id").alias("loanconditionevent_id"),
        safe_col(loanconditionevents_df, "loanConditionId").alias("loancondition_id"),  # loanConditionId
        safe_col(loanconditionevents_df, "eventType").alias("event_type"),
        safe_col(loanconditionevents_df, "at", "timestamp").alias("event_at"),
        safe_col(loanconditionevents_df, "byId").alias("event_by_id"),  # byId
        safe_col(loanconditionevents_df, "text").alias("event_text"),
        F.lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S")).alias("_etl_loaded_at")
    )
    print(f"dim_loanconditionevent records: {dim_loanconditionevent_df.count():,}")
except Exception as e:
    print(f"Warning: Could not read loanconditionevents - {str(e)}")
    dim_loanconditionevent_df = None

# ============================================================================
# STEP 10b: Create dim_loanexception
# ============================================================================
print("\n" + "-" * 40)
print("STEP 10b: Creating dim_loanexception...")
print("-" * 40)

S3_RAW_LOANEXCEPTIONS = "s3://arrivehome-bi-prod/raw-zone/loanexceptions/"

try:
    loanexceptions_df = spark.read.option("mergeSchema", "true").parquet(S3_RAW_LOANEXCEPTIONS)
    print(f"Loan exceptions raw records: {loanexceptions_df.count():,}")

    dim_loanexception_df = loanexceptions_df.select(
        safe_col(loanexceptions_df, "_id").alias("loanexception_id"),
        ref_fk(loanexceptions_df, "loan").alias("loan_id"),
        safe_col(loanexceptions_df, "exceptionType").alias("exception_type"),
        safe_col(loanexceptions_df, "description").alias("description"),
        safe_col(loanexceptions_df, "requestedAt", "timestamp").alias("requested_at"),
        ref_fk(loanexceptions_df, "requestedBy").alias("requested_by_id"),
        safe_col(loanexceptions_df, "approvedAt", "timestamp").alias("approved_at"),
        ref_fk(loanexceptions_df, "approvedBy").alias("approved_by_id"),
        safe_col(loanexceptions_df, "deniedAt", "timestamp").alias("denied_at"),
        ref_fk(loanexceptions_df, "deniedBy").alias("denied_by_id"),
        safe_col(loanexceptions_df, "createdAt", "timestamp").alias("created_at"),
        ref_fk(loanexceptions_df, "createdBy").alias("created_by_id"),
        F.lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S")).alias("_etl_loaded_at"),
    )
    print(f"dim_loanexception records: {dim_loanexception_df.count():,}")
except Exception as e:
    print(f"Warning: Could not read loanexceptions - {str(e)}")
    dim_loanexception_df = None

# ============================================================================
# STEP 11: Create dim_holiday
# ============================================================================
print("\n" + "-" * 40)
print("STEP 11: Creating dim_holiday...")
print("-" * 40)

S3_RAW_HOLIDAYS = "s3://arrivehome-bi-prod/raw-zone/holidays/"

try:
    holidays_df = spark.read.option("mergeSchema", "true").parquet(S3_RAW_HOLIDAYS)
    print(f"Holidays raw records: {holidays_df.count():,}")
    
    dim_holiday_df = holidays_df.select(
        safe_col(holidays_df, "_id").alias("holiday_id"),
        safe_col(holidays_df, "date", "date").alias("holiday_date"),
        safe_col(holidays_df, "name").alias("holiday_name"),
        safe_col(holidays_df, "officeClosed").alias("office_closed"),
        safe_col(holidays_df, "lockDeskClosesAt").alias("lock_desk_closes_at"),
        # Add year/month for easy filtering
        F.year(safe_col(holidays_df, "date", "date")).alias("holiday_year"),
        F.month(safe_col(holidays_df, "date", "date")).alias("holiday_month"),
        F.lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S")).alias("_etl_loaded_at")
    )
    print(f"dim_holiday records: {dim_holiday_df.count():,}")
except Exception as e:
    print(f"Warning: Could not read holidays - {str(e)}")
    dim_holiday_df = None

# ============================================================================
# STEP 12: Create dim_loandocument
# ============================================================================
print("\n" + "-" * 40)
print("STEP 12: Creating dim_loandocument...")
print("-" * 40)

S3_RAW_LOANDOCUMENTS = "s3://arrivehome-bi-prod/raw-zone/loandocuments/"

try:
    loandocuments_df = spark.read.option("mergeSchema", "true").parquet(S3_RAW_LOANDOCUMENTS)
    print(f"Loan documents raw records: {loandocuments_df.count():,}")

    dim_loandocument_df = loandocuments_df.select(
        safe_col(loandocuments_df, "_id").alias("loandocument_id"),
        safe_col(loandocuments_df, "postgresId", "integer").alias("postgres_id"),
        safe_col(loandocuments_df, "postgresName").alias("postgres_name"),
        ref_fk(loandocuments_df, "loan").alias("loan_id"),
        safe_col(loandocuments_df, "name").alias("name"),
        safe_col(loandocuments_df, "description").alias("description"),
        safe_col(loandocuments_df, "contentType").alias("content_type"),
        safe_col(loandocuments_df, "size", "integer").alias("size_bytes"),
        safe_col(loandocuments_df, "pageCount", "integer").alias("page_count"),
        safe_col(loandocuments_df, "s3Key").alias("s3_key"),
        safe_col(loandocuments_df, "s3MultipartUploadId").alias("s3_multipart_upload_id"),
        safe_col(loandocuments_df, "sessionToken").alias("session_token"),
        safe_col(loandocuments_df, "uploadedAt", "timestamp").alias("uploaded_at"),
        ref_fk(loandocuments_df, "uploadedBy").alias("uploaded_by_id"),
        safe_col(loandocuments_df, "archivedAt", "timestamp").alias("archived_at"),
        ref_fk(loandocuments_df, "archivedBy").alias("archived_by_id"),
        safe_col(loandocuments_df, "uploadedToBlueWaterAt", "timestamp").alias("uploaded_to_bluewater_at"),
        safe_col(loandocuments_df, "blueWaterTransferMilliseconds", "double").alias("bluewater_transfer_milliseconds"),
        safe_bool(loandocuments_df, "blueWaterFinalVersion").alias("bluewater_final_version"),
        ref_fk(loandocuments_df, "loanCondition").alias("loancondition_id"),
        ref_fk(loandocuments_df, "loanConditionEvent").alias("loanconditionevent_id"),
        safe_col(loandocuments_df, "documentType").alias("document_type"),
        safe_col(loandocuments_df, "eepConditionTemplateName").alias("eep_condition_template_name"),
        safe_col(loandocuments_df, "hash").alias("file_hash"),
        F.lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S")).alias("_etl_loaded_at")
    )
    print(f"dim_loandocument records: {dim_loandocument_df.count():,}")
except Exception as e:
    print(f"Warning: Could not read loandocuments - {str(e)}")
    dim_loandocument_df = None

# ============================================================================
# STEP 13: Write all tables to S3 Staging Zone
# ============================================================================
print("\n" + "-" * 40)
print("STEP 13: Writing to S3 Staging Zone...")
print("-" * 40)

tables = {
    "dimensions/dim_date": dim_date_df,
    "dimensions/dim_product": dim_product_df,
    "dimensions/dim_correspondent": dim_correspondent_df,
    "dimensions/dim_user": dim_user_df,
    "dimensions/dim_investor": dim_investor_df,
    "dimensions/dim_loan": dim_loan_df,
    "dimensions/dim_borrower": dim_borrower_df,
    "dimensions/dim_eep_processing_entry": dim_eep_processing_entry_df,
    "dimensions/dim_loancondition": dim_loancondition_df,
    "dimensions/dim_loancondition_documenttype": dim_loancondition_documenttype_df,
    "dimensions/dim_loanconditionevent": dim_loanconditionevent_df,
    "dimensions/dim_loanexception": dim_loanexception_df,
    "dimensions/dim_holiday": dim_holiday_df,
    "dimensions/dim_loandocument": dim_loandocument_df,
    "facts/fact_loan_status": fact_loan_status_df,
    "facts/fact_loan_metrics": fact_loan_metrics_df,
}

MIN_VALID_DATE = "1582-10-15"

def fix_ancient_dates(dataframe):
    """Null out date/timestamp values before the Gregorian cutover (1582-10-15)
    to prevent WRITE_ANCIENT_DATETIME errors in Spark 3.x Parquet writes."""
    from pyspark.sql.types import DateType, TimestampType
    for field in dataframe.schema.fields:
        if isinstance(field.dataType, (DateType, TimestampType)):
            dataframe = dataframe.withColumn(
                field.name,
                F.when(F.col(field.name) < F.lit(MIN_VALID_DATE), F.lit(None))
                 .otherwise(F.col(field.name))
            )
    return dataframe

def _spark_schema_to_glue_columns(df):
    """Map Spark DataFrame schema to Glue/Athena column definitions."""
    return [(f.name, f.dataType.simpleString()) for f in df.schema.fields]


def _register_glue_table(database, table_name, s3_location, schema_columns):
    """Create or update Glue catalog table so Athena can query it immediately."""
    glue = boto3.client("glue")
    storage_descriptor = {
        "Columns": [{"Name": name, "Type": dtype} for name, dtype in schema_columns],
        "Location": s3_location.rstrip("/") + "/",
        "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
        "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
        "Compressed": True,
        "SerdeInfo": {
            "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
            "Parameters": {"serialization.format": "1"},
        },
        "StoredAsSubDirectories": False,
    }
    table_input = {
        "Name": table_name,
        "TableType": "EXTERNAL_TABLE",
        "Parameters": {"classification": "parquet", "EXTERNAL": "TRUE"},
        "StorageDescriptor": storage_descriptor,
        "PartitionKeys": [],
    }
    try:
        glue.get_database(Name=database)
    except glue.exceptions.EntityNotFoundException:
        print(f"  Creating Glue database: {database}")
        glue.create_database(DatabaseInput={"Name": database})

    try:
        glue.get_table(DatabaseName=database, Name=table_name)
        print(f"  Updating Glue catalog: {database}.{table_name}")
        glue.update_table(DatabaseName=database, TableInput=table_input)
    except glue.exceptions.EntityNotFoundException:
        print(f"  Creating Glue catalog: {database}.{table_name}")
        glue.create_table(DatabaseName=database, TableInput=table_input)

for table_path, df in tables.items():
    if df is None:
        print(f"Skipping {table_path} (no data available)")
        continue
    
    output_path = f"{S3_STAGING_PATH}{table_path}/"
    table_name = table_path.split("/")[-1]
    print(f"Writing {table_path}...")
    
    df = fix_ancient_dates(df)
    df.write.mode("overwrite").parquet(output_path)
    print(f"  ✓ Written to: {output_path}")

    try:
        _register_glue_table(
            GLUE_DATABASE,
            table_name,
            output_path,
            _spark_schema_to_glue_columns(df),
        )
        print(f"  ✓ Registered in Athena: {GLUE_DATABASE}.{table_name}")
    except Exception as reg_err:
        print(f"  ⚠ Could not register {table_name} in Glue catalog: {reg_err}")
        print(f"    Run staging-zone-crawler manually if needed.")

# ============================================================================
# Summary
# ============================================================================
print("\n" + "=" * 80)
print("TRANSFORM Raw → Staging - COMPLETE")
print("=" * 80)
print(f"\nTables created in S3 Staging Zone:")
print(f"  Dimensions:")
print(f"    - dim_date: {dim_date_df.count():,} records")
print(f"    - dim_product: {dim_product_df.count():,} records")
print(f"    - dim_correspondent: {dim_correspondent_df.count():,} records")
print(f"    - dim_user: {dim_user_df.count():,} records")
print(f"    - dim_investor: {dim_investor_df.count():,} records")
print(f"    - dim_loan: {dim_loan_df.count():,} records")
print(f"    - dim_borrower: {dim_borrower_df.count():,} records")
print(f"    - dim_eep_processing_entry: {dim_eep_processing_entry_df.count():,} records")
if dim_loancondition_df is not None:
    print(f"    - dim_loancondition: {dim_loancondition_df.count():,} records")
if dim_loancondition_documenttype_df is not None:
    print(f"    - dim_loancondition_documenttype: {dim_loancondition_documenttype_df.count():,} records")
if dim_loanconditionevent_df is not None:
    print(f"    - dim_loanconditionevent: {dim_loanconditionevent_df.count():,} records")
if dim_loanexception_df is not None:
    print(f"    - dim_loanexception: {dim_loanexception_df.count():,} records")
if dim_holiday_df is not None:
    print(f"    - dim_holiday: {dim_holiday_df.count():,} records")
if dim_loandocument_df is not None:
    print(f"    - dim_loandocument: {dim_loandocument_df.count():,} records")
print(f"  Facts:")
print(f"    - fact_loan_status: {fact_loan_status_df.count():,} records")
print(f"    - fact_loan_metrics: {fact_loan_metrics_df.count():,} records")
print(f"\nOutput location: {S3_STAGING_PATH}")
print("\nNEXT STEPS:")
print("1. Tables are auto-registered in Glue catalog (arrive_home)")
print("2. Query data via Athena Views")
print("=" * 80)

# Uncache
raw_df.unpersist()

job.commit()
print("\nJob committed successfully!")
