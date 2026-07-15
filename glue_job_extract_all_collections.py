"""
AWS Glue ETL Job: Extract All MongoDB Collections to S3
Purpose: Daily extraction of multiple collections with incremental updates
Collections: loans, correspondents, users, loanconditions, loanconditionevents, loandocuments, investors, holidays
Output: s3://arrivehome-bi-prod/raw-zone/{collection}/

Schedule: Run daily
Mode: Incremental (only new/updated records) or Full Refresh

Glue job parameter (required for loans / loandocuments PyMongo extract):
  --additional-python-modules  pymongo==4.6.3,dnspython==2.6.1
"""

import sys
import os
import json
import subprocess
import importlib
import zipfile
import urllib.request
from awsglue.transforms import *
from awsglue.dynamicframe import DynamicFrame
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType, TimestampType, StringType, StructType, StructField
)
from datetime import datetime, timedelta
import boto3

# Get job parameters
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Optional parameters with defaults
if '--EXTRACTION_MODE' in sys.argv:
    mode_args = getResolvedOptions(sys.argv, ['EXTRACTION_MODE'])
    EXTRACTION_MODE = mode_args['EXTRACTION_MODE']  # 'full' or 'incremental'
else:
    EXTRACTION_MODE = 'full'  # Default to full refresh

if '--DAYS_BACK' in sys.argv:
    days_args = getResolvedOptions(sys.argv, ['DAYS_BACK'])
    DAYS_BACK = int(days_args['DAYS_BACK'])  # For incremental: how many days to look back
else:
    DAYS_BACK = 1  # Default: last 24 hours

# Initialize
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# ============================================================================
# CONFIGURATION
# ============================================================================
S3_OUTPUT_BASE = "s3://arrivehome-bi-prod/raw-zone"
MONGODB_DATABASE = "arrivehome"
CONNECTION_NAME = "ArriveHome"

# Collections to extract with their timestamp fields
COLLECTIONS = {
    "loans": {
        "collection": "loans",
        "timestamp_field": "updatedAt",
        "output_path": f"{S3_OUTPUT_BASE}/loans/",
        # Glue connector schema inference fails (MapType vs StructType).
        "use_pymongo": True,
    },
    "correspondents": {
        "collection": "correspondents",
        "timestamp_field": "updatedAt",
        "output_path": f"{S3_OUTPUT_BASE}/correspondents/"
    },
    "users": {
        "collection": "users",
        "timestamp_field": "updatedAt",
        "output_path": f"{S3_OUTPUT_BASE}/users/"
    },
    "loanconditions": {
        "collection": "loanConditions",  # camelCase - exact MongoDB name
        "timestamp_field": "createdAt",
        "output_path": f"{S3_OUTPUT_BASE}/loanconditions/"
    },
    "loanconditionevents": {
        "collection": "loanConditionEvents",  # camelCase - exact MongoDB name
        "timestamp_field": "at",
        "output_path": f"{S3_OUTPUT_BASE}/loanconditionevents/"
    },
    "loandocuments": {
        "collection": "loanDocuments",  # camelCase - exact MongoDB name
        "timestamp_field": "uploadedAt",
        "output_path": f"{S3_OUTPUT_BASE}/loandocuments/",
        # Glue MongoDB connector schema inference fails on this collection
        # (MapType vs StructType ClassCastException) — use PyMongo instead.
        "use_pymongo": True,
    },
    "investors": {
        "collection": "investors",
        "timestamp_field": "updatedAt",
        "output_path": f"{S3_OUTPUT_BASE}/investors/"
    },
    "holidays": {
        "collection": "holidays",
        "timestamp_field": None,  # Holidays don't have updatedAt, use full refresh
        "output_path": f"{S3_OUTPUT_BASE}/holidays/"
    }
}

# Loan IDs known to have Decimal128 values - used for targeted diagnostic logging.
KNOWN_DECIMAL128_LOAN_IDS = [
    "6850b3258dc5d99b0cd74113",
    "68cb20ef437101d8b58a9e55",
    "68bf14dc39e6b0e83222a303",
    "6642875e2cd5591d83cfacef",
    "6642873a2cd5591d83cf20f4",  # ahLoanNumber 230217018 — secondMortgageUpb verification
]

# Fields to fix via PyMongo secondary read when the Glue connector nulls them
# due to Decimal128 vs Double schema inference mismatch.
DECIMAL128_FIELDS = {
    # Fields OR-combined in one PyMongo pass: null in any triggers a fetch for that row.
    "loans": ["purchasePrice", "appraisedValue"],
}
# Loan fields that need the same Decimal128 fix but must NOT be OR'd with price fields
# (many loans legitimately have null second mortgage UPB; combining would over-fetch).
LOANS_DECIMAL128_FIELDS_INDEPENDENT = ["secondMortgageUpb"]

# Rows per Spark DataFrame batch when reading large collections via PyMongo.
PYMONGO_SPARK_BATCH_SIZE = 25000
PYMONGO_INSTALL_DIR = "/tmp/glue_pymongo_libs"

# MongoDB Spark connector options — helps collections with BSON map fields.
MONGO_CONNECTOR_READ_OPTIONS = {
    "spark.mongodb.read.inferSchema.mapTypes.enabled": "true",
}

# ============================================================================
# HELPERS
# ============================================================================

_pymongo_ready = False
PYMONGO_WHEEL_PACKAGES = [
    ("dnspython", "2.6.1"),
    ("pymongo", "4.6.3"),
]


def _add_pymongo_to_syspath():
    for lib_dir in (PYMONGO_INSTALL_DIR, "/tmp/pymongo_libs"):
        if lib_dir not in sys.path:
            sys.path.insert(0, lib_dir)


def _import_pymongo_module():
    importlib.invalidate_caches()
    return importlib.import_module("pymongo")


def _download_pypi_wheel(package, version, target_dir):
    """Download and extract a wheel from PyPI (works when pip is blocked)."""
    meta_url = f"https://pypi.org/pypi/{package}/{version}/json"
    with urllib.request.urlopen(meta_url, timeout=120) as resp:
        meta = json.loads(resp.read().decode())

    py_tag = f"cp{sys.version_info.major}{sys.version_info.minor}"
    chosen = None
    for url_info in meta.get("urls", []):
        filename = url_info.get("filename", "")
        if not filename.endswith(".whl"):
            continue
        if package == "dnspython" and "py3-none-any" in filename:
            chosen = url_info
            break
        if package == "pymongo" and py_tag in filename and "manylinux" in filename:
            chosen = url_info
            break

    if not chosen:
        for url_info in meta.get("urls", []):
            if url_info.get("filename", "").endswith(".whl"):
                chosen = url_info
                break

    if not chosen:
        raise RuntimeError(f"No wheel on PyPI for {package}=={version}")

    os.makedirs(target_dir, exist_ok=True)
    wheel_path = os.path.join(target_dir, chosen["filename"])
    print(f"[setup] Downloading {package} wheel: {chosen['filename']}")
    urllib.request.urlretrieve(chosen["url"], wheel_path)
    with zipfile.ZipFile(wheel_path, "r") as zf:
        zf.extractall(target_dir)


def ensure_pymongo():
    """
    Import pymongo, installing into /tmp if the Glue job lacks
    --additional-python-modules pymongo,dnspython.
    """
    global _pymongo_ready
    if _pymongo_ready:
        return True

    _add_pymongo_to_syspath()
    try:
        _import_pymongo_module()
        _pymongo_ready = True
        return True
    except ImportError:
        pass

    os.makedirs(PYMONGO_INSTALL_DIR, exist_ok=True)
    print("[setup] pymongo not found — attempting install...")

    # Method 1: pip --target
    try:
        result = subprocess.run(
            [
                sys.executable, "-m", "pip", "install",
                "pymongo==4.6.3", "dnspython==2.6.1",
                "--target", PYMONGO_INSTALL_DIR,
                "--no-cache-dir", "-q",
            ],
            check=True,
            timeout=300,
            capture_output=True,
            text=True,
        )
        if result.stdout:
            print(f"[setup] pip stdout: {result.stdout[:500]}")
        _add_pymongo_to_syspath()
        _import_pymongo_module()
        print("[setup] pymongo installed via pip --target")
        _pymongo_ready = True
        return True
    except Exception as e:
        print(f"[setup] pip --target failed: {e}")

    # Method 2: download wheels directly from PyPI (no pip)
    try:
        for package, version in PYMONGO_WHEEL_PACKAGES:
            _download_pypi_wheel(package, version, PYMONGO_INSTALL_DIR)
        _add_pymongo_to_syspath()
        _import_pymongo_module()
        print("[setup] pymongo installed via PyPI wheel download")
        _pymongo_ready = True
        return True
    except Exception as e:
        print(f"[setup] PyPI wheel download failed: {e}")

    raise RuntimeError(
        "Could not install pymongo in this Glue job. "
        "In the Glue job → Job details → Advanced properties, add:\n"
        "  --additional-python-modules  pymongo==4.6.3,dnspython==2.6.1\n"
        "Ensure the job network can reach pypi.org (for auto-install) or use that parameter."
    )


extraction_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
extraction_date = datetime.now().strftime("%Y-%m-%d")

print("=" * 80)
print("EXTRACT ALL MONGODB COLLECTIONS TO S3")
print("=" * 80)
print(f"Timestamp: {extraction_timestamp}")
print(f"Mode: {EXTRACTION_MODE}")
if EXTRACTION_MODE == 'incremental':
    print(f"Days Back: {DAYS_BACK}")
print(f"Collections: {list(COLLECTIONS.keys())}")

# Install pymongo up front when any collection needs it (loans, loandocuments).
if any(cfg.get("use_pymongo") for cfg in COLLECTIONS.values()):
    try:
        ensure_pymongo()
    except RuntimeError as setup_err:
        print(f"[setup] ERROR: {setup_err}")
        raise


def get_mongo_uri():
    """Resolve MongoDB URI from the Glue connection (shared by PyMongo paths)."""
    glue_client = boto3.client("glue")
    conn_resp = glue_client.get_connection(Name=CONNECTION_NAME)
    props = conn_resp["Connection"]["ConnectionProperties"]
    mongo_uri = props.get("CONNECTION_URL", props.get("JDBC_CONNECTION_URL", ""))
    username = props.get("USERNAME", "")
    password = props.get("PASSWORD", "")
    if username and password and mongo_uri and "@" not in mongo_uri:
        scheme, host = mongo_uri.split("://", 1)
        mongo_uri = f"{scheme}://{username}:{password}@{host}"
    return mongo_uri


def bson_to_flat_value(value):
    """Convert BSON values to Spark-safe scalars (strings avoid Long/Double merge errors)."""
    try:
        from bson import ObjectId, Decimal128
    except ImportError:
        ObjectId = type(None)
        Decimal128 = type(None)

    if value is None:
        return None
    if isinstance(value, ObjectId):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        return str(value)
    if Decimal128 is not type(None) and isinstance(value, Decimal128):
        return str(value)
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    if isinstance(value, dict):
        if "$oid" in value and len(value) == 1:
            return str(value["$oid"])
        if "_id" in value:
            oid = value["_id"]
            return str(oid) if isinstance(oid, ObjectId) else oid
        return json.dumps(value, default=str)
    if isinstance(value, list):
        return json.dumps(value, default=str)
    return str(value) if value is not None else None


def _is_likely_reference(value):
    """Small subdocuments with _id are FK refs (correspondent, uploadedBy, etc.)."""
    return isinstance(value, dict) and "_id" in value and len(value) <= 6


def flatten_mongo_document(doc):
    """Flatten a MongoDB document to top-level Spark-safe columns."""
    try:
        from bson import ObjectId
    except ImportError:
        ObjectId = type(None)

    flat = {}
    for key, value in doc.items():
        if key == "_id":
            flat["_id"] = str(value)
            continue
        if isinstance(value, ObjectId):
            flat[key] = str(value)
            continue
        if _is_likely_reference(value):
            flat[f"{key}Id"] = str(value["_id"])
            continue
        if isinstance(value, list):
            # Serialize arrays of subdocuments as JSON (avoids Spark struct merge errors).
            if value and all(isinstance(x, dict) for x in value):
                flat[key] = json.dumps(
                    [{k: bson_to_flat_value(v) for k, v in item.items()} for item in value],
                    default=str,
                )
            else:
                flat[key] = bson_to_flat_value(value)
            continue
        flat[key] = bson_to_flat_value(value)
    return flat


def _prefer_field_name(existing, new):
    """Prefer Mongo-style camelCase when the same field appears with different casing."""
    if existing == new:
        return existing

    def score(name):
        # Reward interior capitals (camelCase); slight preference for longer names.
        return (1 if any(c.isupper() for c in name[1:]) else 0, len(name))

    return existing if score(existing) >= score(new) else new


def register_canonical_keys(row, canonical_keys):
    """One column name per field (case-insensitive) — avoids union duplicate columns."""
    for key in row.keys():
        lower = key.lower()
        if lower not in canonical_keys:
            canonical_keys[lower] = key
        else:
            canonical_keys[lower] = _prefer_field_name(canonical_keys[lower], key)


def merge_row_to_canonical(row, canonical_keys):
    """Merge row values onto canonical column names (case-insensitive keys)."""
    merged = {}
    for key, value in row.items():
        canon = canonical_keys[key.lower()]
        if canon not in merged or merged[canon] is None:
            merged[canon] = value
        elif value is not None and merged[canon] in (None, ""):
            merged[canon] = value
    return merged


def coerce_value_to_string(value):
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        return json.dumps(value, default=str)
    if isinstance(value, bool):
        return str(value).lower()
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


def create_dataframe_from_flat_rows(rows, mongo_collection_name, canonical_keys):
    """
    Build a Spark DataFrame with all-string columns (no schema inference).
    Prevents DoubleType/LongType merge and CANNOT_DETERMINE_TYPE errors across batches.
    """
    if not rows:
        return None

    for row in rows:
        register_canonical_keys(row, canonical_keys)

    all_keys = sorted(canonical_keys.values(), key=lambda name: name.lower())
    schema = StructType([StructField(k, StringType(), True) for k in all_keys])
    string_rows = []
    for row in rows:
        merged = merge_row_to_canonical(row, canonical_keys)
        str_row = {k: coerce_value_to_string(merged.get(k)) for k in all_keys}
        string_rows.append(str_row)

    return spark.createDataFrame(string_rows, schema=schema)


def read_collection_via_pymongo(mongo_collection_name, timestamp_field, mode, days_back):
    """
    Read a MongoDB collection via PyMongo and return a Spark DataFrame.
    Bypasses Glue connector schema inference (fixes MapType/StructType errors).
    """
    ensure_pymongo()
    pymongo = _import_pymongo_module()

    mongo_uri = get_mongo_uri()
    if not mongo_uri:
        raise RuntimeError("MongoDB URI is empty — cannot use PyMongo extraction.")

    print(f"  [pymongo-extract] Connecting to {MONGODB_DATABASE}.{mongo_collection_name}...")
    client = pymongo.MongoClient(mongo_uri, serverSelectionTimeoutMS=60000)
    coll = client[MONGODB_DATABASE][mongo_collection_name]

    query = {}
    if mode == "incremental" and timestamp_field:
        cutoff_date = datetime.now() - timedelta(days=days_back)
        query[timestamp_field] = {"$gte": cutoff_date}
        print(f"  [pymongo-extract] Filter: {timestamp_field} >= {cutoff_date.isoformat()}")

    cursor = coll.find(query).batch_size(2000)
    spark_batches = []
    current_batch = []
    canonical_keys = {}
    total_read = 0

    for doc in cursor:
        flat = flatten_mongo_document(doc)
        register_canonical_keys(flat, canonical_keys)
        current_batch.append(flat)
        if len(current_batch) >= PYMONGO_SPARK_BATCH_SIZE:
            spark_batches.append(
                create_dataframe_from_flat_rows(
                    current_batch, mongo_collection_name, canonical_keys
                )
            )
            total_read += len(current_batch)
            print(f"  [pymongo-extract] Loaded {total_read:,} documents...")
            current_batch = []

    if current_batch:
        spark_batches.append(
            create_dataframe_from_flat_rows(
                current_batch, mongo_collection_name, canonical_keys
            )
        )
        total_read += len(current_batch)

    cursor.close()
    client.close()

    if not spark_batches:
        print("  [pymongo-extract] No documents matched query.")
        return spark.createDataFrame([], StructType([StructField("_id", StringType(), True)]))

    print(f"  [pymongo-extract] Building Spark DataFrame from {len(spark_batches)} batch(es)...")
    df = spark_batches[0]
    for batch_df in spark_batches[1:]:
        df = df.unionByName(batch_df, allowMissingColumns=True)

    print(f"  [pymongo-extract] Total documents read: {total_read:,}")
    return df


def finalize_and_write_dataframe(df, collection_name, collection, output_path, mode):
    """Shared post-read steps: dedupe columns, dates, metadata, write to S3."""
    original_cols = df.columns
    seen_lower = set()
    new_cols = []
    for idx, c in enumerate(original_cols):
        lower = c.lower()
        if lower in seen_lower:
            new_name = f"{c}__dup{idx}"
        else:
            new_name = c
            seen_lower.add(lower)
        new_cols.append(new_name)

    if new_cols != original_cols:
        print("  Detected duplicate/ambiguous columns. Renaming:")
        for old, new in zip(original_cols, new_cols):
            if old != new:
                print(f"    - {old} -> {new}")
        df = df.toDF(*new_cols)

    record_count = df.count()
    print(f"  Records found: {record_count:,}")

    if record_count == 0:
        print("  No records to extract, skipping...")
        return {"collection": collection_name, "records": 0, "status": "skipped"}

    MIN_VALID_DATE = "1582-10-15"
    print(f"  Fixing dates before {MIN_VALID_DATE} (Gregorian calendar cutover)...")

    date_cols = []
    for field in df.schema.fields:
        if isinstance(field.dataType, (DateType, TimestampType)):
            date_cols.append(field.name)

    if date_cols:
        print(f"  Found {len(date_cols)} date/timestamp columns to validate")
        for col_name in date_cols:
            df = df.withColumn(
                col_name,
                F.when(F.col(col_name) < F.lit(MIN_VALID_DATE), F.lit(None))
                 .otherwise(F.col(col_name))
            )
    else:
        print("  No date/timestamp columns found")

    df = df.withColumn("_etl_extracted_at", F.lit(extraction_timestamp))
    df = df.withColumn("_etl_extraction_date", F.lit(extraction_date))
    df = df.withColumn("_etl_source", F.lit("mongodb"))
    df = df.withColumn("_etl_collection", F.lit(collection))

    num_partitions = max(1, record_count // 10000)
    if num_partitions > 200:
        num_partitions = 200
    print(f"  Repartitioning to {num_partitions} partitions...")
    df = df.repartition(num_partitions)

    output_frame = DynamicFrame.fromDF(df, glueContext, f"output_{collection_name}")

    print("  Writing to S3...")
    if mode == "full":
        try:
            glueContext.purge_s3_path(output_path, options={"retentionPeriod": 0})
            print("  Purged existing data")
        except Exception as e:
            print(f"  Note: Could not purge existing data: {e}")

    glueContext.write_dynamic_frame.from_options(
        frame=output_frame,
        connection_type="s3",
        connection_options={"path": output_path},
        format="parquet",
        format_options={"compression": "snappy"},
        transformation_ctx=f"write_{collection_name}"
    )

    print("  Verifying write succeeded...")
    written_count = record_count
    try:
        written_df = spark.read.parquet(output_path)
        written_count = written_df.count()
        print(f"  ✓ Verification: {written_count:,} records written to S3")
        if written_count != record_count:
            print("  ⚠ WARNING: Record count mismatch!")
            print(f"     Expected: {record_count:,}")
            print(f"     Written:  {written_count:,}")
            print(f"     Missing:  {record_count - written_count:,} records")
        else:
            print(f"  ✓ All {record_count:,} records successfully written!")
    except Exception as e:
        print(f"  ⚠ Could not verify write: {e}")

    print(f"  SUCCESS: {record_count:,} records written to {output_path}")
    return {
        "collection": collection_name,
        "records": record_count,
        "status": "success",
        "written": written_count,
    }

# All type-name tokens that appear as field names inside a choice-resolved
# struct (produced by resolveChoice(make_struct)).  Includes MongoDB-specific
# variants so we don't accidentally skip real numeric fields.
_CHOICE_STRUCT_TYPE_TOKENS = frozenset({
    # Standard Spark/Glue types
    'byte', 'short', 'int', 'long', 'float', 'double', 'decimal',
    'string', 'boolean', 'date', 'timestamp', 'binary',
    'struct', 'array', 'map',
    # MongoDB / BSON-specific variants that appear in ChoiceType structs
    'null',        # BSON null  — present when some docs have a null value
    'decimal128',  # MongoDB NumberDecimal stored as Decimal128
    'objectid',
    'bindata',
})

def flatten_choice_structs(df):
    """After resolveChoice(make_struct), any field that had mixed BSON types
    becomes a struct whose sub-field names are the Spark/BSON type tokens above.

    This function:
    1. Detects those auto-generated structs (vs legitimate nested documents).
    2. Coalesces all non-null sub-fields back to a single string column so the
       transform job can cast them to the right numeric type.

    The 'null' variant is intentionally skipped during coalesce — it carries no
    value.  The 'decimal128' variant may itself be a struct<value:string> (BSON
    extended JSON wrapper), so we recurse one level deeper for nested structs.
    """
    from pyspark.sql.types import StructType as ST

    for field in df.schema.fields:
        if not isinstance(field.dataType, ST):
            continue

        inner_names_lower = {f.name.lower() for f in field.dataType.fields}

        # If ANY inner field name is NOT a known type token, this is a real
        # nested document (e.g. borrower, correspondent) — leave it alone.
        if not inner_names_lower.issubset(_CHOICE_STRUCT_TYPE_TOKENS):
            continue

        print(f"  [flatten] Detected choice struct: {field.name} → {field.dataType}")

        parts = []
        for inner in field.dataType.fields:
            if inner.name.lower() == 'null':
                # Null variant contributes nothing to coalesce
                continue

            path = f"`{field.name}`.`{inner.name}`"

            if isinstance(inner.dataType, ST):
                # Nested struct — Decimal128 often arrives as struct<value:string>
                # or struct<$numberDecimal:string>.  Grab every leaf sub-field.
                for sub in inner.dataType.fields:
                    parts.append(F.col(f"{path}.`{sub.name}`").cast("string"))
                    print(f"      adding nested leaf: {path}.{sub.name}")
            else:
                parts.append(F.col(path).cast("string"))
                print(f"      adding scalar: {path}")

        if parts:
            df = df.withColumn(field.name, F.coalesce(*parts))
            print(f"  [flatten] Replaced {field.name} with coalesce of {len(parts)} candidate(s)")

    return df


def clean_decimal128_strings(df, fields):
    """Some MongoDB drivers serialize Decimal128 as '{value=55129409000.00}'
    instead of a clean number string.  Strip the wrapper so Spark can cast it.

    Cast through string first so double/decimal columns are normalized too, not
    only columns already inferred as StringType.
    """
    for col_name in fields:
        if col_name not in df.columns:
            continue
        c = F.col(col_name).cast("string")
        df = df.withColumn(
            col_name,
            F.when(
                c.rlike(r'^\{value=.*\}$'),
                F.regexp_extract(c, r'^\{value=(.*)\}$', 1)
            ).otherwise(c)
        )
    return df


def fix_decimal128_via_pymongo(df, mongo_collection_name, fields_to_fix):
    """
    The Glue MongoDB connector samples a subset of documents for schema
    inference.  If NONE of the sampled docs have a Decimal128 value for a
    field, the connector infers that field as DoubleType and silently returns
    NULL for any document where the actual BSON type is Decimal128.

    This function detects which records have unexpected NULLs in the listed
    fields, queries MongoDB directly via PyMongo (bypassing Glue's schema
    inference entirely), and patches those NULLs back with the real values.

    Prerequisites:
        - Add  pymongo  to the Glue job's --additional-python-modules setting.
        - The Glue connection (CONNECTION_NAME) must expose CONNECTION_URL,
          USERNAME, and PASSWORD in its ConnectionProperties.
    """
    try:
        ensure_pymongo()
        import pymongo
        from bson import ObjectId
    except Exception as e:
        print(f"  [pymongo] pymongo not available – skipping Decimal128 fix: {e}")
        return df

    # Only operate on fields that actually exist in the DataFrame
    present = [f for f in fields_to_fix if f in df.columns]
    if not present:
        return df

    # Cast present fields to string so coalesce works uniformly below
    for f in present:
        df = df.withColumn(f, F.col(f).cast("string"))

    def _field_missing_or_blank(col_name):
        c = F.col(col_name)
        return c.isNull() | (F.length(F.trim(c)) == 0)

    # Rows where Glue left the field empty (null, "", or whitespace-only)
    null_cond = _field_missing_or_blank(present[0])
    for f in present[1:]:
        null_cond = null_cond | _field_missing_or_blank(f)

    null_ids_rows = (
        df.filter(null_cond)
        .select(F.col("_id").cast("string").alias("_id_key"))
        .distinct()
        .collect()
    )
    if not null_ids_rows:
        print(f"  [pymongo] No empty-string/NULL records for {present} – nothing to fix.")
        return df

    null_id_strs = [r["_id_key"] for r in null_ids_rows]
    print(f"  [pymongo] {len(null_id_strs)} distinct loan _id keys need patch for {present}; "
          f"fetching via PyMongo (batched)…")

    try:
        mongo_uri = get_mongo_uri()
        print(f"  [pymongo] Using URI scheme: {mongo_uri.split('://')[0]}://***")
    except Exception as e:
        print(f"  [pymongo] Cannot get connection properties: {e}")
        return df

    if not mongo_uri:
        print("  [pymongo] Empty URI – skipping fix.")
        return df

    # ── Query MongoDB ──────────────────────────────────────────────────────────
    try:
        client = pymongo.MongoClient(mongo_uri, serverSelectionTimeoutMS=30000)
        coll   = client[MONGODB_DATABASE][mongo_collection_name]

        # Try ObjectId first, fall back to plain string for non-ObjectId _ids
        bson_ids = []
        for id_str in null_id_strs:
            try:
                bson_ids.append(ObjectId(str(id_str)))
            except Exception:
                bson_ids.append(str(id_str))

        # $convert handles Decimal128/double/int more reliably than $toString alone.
        project = {"_id": 1}
        for f in present:
            project[f] = {
                "$convert": {
                    "input": f"${f}",
                    "to": "string",
                    "onError": None,
                    "onNull": None,
                }
            }

        batch_size = 3500
        fix_rows = []
        total_fetched = 0
        for i in range(0, len(bson_ids), batch_size):
            batch = bson_ids[i : i + batch_size]
            docs = list(
                coll.aggregate(
                    [
                        {"$match": {"_id": {"$in": batch}}},
                        {"$project": project},
                    ]
                )
            )
            total_fetched += len(docs)
            for doc in docs:
                row = {"_id_join_key": str(doc["_id"])}
                for f in present:
                    v = doc.get(f)
                    row[f"_fix_{f}"] = str(v).strip() if v is not None and str(v).strip() != "" else None
                fix_rows.append(row)

        client.close()
        print(f"  [pymongo] Retrieved {total_fetched} documents from MongoDB across "
              f"{(len(bson_ids) + batch_size - 1) // batch_size} batch(es).")

        if not fix_rows:
            return df

        fix_schema = StructType(
            [StructField("_id_join_key", StringType(), True)]
            + [StructField(f"_fix_{f}", StringType(), True) for f in present]
        )
        fix_df = spark.createDataFrame(fix_rows, fix_schema)

        # Join on normalized string _id — Spark _id type often mismatches join(fix._id string)
        # which previously left every _fix_* column NULL.
        df = df.withColumn("_id_join_key", F.col("_id").cast("string")).join(
            fix_df, on="_id_join_key", how="left"
        ).drop("_id_join_key")

        for f in present:
            df = df.withColumn(
                f,
                F.when(
                    F.col(f).isNotNull() & (F.length(F.trim(F.col(f))) > 0),
                    F.col(f),
                ).otherwise(F.col(f"_fix_{f}")),
            )
        df = df.drop(*[f"_fix_{f}" for f in present])

        # Final diagnostic sample
        if mongo_collection_name == "loans":
            sample = df.filter(
                F.col("_id").cast("string").isin(KNOWN_DECIMAL128_LOAN_IDS)
            ).select("_id", *present).collect()
            print(f"  [pymongo] Post-fix sample:")
            for row in sample:
                vals = {f: row[f] for f in present}
                print(f"    _id={row['_id']}  {vals}")

        print(f"  [pymongo] Decimal128 fix complete for: {present}")

    except Exception as e:
        import traceback
        print(f"  [pymongo] Error: {e}")
        print(traceback.format_exc())

    return df

# ============================================================================
# EXTRACTION FUNCTION
# ============================================================================
def extract_collection(collection_name, config, mode, days_back):
    """
    Extract a single collection from MongoDB to S3.
    Supports full refresh or incremental mode.
    """
    print(f"\n{'='*60}")
    print(f"Extracting: {collection_name}")
    print(f"{'='*60}")
    
    collection = config['collection']
    timestamp_field = config['timestamp_field']
    output_path = config['output_path']
    
    print(f"  MongoDB Collection: {collection}")
    print(f"  Output Path: {output_path}")
    print(f"  Mode: {mode}")

    # PyMongo path — avoids Glue connector MapType/StructType schema inference crash.
    if config.get("use_pymongo"):
        try:
            print("  Using PyMongo extraction (use_pymongo=True)...")
            df = read_collection_via_pymongo(collection, timestamp_field, mode, days_back)
            if collection_name == "loans":
                financial_fields = {
                    "purchasePrice", "appraisedValue",
                    "firstMortgageTotalLoanAmount", "firstMortgageBaseLoanAmount",
                    "secondMortgageUpb",
                }
                df = clean_decimal128_strings(df, list(financial_fields))
            return finalize_and_write_dataframe(
                df, collection_name, collection, output_path, mode
            )
        except Exception as e:
            print(f"  ERROR (PyMongo): {str(e)}")
            import traceback
            print(f"  Traceback: {traceback.format_exc()}")
            return {"collection": collection_name, "records": 0, "status": "error", "error": str(e)}
    
    try:
        # Build connection options
        connection_options = {
            "connectionName": CONNECTION_NAME,
            "database": MONGODB_DATABASE,
            "collection": collection,
            **MONGO_CONNECTOR_READ_OPTIONS,
        }
        
        # Incremental filter pipeline (only $match — Glue's connector reliably
        # supports $match; $addFields/$project have proven unreliable here).
        if mode == 'incremental' and timestamp_field:
            cutoff_date = datetime.now() - timedelta(days=days_back)
            cutoff_str = cutoff_date.strftime("%Y-%m-%dT%H:%M:%S.000Z")
            connection_options["pipeline"] = (
                f'[{{"$match": {{"{timestamp_field}": '
                f'{{"$gte": {{"$date": "{cutoff_str}"}}}}}}}}]'
            )
            print(f"  Filter: {timestamp_field} >= {cutoff_str}")
        elif mode == 'incremental' and not timestamp_field:
            print(f"  Note: No timestamp field - using full refresh for this collection")

        # Read from MongoDB
        print(f"  Reading from MongoDB...")
        dynamic_frame = glueContext.create_dynamic_frame.from_options(
            connection_type="mongodb",
            connection_options=connection_options,
            transformation_ctx=f"read_{collection_name}"
        )

        # ── Diagnostic: log raw DynamicFrame schema ──────────────────────────
        raw_schema_str = str(dynamic_frame.schema())
        print(f"  Raw DynamicFrame schema (first 2000 chars): {raw_schema_str[:2000]}")

        # Detect ChoiceType fields BEFORE resolveChoice so we can report them.
        choice_fields = [
            f.name for f in dynamic_frame.schema()
            if "ChoiceType" in str(type(f.dataType))
        ]
        if choice_fields:
            print(f"  ChoiceType field(s) detected: {choice_fields}")
        else:
            print(f"  No ChoiceType fields detected (Decimal128 records may have "
                  f"been missed during schema sampling if purchasePrice is absent).")

        # resolveChoice(make_struct) preserves every BSON-type variant in a
        # struct so no data is silently dropped.  flatten_choice_structs then
        # collapses those structs back to scalar string columns.
        dynamic_frame = dynamic_frame.resolveChoice(choice="make_struct")

        df = dynamic_frame.toDF()

        # --------------------------------------------------------------------
        # Handle duplicate / ambiguous column names EARLY — before any
        # withColumn / col() calls that would fail with AMBIGUOUS_REFERENCE.
        # --------------------------------------------------------------------
        original_cols = df.columns
        seen_lower = set()
        new_cols = []
        for idx, c in enumerate(original_cols):
            lower = c.lower()
            if lower in seen_lower:
                new_name = f"{c}__dup{idx}"
            else:
                new_name = c
                seen_lower.add(lower)
            new_cols.append(new_name)

        if new_cols != original_cols:
            print("  Detected duplicate/ambiguous columns. Renaming:")
            for old, new in zip(original_cols, new_cols):
                if old != new:
                    print(f"    - {old} -> {new}")
            df = df.toDF(*new_cols)

        # ── Diagnostic: log DataFrame schema for financial fields ─────────────
        financial_fields = {
            "purchasePrice", "appraisedValue",
            "firstMortgageTotalLoanAmount", "firstMortgageBaseLoanAmount",
            "secondMortgageUpb",
        }
        print(f"  DataFrame schema for key fields:")
        for fld in df.schema.fields:
            if fld.name in financial_fields:
                print(f"    {fld.name}: {fld.dataType}")

        def log_target_loan(label):
            """Log every column for ahLoanNumber 25090819949 at a given stage."""
            if collection_name != "loans":
                return
            ah_col = next((c for c in df.columns
                           if c.lower() == "ahloannumber"), None)
            if not ah_col:
                print(f"  [LOAN-25090819949] [{label}] ahLoanNumber column not found")
                return
            try:
                rows = df.filter(F.col(ah_col) == "25090819949").collect()
                if not rows:
                    print(f"  [LOAN-25090819949] [{label}] record not found in DataFrame")
                    return
                row = rows[0]
                # Print every field so nothing is hidden
                print(f"  [LOAN-25090819949] [{label}] Full record:")
                for col_name in df.columns:
                    try:
                        val = row[col_name]
                        if val is not None:
                            print(f"    {col_name} = {val}")
                    except Exception:
                        pass
            except Exception as e:
                print(f"  [LOAN-25090819949] [{label}] Error: {e}")

        # Stage 1 — straight out of the Glue connector (before any Spark fixes)
        log_target_loan("AFTER GLUE READ")

        # Always run flatten_choice_structs — it is now safe to run even when
        # no ChoiceType was detected (it will simply skip non-choice structs).
        df = flatten_choice_structs(df)

        # Extra safety: strip any '{value=...}' wrappers that some MongoDB
        # driver versions produce for Decimal128 when serialised as strings.
        df = clean_decimal128_strings(df, list(financial_fields))

        # Stage 2 — after flatten + clean (before PyMongo fix)
        log_target_loan("AFTER FLATTEN/CLEAN")

        # ── PyMongo secondary read for confirmed Decimal128 fields ────────────
        # The Glue connector silently returns NULL for Decimal128 values when
        # its schema sampling missed those records and inferred DoubleType.
        # PyMongo bypasses schema inference and fetches the exact BSON value.
        if collection_name in DECIMAL128_FIELDS:
            df = fix_decimal128_via_pymongo(
                df, collection, DECIMAL128_FIELDS[collection_name]
            )

        # secondMortgageUpb: same Glue Double vs Decimal128 NULL issue, but most loans
        # have no second lien — run a separate PyMongo pass so we only fetch rows
        # where this field is null (not every row missing purchase/appraised).
        if collection_name == "loans":
            for fld in LOANS_DECIMAL128_FIELDS_INDEPENDENT:
                df = fix_decimal128_via_pymongo(df, collection, [fld])

        # Stage 3 — after PyMongo fix (this is what gets written to S3)
        log_target_loan("AFTER PYMONGO FIX")
        return finalize_and_write_dataframe(
            df, collection_name, collection, output_path, mode
        )
        
    except Exception as e:
        err_msg = str(e)
        # Auto-fallback when Glue connector cannot infer schema (MapType vs StructType).
        if "MapType cannot be cast" in err_msg or "ClassCastException" in err_msg:
            print("  Glue schema inference failed (MapType/StructType) — retrying via PyMongo...")
            try:
                ensure_pymongo()
                df = read_collection_via_pymongo(collection, timestamp_field, mode, days_back)
                if collection_name == "loans":
                    financial_fields = {
                        "purchasePrice", "appraisedValue",
                        "firstMortgageTotalLoanAmount", "firstMortgageBaseLoanAmount",
                        "secondMortgageUpb",
                    }
                    df = clean_decimal128_strings(df, list(financial_fields))
                return finalize_and_write_dataframe(
                    df, collection_name, collection, output_path, mode
                )
            except Exception as pymongo_err:
                print(f"  ERROR (PyMongo fallback): {pymongo_err}")
                import traceback
                print(f"  Traceback: {traceback.format_exc()}")
                print("  TIP: Enable outbound internet on the Glue job, or set:")
                print("       --additional-python-modules  pymongo==4.6.3,dnspython==2.6.1")
                return {
                    "collection": collection_name,
                    "records": 0,
                    "status": "error",
                    "error": str(pymongo_err),
                }

        print(f"  ERROR: {err_msg}")
        import traceback
        print(f"  Traceback: {traceback.format_exc()}")
        return {"collection": collection_name, "records": 0, "status": "error", "error": err_msg}

# ============================================================================
# MAIN EXTRACTION LOOP
# ============================================================================
results = []

for name, config in COLLECTIONS.items():
    result = extract_collection(name, config, EXTRACTION_MODE, DAYS_BACK)
    results.append(result)

# ============================================================================
# SUMMARY
# ============================================================================
print("\n" + "=" * 80)
print("EXTRACTION SUMMARY")
print("=" * 80)

total_records = 0
total_written = 0
success_count = 0
error_count = 0
skipped_count = 0

for result in results:
    status_icon = "✓" if result['status'] == 'success' else "⚠" if result['status'] == 'skipped' else "✗"
    print(f"  {status_icon} {result['collection']}: {result['records']:,} records ({result['status']})")
    
    if result['status'] == 'success':
        total_records += result['records']
        total_written += result.get('written', result['records'])
        success_count += 1
    elif result['status'] == 'skipped':
        skipped_count += 1
    elif result['status'] == 'error':
        error_count += 1
        if 'error' in result:
            print(f"      Error: {result['error'][:200]}")

print(f"\nTotal Records Extracted: {total_records:,}")
if total_written != total_records:
    print(f"Total Records Written: {total_written:,} (⚠ {total_records - total_written:,} missing)")
else:
    print(f"Total Records Written: {total_written:,} (✓ All records written)")
print(f"Collections Succeeded: {success_count}/{len(COLLECTIONS)}")
if skipped_count > 0:
    print(f"Collections Skipped: {skipped_count}")
if error_count > 0:
    print(f"Collections Failed: {error_count}")

print(f"\nOutput Location: {S3_OUTPUT_BASE}/")
print(f"Extraction Mode: {EXTRACTION_MODE}")
print(f"Extraction Time: {extraction_timestamp}")

print("\nNEXT STEPS:")
print("1. Run Transform job to create staging tables")
print("2. Run Glue Crawler to update Data Catalog")
print("3. Query via Athena or refresh report tables")
print("=" * 80)

job.commit()
print("\nJob committed successfully!")
