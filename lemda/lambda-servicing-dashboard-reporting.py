import os

import boto3

s3 = boto3.client("s3")

BUCKET_NAME = "servicing-dashboard-reporting"

# AppFlow drops both files into the same SharePoint folder under this prefix.
SOURCE_PREFIX = "ServicingDashboardReporting/"

# Route incoming files by filename substring → latest/ destination for Glue.
ROUTES = [
    {
        "name": "ServicingReporting",
        "match_substr": "unitedsecurity",
        "dest_prefix": "ServicingDashboardReporting/latest/",
        "dest_stem": "ServicingReporting",
        "preserve_extension": False,
    },
    {
        "name": "USALoanTrialBal",
        "match_substr": "usaloantrialbal",
        "dest_prefix": "USALoanTrialBal/latest/",
        "dest_stem": "USALoanTrialBal",
        "preserve_extension": True,
    },
]

_EXCEL_SUFFIXES = (".xlsx", ".xls")
_DEST_PREFIXES = {r["dest_prefix"] for r in ROUTES}


def _is_excel_key(key: str) -> bool:
    return key.lower().endswith(_EXCEL_SUFFIXES)


def _list_all_objects(bucket: str, prefix: str) -> list:
    objects = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        objects.extend(page.get("Contents", []))
    return objects


def _clear_previous_dest_files(bucket: str, dest_prefix: str, dest_stem: str) -> None:
    """Remove prior USALoanTrialBal.xls / .xlsx (or other stem variants) in latest/."""
    for ext in _EXCEL_SUFFIXES:
        key = dest_prefix + dest_stem + ext
        try:
            s3.delete_object(Bucket=bucket, Key=key)
            print(f"  Removed previous dest: {key}")
        except Exception:
            pass


def _build_dest_key(route: dict, source_key: str) -> str:
    dest_prefix = route["dest_prefix"]
    dest_stem = route["dest_stem"]
    if route.get("preserve_extension"):
        _, ext = os.path.splitext(source_key)
        ext = ext.lower()
        if ext not in _EXCEL_SUFFIXES:
            raise ValueError(f"Unsupported extension {ext!r} on {source_key}")
        return dest_prefix + dest_stem + ext
    return dest_prefix + dest_stem + ".xlsx"


def _sync_route(bucket: str, route: dict, candidates: list) -> dict:
    """Pick newest Excel object matching route and copy to latest/ destination."""
    name = route["name"]
    match_substr = route["match_substr"].lower()
    dest_prefix = route["dest_prefix"]

    matching = []
    for obj in candidates:
        key = obj["Key"]
        if any(key.startswith(p) for p in _DEST_PREFIXES):
            continue
        if not _is_excel_key(key):
            continue
        filename = os.path.basename(key).lower()
        if match_substr in filename:
            matching.append(obj)

    if not matching:
        return {
            "status": "not_found",
            "message": f"No file matching '{match_substr}' under {SOURCE_PREFIX}",
        }

    latest = max(matching, key=lambda x: x["LastModified"])
    source_key = latest["Key"]
    dest_key = _build_dest_key(route, source_key)

    print(f"[{name}] Latest match: {source_key}")
    print(f"[{name}] Destination: {dest_key}")

    _clear_previous_dest_files(bucket, dest_prefix, route["dest_stem"])

    s3.copy_object(
        Bucket=bucket,
        CopySource={"Bucket": bucket, "Key": source_key},
        Key=dest_key,
    )

    print(f"[{name}] Copied to: {dest_key}")

    return {
        "status": "ok",
        "source_key": source_key,
        "dest_key": dest_key,
    }


def lambda_handler(event, context):
    print("Event:", event)

    all_objects = _list_all_objects(BUCKET_NAME, SOURCE_PREFIX)
    if not all_objects:
        return {
            "statusCode": 404,
            "message": f"No objects found under {SOURCE_PREFIX}",
            "results": {},
        }

    print(f"Found {len(all_objects)} object(s) under {SOURCE_PREFIX}")

    results = {}
    errors = []

    for route in ROUTES:
        name = route["name"]
        print(f"\n--- Processing route: {name} ---")
        try:
            result = _sync_route(BUCKET_NAME, route, all_objects)
            results[name] = result
            if result["status"] != "ok":
                errors.append(f"{name}: {result['message']}")
        except Exception as e:
            print(f"ERROR processing {name}: {e}")
            results[name] = {"status": "error", "message": str(e)}
            errors.append(f"{name}: {e}")

    if errors:
        all_missing = all(r.get("status") == "not_found" for r in results.values())
        return {
            "statusCode": 404 if all_missing else 500,
            "message": "; ".join(errors),
            "results": results,
        }

    return {
        "statusCode": 200,
        "message": "All routes synced.",
        "results": results,
    }
