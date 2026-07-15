# Scheduling Extract and Transform – Avoiding NULLs (e.g. purchase_price)

## What’s going wrong

When **Extract** and **Transform** run on a **schedule**, fields like `purchase_price` in `fact_loan_metrics` can be **NULL**, even though the same pipeline shows values when you run it **manually**.

**Cause:** On a schedule, **Transform often runs before Extract has finished** writing to S3 (or reads from an older run). Transform then reads **incomplete or stale** raw data, so some columns end up NULL.

When you run manually, you typically run Extract, wait for it to finish, then run Transform, so Transform always sees the **complete** raw data.

## Fix: Run Transform only after Extract succeeds

Transform **must** run **after** Extract has completed successfully. Two ways to do that:

### Option A: Glue Workflow (recommended)

1. In **AWS Glue** → **Workflows**, create a new workflow (e.g. `arrivehome-extract-then-transform`).
2. Add two nodes:
   - **Node 1:** Start trigger (e.g. schedule or on-demand).
   - **Node 2:** Your **Extract** job (e.g. `glue_job_extract_all_collections`).
   - **Node 3:** Your **Transform** job (e.g. `glue_job_transform_to_staging`).
3. Set the dependency so that **Node 3 runs only after Node 2 succeeds**:
   - Node 3’s predecessor = Node 2, and choose “Run after Node 2 completes with Success”.
4. Schedule or run the **workflow**, not the two jobs separately. That way Transform never runs until Extract has finished.

### Option B: Separate schedules with enough delay

If you keep using two separate scheduled triggers:

1. Schedule **Extract** at a time that fits your process (e.g. 2:00 AM).
2. Schedule **Transform** at a **later** time, with enough delay for Extract to finish. A **30-minute gap** is a common choice (e.g. Extract at 2:00 AM, Transform at 2:30 AM).

Risks: If Extract sometimes runs longer than the gap (e.g. a large `loans` run takes 45+ minutes), Transform can still read incomplete data. Option A (workflow) avoids that; otherwise, increase the gap or monitor Extract run duration in CloudWatch.

## Safeguard in the Transform job

The Transform script now checks whether the raw loans data has a recent `_etl_extraction_date` (if that column exists). If the latest extraction date is **not today**, it logs a **WARNING** so you can see in CloudWatch that Transform may have run before Extract completed or that raw data is stale.

- Fix the **order** of execution (Option A or B above); that is what actually prevents NULLs.
- The warning is there to make scheduling/ordering issues visible when they happen.

## Summary

| Approach | What to do |
|----------|------------|
| **Recommended** | Use a **Glue Workflow**: run Extract first, then run Transform only **after Extract succeeds**. |
| **Alternative** | Schedule Extract and Transform separately with a **delay** (e.g. **30 minutes**) so Extract finishes before Transform. Increase the gap if Extract sometimes runs longer. |
| **In the job** | Rely on the Transform job’s **stale-data warning** in logs to spot when raw data is not from the latest run. |

Once Transform runs only after a completed Extract, values like `purchase_price` in `fact_loan_metrics` should match what you see when running the pipeline manually.
