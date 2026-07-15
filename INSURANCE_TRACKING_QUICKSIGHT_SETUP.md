# Insurance Tracking — QuickSight Setup Guide

Recreate the **Executive Summary • Performance** Power BI dashboard in Amazon QuickSight using Athena curated tables from the Glue job.

## Prerequisites

Glue job outputs (database default: `insurance_tracking_sharepoint`):

| Table | Purpose |
|---|---|
| `fact_procurement` | Submission/policy grain facts |
| `dim_loan_master` | One row per `loan_key_v3` |
| `dim_month` | Month spine for charts |
| `dim_date` | Calendar (optional) |

S3 location (default): `s3://arrivehome-bi-prod/curated/<table_name>/`

---

## Step 1 — Create the Athena view

1. Open **Amazon Athena** → query editor.
2. Set database to `insurance_tracking_sharepoint`.
3. Run these SQL files (in order):

   - `athena_views/vw_dim_month.sql` (optional DimMonth parity view)
   - `athena_views/vw_insurance_tracking_procurement_analysis.sql`

4. Verify month labels are populated:

   ```sql
   SELECT
       COUNT(*) AS rows,
       COUNT(month_label) AS with_month_label,
       COUNT(request_month_label) AS with_request_month,
       COUNT(close_month_label) AS with_close_month
   FROM insurance_tracking_sharepoint.vw_insurance_tracking_procurement_analysis;
   ```

   `month_label` = request month if present, else close month (DimMonth is the UNION of both).

5. (Optional) Validate KPI numbers against Power BI using:

   `athena_views/queries_insurance_tracking_executive_summary.sql`

---

## Step 2 — Create the QuickSight dataset

1. Open **Amazon QuickSight** → **Datasets** → **New dataset**.
2. Choose **Athena**.
3. Select:
   - Data source: your Athena connection (same AWS account/region)
   - Database: `insurance_tracking_sharepoint`
   - Table: `vw_insurance_tracking_procurement_analysis`
4. Choose **Directly query your data** (filters stay live) **or** **Import to SPICE** (faster; refresh daily after Glue job).
5. Click **Edit/Preview data** and confirm types:

   | Field | Type |
   |---|---|
   | `request_date`, `close_bind_date`, `request_month_start`, `close_month_start` | Date |
   | `dwelling_amount`, `premium` | Decimal |
   | `closed_flag`, `is_closed_submission`, `is_closed_loan_row` | Integer |
   | `request_year_month_sort`, `close_year_month_sort` | Integer |
   | Everything else | String |

6. Save dataset as: **`Insurance Tracking — Executive Summary`**.

---

## Step 3 — Add calculated fields (Analysis → Calculated field)

Create these on the dataset or in the analysis:

| Calculated field | Formula |
|---|---|
| **Worked Submissions** | `count({is_worked_submission})` |
| **Closed Submissions** | `sum({is_closed_submission})` |
| **Worked Loans** | `distinctCount({loan_key_v3})` |
| **Closed Loans** | `distinctCount(ifelse({is_closed_loan_row} = 1, {loan_key_v3}, NULL))` |
| **Loans Close Rate** | See formula below — **do not** write `{Closed Loans}/{Worked Loans}` |
| **Loans Flood Required** | `distinctCount(ifelse({flood_required_inferred_code} = "REQ", {loan_key_v3}, NULL))` |
| **1978 or Older Worked Loans** | `distinctCount(ifelse({age_bucket} = "1978 or older", {loan_key_v3}, NULL))` |
| **Newer than 1978 Worked Loans** | `distinctCount(ifelse({age_bucket} = "Newer than 1978", {loan_key_v3}, NULL))` |
| **Closed Loan Key** (row-level) | `ifelse({is_closed_loan_row} = 1, {loan_key_v3}, NULL)` |

### Loans Close Rate (fix this exactly)

QuickSight looks up `{Closed Loans}` as a **dataset column**. Other calculated-field names are not dataset fields, so you get:

> Dataset field Closed Loans does not exist

Use the full expression in **one** analysis calculated field named `close_rate_operational`:

```
distinctCount(ifelse({is_closed_loan_row} = 1, {loan_key_v3}, NULL))
/
distinctCount({loan_key_v3})
```

Then format the field as **Percent** with 2 decimal places.

**Tip:** Create these as **Analysis → Add calculated field** (not Dataset edit), because `distinctCount` / `sum` / `count` are aggregations.

---

## Step 4 — Create the analysis

1. **Analyses** → **New analysis** → select dataset `Insurance Tracking — Executive Summary`.
2. Apply a dark theme (optional): **Themes** → dark background to match Power BI.

### Filters (left panel — match Power BI slicers)

Add **Filter controls** for:

| Field | Control type |
|---|---|
| `source` | Multi-select dropdown |
| `state` | Multi-select dropdown |
| `status` | Multi-select dropdown |
| `policy_type` | Multi-select dropdown |
| `month_label` | Dropdown (single-select) — **default = current month** (see below) |

Apply all filters to **all visuals**.

### Default Month filter to current year-month

Your month field is a **string** (`Jul 2026`), so QuickSight cannot use a relative-date filter on it. Use a **parameter + dynamic default**.

#### A) Create helper dataset (one row)

1. Run in Athena: `athena_views/vw_current_month_default.sql`
2. QuickSight → **Datasets** → **New dataset** → Athena
3. Table: `vw_current_month_default`
4. Import to SPICE (or Direct Query)
5. Name it: **`Insurance Tracking — Current Month Default`**

#### B) Create parameter

1. In the analysis → **Parameters** → **Create**
2. Name: `pMonth`
3. Data type: **String**
4. Values: **Dynamic values**
5. **Static default** (optional fallback): type today’s label, e.g. `Jul 2026`
6. **Dynamic default**:
   - Dataset: `Insurance Tracking — Current Month Default`
   - Column: `current_month_label`
   - User name column: leave blank / use QuickSight username if required for mapping — for a shared default for everyone, use **Set a static default** plus a **calculated default**, OR:

**Simpler shared default (works for all users):**

1. Parameter `pMonth` — String
2. Default value: create an **analysis calculated field** is not used for param defaults.
3. Instead set **Dynamic default** where:
   - Dataset = `Insurance Tracking — Current Month Default`
   - Set column = `current_month_label`
   - Username column = any constant user mapping dataset, **or** use the approach below.

**Easiest reliable method (recommended):**

1. Create parameter `pMonth` (String), values = **Set a fixed list** → **Link to dataset field** `month_label` on your main dataset.
2. For default: open **Dynamic defaults** → Select dataset `Insurance Tracking — Current Month Default` → Column `current_month_label`.
3. Create a tiny mapping: some QuickSight tenants require UserName. If so, use custom SQL as dataset:

```sql
SELECT
    'ANY_USER' AS user_name,   -- ignore if your QS version doesn't need this
    date_format(current_date, '%b %Y') AS current_month_label
```

If Dynamic default UI asks for “User name column”, create this dataset with your QuickSight username repeated, or use **Set default for all users** when available.

#### C) Wire the filter (replace Custom filter Equals with empty Value)

1. Delete the current `month_label` **Custom filter** that has an empty Value.
2. Add filter on `month_label` again:
   - Filter type: **Custom filter**
   - Condition: **Equals**
   - Check **Use parameters**
   - Parameter: `pMonth`
3. Add a **parameter control** (dropdown) for `pMonth` on the sheet, sourced from `month_label` values so users can change month.
4. Publish / reload — dashboard should open on **current month** (e.g. `Jul 2026`).

#### D) Alternative (no parameter): filter on date field

If you prefer relative dates:

1. Filter on `request_month_start` (Date)
2. Filter type: **Relative dates** → **This month**
3. Note: this filters by **request** month only (not the coalesce month_label). Use parameter method above to match Power BI MonthLabel behavior.

#### E) Optional flag on main dataset

After recreating `vw_insurance_tracking_procurement_analysis`, fields available:

| Field | Use |
|---|---|
| `current_month_label` | Same string every row (e.g. `Jul 2026`) |
| `is_current_month` | `1` if row’s `month_label` = current month |

You can filter `is_current_month = 1` for a static “current only” view, but that is harder for users to change — prefer the **parameter** approach.

## Step 5 — Build visuals

### Row 1 — KPI cards (8 visuals)

Visual type: **KPI** for each calculated field:

1. Worked Submissions  
2. Closed Submissions  
3. Worked Loans  
4. Closed Loans  
5. Loans Close Rate (percent)  
6. Loans Flood Required  
7. 1978 or Older Worked Loans  
8. Newer than 1978 Worked Loans  

Arrange in a single row across the top.

---

### Row 2 — Line charts (3 visuals)

#### A) Worked Loans vs Closed Loans vs Worked Submissions

`worked_loans` / `closed_loans` / `worked_submissions` are **not** columns on `vw_insurance_tracking_procurement_analysis`.  
They only exist if you build a **second Athena/SQL dataset** (Option A), **or** you use calculated fields on the main dataset (Option B).

---

**Option A — Second dataset (Power BI-style, 3 lines on one chart)**

1. QuickSight → **Datasets** → **New dataset** → Athena → **Custom SQL**.
2. Paste only query **#2** from `athena_views/queries_insurance_tracking_executive_summary.sql` (the `WITH base AS (...)` block that returns `month_label`, `year_month_sort`, `worked_loans`, `closed_loans`, `worked_submissions`).
3. Name dataset: `Insurance Tracking — Monthly Trends`.
4. Import to SPICE.
5. In analysis → **Add** this dataset (does not replace the KPI dataset).
6. Line chart from **this** dataset:

| Well | Field |
|---|---|
| X axis | `month_label` (sort by `year_month_sort` Asc) |
| Value 1 | `worked_loans` → Sum or blank agg (already counted) |
| Value 2 | `closed_loans` |
| Value 3 | `worked_submissions` |

---

**Option B — Same main dataset (what you have now)**

Use your **calculated fields** (not `worked_loans` columns):

| Line / Value | Put this in Values |
|---|---|
| Worked Loans | `distinctCount({loan_key_v3})` — or your calc field **Worked Loans** |
| Closed Loans | `distinctCount(ifelse({is_closed_loan_row} = 1, {loan_key_v3}, NULL))` — or **Closed Loans** |
| Worked Submissions | `count({is_worked_submission})` — or **Worked Submissions** |

**X axis (pick one):**

| Goal | X axis field | Sort by |
|---|---|---|
| Worked / submissions trend | `request_month_label` | `request_year_month_sort` |
| Closed loans trend | `close_month_label` | `close_year_month_sort` |
| Combined filter month | `month_label` | `year_month_sort` |

**Practical layout matching Power BI closely:**

1. Chart titled **Worked Loans vs Closed Loans** — X = `month_label`, Values = **Worked Loans** + **Closed Loans** + **Worked Submissions** (all calculated fields above).
2. Filter Blank months off the axis if needed: exclude `month_label` = `Blank`.

Note: On the main grain, “Closed Loans by month_label” uses request-or-close coalesce month, so closed counts by month will not match Power BI’s pure *CloseMonthStart* axis perfectly. For exact Power BI parity, use **Option A**.

---

#### C) Avg Premium — Worked vs Closed by Loan (full steps)

Power BI chart: **Avg Premium - Worked vs Closed by Loan**  
Two lines over `MonthLabel`: average premium for **worked** loans (by request month) vs **closed** loans (by close month).

---

##### What the metric means

| Line | How it is calculated |
|---|---|
| **Avg Premium Worked** | For each loan (`loan_key_v3`) in a **request** month, take `MAX(premium)`, then `AVG` those loan-level premiums for that month |
| **Avg Premium Closed** | Same idea, but only closed loans (`is_closed_loan_row = 1`), grouped by **close** month |

Using `MAX(premium)` per loan first avoids double-counting loans with multiple submissions/policies.

---

##### Option A — Recommended (dedicated dataset, matches Power BI)

**Step 1 — Create the Athena view**

Run in Athena:

```sql
-- file: athena_views/vw_insurance_tracking_avg_premium_by_month.sql
```

Or paste that file’s full `CREATE OR REPLACE VIEW ...` statement.

**Step 2 — Verify in Athena**

```sql
SELECT *
FROM insurance_tracking_sharepoint.vw_insurance_tracking_avg_premium_by_month
ORDER BY year_month_sort;
```

You should see columns:

| Column | Example |
|---|---|
| `month_label` | `Nov 2024` |
| `year_month_sort` | `202411` |
| `month_start` | `2024-11-01` |
| `avg_premium_worked` | `2150.33` |
| `avg_premium_closed` | `1980.10` |

**Step 3 — Create QuickSight dataset**

1. QuickSight → **Datasets** → **New dataset** → **Athena**
2. Database: `insurance_tracking_sharepoint`
3. Table: `vw_insurance_tracking_avg_premium_by_month`
4. Choose **Import to SPICE** (recommended)
5. Confirm types:
   - `month_label` → String  
   - `year_month_sort` → Integer  
   - `month_start` → Date  
   - `avg_premium_worked` → Decimal  
   - `avg_premium_closed` → Decimal  
6. Save as: **`Insurance Tracking — Avg Premium by Month`**

**Step 4 — Add dataset to your analysis**

1. Open analysis **Insurance - Executive Summary**
2. Top-left dataset picker → **Add data** / **Edit data** → add **`Insurance Tracking — Avg Premium by Month`**
3. Keep your main KPI dataset as well (do not replace it)

**Step 5 — Build the line chart**

1. Add visual → **Line chart**
2. Title: `Avg Premium - Worked vs Closed by Loan`
3. Field wells:

| Well | Field | Aggregation | Notes |
|---|---|---|---|
| **X axis** | `month_label` | — | Sort by `year_month_sort` **Ascending** |
| **Value** | `avg_premium_worked` | **Average** or **Sum** | Prefer **Average**. Rename display to **Avg Premium Worked** |
| **Value** | `avg_premium_closed` | **Average** or **Sum** | Rename display to **Avg Premium Closed** |

4. Because each month is already one row, aggregation type barely matters (`Sum` = `Average` when one row/month). Use **Average** to be safe.
5. Format values as **Currency** (USD) or number with `$` prefix, 0–2 decimals.
6. Y-axis: leave auto, or set max similar to Power BI (~$5K).

**Step 6 — Sort months correctly**

1. Click `month_label` on X axis
2. **Sort by** → `year_month_sort` → **Ascending**  
   (Do **not** sort alphabetically by `month_label`.)

**Step 7 — Filters / controls**

This monthly dataset has **no** `source` / `state` / `status` columns.  
Dashboard filters on the main dataset will **not** apply to this chart unless you either:

- Leave it unfiltered (global monthly trend), **or**
- Rebuild the view with filters applied, **or**
- Use Option B on the main dataset so shared filters apply.

---

##### Option B — Same main dataset (shared filters work)

Use `vw_insurance_tracking_procurement_analysis` + calculated fields.

**Calculated fields (Analysis):**

| Name | Formula |
|---|---|
| **Avg Premium Worked** | `avg({premium})` with visual filtered/grouped by request month — see chart setup below |
| **Avg Premium Closed** | `avg(ifelse({is_closed_loan_row} = 1, {premium}, NULL))` |

**Chart setup (two lines, shared filters):**

1. Visual: **Line chart**
2. Title: `Avg Premium - Worked vs Closed by Loan`
3. Field wells:

| Well | Field | Aggregate |
|---|---|---|
| X axis | `request_month_label` | — (sort by `request_year_month_sort`) |
| Value 1 | `premium` | **Average** → rename **Avg Premium Worked** |
| Value 2 | `ifelse({is_closed_loan_row} = 1, {premium}, NULL)` or calc **Avg Premium Closed** | **Average** |

**Limitation:** Both lines share the **request** month axis. Closed premiums by **close** month need Option A for exact Power BI parity.

For a closer Option B closed line:

1. Second small line chart, X = `close_month_label`, Value = `avg(ifelse(is_closed_loan_row = 1, premium, NULL))`
2. Or stick with Option A for one combined chart.

---

##### Option C — Custom SQL dataset (no view)

QuickSight → New dataset → Athena → **Custom SQL** → paste:

```sql
WITH loan_worked AS (
    SELECT
        loan_key_v3,
        CAST(request_month_start AS DATE) AS month_start,
        MAX(premium) AS premium
    FROM insurance_tracking_sharepoint.vw_insurance_tracking_procurement_analysis
    WHERE loan_key_v3 IS NOT NULL
      AND request_month_start IS NOT NULL
      AND premium IS NOT NULL
    GROUP BY loan_key_v3, CAST(request_month_start AS DATE)
),
loan_closed AS (
    SELECT
        loan_key_v3,
        CAST(close_month_start AS DATE) AS month_start,
        MAX(premium) AS premium
    FROM insurance_tracking_sharepoint.vw_insurance_tracking_procurement_analysis
    WHERE is_closed_loan_row = 1
      AND close_month_start IS NOT NULL
      AND premium IS NOT NULL
    GROUP BY loan_key_v3, CAST(close_month_start AS DATE)
),
all_months AS (
    SELECT month_start FROM loan_worked
    UNION
    SELECT month_start FROM loan_closed
)
SELECT
    date_format(m.month_start, '%b %Y') AS month_label,
    year(m.month_start) * 100 + month(m.month_start) AS year_month_sort,
    AVG(w.premium) AS avg_premium_worked,
    AVG(c.premium) AS avg_premium_closed
FROM all_months m
LEFT JOIN loan_worked w ON m.month_start = w.month_start
LEFT JOIN loan_closed c ON m.month_start = c.month_start
GROUP BY m.month_start
ORDER BY year_month_sort
```

Then follow Option A Steps 4–7.

---

##### Checklist

- [ ] Athena view (or Custom SQL) returns rows with `avg_premium_worked` / `avg_premium_closed`
- [ ] QuickSight dataset created and added to analysis
- [ ] Line chart X = `month_label`, sort = `year_month_sort`
- [ ] Two value fields: worked + closed
- [ ] Format as currency
- [ ] SPICE refresh scheduled after Glue job

---

#### B) Avg Coverage — Worked vs Closed by Loan

Use custom SQL query #3 as a separate dataset **or** in-analysis:

- Create loan-level aggregates with calculated fields at visual level:
  - X: `request_month_label` (sort by `request_year_month_sort`)
  - Value: `avg(dwelling_amount)` — label **Avg Coverage Worked**
- Second line from close month dataset/query for **Avg Coverage Closed**
---

### Row 3 — Donut charts (2 visuals)

#### A) Worked Loans Flood Required

- Visual: **Donut chart**
- Group/color: `flood_required_bucket`
- Value: `Worked Loans` (`distinct_count(loan_key_v3)`)

#### B) Closed Loans by Source

- Visual: **Donut chart**
- Group/color: `source`
- Value: `Closed Loans`

---

## Step 6 — Sort months correctly

For any chart using `month_label` or `request_month_label`:

1. Select the visual → field well → `month_label`.
2. Add sort field: `request_year_month_sort` (Ascending).

Without this, months sort alphabetically (`Apr` before `Aug`).

---

## Step 7 — Schedule refresh

1. **Datasets** → `Insurance Tracking — Executive Summary` → **Schedule refresh**.
2. Set daily refresh after the Glue job (e.g. 06:00 UTC).
3. If using the monthly trends SQL dataset, schedule that too.

---

## Step 8 — Publish & share

1. **Share** → add QuickSight users/groups.
2. **Publish dashboard** from the analysis.
3. Embed logo: **Actions** → **Add image** → upload Arrive Home logo.

---

## Metric definitions (Power BI parity)

| Metric | Definition |
|---|---|
| **Worked Submission** | Each row in `fact_procurement` (policy/request). |
| **Closed Submission** | Row with `closed_flag = 1` (has a valid close/bind date). |
| **Worked Loan** | Distinct `loan_key_v3` across filtered submissions. |
| **Closed Loan** | Distinct `loan_key_v3` where `closed_flag = 1`. |
| **Loans Close Rate** | Closed Loans ÷ Worked Loans. |
| **Flood Required** | Loan with `flood_required_inferred_code = 'REQ'`. |
| **MonthLabel** | Request month (`request_month_label`) for slicer; charts also use close month for closed loan trends. |

---

## Troubleshooting

| Issue | Fix |
|---|---|
| KPI numbers differ from Power BI | Run query #1 in `queries_insurance_tracking_executive_summary.sql` and compare; check Source filter (External only vs All). |
| Months out of order | Sort by `request_year_month_sort`, not `month_label`. |
| Blank dates / job errors | Glue job now nulls invalid dates (e.g. `07/01/2026/2026`). Re-run Glue job. |
| No Internal Procured data | Confirm AH sheet loaded; `source` should show `Internal Procured`. |
| Lender groups wrong | Power BI uses `qLenderAliasCurated`; Glue falls back to `lender_name_norm` until alias table is added. |

---

## Files in this repo

| File | Purpose |
|---|---|
| `athena_views/vw_insurance_tracking_procurement_analysis.sql` | Primary QuickSight dataset view |
| `athena_views/queries_insurance_tracking_executive_summary.sql` | KPI/chart validation SQL |
| `glue_job_insurance_tracking_lender_procured_to_curated.py` | ETL producing curated tables |
