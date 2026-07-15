# MongoDB to BI Analytics: Cost Comparison & Recommendation

## Executive Summary

This document compares different approaches for building a Business Intelligence (BI) solution 
from MongoDB data, analyzing costs, capabilities, and operational effort.

---

## 1. Current Architecture Overview

```
MongoDB (Production) 
    ↓
AWS Glue (Extract & Transform)
    ↓
Amazon S3 (Parquet Storage)
    ↓
Data Warehouse (Redshift/Athena)
    ↓
BI Tools (QuickSight/Tableau)
```

---

## 2. Option A: AWS DMS + Redshift Serverless

| Service | Monthly Cost (USD) |
|---------|-------------------|
| AWS DMS (compute + storage) | ~$230 |
| AWS Glue (1 transform job) | ~$600 |
| Amazon Redshift Serverless | ~$2,160 |
| Amazon S3 | ~$12 |
| Amazon QuickSight | ~$220 |
| **Total** | **~$3,220/month** |

### Pros:
- Real-time CDC (Change Data Capture) from MongoDB
- Fully managed, minimal ops effort
- Excellent BI tool compatibility

### Cons:
- Highest cost option
- Redshift Serverless is expensive for low-usage scenarios

---

## 3. Option B: AWS Glue + Redshift Serverless (Current Approach)

| Service | Usage | Monthly Cost (USD) |
|---------|-------|-------------------|
| AWS Glue Jobs | 15 jobs, ~100 DPU-hours/day | ~$1,320 |
| Glue Crawlers | 10 crawlers | ~$50 |
| Redshift Serverless | 200 RPU-hours/day | ~$2,160 |
| Amazon S3 Storage | 500 GB | ~$12 |
| AWS Step Functions | 1,000 executions | ~$25 |
| Amazon QuickSight | 5 authors + 20 readers | ~$220 |
| **Total** | | **~$3,800/month** |

### Pros:
- Full control over ETL logic
- Star schema data warehouse
- Best query performance

### Cons:
- Highest total cost
- Redshift Serverless running continuously is expensive

---

## 4. Cost Comparison Summary: All Options

| Option | Monthly Cost | BI Ready | Ops Effort | Notes |
|--------|-------------|----------|------------|-------|
| **Redshift Serverless** | ~$2,160 | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐⭐ | Best features, highest cost |
| **Redshift Provisioned** | ~$600–$900 | ⭐⭐⭐⭐⭐ | ⭐⭐⭐ | Same features, 70% cheaper |
| **Amazon Athena** | ~$50–$200 | ⭐⭐⭐ | ⭐⭐⭐⭐⭐ | Serverless, pay per query |
| **Aurora PostgreSQL** | ~$350 | ⭐⭐⭐ | ⭐⭐⭐ | Good for mixed workloads |
| **EC2 + ClickHouse** | ~$200 | ⭐⭐⭐⭐ | ⭐ | Cheapest, most ops effort |

---

## 5. Detailed Option Analysis

### Option 1: Redshift Serverless (Current)
- **Cost**: ~$2,160/month (just Redshift)
- **Best For**: Heavy daily dashboard usage, real-time queries
- **BI Compatibility**: Excellent (Tableau, QuickSight, Power BI)
- **Recommendation**: Only if you have 100+ daily active BI users

### Option 2: Redshift Provisioned (dc2.large)
- **Cost**: ~$600–$900/month
- **Best For**: Predictable workloads, cost optimization
- **BI Compatibility**: Excellent (same as Serverless)
- **Recommendation**: ✅ **BEST BALANCED CHOICE** for most cases

### Option 3: Amazon Athena
- **Cost**: ~$50–$200/month (depends on query volume)
- **Best For**: Ad-hoc queries, monthly reports, low query volume
- **BI Compatibility**: Good (QuickSight native, others via JDBC)
- **Recommendation**: Best for infrequent reporting needs

### Option 4: Aurora PostgreSQL
- **Cost**: ~$350/month
- **Best For**: Mixed OLTP + analytics, SQL familiarity
- **BI Compatibility**: Good
- **Recommendation**: If you also need transactional workloads

### Option 5: EC2 + ClickHouse
- **Cost**: ~$200/month
- **Best For**: Maximum cost savings, technical team available
- **BI Compatibility**: Good (JDBC/ODBC)
- **Recommendation**: Only if you have DevOps capacity

---

## 6. Athena-Only Architecture (Lowest Cost)

If you choose Athena, here's the simplified architecture:

```
MongoDB 
    ↓
Glue Job (Daily Extract)      Cost: ~$50-100/month
    ↓
S3 Staging Zone (Parquet)     Cost: ~$12/month
    ↓
Glue Crawler                  Cost: ~$5/month
    ↓
Athena Queries                Cost: ~$5/TB scanned (~$20-50/month)
    ↓
QuickSight                    Cost: ~$220/month
────────────────────────────────────────────────────
TOTAL:                        ~$300-400/month
```

### Athena Savings:
- **Current (Redshift Serverless)**: ~$3,800/month
- **Athena Approach**: ~$300-400/month
- **Savings**: ~$3,400/month (90% reduction!)

---

## 7. Final Recommendation

### For Your Use Case (MongoDB → BI with QuickSight):

| Scenario | Recommended Option | Est. Cost |
|----------|-------------------|-----------|
| **Frequent dashboards (daily use)** | Redshift Provisioned | ~$900/month |
| **Weekly/Monthly reports** | Athena | ~$300/month |
| **Budget is primary concern** | Athena | ~$300/month |
| **Need real-time data** | Redshift Provisioned | ~$900/month |

---

## 8. Our Recommendation: Start with Athena

### Why Athena First?

1. **You already have the data in S3** (Parquet in staging zone)
2. **Zero additional infrastructure** - just run a crawler
3. **Pay only when you query** - $5 per TB scanned
4. **Easy to upgrade later** - can add Redshift if needed

### Migration Path:

```
Phase 1 (Now): Athena + QuickSight     → ~$300/month
Phase 2 (If needed): Add Redshift      → ~$900/month
Phase 3 (Scale): Redshift Serverless   → ~$2,160/month
```

---

## 9. Quick Start: Athena Setup

### Step 1: Create Glue Crawler
```
Name: staging-zone-crawler
S3 Path: s3://arrivehome-bi-prod/staging-zone/
Database: arrive_home_dw
Schedule: Daily
```

### Step 2: Set Athena Query Location
```
S3 Path: s3://arrivehome-bi-prod/athena-results/
```

### Step 3: Query Your Data
```sql
SELECT 
    product_type,
    COUNT(*) as loan_count,
    SUM(first_mortgage_total_amount) as total_volume
FROM arrive_home_dw.fact_loan_metrics
GROUP BY product_type;
```

### Step 4: Connect QuickSight
- Data Source: Athena
- Database: arrive_home_dw
- Build dashboards!

---

## 10. Cost Savings Summary

| Current Setup | Proposed (Athena) | Monthly Savings |
|---------------|-------------------|-----------------|
| $3,800/month | $300-400/month | **$3,400+/month** |
| $45,600/year | $3,600-4,800/year | **$40,000+/year** |

---

## 11. Decision Matrix

| Criteria | Weight | Redshift Serverless | Redshift Provisioned | Athena |
|----------|--------|--------------------|--------------------|--------|
| Cost | 40% | ❌ Poor | ✅ Good | ✅ Best |
| Query Performance | 25% | ✅ Best | ✅ Best | ⚠️ Good |
| BI Compatibility | 20% | ✅ Best | ✅ Best | ✅ Good |
| Ops Effort | 15% | ✅ Low | ⚠️ Medium | ✅ Low |
| **Overall Score** | | 65% | 80% | **90%** |

---

## 12. Conclusion

**For ArriveHome's MongoDB to BI use case:**

### 🏆 Recommended: Amazon Athena

- **90% cost reduction** compared to current Redshift Serverless approach
- **Zero infrastructure** to manage
- **Works with your existing S3 data** (no changes needed to Glue jobs)
- **Easy QuickSight integration**
- **Can upgrade to Redshift later** if query performance becomes critical

### Action Items:
1. ✅ Keep Glue jobs for MongoDB → S3 extraction
2. ✅ Create Glue Crawler for staging zone
3. ✅ Set up Athena query location
4. ✅ Connect QuickSight to Athena
5. ❌ Delete Redshift Serverless workgroup (saves $2,160/month)

---

*Document created: January 23, 2026*
*Last updated: January 23, 2026*
