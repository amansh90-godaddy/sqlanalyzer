# SQL Query Improvements: cancel_rebuy.sql

## 1. Performance Optimizations

### 1.1 Remove Unnecessary DISTINCT Operations
**Issue:** Both CTEs use `DISTINCT` without clear justification, which can be expensive and may hide underlying data quality issues.

**Current:**
```sql
with cte as (select distinct resource_id, prior_bill_shopper_id, ...
```

**Improved:**
```sql
-- If duplicates exist, identify the root cause first
-- Option 1: Use GROUP BY with explicit aggregation
with cte as (
  select 
    resource_id,
    prior_bill_shopper_id,
    product_family_name,
    max(entitlement_cancel_mst_date) as entitlement_cancel_mst_date
  from dev.dna_approved.renewal_360
  where ...
  group by 1,2,3,...
)
```

**Impact:** High - DISTINCT can be 2-3x slower than GROUP BY and hides data quality issues  
**Trade-off:** Need to identify which columns should be aggregated vs grouped

---

### 1.2 Consolidate Redundant Table Scans
**Issue:** The query scans `renewal_360` twice with similar filters. This doubles I/O costs.

**Current:**
```sql
with cte as (select ... from renewal_360 where ...)
, cte_2 as (select ... from renewal_360 where ...)
```

**Improved:**
```sql
with base_data as (
  select 
    resource_id,
    prior_bill_shopper_id,
    product_family_name,
    entitlement_cancel_mst_date,
    prior_bill_modified_mst_date,
    prior_bill_product_pnl_new_renewal_name,
    prior_bill_sequence_number,
    -- ... other columns
  from dev.dna_approved.renewal_360
  where bill_exclude_reason_desc is null
    and prior_bill_primary_product_flag = true
    and lower(prior_bill_product_pnl_group_name) <> 'domains'
    and (
      (entitlement_cancel_mst_date between '2024-01-01' and current_date - 60)
      or (prior_bill_modified_mst_date between '2024-01-01' and current_date - 1)
    )
),
cte as (select ... from base_data where entitlement_cancel_mst_date is not null),
cte_2 as (select ... from base_data where prior_bill_product_pnl_new_renewal_name = 'New Purchase')
```

**Impact:** High - Reduces I/O by ~50%  
**Trade-off:** Slightly more complex query structure

---

### 1.3 Optimize Join Date Range Calculation
**Issue:** Computing date ranges in the join condition prevents index usage and forces full computation for every row.

**Current:**
```sql
and cte_2.prior_bill_modified_mst_date between dateadd(day, 30, cte.entitlement_cancel_mst_date)
   and dateadd(day, 60, cte.entitlement_cancel_mst_date)
```

**Improved:**
```sql
-- Pre-compute date boundaries in CTEs
with cte as (
  select 
    ...,
    entitlement_cancel_mst_date,
    dateadd(day, 30, entitlement_cancel_mst_date) as rebuy_window_start,
    dateadd(day, 60, entitlement_cancel_mst_date) as rebuy_window_end
  from ...
)
...
left join cte_2
  on cte.prior_bill_shopper_id = cte_2.prior_bill_shopper_id
 and cte.product_family_name = cte_2.product_family_name
 and cte_2.prior_bill_modified_mst_date >= cte.rebuy_window_start
 and cte_2.prior_bill_modified_mst_date <= cte.rebuy_window_end
```

**Impact:** Medium - Better query optimization opportunities, clearer logic  
**Trade-off:** Two additional columns in CTE

---

### 1.4 Index Recommendations
**Issue:** Query likely performs full table scans without proper indexing.

**Recommended Indexes:**
```sql
-- For cancellation lookups (cte)
CREATE INDEX IF NOT EXISTS idx_renewal_cancel 
ON dev.dna_approved.renewal_360(
  prior_bill_primary_product_flag,
  entitlement_cancel_mst_date,
  prior_bill_product_pnl_group_name
) WHERE bill_exclude_reason_desc IS NULL;

-- For new purchase lookups (cte_2)
CREATE INDEX IF NOT EXISTS idx_renewal_new_purchase
ON dev.dna_approved.renewal_360(
  prior_bill_primary_product_flag,
  prior_bill_product_pnl_new_renewal_name,
  prior_bill_modified_mst_date
) WHERE bill_exclude_reason_desc IS NULL;

-- For join operations
CREATE INDEX IF NOT EXISTS idx_renewal_shopper_family
ON dev.dna_approved.renewal_360(
  prior_bill_shopper_id,
  product_family_name,
  prior_bill_modified_mst_date
);
```

**Impact:** High - Can reduce query time by 10-100x depending on table size  
**Trade-off:** Increased storage and insert/update overhead

---

### 1.5 Partition Strategy
**Issue:** No apparent partitioning strategy for time-series data.

**Recommended:**
```sql
-- Partition renewal_360 by date if not already done
-- Snowflake example:
ALTER TABLE dev.dna_approved.renewal_360
CLUSTER BY (entitlement_cancel_mst_date, prior_bill_modified_mst_date);
```

**Impact:** High - Improves pruning for date range queries  
**Trade-off:** Requires table rebuild/recluster maintenance

---

## 2. Code Quality

### 2.1 Remove Commented Code
**Issue:** Multiple lines of commented code create confusion and clutter.

**Current:**
```sql
--  and coa_resource_id is null 
--and (LOWER(subscription_cancel_by_name ) not LIKE '%migration%'
--    OR LOWER(subscription_cancel_by_name) not LIKE '%migr%'
 --   OR LOWER(subscription_cancel_by_name) not LIKE '%transferaway%') )
```

**Improved:**
```sql
-- Remove entirely or document why it was removed
-- If needed for history, use version control (git)
```

**Impact:** Low - Improves readability  
**Trade-off:** None

---

### 2.2 Standardize Formatting and Spacing
**Issue:** Inconsistent indentation, spacing, and alignment makes code hard to read.

**Current:**
```sql
with cte as (select distinct resource_id,  prior_bill_shopper_id, product_family_name, 
prior_bill_product_pnl_group_name,prior_bill_product_pnl_line_name, prior_bill_product_pnl_Category_name,
```

**Improved:**
```sql
with cte as (
  select distinct 
    resource_id,
    prior_bill_shopper_id,
    product_family_name,
    prior_bill_product_pnl_group_name,
    prior_bill_product_pnl_line_name,
    prior_bill_product_pnl_category_name,
    prior_bill_product_pnl_subline_name,
    prior_bill_product_pnl_version_name,
    entitlement_cancel_mst_date
  from dev.dna_approved.renewal_360
  where ...
)
```

**Impact:** Low - Improves maintainability  
**Trade-off:** None

---

### 2.3 Fix Column Name Inconsistency
**Issue:** Using `prior_bill_product_pnl_Category_name` (capital C) creates confusion.

**Current:**
```sql
prior_bill_product_pnl_Category_name
```

**Improved:**
```sql
prior_bill_product_pnl_category_name
```

**Impact:** Low - Prevents case-sensitivity issues  
**Trade-off:** None

---

### 2.4 Add Meaningful Business Logic Comments
**Issue:** Complex business logic lacks explanation.

**Current:**
```sql
---expirations that got cancelled
with cte as (...)
```

**Improved:**
```sql
-- CTE 1: Cancelled Subscriptions
-- Identifies subscriptions that were cancelled between 2024-01-01 and 60 days ago
-- Excludes: domains, non-primary products, and billable exclusions
-- Purpose: Base population for rebuy analysis (HAT-3923)
with cte as (...)

-- CTE 2: New Purchases
-- Identifies new purchase orders (not renewals) in the analysis window
-- Used to detect if cancelled customers made new purchases within 30-60 days
with cte_2 as (...)
```

**Impact:** Medium - Improves team understanding and maintenance  
**Trade-off:** None

---

### 2.5 Remove Duplicate JIRA References
**Issue:** JIRA ticket mentioned three times.

**Current:**
```sql
--jira ticket
--https://godaddy-corp.atlassian.net/browse/HAT-3923
 --JIRA: HAT-3923
```

**Improved:**
```sql
-- JIRA: HAT-3923
-- Purpose: Analyze cancel/rebuy behavior for non-domain products
-- https://godaddy-corp.atlassian.net/browse/HAT-3923
```

**Impact:** Low - Reduces clutter  
**Trade-off:** None

---

## 3. Maintainability

### 3.1 Parameterize Hardcoded Dates
**Issue:** Hardcoded date '2024-01-01' makes query inflexible and requires manual updates.

**Current:**
```sql
where entitlement_cancel_mst_date between '2024-01-01' and current_Date-60
```

**Improved:**
```sql
-- Option 1: Use relative dates
where entitlement_cancel_mst_date between dateadd(year, -1, date_trunc('year', current_date)) 
  and current_date - 60

-- Option 2: Use session variables
set analysis_start_date = '2024-01-01';
where entitlement_cancel_mst_date between $analysis_start_date and current_date - 60

-- Option 3: Convert to stored procedure with parameters
CREATE OR REPLACE PROCEDURE ba_ecommerce.sp_cancel_rebuy(
  start_date DATE,
  cancel_lookback_days INT,
  rebuy_window_start INT,
  rebuy_window_end INT
)
```

**Impact:** High - Makes query reusable and easier to maintain  
**Trade-off:** Requires procedure/parameter infrastructure

---

### 3.2 Extract Magic Numbers as Named Constants
**Issue:** Numbers 30, 60 lack context for their business meaning.

**Current:**
```sql
and cte_2.prior_bill_modified_mst_date between dateadd(day, 30, cte.entitlement_cancel_mst_date)
   and dateadd(day, 60, cte.entitlement_cancel_mst_date)
```

**Improved:**
```sql
-- Define business constants at query start
set rebuy_window_start_days = 30;  -- Minimum days after cancellation to consider rebuy
set rebuy_window_end_days = 60;    -- Maximum days after cancellation to consider rebuy
set cancel_data_lag_days = 60;     -- Days to exclude recent cancellations (data completeness)

-- Use in query
and cte_2.prior_bill_modified_mst_date between 
  dateadd(day, $rebuy_window_start_days, cte.entitlement_cancel_mst_date)
  and dateadd(day, $rebuy_window_end_days, cte.entitlement_cancel_mst_date)
```

**Impact:** Medium - Improves understanding and flexibility  
**Trade-off:** Slightly more setup code

---

### 3.3 Simplify Repetitive CASE Logic
**Issue:** Five nearly identical CASE statements create maintenance burden.

**Current:**
```sql
max(case when cte_2.prior_Bill_shopper_id is not null then 1 else 0 end) as cancel_rebuy_flag,
max(case when cte_2.prior_Bill_shopper_id is not null and cte.X = cte_2.X then 1 else 0 end) as flag_X,
-- ... repeated 4 more times
```

**Improved:**
```sql
-- Option 1: Use boolean logic (if database supports)
max(case when cte_2.prior_bill_shopper_id is not null then 1 else 0 end)::int 
  as cancel_rebuy_flag,
max((cte_2.prior_bill_shopper_id is not null 
     and cte.prior_bill_product_pnl_group_name = cte_2.prior_bill_product_pnl_group_name)::int)
  as cancel_rebuy_product_pnl_group_flag,

-- Option 2: Create a function for reuse
CREATE OR REPLACE FUNCTION match_flag(shopper_match BOOLEAN, attr_match BOOLEAN)
RETURNS INT AS $$
  SELECT CASE WHEN shopper_match AND attr_match THEN 1 ELSE 0 END
$$ LANGUAGE SQL;
```

**Impact:** Medium - Reduces code duplication  
**Trade-off:** May reduce readability for simple CASE statements

---

### 3.4 Externalize Schema/Table Names
**Issue:** Hardcoded schema/table names make migration difficult.

**Current:**
```sql
Drop table if exists ba_Ecommerce.cancel_rebuy;
create table ba_ecommerce.cancel_rebuy as
```

**Improved:**
```sql
-- Use variables for environment flexibility
set target_schema = 'ba_ecommerce';
set target_table = 'cancel_rebuy';
set source_schema = 'dev.dna_approved';
set source_table = 'renewal_360';

-- Or better: use stored procedure with parameters
CREATE OR REPLACE PROCEDURE generate_cancel_rebuy_table(
  target_db STRING,
  target_schema STRING
)
```

**Impact:** Medium - Enables environment promotion (dev/staging/prod)  
**Trade-off:** Requires variable/procedure infrastructure

---

## 4. Data Quality

### 4.1 Add Explicit NULL Handling in Comparisons
**Issue:** NULL values in comparison columns may cause unexpected join mismatches.

**Current:**
```sql
and cte.prior_bill_product_pnl_group_name = cte_2.prior_bill_product_pnl_group_name
```

**Improved:**
```sql
and coalesce(cte.prior_bill_product_pnl_group_name, 'UNKNOWN') = 
    coalesce(cte_2.prior_bill_product_pnl_group_name, 'UNKNOWN')

-- Or better: filter out NULLs if they're invalid
where cte.prior_bill_product_pnl_group_name is not null
  and cte_2.prior_bill_product_pnl_group_name is not null
```

**Impact:** Medium - Prevents silent data loss in joins  
**Trade-off:** Need to define business rules for NULL handling

---

### 4.2 Validate Shopper and Resource IDs
**Issue:** No validation that IDs are valid or non-null.

**Current:**
```sql
where entitlement_cancel_mst_date is not null
```

**Improved:**
```sql
where entitlement_cancel_mst_date is not null
  and prior_bill_shopper_id is not null
  and resource_id is not null
  and length(trim(prior_bill_shopper_id)) > 0
  and length(trim(resource_id)) > 0
```

**Impact:** Medium - Prevents invalid data from polluting results  
**Trade-off:** May filter out records that need investigation

---

### 4.3 Add Data Quality Checks
**Issue:** Using DISTINCT suggests duplicate issues that should be investigated.

**Improved:**
```sql
-- Add diagnostic query before main query
-- Check for duplicates in source data
select 
  resource_id,
  prior_bill_shopper_id,
  entitlement_cancel_mst_date,
  count(*) as duplicate_count
from dev.dna_approved.renewal_360
where entitlement_cancel_mst_date is not null
  and bill_exclude_reason_desc is null
  and prior_bill_primary_product_flag = true
group by 1,2,3
having count(*) > 1
order by duplicate_count desc
limit 100;
```

**Impact:** High - Identifies root cause of data quality issues  
**Trade-off:** Additional diagnostic overhead

---

### 4.4 Handle Edge Case: Same-Day Purchases
**Issue:** 30-day minimum window might miss rapid rebuys (same week or within 30 days).

**Current:**
```sql
and cte_2.prior_bill_modified_mst_date between dateadd(day, 30, ...)
```

**Improved:**
```sql
-- Consider if business definition should include 0-30 day rebuys
-- Add separate flag for immediate rebuys
max(case
  when cte_2.prior_bill_shopper_id is not null
   and cte_2.prior_bill_modified_mst_date between cte.entitlement_cancel_mst_date
       and dateadd(day, 30, cte.entitlement_cancel_mst_date)
  then 1 else 0
end) as cancel_rebuy_immediate_flag,

-- Extend window to 0-60 days if appropriate
and cte_2.prior_bill_modified_mst_date between cte.entitlement_cancel_mst_date
   and dateadd(day, 60, cte.entitlement_cancel_mst_date)
```

**Impact:** Medium - Captures potentially important customer behavior  
**Trade-off:** Need business validation of definition

---

### 4.5 Add Row Count Validation
**Issue:** No validation that query produces expected results.

**Improved:**
```sql
-- Add validation checks at end
create or replace table ba_ecommerce.cancel_rebuy_validation as
select
  'total_cancellations' as metric,
  count(*) as value
from ba_ecommerce.cancel_rebuy
union all
select
  'total_rebuys',
  sum(cancel_rebuy_flag)
from ba_ecommerce.cancel_rebuy
union all
select
  'rebuy_rate',
  round(sum(cancel_rebuy_flag) * 100.0 / count(*), 2)
from ba_ecommerce.cancel_rebuy;

-- Alert if values are outside expected ranges
select * from ba_ecommerce.cancel_rebuy_validation
where (metric = 'rebuy_rate' and value not between 5 and 25);
```

**Impact:** Medium - Catches unexpected data shifts early  
**Trade-off:** Additional validation table to maintain

---

## 5. Scalability Concerns

### 5.1 Implement Incremental Processing
**Issue:** Full historical scan from '2024-01-01' will grow unbounded over time.

**Current:**
```sql
where entitlement_cancel_mst_date between '2024-01-01' and current_date - 60
```

**Improved:**
```sql
-- Option 1: Process monthly partitions
where entitlement_cancel_mst_date between 
  date_trunc('month', dateadd(month, -2, current_date))
  and last_day(dateadd(month, -2, current_date))

-- Option 2: Maintain as incremental updates
merge into ba_ecommerce.cancel_rebuy target
using (
  -- Only process new cancellations from last 90 days
  ... where entitlement_cancel_mst_date >= current_date - 90
) source
on target.resource_id = source.resource_id
  and target.entitlement_cancel_mst_date = source.entitlement_cancel_mst_date
when matched then update ...
when not matched then insert ...;
```

**Impact:** High - Prevents exponential query time growth  
**Trade-off:** More complex ETL logic

---

### 5.2 Add Query Timeout Guards
**Issue:** No protection against long-running queries on large datasets.

**Improved:**
```sql
-- Add at query start
alter session set statement_timeout_in_seconds = 1800; -- 30 minutes

-- Add result size limit for testing
select * from ba_ecommerce.cancel_rebuy
limit 1000000;  -- Prevent accidental massive result sets
```

**Impact:** Medium - Prevents resource exhaustion  
**Trade-off:** May need tuning based on actual runtime

---

### 5.3 Consider Materialized View for Repeated Access
**Issue:** If this table is queried frequently, recreating it fully each time is wasteful.

**Improved:**
```sql
-- Option 1: Create as materialized view with incremental refresh
create materialized view ba_ecommerce.cancel_rebuy as
... query ...;

-- Schedule incremental refresh
alter materialized view ba_ecommerce.cancel_rebuy
  set auto_refresh = true
  refresh_interval = '1 day';

-- Option 2: Use change data capture (CDC)
-- Track changes to renewal_360 and only process deltas
```

**Impact:** High - Reduces repeated computation costs  
**Trade-off:** Data freshness vs performance trade-off

---

### 5.4 Add Resource Monitoring
**Issue:** No visibility into query performance metrics.

**Improved:**
```sql
-- Add query metadata
create or replace table ba_ecommerce.cancel_rebuy_metadata as
select
  current_timestamp() as load_timestamp,
  (select count(*) from ba_ecommerce.cancel_rebuy) as row_count,
  (select sum(cancel_rebuy_flag) from ba_ecommerce.cancel_rebuy) as rebuy_count,
  '${query_duration_seconds}' as query_duration_sec,
  current_user() as executed_by;

-- Query history analysis
select
  query_text,
  execution_time / 1000 as exec_time_sec,
  bytes_scanned / (1024*1024*1024) as gb_scanned
from snowflake.account_usage.query_history
where query_text like '%ba_ecommerce.cancel_rebuy%'
  and start_time >= dateadd(day, -7, current_date)
order by start_time desc;
```

**Impact:** Medium - Enables performance tracking and optimization  
**Trade-off:** Additional metadata management

---

### 5.5 Optimize MAX Aggregation Pattern
**Issue:** Multiple MAX aggregations on the same grouping may be inefficient for very large result sets.

**Current:**
```sql
select
  cte.resource_id,
  ...,
  max(case when ... then 1 else 0 end) as flag1,
  max(case when ... then 1 else 0 end) as flag2,
  ...
group by ...
```

**Improved:**
```sql
-- If only 1 row per resource_id expected, remove aggregation entirely
select
  cte.resource_id,
  ...,
  case when cte_2.prior_bill_shopper_id is not null then 1 else 0 end as flag1,
  ...
-- No GROUP BY needed if join guarantees 1:1 relationship

-- Or use window functions if multiple matches need evaluation
select distinct
  cte.resource_id,
  ...,
  max(case when ... then 1 else 0 end) 
    over (partition by cte.resource_id, ...) as flag1,
  ...
```

**Impact:** Medium - Reduces aggregation overhead  
**Trade-off:** Need to verify join cardinality assumptions

---

## Summary Priority Matrix

| Priority | Impact | Effort | Recommendation |
|----------|--------|--------|----------------|
| **P0** | High | Medium | Remove DISTINCT, add indexes, parameterize dates |
| **P1** | High | High | Consolidate table scans, implement incremental processing |
| **P2** | Medium | Low | Fix formatting, remove commented code, add NULL checks |
| **P3** | Medium | Medium | Optimize join date logic, add data quality checks |
| **P4** | Low | Low | Naming consistency, additional comments |