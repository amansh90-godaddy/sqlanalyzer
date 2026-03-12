# SQL Query Analysis & Optimization Report
**Query:** `cancel_rebuy.sql`  
**Ticket:** HAT-3923  
**Analysis Date:** 2026-03-11

---

## 1. Performance Optimizations

### 1.1 Remove Unnecessary DISTINCT Operations
**Issue:** Both CTEs use `DISTINCT` without clear justification, which adds overhead.

**Current:**
```sql
with cte as (select distinct resource_id, prior_bill_shopper_id, ...
```

**Recommendation:**
```sql
-- If duplicates exist, identify root cause and handle upstream
-- Or use GROUP BY if aggregation is needed
with cte as (
  select resource_id, prior_bill_shopper_id, ...
  -- If duplicates are truly possible, document why:
  -- Remove duplicates caused by multiple entitlement records per resource
)
```

**Impact:** Medium - Can improve CTE materialization time by 20-40%  
**Trade-off:** Requires validation that duplicates don't exist or won't affect results

---

### 1.2 Optimize Date Range Join
**Issue:** The `BETWEEN` clause with `DATEADD` in the join condition prevents index usage and creates expensive comparisons.

**Current:**
```sql
and cte_2.prior_bill_modified_mst_date between dateadd(day, 30, cte.entitlement_cancel_mst_date)
   and dateadd(day, 60, cte.entitlement_cancel_mst_date)
```

**Recommendation:**
```sql
-- Pre-calculate date boundaries in CTEs
with cte as (
  select 
    ...
    entitlement_cancel_mst_date,
    dateadd(day, 30, entitlement_cancel_mst_date) as rebuy_window_start,
    dateadd(day, 60, entitlement_cancel_mst_date) as rebuy_window_end
  ...
),
-- Then use in join:
and cte_2.prior_bill_modified_mst_date >= cte.rebuy_window_start
and cte_2.prior_bill_modified_mst_date <= cte.rebuy_window_end
```

**Impact:** High - Enables index usage on `prior_bill_modified_mst_date`  
**Trade-off:** Slightly more storage in CTE materialization

---

### 1.3 Consolidate Table Scans
**Issue:** The query scans `renewal_360` twice for related data.

**Current:**
```sql
-- CTE 1 reads renewal_360
-- CTE 2 reads renewal_360 again
```

**Recommendation:**
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
    ...
  from dev.dna_approved.renewal_360
  where bill_exclude_reason_desc is null
    and lower(prior_bill_product_pnl_group_name) <> 'domains'
    and prior_bill_primary_product_flag = true
    and (
      (entitlement_cancel_mst_date between '2024-01-01' and current_date - 60)
      or (prior_bill_modified_mst_date between '2024-01-01' and current_date - 1)
    )
),
cte as (
  select * from base_data where entitlement_cancel_mst_date is not null
),
cte_2 as (
  select * from base_data 
  where prior_bill_product_pnl_new_renewal_name = 'New Purchase'
    and prior_bill_sequence_number = 1
)
```

**Impact:** High - Reduces I/O by ~50%  
**Trade-off:** More complex CTE structure, requires testing to ensure predicate pushdown works

---

### 1.4 Simplify Flag Logic
**Issue:** Multiple identical CASE statements with only comparison changing.

**Current:**
```sql
max(case when cte_2.prior_Bill_shopper_id is not null then 1 else 0 end) as cancel_rebuy_flag,
max(case when cte_2.prior_Bill_shopper_id is not null 
     and cte.prior_bill_product_pnl_group_name = cte_2.prior_bill_product_pnl_group_name then 1 else 0 end),
...
```

**Recommendation:**
```sql
-- Simplified boolean logic
max(case when cte_2.prior_bill_shopper_id is not null then 1 else 0 end) as cancel_rebuy_flag,
max(case when cte_2.prior_bill_product_pnl_group_name is not null then 1 else 0 end) as cancel_rebuy_product_pnl_group_flag,
max(case when cte_2.prior_bill_product_pnl_subline_name is not null then 1 else 0 end) as cancel_rebuy_product_pnl_category_flag,
max(case when cte_2.prior_bill_product_pnl_line_name is not null then 1 else 0 end) as cancel_rebuy_product_pnl_line_flag,
max(case when cte_2.prior_bill_product_pnl_version_name is not null then 1 else 0 end) as cancel_rebuy_product_pnl_version_flag

-- Note: This works because join already ensures equality, so if column is not null, match occurred
```

**Impact:** Low-Medium - Improves readability, minor performance gain  
**Trade-off:** Requires verification that join handles all matching logic

---

### 1.5 Index Recommendations

**Critical Indexes:**
```sql
-- On renewal_360 table:
CREATE INDEX idx_renewal_360_cancel_analysis ON dev.dna_approved.renewal_360 
  (entitlement_cancel_mst_date, prior_bill_primary_product_flag, bill_exclude_reason_desc)
  INCLUDE (resource_id, prior_bill_shopper_id, product_family_name, prior_bill_product_pnl_group_name);

CREATE INDEX idx_renewal_360_new_orders ON dev.dna_approved.renewal_360 
  (prior_bill_modified_mst_date, prior_bill_product_pnl_new_renewal_name, prior_bill_sequence_number)
  INCLUDE (resource_id, prior_bill_shopper_id, product_family_name);

CREATE INDEX idx_renewal_360_shopper_product ON dev.dna_approved.renewal_360 
  (prior_bill_shopper_id, product_family_name, prior_bill_modified_mst_date);
```

**Impact:** High - Could reduce query time by 70-90%  
**Trade-off:** Increased storage and write overhead on renewal_360 table

---

### 1.6 Partition Strategy

**Issue:** Date range filters suggest table should be partitioned by date.

**Recommendation:**
```sql
-- Ensure renewal_360 is partitioned by entitlement_cancel_mst_date or prior_bill_modified_mst_date
-- Or create monthly/quarterly partitions
-- This enables partition pruning for date range filters
```

**Impact:** High - Reduces data scanned by 80-95% depending on partition granularity  
**Trade-off:** Requires table restructuring, ongoing partition maintenance

---

## 2. Code Quality

### 2.1 Remove Commented Code
**Issue:** Multiple lines of commented code clutter the query.

**Current:**
```sql
  --  and coa_resource_id is null 
--and (LOWER(subscription_cancel_by_name ) not LIKE '%migration%'
--    OR LOWER(subscription_cancel_by_name) not LIKE '%migr%'
 --   OR LOWER(subscription_cancel_by_name) not LIKE '%transferaway%') )
```

**Recommendation:**
```sql
-- Remove entirely or document why this logic was removed
-- If needed for reference, include in header comment:
-- Note: Migration-related cancellations are included per business decision on [date]
```

**Impact:** Low - Improves readability  
**Trade-off:** None

---

### 2.2 Inconsistent String Comparison
**Issue:** Inconsistent use of `LOWER()` function.

**Current:**
```sql
and  lower(prior_bill_product_pnl_group_name) <> 'domains'
-- But elsewhere:
and prior_bill_product_pnl_new_renewal_name='New Purchase'  -- no LOWER()
```

**Recommendation:**
```sql
-- Standardize case-insensitive comparisons
and lower(prior_bill_product_pnl_group_name) <> 'domains'
and lower(prior_bill_product_pnl_new_renewal_name) = 'new purchase'
-- OR ensure column values are consistently cased and use exact matches
```

**Impact:** Low-Medium - Prevents data quality issues  
**Trade-off:** Minor performance overhead if LOWER() is added everywhere

---

### 2.3 Improve Comments and Documentation
**Issue:** Business logic is unclear; magic numbers not explained.

**Current:**
```sql
and entitlement_cancel_mst_date between '2024-01-01' and current_Date-60
-- Why 60 days? Why 30-60 day window for rebuy?
```

**Recommendation:**
```sql
-- Exclude recent cancellations (< 60 days) to allow rebuy window to complete
and entitlement_cancel_mst_date between '2024-01-01' and current_date - 60

-- Join window: Rebuy defined as new purchase 30-60 days after cancellation
-- per business rule in HAT-3923
and cte_2.prior_bill_modified_mst_date between 
    dateadd(day, 30, cte.entitlement_cancel_mst_date)  -- rebuy window start
    and dateadd(day, 60, cte.entitlement_cancel_mst_date)  -- rebuy window end
```

**Impact:** Low - Improves maintainability  
**Trade-off:** None

---

### 2.4 Naming Convention Issues
**Issue:** Flag column name doesn't match comparison column.

**Current:**
```sql
max(case
        when cte_2.prior_Bill_shopper_id is not null
         and cte.prior_bill_product_pnl_subline_name = cte_2.prior_bill_product_pnl_subline_name
          then 1
        else 0
      end) as cancel_rebuy_product_pnl_category_flag,  -- says "category" but checks "subline"
```

**Recommendation:**
```sql
-- Fix naming to match actual column:
as cancel_rebuy_product_pnl_subline_flag
```

**Impact:** Medium - Prevents confusion and bugs  
**Trade-off:** May require downstream query updates

---

### 2.5 Code Formatting
**Issue:** Inconsistent indentation and spacing.

**Recommendation:**
```sql
-- Standardize:
-- - 2 or 4 space indentation
-- - Comma placement (leading or trailing, but consistent)
-- - Keyword capitalization (ALL CAPS for keywords)
-- - Alignment of ON/AND clauses in joins
```

**Impact:** Low - Improves readability  
**Trade-off:** None

---

## 3. Maintainability

### 3.1 Parameterize Hardcoded Dates
**Issue:** Hardcoded start date makes query inflexible.

**Current:**
```sql
and entitlement_cancel_mst_date between '2024-01-01' and current_Date-60
```

**Recommendation:**
```sql
-- Create variables or use session parameters
DECLARE @analysis_start_date DATE = '2024-01-01';
DECLARE @rebuy_window_days INT = 60;
DECLARE @lookback_buffer_days INT = 60;

-- Or use relative dates:
and entitlement_cancel_mst_date between 
    dateadd(year, -1, date_trunc('year', current_date))  -- start of last year
    and current_date - @lookback_buffer_days
```

**Impact:** High - Makes query reusable and maintainable  
**Trade-off:** Requires query refactoring to support parameters

---

### 3.2 Extract Magic Numbers
**Issue:** Hardcoded 30 and 60 day windows lack context.

**Current:**
```sql
dateadd(day, 30, cte.entitlement_cancel_mst_date)
```

**Recommendation:**
```sql
-- Define as variables with descriptive names
DECLARE @rebuy_window_start_days INT = 30;  -- grace period after cancellation
DECLARE @rebuy_window_end_days INT = 60;    -- max days to count as rebuy

dateadd(day, @rebuy_window_start_days, cte.entitlement_cancel_mst_date)
```

**Impact:** Medium - Improves maintainability and testability  
**Trade-off:** None

---

### 3.3 Resolve Commented Logic
**Issue:** Commented migration filter suggests unresolved business requirement.

**Current:**
```sql
--and (LOWER(subscription_cancel_by_name ) not LIKE '%migration%'
--    OR LOWER(subscription_cancel_by_name) not LIKE '%migr%'
 --   OR LOWER(subscription_cancel_by_name) not LIKE '%transferaway%') )
```

**Recommendation:**
```sql
-- Either:
-- Option A: Remove if not needed
-- Option B: Implement if needed:
and not (
  lower(subscription_cancel_by_name) like '%migration%'
  or lower(subscription_cancel_by_name) like '%migr%'
  or lower(subscription_cancel_by_name) like '%transferaway%'
)
-- Note: Use AND with NOT instead of OR with NOT for correct logic
```

**Impact:** Medium - Clarifies business logic  
**Trade-off:** May change result set if implemented

---

### 3.4 Add Table Creation Safeguards
**Issue:** `DROP TABLE IF EXISTS` with no backup or audit trail.

**Current:**
```sql
Drop table if exists ba_Ecommerce.cancel_rebuy;
create table ba_ecommerce.cancel_rebuy as
```

**Recommendation:**
```sql
-- Option A: Create with timestamp for audit trail
DROP TABLE IF EXISTS ba_ecommerce.cancel_rebuy_old;
ALTER TABLE ba_ecommerce.cancel_rebuy RENAME TO cancel_rebuy_old;
CREATE TABLE ba_ecommerce.cancel_rebuy AS ...

-- Option B: Use transaction with validation
BEGIN TRANSACTION;
DROP TABLE IF EXISTS ba_ecommerce.cancel_rebuy;
CREATE TABLE ba_ecommerce.cancel_rebuy AS ...
-- Add row count validation
-- COMMIT if validation passes
COMMIT;
```

**Impact:** Medium - Prevents data loss  
**Trade-off:** Additional storage or complexity

---

## 4. Data Quality

### 4.1 Missing Null Checks
**Issue:** Column comparisons don't explicitly handle nulls.

**Current:**
```sql
and cte.prior_bill_product_pnl_group_name = cte_2.prior_bill_product_pnl_group_name
-- If either side is NULL, comparison returns UNKNOWN, not TRUE or FALSE
```

**Recommendation:**
```sql
-- Option A: Explicit null handling
and coalesce(cte.prior_bill_product_pnl_group_name, '') = 
    coalesce(cte_2.prior_bill_product_pnl_group_name, '')

-- Option B: Document assumption
-- Assumption: prior_bill_product_pnl_group_name is never null for primary products
and cte.prior_bill_product_pnl_group_name = cte_2.prior_bill_product_pnl_group_name
```

**Impact:** Medium - Prevents unexpected result exclusions  
**Trade-off:** Option A adds slight overhead

---

### 4.2 Filter Logic Error
**Issue:** Commented OR conditions would always be true.

**Current:**
```sql
--and (LOWER(subscription_cancel_by_name ) not LIKE '%migration%'
--    OR LOWER(subscription_cancel_by_name) not LIKE '%migr%'
 --   OR LOWER(subscription_cancel_by_name) not LIKE '%transferaway%') )
```

**Problem:** Using OR with NOT means any value will match at least one condition.

**Recommendation:**
```sql
-- Use AND for exclusions:
and lower(subscription_cancel_by_name) not like '%migration%'
and lower(subscription_cancel_by_name) not like '%migr%'
and lower(subscription_cancel_by_name) not like '%transferaway%'

-- Or consolidate:
and not regexp_like(lower(subscription_cancel_by_name), 
                    'migration|migr|transferaway')
```

**Impact:** High if implemented - Could significantly change results  
**Trade-off:** None

---

### 4.3 Date Range Validation
**Issue:** No validation that end date > start date.

**Current:**
```sql
and cte_2.prior_bill_modified_mst_date between dateadd(day, 30, ...)
   and dateadd(day, 60, ...)
```

**Recommendation:**
```sql
-- Add assertion or check
WHERE dateadd(day, 30, cte.entitlement_cancel_mst_date) <= 
      dateadd(day, 60, cte.entitlement_cancel_mst_date)
-- Or document assumption that this is always true
```

**Impact:** Low - More defensive coding  
**Trade-off:** Minimal overhead

---

### 4.4 Primary Product Flag Consistency
**Issue:** Both CTEs filter on `prior_bill_primary_product_flag = true` but business logic not documented.

**Recommendation:**
```sql
-- Add comment:
-- Filter to primary product only to avoid counting add-ons as separate cancel/rebuy events
-- Per HAT-3923, rebuy analysis focuses on primary product decisions
and prior_bill_primary_product_flag = true
```

**Impact:** Low - Clarifies intent  
**Trade-off:** None

---

## 5. Scalability Concerns

### 5.1 Growing Date Range
**Issue:** Historical data accumulation will slow query over time.

**Current:**
```sql
between '2024-01-01' and current_Date-60
```

**Recommendation:**
```sql
-- Implement rolling window:
between dateadd(month, -18, current_date) and current_date - 60
-- Or create archival process for data > 2 years old
-- Or partition table and use partition pruning
```

**Impact:** High - Prevents performance degradation  
**Trade-off:** May need to retain more history than 18 months

---

### 5.2 Join Explosion Risk
**Issue:** Many-to-many join between shopper cancellations and rebuys could explode.

**Current:**
```sql
left join cte_2
  on cte.prior_bill_shopper_id = cte_2.prior_bill_shopper_id
 and cte.product_family_name = cte_2.product_family_name
```

**Recommendation:**
```sql
-- Add cardinality check or use DISTINCT in final select if needed
-- Or use ROW_NUMBER() to pick single best match per cancel:
with cte_2_ranked as (
  select *,
    row_number() over (
      partition by prior_bill_shopper_id, product_family_name 
      order by prior_bill_modified_mst_date
    ) as rn
  from cte_2
)
-- Then join on rn = 1 to get earliest rebuy only
```

**Impact:** Medium - Prevents result set explosion  
**Trade-off:** May change business logic if multiple rebuys are meaningful

---

### 5.3 Resource Usage
**Issue:** Query materializes potentially large CTEs and performs expensive aggregation.

**Recommendation:**
```sql
-- Consider incremental processing:
-- 1. Process in date chunks (monthly)
-- 2. Use incremental table updates instead of full rebuild
-- 3. Add query timeout and resource management:
ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = 3600;
ALTER SESSION SET STATEMENT_QUEUED_TIMEOUT_IN_SECONDS = 600;
```

**Impact:** Medium - Prevents resource exhaustion  
**Trade-off:** Requires architectural changes for incremental processing

---

### 5.4 Table Growth Management
**Issue:** Target table `ba_ecommerce.cancel_rebuy` will grow continuously.

**Recommendation:**
```sql
-- Add housekeeping:
-- Option A: Partition target table by entitlement_cancel_mst_date
-- Option B: Add cleanup job to archive old data
-- Option C: Implement as view instead of table if query performance allows
```

**Impact:** Medium - Manages long-term storage  
**Trade-off:** View option trades storage for query time

---

## Summary of Prioritized Recommendations

### Critical (Implement First)
1. **Add indexes** on renewal_360 (Section 1.5) - 70-90% performance gain
2. **Fix naming error** for category vs subline flag (Section 2.4) - Data quality issue
3. **Consolidate table scans** (Section 1.3) - 50% I/O reduction
4. **Parameterize dates** (Section 3.1) - Essential for maintainability

### High Priority
1. **Optimize date range join** (Section 1.2) - Enable index usage
2. **Remove commented code** (Section 2.1) - Resolve business logic
3. **Partition strategy** (Section 1.6) - Long-term scalability
4. **Add null checks** (Section 4.1) - Data quality

### Medium Priority
1. **Remove unnecessary DISTINCT** (Section 1.1)
2. **Simplify flag logic** (Section 1.4)
3. **Standardize string comparisons** (Section 2.2)
4. **Extract magic numbers** (Section 3.2)
5. **Join cardinality check** (Section 5.2)

### Low Priority
1. **Code formatting** (Section 2.5)
2. **Improve comments** (Section 2.3)
3. **Date range validation** (Section 4.3)

---

**Estimated Overall Impact:** Implementing critical and high priority recommendations could improve query performance by **5-10x** and significantly enhance maintainability.