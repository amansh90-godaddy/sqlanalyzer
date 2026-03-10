# SQL Query Analysis: WAM Site Performance Optimization Report

## 1. Performance Optimizations

### 1.1 Critical: Replace Massive CASE WHEN with Table-Driven Lookup
**Impact: HIGH** | **Effort: Medium** | **Estimated Performance Gain: 50-80%**

**Issue**: The 253-line CASE WHEN statement with LIKE pattern matching is the single biggest performance bottleneck in this query.

```sql
-- Current approach (SLOW):
CASE
    WHEN all_itc_combined LIKE '%|~|upp_f2p_upgrade|~|%' THEN 'upp_f2p_upgrade'
    WHEN all_itc_combined LIKE '%|~|slp_wsb_ft_nocc_config|~|%' THEN 'slp_wsb_ft_nocc_config'
    -- ... 251 more conditions
END
```

**Recommended Solution**: Create a tracking code ranking table with pre-computed patterns:

```sql
-- Create persistent lookup table:
CREATE TABLE dev.ba_corporate.tracking_code_rankings (
    tracking_code VARCHAR(200) PRIMARY KEY,
    rank_order INT NOT NULL,
    gcr_value DECIMAL(18,2),
    quantity INT,
    is_active BOOLEAN DEFAULT TRUE,
    last_updated TIMESTAMP
) SORTKEY(rank_order);

-- Populate with current rankings
INSERT INTO dev.ba_corporate.tracking_code_rankings VALUES
('upp_f2p_upgrade', 1, 37541749.14, NULL, TRUE, CURRENT_TIMESTAMP),
('slp_wsb_ft_nocc_config', 2, 1597432.08, NULL, TRUE, CURRENT_TIMESTAMP),
-- ... etc

-- Replace CASE WHEN with join + window function:
tracking_code_match AS (
    SELECT 
        tre.*,
        tcr.tracking_code,
        tcr.rank_order,
        ROW_NUMBER() OVER (PARTITION BY tre.session_id ORDER BY tcr.rank_order) as match_rank
    FROM top_ranked_extract tre
    CROSS JOIN dev.ba_corporate.tracking_code_rankings tcr
    WHERE tcr.is_active = TRUE
        AND tre.all_itc_combined LIKE '%|~|' || tcr.tracking_code || '|~|%'
),
best_match AS (
    SELECT *
    FROM tracking_code_match
    WHERE match_rank = 1
)
```

**Trade-offs**: Requires maintaining a separate table, but enables dynamic updates without query changes.

---

### 1.2 Optimize String Concatenation Strategy
**Impact: MEDIUM** | **Effort: Low** | **Estimated Performance Gain: 15-25%**

**Issue**: Concatenating all ITC fields with separators for pattern matching is inefficient.

**Current Approach**:
```sql
'|~|' || COALESCE(item_tracking_code_payment_attempt_list, '') || '|~|' ||
COALESCE(item_tracking_code_begin_checkout_list, '') || '|~|' ||
-- ... more concatenations
```

**Recommended Solutions**:

**Option A**: Use UNION ALL to normalize ITC fields first:
```sql
session_itc_normalized AS (
    SELECT session_id, website_activity_mst_date, 
           SPLIT_PART(item_tracking_code_payment_attempt_list, ',', numbers.n) as itc,
           'payment_attempt' as source
    FROM base_traffic_data
    CROSS JOIN (SELECT ROW_NUMBER() OVER() as n FROM large_table LIMIT 100) numbers
    WHERE SPLIT_PART(item_tracking_code_payment_attempt_list, ',', numbers.n) != ''
    
    UNION ALL
    
    SELECT session_id, website_activity_mst_date,
           SPLIT_PART(item_tracking_code_begin_checkout_list, ',', numbers.n),
           'begin_checkout'
    FROM base_traffic_data
    -- ... repeat for other ITC fields
)
```

**Option B**: If Redshift version supports it, use array functions to flatten lists, then join directly against tracking_code_rankings.

---

### 1.3 Eliminate Redundant Window Functions
**Impact: MEDIUM** | **Effort: Low** | **Estimated Performance Gain: 10-15%**

**Issue**: Multiple ROW_NUMBER() calculations on the same partition.

**Current Code**:
```sql
-- In base_product_data:
CASE WHEN ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY item_tracking_code) = 1 
     THEN 1 ELSE 0 END as wam_sessions

-- In base_data:
CASE WHEN ROW_NUMBER() OVER (PARTITION BY a.session_id, a.website_activity_mst_date 
                             ORDER BY b.item_tracking_code NULLS LAST) = 1 
     THEN 1 ELSE 0 END as traffic_row_flag
```

**Recommended**:
```sql
-- Calculate once and reuse:
WITH session_product_ranking AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY item_tracking_code) as product_rank,
        ROW_NUMBER() OVER (PARTITION BY session_id, website_activity_mst_date 
                          ORDER BY item_tracking_code NULLS LAST) as traffic_rank
    FROM base_product_data_raw
),
base_product_data AS (
    SELECT *, 
        CASE WHEN product_rank = 1 THEN 1 ELSE 0 END as wam_sessions,
        CASE WHEN traffic_rank = 1 THEN 1 ELSE 0 END as traffic_row_flag
    FROM session_product_ranking
)
```

---

### 1.4 Add Distribution Key for Better Join Performance
**Impact: MEDIUM** | **Effort: Low**

**Issue**: No DISTKEY specified for the table creation. This can cause data shuffling during joins.

**Recommended**:
```sql
CREATE TABLE dev.ba_corporate.wam_site_performance
DISTKEY(website_date)  -- or consider DISTSTYLE ALL for small dimension tables
SORTKEY(website_date)
AS (...)
```

**Alternative**: If joining frequently on `top_ranked_tracking_code`, consider:
```sql
DISTKEY(top_ranked_tracking_code)
COMPOUND SORTKEY(website_date, top_ranked_tracking_code)
```

---

### 1.5 Push Down Filters Earlier in CTEs
**Impact: MEDIUM** | **Effort: Low** | **Estimated Performance Gain: 10-20%**

**Issue**: Filters and joins happen later than necessary, processing more data than needed.

**Recommended Changes**:

```sql
-- Add session_id IS NOT NULL filter earlier:
base_traffic_data AS (
    SELECT ...
    FROM dev.website_prod.analytic_traffic_detail
    WHERE website_activity_mst_date BETWEEN '2026-01-01' AND '2026-01-31'
        AND gd_sales_flag = TRUE
        AND session_id IS NOT NULL  -- Already there - good!
        AND website_activity_exclusion_reason_desc IS NULL
        AND (
            order_item_tracking_code_list IS NOT NULL OR
            item_tracking_code_payment_attempt_list IS NOT NULL OR
            item_tracking_code_begin_checkout_list IS NOT NULL OR
            item_tracking_code_add_to_cart_list IS NOT NULL OR
            item_tracking_code_click_list IS NOT NULL OR
            item_tracking_code_impression_list IS NOT NULL
        )  -- ADD: Skip sessions with no ITCs at all
)
```

---

### 1.6 Consider Materialized CTEs for Large Intermediate Results
**Impact: LOW-MEDIUM** | **Effort: Medium**

**Issue**: Complex CTEs are re-evaluated if referenced multiple times.

**Recommended**: For very large result sets, consider breaking into temp tables:
```sql
CREATE TEMP TABLE temp_base_traffic AS
SELECT * FROM base_traffic_data;

CREATE TEMP TABLE temp_base_product AS
SELECT * FROM base_product_data;

ANALYZE temp_base_traffic;
ANALYZE temp_base_product;
```

**Trade-off**: Adds I/O overhead but can improve complex multi-pass queries.

---

## 2. Code Quality

### 2.1 Remove Hardcoded Date Ranges
**Impact: HIGH** | **Maintainability: Critical**

**Issue**: Date range `'2026-01-01' AND '2026-01-31'` appears in two places.

**Recommended**:
```sql
-- Option 1: Use session variables (if supported):
SET query_start_date = '2026-01-01';
SET query_end_date = '2026-01-31';

-- Option 2: Define at top of query:
WITH date_params AS (
    SELECT 
        '2026-01-01'::DATE as start_date,
        '2026-01-31'::DATE as end_date
),
base_traffic_data AS (
    SELECT ...
    FROM dev.website_prod.analytic_traffic_detail, date_params
    WHERE website_activity_mst_date BETWEEN date_params.start_date AND date_params.end_date
    ...
)
```

---

### 2.2 Extract Plan Type Mapping to Lookup Table
**Impact: MEDIUM** | **Maintainability: High**

**Issue**: Plan type CASE statement is hardcoded business logic.

**Current**:
```sql
CASE WHEN lower(product_pnl_subline_name) IN ('gocentral seo', 'gocentral marketing') THEN 'Marketing'
     WHEN lower(product_pnl_subline_name) IN ('commerce plus') THEN 'Commerce Plus'
     -- ... more mappings
     ELSE product_pnl_subline_name END plan_type
```

**Recommended**:
```sql
-- Create lookup table:
CREATE TABLE dev.ba_corporate.product_plan_type_mapping (
    product_pnl_subline_name VARCHAR(200),
    plan_type VARCHAR(100),
    is_active BOOLEAN DEFAULT TRUE
);

-- Use LEFT JOIN instead of CASE:
LEFT JOIN dev.ba_corporate.product_plan_type_mapping ptm
    ON LOWER(product_pnl_subline_name) = LOWER(ptm.product_pnl_subline_name)
    AND ptm.is_active = TRUE
-- Then: COALESCE(ptm.plan_type, product_pnl_subline_name) as plan_type
```

---

### 2.3 Remove Commented-Out Code
**Impact: LOW** | **Maintainability: Medium**

**Issue**: Multiple commented lines clutter the query.

```sql
-- Remove these:
-- '|~|' || COALESCE(order_item_tracking_code_list, '') || '|~|' ||
-- '|~|' || COALESCE(order_item_tracking_code_list, '') || '|~|' AS order_itc_search,
-- AND gcr_usd_amt >0
-- create or replace view dna_sandbox.wam_site_marketing_v1
-- select * from dna_sandbox.wam_site_marketing_v1 where top_ranked_tracking_code <> 'Not attributed'
```

**Recommendation**: Remove all commented code or move to separate documentation if needed for reference.

---

### 2.4 Improve Column Naming Consistency
**Impact: LOW** | **Maintainability: Medium**

**Issues**:
- Mixing of `cnt` suffix vs explicit names (`session_cnt` vs `sessions`)
- `WAM_gcr` uses uppercase in CTE but lowercase in final output
- `traffic_row_flag` is technical, not business-friendly

**Recommended Naming Convention**:
```sql
-- Be consistent:
session_count (not session_cnt)
gcr_session_count (not gcr_session_cnt)
wam_gross_customer_revenue (not WAM_gcr)
is_primary_traffic_row (not traffic_row_flag)
```

---

### 2.5 Add Query Header Documentation
**Impact: LOW** | **Maintainability: High**

**Current**: Minimal documentation at top.

**Recommended**:
```sql
--------------------------------------------------------------------------------
-- TABLE: dev.ba_corporate.wam_site_performance
-- PURPOSE: Track WAM (Websites & Marketing) site performance with attribution
-- JIRA: HAT-3917
-- OWNER: [Team/Person]
-- REFRESH: [Daily/Weekly/Monthly]
-- DEPENDENCIES:
--   - dev.website_prod.analytic_traffic_detail
--   - dev.dna_approved.bill_line_traffic_ext
--   - dev.ba_corporate.wam_itc_site
--   - dev.ba_corporate.tracking_code_rankings (if implemented)
-- NOTES:
--   - Uses top-ranked tracking code attribution logic
--   - Deduplicates sessions to prevent double-counting
--   - Date range MUST be updated before each run
-- LAST MODIFIED: 2026-03-09
-- CHANGELOG:
--   2026-03-09: Initial creation
--------------------------------------------------------------------------------
```

---

### 2.6 Standardize NULL Handling
**Impact: MEDIUM** | **Data Quality: Medium**

**Issue**: Inconsistent COALESCE with different default values:
- `'Unknown'` for dimensions
- `'N/A'` for product fields
- `'Not attributed'` for tracking code

**Recommendation**: Create a standard or at minimum document the convention:
```sql
-- Traffic dimensions: 'Unknown'
-- Product dimensions: 'Not Applicable' (more explicit than 'N/A')
-- Attribution fields: 'Not Attributed'
-- Consider: Use NULL instead and handle in BI layer for flexibility
```

---

## 3. Maintainability

### 3.1 Externalize Business Logic to Config Tables
**Impact: HIGH** | **Effort: Medium**

**Issues to Externalize**:
1. Tracking code rankings (addressed in 1.1)
2. Plan type mappings (addressed in 2.2)
3. Product line filters
4. Channel grouping definitions (if any transformations exist)

**Additional Recommendations**:
```sql
-- Create configuration table for reusable filters:
CREATE TABLE dev.ba_corporate.wam_config (
    config_key VARCHAR(100),
    config_value VARCHAR(500),
    config_type VARCHAR(50),
    is_active BOOLEAN DEFAULT TRUE
);

INSERT INTO dev.ba_corporate.wam_config VALUES
('product_pnl_line', 'Websites and Marketing', 'product_filter', TRUE),
('product_pnl_line', 'Website Builder', 'product_filter', TRUE),
('point_of_purchase', 'Web', 'purchase_filter', TRUE);
```

---

### 3.2 Break Into Modular Sub-Queries
**Impact: MEDIUM** | **Effort: Medium**

**Issue**: Single 500+ line query is difficult to test, debug, and modify.

**Recommended Approach**: Break into incremental tables/views:
```sql
-- Step 1: Base traffic (can be a view for testing)
CREATE VIEW dev.ba_corporate.wam_base_traffic_v AS
SELECT ... FROM base_traffic_data;

-- Step 2: Base product
CREATE VIEW dev.ba_corporate.wam_base_product_v AS
SELECT ... FROM base_product_data;

-- Step 3: Attribution logic
CREATE VIEW dev.ba_corporate.wam_attribution_v AS
SELECT ... FROM final_attribution;

-- Step 4: Final aggregation
CREATE TABLE dev.ba_corporate.wam_site_performance AS
SELECT ... FROM wam_attribution_v;
```

**Benefits**:
- Each step can be tested independently
- Easier to identify performance bottlenecks
- Can reuse intermediate views for other analyses
- Simplifies debugging

---

### 3.3 Add Data Quality Checks
**Impact: MEDIUM** | **Effort: Low**

**Recommended**: Add validation CTEs before final output:

```sql
data_quality_checks AS (
    SELECT
        COUNT(*) as total_rows,
        COUNT(DISTINCT session_id) as unique_sessions,
        SUM(CASE WHEN website_date IS NULL THEN 1 ELSE 0 END) as null_dates,
        SUM(CASE WHEN session_cnt < 0 THEN 1 ELSE 0 END) as negative_sessions,
        SUM(CASE WHEN GCR < 0 THEN 1 ELSE 0 END) as negative_gcr,
        SUM(CASE WHEN sessions != session_cnt THEN 1 ELSE 0 END) as session_mismatch
    FROM final_output
),
validation AS (
    SELECT
        CASE 
            WHEN null_dates > 0 THEN 'FAIL: Null dates found'
            WHEN negative_sessions > 0 THEN 'FAIL: Negative session counts'
            WHEN negative_gcr > 0 THEN 'FAIL: Negative GCR values'
            ELSE 'PASS'
        END as validation_status
    FROM data_quality_checks
)
-- Log or raise error if validation fails
```

---

### 3.4 Separate Delimiter from Column Values
**Impact: LOW** | **Effort: Low**

**Issue**: The `|~|` delimiter is hardcoded throughout the query.

**Recommended**:
```sql
-- Define once at the top:
WITH constants AS (
    SELECT 
        '|~|' as itc_delimiter,
        '2026-01-01'::DATE as start_date,
        '2026-01-31'::DATE as end_date
),
base_traffic_data AS (
    SELECT
        ...,
        constants.itc_delimiter || COALESCE(item_tracking_code_payment_attempt_list, '') || 
        constants.itc_delimiter || ... AS all_itc_combined
    FROM dev.website_prod.analytic_traffic_detail, constants
    ...
)
```

---

## 4. Data Quality

### 4.1 Validate Date Range Boundaries
**Impact: MEDIUM** | **Effort: Low**

**Issue**: No validation that date ranges align between traffic and product data.

**Recommended**:
```sql
-- Add assertion CTE:
date_alignment_check AS (
    SELECT
        MIN(t.website_activity_mst_date) as traffic_min_date,
        MAX(t.website_activity_mst_date) as traffic_max_date,
        MIN(p.website_activity_mst_date) as product_min_date,
        MAX(p.website_activity_mst_date) as product_max_date
    FROM base_traffic_data t
    FULL OUTER JOIN base_product_data p ON 1=1
),
date_validation AS (
    SELECT
        CASE 
            WHEN traffic_min_date != product_min_date 
                OR traffic_max_date != product_max_date 
            THEN 'WARNING: Date ranges do not align'
            ELSE 'OK'
        END as date_check_status
    FROM date_alignment_check
)
```

---

### 4.2 Add Session Deduplication Validation
**Impact: HIGH** | **Effort: Low**

**Issue**: The `traffic_row_flag` prevents double-counting, but there's no validation that it works correctly.

**Recommended**:
```sql
-- Add validation CTE:
session_count_validation AS (
    SELECT 
        session_id,
        website_activity_mst_date,
        COUNT(*) as row_count,
        SUM(traffic_row_flag) as flag_sum
    FROM base_data
    GROUP BY 1, 2
    HAVING COUNT(*) > 1 AND SUM(traffic_row_flag) != 1
)
-- If this returns rows, deduplication logic is broken
```

---

### 4.3 Handle Edge Case: Zero GCR vs NULL GCR
**Impact: MEDIUM** | **Effort: Low**

**Issue**: The `free_or_paid` logic uses `COALESCE(gcr_usd_amt, 0) = 0` which treats NULL as Free.

**Current**:
```sql
CASE 
    WHEN COALESCE(gcr_usd_amt, 0) = 0 THEN 'Free'
    WHEN gcr_usd_amt > 0 AND product_free_trial_conversion_flag = 'True' THEN 'Free to Paid'
    WHEN gcr_usd_amt > 0 AND product_free_trial_conversion_flag = 'False' THEN 'Paid'
END as free_or_paid
```

**Issue**: What if `gcr_usd_amt` is NULL (no revenue data)? Is that the same as "Free" (0 revenue)?

**Recommended**:
```sql
CASE 
    WHEN gcr_usd_amt IS NULL THEN 'Unknown Revenue'
    WHEN gcr_usd_amt = 0 THEN 'Free'
    WHEN gcr_usd_amt > 0 AND product_free_trial_conversion_flag = 'True' THEN 'Free to Paid'
    WHEN gcr_usd_amt > 0 AND product_free_trial_conversion_flag = 'False' THEN 'Paid'
    ELSE 'Other'
END as free_or_paid
```

---

### 4.4 Validate Product Filter Consistency
**Impact: MEDIUM** | **Effort: Low**

**Issue**: Product line filter uses `IN ('Websites and Marketing', 'Website Builder')` - could miss variations in capitalization or spacing.

**Recommended**:
```sql
-- Make filter case-insensitive and trim whitespace:
WHERE LOWER(TRIM(product_pnl_line_name)) IN ('websites and marketing', 'website builder')
```

---

### 4.5 Add Referential Integrity Checks
**Impact: MEDIUM** | **Effort: Low**

**Issue**: No validation that joined data actually matches.

**Recommended**:
```sql
-- Add to validation suite:
orphaned_traffic AS (
    SELECT COUNT(*) as orphan_count
    FROM base_traffic_data t
    LEFT JOIN base_product_data p 
        ON t.session_id = p.session_id 
        AND t.website_activity_mst_date = p.website_activity_mst_date
    WHERE t.gcr_session_cnt > 0  -- Should have product data
        AND p.session_id IS NULL
),
orphaned_products AS (
    SELECT COUNT(*) as orphan_count
    FROM base_product_data p
    LEFT JOIN base_traffic_data t
        ON p.session_id = t.session_id 
        AND p.website_activity_mst_date = t.website_activity_mst_date
    WHERE t.session_id IS NULL
)
```

---

### 4.6 Validate String Parsing Assumptions
**Impact: MEDIUM** | **Effort: Low**

**Issue**: The query assumes ITC lists are properly delimited and don't contain the delimiter string `|~|` within values.

**Recommended Testing**:
```sql
-- Check for delimiter in raw values:
SELECT 
    session_id,
    item_tracking_code_payment_attempt_list
FROM dev.website_prod.analytic_traffic_detail
WHERE website_activity_mst_date BETWEEN '2026-01-01' AND '2026-01-31'
    AND item_tracking_code_payment_attempt_list LIKE '%|~|%'
LIMIT 100;

-- If found, need to escape or use different delimiter
```

---

## 5. Scalability Concerns

### 5.1 Partitioning Strategy for Long-Term Growth
**Impact: HIGH** | **Effort: High**

**Issue**: Table will grow continuously with daily/monthly data. Current structure has SORTKEY but no partitioning.

**Recommended**:
```sql
-- If Redshift supports date partitioning (or use separate tables per month):
CREATE TABLE dev.ba_corporate.wam_site_performance_202601
DISTKEY(website_date)
SORTKEY(website_date)
AS (
    SELECT * FROM final_output
    WHERE website_date BETWEEN '2026-01-01' AND '2026-01-31'
);

-- Create a view union for easy querying:
CREATE VIEW dev.ba_corporate.wam_site_performance AS
SELECT * FROM dev.ba_corporate.wam_site_performance_202601
UNION ALL
SELECT * FROM dev.ba_corporate.wam_site_performance_202602
-- ... etc
```

**Alternative**: Implement automated table rotation and archival strategy.

---

### 5.2 Query Timeout Risk Mitigation
**Impact: HIGH** | **Effort: Medium**

**Issue**: With 253 LIKE operations per row and growing data, query timeout is likely.

**Immediate Mitigations**:
1. Implement table-driven approach (see 1.1)
2. Add query timeout parameter:
```sql
SET statement_timeout = '30min';  -- Adjust as needed
```
3. Consider incremental processing:
```sql
-- Process one day at a time and merge:
FOR each_date IN date_range LOOP
    INSERT INTO wam_site_performance
    SELECT * FROM attribution_logic
    WHERE website_date = each_date;
END LOOP;
```

---

### 5.3 Memory Usage Optimization
**Impact: MEDIUM** | **Effort: Medium**

**Issue**: Multiple CTEs with aggregations and window functions consume memory.

**Recommended**:
```sql
-- Add WLM (Workload Management) query group for resource allocation:
SET query_group TO 'etl_large';

-- Monitor actual memory usage:
SELECT query, label, is_diskbased, workmem, rows
FROM svl_query_summary
WHERE query = pg_last_query_id()
ORDER BY workmem DESC;
```

**If disk-based**: Consider increasing memory allocation or breaking into smaller chunks.

---

### 5.4 Handling Tracking Code Growth
**Impact: HIGH** | **Effort: Low** (if table-driven)

**Issue**: Currently 253 tracking codes; will likely grow over time.

**With Table-Driven Approach**:
```sql
-- Just add new row:
INSERT INTO dev.ba_corporate.tracking_code_rankings 
VALUES ('new_tracking_code', 254, 0, 0, TRUE, CURRENT_TIMESTAMP);

-- No query changes needed!
```

**Without Table-Driven Approach**: Every new tracking code requires:
1. Query modification
2. Testing
3. Redeployment
4. Risk of introducing errors

---

### 5.5 Incremental Processing Strategy
**Impact: MEDIUM** | **Effort: High**

**Issue**: Full reprocessing every run is inefficient for historical data.

**Recommended Approach**:
```sql
-- Track what's been processed:
CREATE TABLE dev.ba_corporate.wam_processing_log (
    process_date DATE PRIMARY KEY,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    row_count BIGINT,
    status VARCHAR(20)
);

-- Only process new/changed dates:
DELETE FROM dev.ba_corporate.wam_site_performance
WHERE website_date BETWEEN @start_date AND @end_date;

INSERT INTO dev.ba_corporate.wam_site_performance
SELECT * FROM final_output
WHERE website_date BETWEEN @start_date AND @end_date;

-- Log completion:
INSERT INTO dev.ba_corporate.wam_processing_log VALUES
(@process_date, @start_time, CURRENT_TIMESTAMP, @@rowcount, 'SUCCESS');
```

---

### 5.6 Index Strategy for Final Table
**Impact: MEDIUM** | **Effort: Low**

**Issue**: SORTKEY on `website_date` is good for time-series queries, but other query patterns may suffer.

**Recommended Analysis**:
```sql
-- Analyze common query patterns:
-- 1. Filter by tracking code?
-- 2. Filter by channel + device?
-- 3. Filter by date + tracking code?

-- Consider interleaved sortkey for multiple access patterns:
CREATE TABLE dev.ba_corporate.wam_site_performance
DISTKEY(website_date)
INTERLEAVED SORTKEY(website_date, top_ranked_tracking_code, channel_grouping_name)
AS (...)
```

**Trade-off**: Interleaved sortkeys are slower to maintain but better for multiple query patterns.

---

### 5.7 Add Monitoring and Alerting
**Impact: MEDIUM** | **Effort: Medium**

**Recommended Instrumentation**:
```sql
-- Add metrics table:
CREATE TABLE dev.ba_corporate.wam_performance_metrics (
    metric_date DATE,
    metric_name VARCHAR(100),
    metric_value DECIMAL(18,2),
    execution_time_seconds INT,
    query_id VARCHAR(100),
    recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Log key metrics:
INSERT INTO dev.ba_corporate.wam_performance_metrics
SELECT
    CURRENT_DATE as metric_date,
    'total_sessions' as metric_name,
    SUM(sessions) as metric_value,
    EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - @query_start_time)) as execution_time_seconds,
    pg_last_query_id() as query_id,
    CURRENT_TIMESTAMP
FROM dev.ba_corporate.wam_site_performance
WHERE website_date = CURRENT_DATE - 1;
```

---

## Priority Implementation Roadmap

### Phase 1: Critical (Immediate - Week 1)
1. **Replace CASE WHEN with table-driven lookup** (Performance: +50-80%)
2. **Remove hardcoded dates** (Maintainability)
3. **Add data quality validation checks** (Data Quality)

### Phase 2: High Priority (Week 2-3)
4. **Optimize string concatenation strategy** (Performance: +15-25%)
5. **Externalize plan type mapping** (Maintainability)
6. **Add session deduplication validation** (Data Quality)
7. **Implement incremental processing** (Scalability)

### Phase 3: Medium Priority (Week 4-6)
8. **Break into modular sub-queries** (Maintainability)
9. **Eliminate redundant window functions** (Performance: +10-15%)
10. **Add monitoring and alerting** (Operations)
11. **Implement partitioning strategy** (Scalability)

### Phase 4: Low Priority (Ongoing)
12. **Code cleanup** (remove comments, standardize naming)
13. **Documentation improvements**
14. **Optimize indexes and distribution keys** (tune based on usage)

---

## Estimated Impact Summary

| Category | Current State | After Optimizations | Improvement |
|----------|--------------|---------------------|-------------|
| **Query Runtime** | ~30-60 min (estimated) | ~5-10 min | **70-85% faster** |
| **Maintainability** | Low (hardcoded logic) | High (table-driven) | **Critical** |
| **Scalability** | Limited (pattern matching) | Good (indexed lookups) | **10x headroom** |
| **Data Quality** | Medium (implicit assumptions) | High (explicit validation) | **Significant** |
| **Code Lines** | 500+ lines | ~200 lines (modular) | **60% reduction** |