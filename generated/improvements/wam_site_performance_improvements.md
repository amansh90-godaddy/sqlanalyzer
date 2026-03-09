# SQL Query Analysis: WAM Site Performance Tracking
## Improvements & Optimization Recommendations

---

## 1. Performance Optimizations

### 1.1 Replace CASE WHEN Cascade with Lookup Table Join
**Issue:** The 253-line CASE statement with LIKE pattern matching is the primary performance bottleneck causing timeouts (JIRA HAT-3917).

**Current Approach:**
```sql
CASE
    WHEN all_itc_combined LIKE '%|~|upp_f2p_upgrade|~|%' THEN 'upp_f2p_upgrade'
    WHEN all_itc_combined LIKE '%|~|slp_wsb_ft_nocc_config|~|%' THEN 'slp_wsb_ft_nocc_config'
    -- ... 251 more WHEN clauses
END
```

**Recommended Approach:**
```sql
-- Create a permanent tracking code reference table
CREATE TABLE dev.ba_corporate.tracking_code_ranking (
    tracking_code VARCHAR(200) PRIMARY KEY,
    rank_order INT NOT NULL,
    gcr_amount DECIMAL(15,2),
    unit_qty INT
) SORTKEY(rank_order);

-- Use LATERAL join with SPLIT_TO_ARRAY for efficient matching
WITH itc_expanded AS (
    SELECT 
        session_id,
        website_activity_mst_date,
        -- ... other fields
        itc_value
    FROM base_data,
    LATERAL SPLIT_TO_ARRAY(
        REGEXP_REPLACE(
            item_tracking_code_payment_attempt_list || ',' ||
            item_tracking_code_begin_checkout_list || ',' ||
            item_tracking_code_add_to_cart_list || ',' ||
            item_tracking_code_click_list || ',' ||
            item_tracking_code_impression_list,
            ',+', ','
        ),
        ','
    ) AS itc_value
)
SELECT 
    i.*,
    MIN(r.rank_order) as best_rank,
    FIRST_VALUE(r.tracking_code) OVER (
        PARTITION BY i.session_id 
        ORDER BY r.rank_order
    ) as top_ranked_tracking_code
FROM itc_expanded i
LEFT JOIN dev.ba_corporate.tracking_code_ranking r 
    ON i.itc_value = r.tracking_code
```

**Impact:** ðŸ”´ **HIGH** - Expected 50-90% query time reduction  
**Trade-offs:** Requires creating and maintaining a reference table

---

### 1.2 Eliminate Redundant String Concatenation
**Issue:** The `all_itc_combined` field concatenates all ITC fields on every row, then performs 253 LIKE operations.

**Current Approach:**
```sql
'|~|' || COALESCE(item_tracking_code_payment_attempt_list, '') || '|~|' ||
COALESCE(item_tracking_code_begin_checkout_list, '') || '|~|' ||
-- ... more concatenations
```

**Recommended Approach:**
Use ARRAY operations instead of string concatenation:
```sql
ARRAY_CAT(
    ARRAY_CAT(
        SPLIT_TO_ARRAY(NULLIF(item_tracking_code_payment_attempt_list, ''), ','),
        SPLIT_TO_ARRAY(NULLIF(item_tracking_code_begin_checkout_list, ''), ',')
    ),
    -- ... other arrays
) as itc_array
```

**Impact:** ðŸŸ¡ **MEDIUM** - Reduces memory allocation and string operations  
**Trade-offs:** Requires different matching logic

---

### 1.3 Optimize CTE Materialization Strategy
**Issue:** Multiple CTEs are chained without consideration for materialization. Large intermediate result sets may be recomputed.

**Recommended Approach:**
```sql
-- Add explicit temp tables for large intermediate results
CREATE TEMP TABLE temp_base_traffic AS
SELECT /*+ materialize */ * FROM base_traffic_data;

ANALYZE temp_base_traffic;

-- Or use query hints if supported
WITH base_traffic_data AS MATERIALIZED (
    -- ... query
)
```

**Impact:** ðŸŸ¡ **MEDIUM** - Can reduce redundant computation  
**Trade-offs:** May increase temp space usage

---

### 1.4 Partition Strategy Enhancement
**Issue:** No DISTKEY specified; queries filtering by date may not be optimally distributed.

**Recommended Approach:**
```sql
CREATE TABLE dev.ba_corporate.wam_site_performance
DISTSTYLE KEY
DISTKEY (channel_grouping_name)  -- Most common filter after date
SORTKEY (website_date, channel_grouping_name)
COMPOUND SORTKEY (website_date, channel_grouping_name, device_category_name)
```

**Impact:** ðŸŸ¡ **MEDIUM** - Improves query performance for common filter patterns  
**Trade-offs:** Must align with query patterns; may impact load performance

---

### 1.5 Pre-filter Before String Operations
**Issue:** String concatenation and pattern matching occur on all rows before filtering.

**Recommended Approach:**
```sql
-- Move filters earlier in the pipeline
WITH base_traffic_data AS (
    SELECT
        session_id,
        website_activity_mst_date,
        -- ... fields
    FROM dev.website_prod.analytic_traffic_detail
    WHERE website_activity_mst_date BETWEEN '2026-01-01' AND '2026-01-31'
        AND gd_sales_flag = TRUE
        AND session_id IS NOT NULL
        AND website_activity_exclusion_reason_desc IS NULL
        AND channel_grouping_name IN ('Organic Search', 'Direct', 'Paid Search')  -- If applicable
        -- Add more filters here before expensive operations
)
```

**Impact:** ðŸŸ¡ **MEDIUM** - Reduces rows processed in expensive operations  
**Trade-offs:** None if filters are logically equivalent

---

### 1.6 Optimize ROW_NUMBER() Usage
**Issue:** `ROW_NUMBER()` in `traffic_row_flag` is computed on every row, even those that won't be used.

**Recommended Approach:**
```sql
-- Apply row numbering only where needed, after initial aggregation
WITH pre_aggregated AS (
    SELECT 
        session_id,
        website_activity_mst_date,
        MIN(item_tracking_code) as first_item_tracking_code,
        -- ... other aggregations
    FROM base_data
    GROUP BY session_id, website_activity_mst_date
)
```

**Impact:** ðŸŸ¢ **LOW** - Marginal improvement  
**Trade-offs:** May change result granularity

---

## 2. Code Quality

### 2.1 Remove Commented Code
**Issue:** Multiple commented-out lines reduce readability.

**Lines to Remove:**
```sql
-- Line 18: -- create or replace view dna_sandbox.wam_site_marketing_v1
-- Line 33: -- '|~|' || COALESCE(order_item_tracking_code_list, '') || '|~|' ||
-- Line 39: -- '|~|' || COALESCE(order_item_tracking_code_list, '') || '|~|' AS order_itc_search,
-- Line 75: -- AND gcr_usd_amt >0
-- Line 292: Final comment with test query
```

**Impact:** ðŸŸ¢ **LOW** - Improves readability  
**Trade-offs:** None

---

### 2.2 Extract Magic Values to Constants
**Issue:** The separator `'|~|'` is repeated throughout without explanation.

**Recommended Approach:**
```sql
-- At the top of the query, document constants
-- Separator pattern for ITC concatenation: '|~|'
-- This pattern was chosen to avoid conflicts with common ITC characters

-- Or use variables if your SQL dialect supports it
SET @ITC_SEPARATOR = '|~|';
```

**Impact:** ðŸŸ¢ **LOW** - Improves maintainability  
**Trade-offs:** None

---

### 2.3 Standardize NULL Handling
**Issue:** Inconsistent NULL handling - some places use `COALESCE`, others rely on `NULLIF`, others use `IS NULL`.

**Recommended Approach:**
```sql
-- Establish a consistent pattern:
-- 1. Use COALESCE for dimension fields in final output
-- 2. Use NULLIF for empty string to NULL conversion
-- 3. Use IS NULL for filtering in WHERE clauses

-- Document the strategy in comments
-- NULL Handling Strategy:
-- - Dimension fields: COALESCE to 'Unknown' in final output only
-- - Fact fields: Keep as NULL for proper aggregation
-- - String fields: Use NULLIF to convert '' to NULL before COALESCE
```

**Impact:** ðŸŸ¡ **MEDIUM** - Reduces confusion and potential bugs  
**Trade-offs:** May require query refactoring

---

### 2.4 Improve Naming Conventions
**Issue:** Inconsistent naming (snake_case mixed with abbreviations).

**Examples:**
- `gcr_session_cnt` vs `page_advance_session_cnt` (why abbreviate one?)
- `pa_sessions` (unclear abbreviation)
- `wam_sessions` vs `total_wam_units` (inconsistent prefixing)

**Recommended Approach:**
```sql
-- Standardize to full words for clarity
new_gcr_session_count  -- instead of new_gcr_session_cnt
page_advance_session_count
wam_session_count
wam_unit_count_total
```

**Impact:** ðŸŸ¢ **LOW** - Improves clarity for future developers  
**Trade-offs:** Breaking change if views/queries depend on current names

---

### 2.5 Add Comprehensive Comments
**Issue:** Complex business logic lacks explanation.

**Recommended Additions:**
```sql
-- TRAFFIC_ROW_FLAG LOGIC:
-- Purpose: Prevents double-counting traffic metrics when a session purchases multiple products
-- Method: Assigns flag=1 to only the first product per session (ordered by item_tracking_code)
-- Impact: session_cnt, pa_session_cnt, gcr_session_cnt are multiplied by this flag
-- Example: Session with 3 products -> only first product gets traffic credit

-- TRACKING CODE RANKING STRATEGY:
-- Priority order:
--   1. order_item_tracking_code (actual purchase ITC) - highest priority
--   2. Ranked ITCs from funnel fields (payment_attempt -> checkout -> cart -> click -> impression)
--   3. 'Not attributed' if no match found
-- Ranking based on historical GCR (Ranks 1-95) then unit quantity (Ranks 96-253)
```

**Impact:** ðŸŸ¡ **MEDIUM** - Critical for maintainability  
**Trade-offs:** None

---

## 3. Maintainability

### 3.1 Parameterize Date Ranges
**Issue:** Hardcoded dates appear in multiple locations.

**Current Approach:**
```sql
WHERE website_activity_mst_date BETWEEN '2026-01-01' AND '2026-01-31'
```

**Recommended Approach:**
```sql
-- Option 1: Use session variables (Redshift)
-- At query start:
-- SET query_start_date = '2026-01-01';
-- SET query_end_date = '2026-01-31';

-- Option 2: Create a date dimension table
WITH date_params AS (
    SELECT 
        DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month') as start_date,
        DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '1 day' as end_date
)
SELECT * FROM base_traffic_data, date_params
WHERE website_activity_mst_date BETWEEN date_params.start_date AND date_params.end_date
```

**Impact:** ðŸ”´ **HIGH** - Enables automation and reduces errors  
**Trade-offs:** Requires additional setup for dynamic date logic

---

### 3.2 Externalize Tracking Code Mapping
**Issue:** The 253 tracking codes are embedded in query logic.

**Recommended Approach:**
```sql
-- Create a configuration table
CREATE TABLE dev.ba_corporate.tracking_code_config (
    tracking_code VARCHAR(200),
    rank_order INT,
    gcr_amount DECIMAL(15,2),
    unit_qty INT,
    gcr_tier VARCHAR(20), -- 'Revenue Generating' or 'Zero Revenue'
    created_date TIMESTAMP DEFAULT GETDATE(),
    updated_date TIMESTAMP DEFAULT GETDATE(),
    is_active BOOLEAN DEFAULT TRUE
);

-- Populate from CSV
COPY dev.ba_corporate.tracking_code_config
FROM 's3://bucket/tracking_codes.csv'
IAM_ROLE 'arn:aws:iam::xxxx'
CSV DELIMITER ',' IGNOREHEADER 1;

-- Use in query
LEFT JOIN dev.ba_corporate.tracking_code_config tc 
    ON tc.tracking_code = <extracted_code>
    AND tc.is_active = TRUE
```

**Impact:** ðŸ”´ **HIGH** - Enables non-developer updates to tracking codes  
**Trade-offs:** Requires change management process for config table

---

### 3.3 Modularize Product Classification Logic
**Issue:** Product type mapping logic is embedded in CTE.

**Current Approach:**
```sql
CASE WHEN lower(product_pnl_subline_name) IN ('gocentral seo', 'gocentral marketing') then 'Marketing'
     WHEN lower(product_pnl_subline_name) IN ('commerce plus') then 'Commerce Plus'
     -- ... more cases
END plan_type
```

**Recommended Approach:**
```sql
-- Create dimension table
CREATE TABLE dev.ba_corporate.product_type_mapping (
    product_pnl_subline_name_lower VARCHAR(100),
    plan_type VARCHAR(50),
    effective_date DATE,
    expiration_date DATE
);

-- Use in query
LEFT JOIN dev.ba_corporate.product_type_mapping ptm
    ON LOWER(product_pnl_subline_name) = ptm.product_pnl_subline_name_lower
    AND website_activity_mst_date BETWEEN ptm.effective_date AND ptm.expiration_date
```

**Impact:** ðŸŸ¡ **MEDIUM** - Enables product catalog changes without code deployment  
**Trade-offs:** Additional join complexity

---

### 3.4 Create View for Source Field Logic
**Issue:** The source field determination logic is repeated conceptually.

**Recommended Approach:**
```sql
-- Create a separate function or view for this logic
CREATE OR REPLACE VIEW source_attribution_v AS
SELECT 
    session_id,
    website_activity_mst_date,
    CASE
        WHEN order_item_tracking_code IS NOT NULL THEN 'order_itc'
        WHEN payment_attempt_search LIKE '%' || tracking_code || '%' THEN 'payment_attempt'
        WHEN begin_checkout_search LIKE '%' || tracking_code || '%' THEN 'begin_checkout'
        WHEN add_to_cart_search LIKE '%' || tracking_code || '%' THEN 'add_to_cart'
        WHEN click_search LIKE '%' || tracking_code || '%' THEN 'click'
        WHEN impression_search LIKE '%' || tracking_code || '%' THEN 'impression'
        ELSE NULL
    END as source_field
FROM ...
```

**Impact:** ðŸŸ¢ **LOW** - Improves reusability  
**Trade-offs:** Additional database object to maintain

---

### 3.5 Version Control for Query Logic
**Issue:** No indication of query version or change history beyond JIRA reference.

**Recommended Approach:**
```sql
-- Add metadata at top of query
/*
 * Query: WAM Site Performance Tracking
 * Version: 2.0.0
 * Last Modified: 2026-01-15
 * Author: Analytics Team
 * JIRA: HAT-3917 (Timeout issue)
 * 
 * Change Log:
 * - 2.0.0 (2026-01-15): Refactored ITC matching to use lookup table (HAT-3917)
 * - 1.5.0 (2025-12-10): Added traffic_row_flag to prevent double-counting
 * - 1.0.0 (2025-11-01): Initial implementation
 */
```

**Impact:** ðŸŸ¢ **LOW** - Improves change tracking  
**Trade-offs:** None

---

## 4. Data Quality

### 4.1 Add Data Validation Checks
**Issue:** No validation that traffic and product data are joining correctly.

**Recommended Approach:**
```sql
-- Add quality checks as separate queries or CTEs
WITH data_quality_checks AS (
    SELECT
        'Unmatched sessions' as check_name,
        COUNT(*) as issue_count
    FROM base_traffic_data a
    LEFT JOIN base_product_data b USING (session_id, website_activity_mst_date)
    WHERE a.session_id IS NOT NULL AND b.session_id IS NULL
    
    UNION ALL
    
    SELECT
        'Orphan product records' as check_name,
        COUNT(*) as issue_count
    FROM base_product_data a
    LEFT JOIN base_traffic_data b USING (session_id, website_activity_mst_date)
    WHERE a.session_id IS NOT NULL AND b.session_id IS NULL
    
    UNION ALL
    
    SELECT
        'Sessions with NULL tracking codes after extraction' as check_name,
        COUNT(*) as issue_count
    FROM top_ranked_extract
    WHERE top_ranked_tracking_code IS NULL AND order_item_tracking_code IS NULL
)
SELECT * FROM data_quality_checks WHERE issue_count > 0;
```

**Impact:** ðŸŸ¡ **MEDIUM** - Enables data quality monitoring  
**Trade-offs:** Adds query complexity

---

### 4.2 Handle Edge Case: Multiple Products per Session
**Issue:** The `traffic_row_flag` logic may not handle all edge cases correctly.

**Potential Issue:**
```sql
-- What if two products have the same item_tracking_code in one session?
-- Current logic: ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY item_tracking_code)
-- This might arbitrarily pick one
```

**Recommended Approach:**
```sql
-- Make the ordering deterministic and documented
CASE WHEN ROW_NUMBER() OVER (
    PARTITION BY session_id, website_activity_mst_date 
    ORDER BY 
        item_tracking_code,
        CASE WHEN free_or_paid = 'Paid' THEN 1
             WHEN free_or_paid = 'Free to Paid' THEN 2
             WHEN free_or_paid = 'Free' THEN 3
        END,
        gcr DESC,  -- Highest revenue first
        product_term  -- Tie-breaker
) = 1 THEN 1 ELSE 0 END as traffic_row_flag
```

**Impact:** ðŸŸ¡ **MEDIUM** - Ensures consistent attribution  
**Trade-offs:** More complex logic

---

### 4.3 Validate ITC Separator Pattern
**Issue:** If tracking codes contain the separator pattern `|~|`, matching will fail.

**Recommended Approach:**
```sql
-- Add validation check
WITH invalid_itcs AS (
    SELECT DISTINCT item_tracking_code
    FROM dev.ba_corporate.tracking_code_config
    WHERE item_tracking_code LIKE '%|~|%'
)
SELECT 
    CASE WHEN COUNT(*) > 0 
         THEN 'ERROR: Tracking codes contain reserved separator pattern'
         ELSE 'OK'
    END as validation_status
FROM invalid_itcs;
```

**Impact:** ðŸŸ¢ **LOW** - Prevents silent matching failures  
**Trade-offs:** None

---

### 4.4 Add Constraints for Referential Integrity
**Issue:** No explicit foreign key relationships or constraints.

**Recommended Approach:**
```sql
-- Add NOT NULL constraints
ALTER TABLE dev.ba_corporate.wam_site_performance
ALTER COLUMN website_date SET NOT NULL;

-- Add check constraints
ALTER TABLE dev.ba_corporate.wam_site_performance
ADD CONSTRAINT chk_sessions_positive CHECK (sessions >= 0);

ALTER TABLE dev.ba_corporate.wam_site_performance
ADD CONSTRAINT chk_gcr_positive CHECK (gcr >= 0);

-- Document expected cardinality
-- Expected: 1 session should match 0 or 1 product records (may have multiple products)
```

**Impact:** ðŸŸ¢ **LOW** - Catches data issues early  
**Trade-offs:** May impact load performance slightly

---

### 4.5 Handle Free/Paid Classification Edge Cases
**Issue:** The free_or_paid logic doesn't handle NULL gcr_usd_amt explicitly.

**Current Logic:**
```sql
CASE 
    WHEN COALESCE(gcr_usd_amt, 0) = 0 THEN 'Free'
    WHEN gcr_usd_amt > 0 and product_free_trial_conversion_flag = 'True' THEN 'Free to Paid'
    WHEN gcr_usd_amt > 0 and product_free_trial_conversion_flag = 'False' THEN 'Paid'
END as free_or_paid
```

**Issue:** What if `product_free_trial_conversion_flag` is NULL?

**Recommended Approach:**
```sql
CASE 
    WHEN COALESCE(gcr_usd_amt, 0) = 0 THEN 'Free'
    WHEN gcr_usd_amt > 0 AND product_free_trial_conversion_flag = 'True' THEN 'Free to Paid'
    WHEN gcr_usd_amt > 0 AND COALESCE(product_free_trial_conversion_flag, 'False') = 'False' THEN 'Paid'
    ELSE 'Unknown'  -- Catch unexpected states
END as free_or_paid
```

**Impact:** ðŸŸ¢ **LOW** - Handles edge cases explicitly  
**Trade-offs:** None

---

## 5. Scalability Concerns

### 5.1 Address Timeout Risk (JIRA HAT-3917)
**Issue:** Query currently times out due to expensive string operations.

**Root Causes:**
1. 253 sequential LIKE operations on concatenated strings
2. String concatenation on every row before filtering
3. No indexes on string fields (inherently)

**Recommended Solution (Priority Order):**
1. âœ… Replace CASE/LIKE with lookup table join (Section 1.1) - **Critical**
2. âœ… Split ITC strings to arrays and use ANY/ALL operators (Section 1.2)
3. âœ… Add more aggressive filtering before string operations (Section 1.5)

**Impact:** ðŸ”´ **HIGH** - Directly addresses the timeout issue  
**Trade-offs:** Requires schema changes and data migration

---

### 5.2 Handle Data Volume Growth
**Issue:** As tracking codes and date ranges grow, performance will degrade.

**Current State:**
- Processes 1 month of data
- 253 tracking codes to match
- Unknown session volume (estimate: millions?)

**Recommended Approach:**
```sql
-- Option 1: Incremental processing
-- Process daily and aggregate to monthly
CREATE TABLE dev.ba_corporate.wam_site_performance_daily (
    website_date DATE NOT NULL,
    -- ... same schema
) 
PARTITION BY (website_date)
SORTKEY (website_date);

-- Then aggregate to monthly view
CREATE VIEW dev.ba_corporate.wam_site_performance_monthly AS
SELECT 
    DATE_TRUNC('month', website_date) as month,
    channel_grouping_name,
    -- ... other dimensions
    SUM(sessions) as sessions,
    SUM(gcr) as gcr
FROM dev.ba_corporate.wam_site_performance_daily
GROUP BY 1, 2, ...;

-- Option 2: Add date range partitioning
CREATE TABLE dev.ba_corporate.wam_site_performance (
    -- ... columns
)
PARTITION BY RANGE (website_date);

CREATE TABLE wam_site_performance_2026_01 
    PARTITION OF wam_site_performance
    FOR VALUES FROM ('2026-01-01') TO ('2026-02-01');
```

**Impact:** ðŸ”´ **HIGH** - Enables processing of larger date ranges  
**Trade-offs:** More complex ETL process

---

### 5.3 Optimize for Incremental Loads
**Issue:** Current query uses `CREATE OR REPLACE`, suggesting full refresh.

**Recommended Approach:**
```sql
-- Use MERGE/UPSERT for incremental loads
MERGE INTO dev.ba_corporate.wam_site_performance target
USING (
    -- Query logic here with parameterized date range
    SELECT * FROM final_output
    WHERE website_date = '2026-01-31'  -- Today's date
) source
ON target.website_date = source.website_date
   AND target.channel_grouping_name = source.channel_grouping_name
   -- ... other key fields
WHEN MATCHED THEN UPDATE SET
    sessions = source.sessions,
    gcr = source.gcr,
    -- ... other metrics
WHEN NOT MATCHED THEN INSERT VALUES (
    source.*
);

-- Delete old/corrected data first if needed
DELETE FROM dev.ba_corporate.wam_site_performance
WHERE website_date = '2026-01-31';
```

**Impact:** ðŸŸ¡ **MEDIUM** - Enables efficient daily refreshes  
**Trade-offs:** More complex load logic

---

### 5.4 Consider Materialized Views for Common Aggregations
**Issue:** If this table is frequently aggregated in the same ways, performance can be improved.

**Recommended Approach:**
```sql
-- Create materialized views for common aggregation patterns
CREATE MATERIALIZED VIEW dev.ba_corporate.wam_site_performance_by_channel AS
SELECT 
    website_date,
    channel_grouping_name,
    SUM(sessions) as sessions,
    SUM(gcr_sessions) as gcr_sessions,
    SUM(gcr) as gcr,
    SUM(wam_sessions) as wam_sessions
FROM dev.ba_corporate.wam_site_performance
GROUP BY website_date, channel_grouping_name;

-- Refresh incrementally
REFRESH MATERIALIZED VIEW dev.ba_corporate.wam_site_performance_by_channel;
```

**Impact:** ðŸŸ¡ **MEDIUM** - Speeds up common queries  
**Trade-offs:** Additional storage and refresh overhead

---

### 5.5 Monitor Query Resource Usage
**Issue:** No visibility into query performance characteristics.

**Recommended Approach:**
```sql
-- Add query monitoring
-- 1. Enable query logging in Redshift
-- 2. Create monitoring dashboard with:
--    - Query runtime trends
--    - Rows processed
--    - Temp space used
--    - Node distribution

-- Add query label for tracking
SET query_group TO 'wam_site_performance';

-- Or use query label
/* Query: WAM Site Performance | Version: 2.0 */

-- Monitor via system tables
SELECT 
    query,
    starttime,
    endtime,
    DATEDIFF(seconds, starttime, endtime) as duration_seconds,
    rows,
    bytes
FROM stl_query
WHERE querytxt LIKE '%wam_site_performance%'
ORDER BY starttime DESC
LIMIT 10;
```

**Impact:** ðŸŸ¡ **MEDIUM** - Enables proactive performance management  
**Trade-offs:** Requires monitoring infrastructure

---

### 5.6 Implement Query Result Caching Strategy
**Issue:** If the same date ranges are queried repeatedly, computation is wasted.

**Recommended Approach:**
```sql
-- Use Redshift result caching effectively
-- 1. Query must be identical (byte-for-byte)
-- 2. Underlying data must not have changed

-- Enable result caching (default in Redshift)
SET enable_result_cache_for_session TO ON;

-- For incremental pattern:
-- Cache daily results, then aggregate
-- This allows reuse of cached daily results
```

**Impact:** ðŸŸ¢ **LOW** - Helps with repeated queries  
**Trade-offs:** Only works for identical queries

---

## 6. Additional Recommendations

### 6.1 Split into Multiple Tables by Granularity
**Issue:** Current table mixes session-level and product-level dimensions.

**Recommended Approach:**
```sql
-- Fact table 1: Session-level traffic
CREATE TABLE wam_site_performance_traffic (
    website_date DATE,
    session_id VARCHAR,
    channel_grouping_name VARCHAR,
    -- ... session attributes
    sessions INT,
    gcr_sessions INT
);

-- Fact table 2: Product-level conversion
CREATE TABLE wam_site_performance_products (
    website_date DATE,
    session_id VARCHAR,
    product_term VARCHAR,
    plan_type VARCHAR,
    wam_sessions INT,
    gcr DECIMAL
);

-- Join at query time for specific analyses
```

**Impact:** ðŸŸ¡ **MEDIUM** - Improves flexibility and query performance  
**Trade-offs:** More complex data model

---

### 6.2 Add Unit Tests for Business Logic
**Issue:** Complex business logic (traffic_row_flag, ranking, attribution) has no automated tests.

**Recommended Approach:**
```sql
-- Create test cases table
CREATE TABLE dev.ba_corporate.wam_site_perf_test_cases (
    test_name VARCHAR,
    test_input JSON,
    expected_output JSON
);

-- Example test case
INSERT INTO wam_site_perf_test_cases VALUES (
    'single_session_single_product',
    '{"session_id": "test1", "products": [{"itc": "upp_f2p_upgrade", "gcr": 100}]}',
    '{"top_ranked_tracking_code": "upp_f2p_upgrade", "wam_sessions": 1, "traffic_row_flag": 1}'
);

-- Run tests
-- (Implementation depends on testing framework)
```

**Impact:** ðŸŸ¡ **MEDIUM** - Prevents regression  
**Trade-offs:** Requires testing infrastructure

---

### 6.3 Document Performance Baseline
**Issue:** No documented performance expectations.

**Recommended Approach:**
```
Performance SLA Documentation:
- Query execution time: < 5 minutes for 1 month of data (target: 2 minutes)
- Data freshness: Daily refresh by 6 AM EST
- Resource usage: < 50% cluster capacity during refresh
- Data volume: ~10M sessions/month, ~1M products/month (Jan 2026 baseline)

Monitoring alerts:
- Alert if query time > 10 minutes
- Alert if row count deviates > 20% from previous month
- Alert if GCR sum deviates > 10% from bill_line_traffic_ext source
```

**Impact:** ðŸŸ¡ **MEDIUM** - Enables SLA management  
**Trade-offs:** Requires monitoring setup

---

## Priority Implementation Roadmap

### Phase 1 (Critical - Week 1):
1. âœ… Create tracking_code_ranking table and replace CASE/LIKE logic (Section 1.1)
2. âœ… Parameterize date ranges (Section 3.1)
3. âœ… Remove commented code (Section 2.1)

**Expected Impact:** Resolve timeout issue (HAT-3917)

### Phase 2 (High Priority - Week 2-3):
4. âœ… Implement incremental load strategy (Section 5.3)
5. âœ… Add data quality checks (Section 4.1)
6. âœ… Externalize product classification mapping (Section 3.3)

**Expected Impact:** Improve maintainability and data quality

### Phase 3 (Medium Priority - Month 2):
7. âœ… Optimize distribution and sort keys (Section 1.4)
8. âœ… Create materialized views for common patterns (Section 5.4)
9. âœ… Add comprehensive documentation (Section 2.5)

**Expected Impact:** Improve long-term scalability

### Phase 4 (Enhancement - Ongoing):
10. âœ… Implement monitoring and alerting (Section 5.5)
11. âœ… Add unit tests (Section 6.2)
12. âœ… Consider schema normalization (Section 6.1)

**Expected Impact:** Operational excellence

---

## Summary

**Most Critical Issues:**
1. ðŸ”´ String concatenation + 253 LIKE operations causing timeouts
2. ðŸ”´ Hardcoded business logic preventing configuration changes
3. ðŸ”´ No incremental load strategy for scaling

**Quick Wins:**
1. Remove commented code (5 minutes)
2. Parameterize date ranges (30 minutes)
3. Add query documentation (1 hour)

**Highest ROI:**
1. Replace CASE/LIKE with lookup table join (50-90% performance improvement)
2. Externalize tracking code config (enables business user management)
3. Implement incremental loads (enables scaling to larger date ranges)