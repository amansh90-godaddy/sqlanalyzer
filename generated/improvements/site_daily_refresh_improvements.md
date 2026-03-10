# Site Daily Refresh Query - Improvement Recommendations

## 1. Performance Optimizations

### 1.1 Replace Massive CASE WHEN with Lookup Table Join
**Issue**: The query contains 253 sequential LIKE clauses checking for tracking codes. This forces Redshift to evaluate up to 253 string pattern matches per row, which is extremely inefficient.

**Current Approach**:
```sql
CASE
    WHEN all_itc_combined LIKE '%|~|upp_f2p_upgrade|~|%' THEN 'upp_f2p_upgrade'
    WHEN all_itc_combined LIKE '%|~|slp_wsb_ft_nocc_config|~|%' THEN 'slp_wsb_ft_nocc_config'
    -- ... 251 more WHEN clauses
END
```

**Improved Approach**:
```sql
-- Create permanent reference table
CREATE TABLE dev.ba_corporate.wam_itc_rankings (
    item_tracking_code VARCHAR(500),
    rank_order INT,
    gcr_value DECIMAL(18,2),
    unit_qty BIGINT
)
SORTKEY(rank_order)
DISTSTYLE ALL;  -- Small table, replicate to all nodes

-- Use LATERAL join or CROSS JOIN UNNEST pattern
WITH itc_exploded AS (
    SELECT 
        session_id,
        website_activity_mst_date,
        itc.value AS tracking_code,
        ROW_NUMBER() OVER (PARTITION BY session_id, website_activity_mst_date 
                          ORDER BY r.rank_order) AS match_rank
    FROM base_traffic_data t
    CROSS JOIN TABLE(SPLIT_TO_ARRAY(
        item_tracking_code_payment_attempt_list || ',' ||
        item_tracking_code_begin_checkout_list || ',' ||
        item_tracking_code_add_to_cart_list, ','
    )) AS itc
    INNER JOIN dev.ba_corporate.wam_itc_rankings r
        ON itc.value = r.item_tracking_code
)
SELECT 
    session_id,
    website_activity_mst_date,
    tracking_code AS top_ranked_tracking_code
FROM itc_exploded
WHERE match_rank = 1;
```

**Impact**: **HIGH** - Could improve query performance by 10-50x for the attribution logic  
**Trade-offs**: Requires maintenance of reference table, but provides much better maintainability

---

### 1.2 Eliminate String Concatenation with Delimiters
**Issue**: Building `all_itc_combined` with pipe delimiters creates large text fields and forces string pattern matching.

**Current Approach**:
```sql
'|~|' || COALESCE(item_tracking_code_payment_attempt_list, '') || '|~|' ||
COALESCE(item_tracking_code_begin_checkout_list, '') || '|~|' AS all_itc_combined
```

**Improved Approach**:
```sql
-- Use ARRAY columns or separate the logic into source-specific checks
-- Store ITC lists as proper arrays if possible
-- Or use EXISTS clauses instead of LIKE
WHERE EXISTS (
    SELECT 1 FROM wam_itc_rankings r
    WHERE item_tracking_code_payment_attempt_list LIKE '%' || r.item_tracking_code || '%'
    LIMIT 1
)
```

**Impact**: **HIGH** - Reduces memory footprint and improves string operation performance  
**Trade-offs**: Requires schema changes if moving to arrays

---

### 1.3 Optimize Window Function Usage
**Issue**: Multiple `ROW_NUMBER()` window functions with similar partitions are calculated separately.

**Current Approach**:
```sql
-- In base_product_data:
ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY item_tracking_code) = 1

-- In base_data:
ROW_NUMBER() OVER (PARTITION BY a.session_id, a.website_activity_mst_date 
                   ORDER BY b.item_tracking_code NULLS LAST) = 1
```

**Improved Approach**:
```sql
-- Calculate once and reuse
WITH flagged_data AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY session_id, website_activity_mst_date 
                          ORDER BY item_tracking_code NULLS LAST) AS row_num
    FROM base_product_data
)
SELECT *,
    CASE WHEN row_num = 1 THEN 1 ELSE 0 END as wam_sessions,
    CASE WHEN row_num = 1 THEN 1 ELSE 0 END as traffic_row_flag
FROM flagged_data;
```

**Impact**: **MEDIUM** - Reduces window function calculations  
**Trade-offs**: Minimal

---

### 1.4 Add Distribution and Sort Keys to Temp Tables
**Issue**: Temp tables have no distribution or sort keys specified, forcing Redshift to use default distribution.

**Current Approach**:
```sql
CREATE TABLE dev.ba_corporate.wam_site_daily
sortkey (website_date)
as
```

**Improved Approach**:
```sql
CREATE TEMP TABLE base_traffic_data_temp 
DISTSTYLE KEY
DISTKEY(session_id)
SORTKEY(website_activity_mst_date, session_id)
AS
SELECT ...;

CREATE TEMP TABLE base_product_data_temp
DISTSTYLE KEY
DISTKEY(session_id)
SORTKEY(website_activity_mst_date, session_id)
AS
SELECT ...;
```

**Impact**: **HIGH** - Improves join performance by 2-5x when data is co-located  
**Trade-offs**: Slight overhead in temp table creation, but worth it for large datasets

---

### 1.5 Push Down Filters Earlier
**Issue**: Some filtering happens late in the query when it could be applied earlier to reduce data volume.

**Current Approach**:
```sql
FROM dev.website_prod.analytic_traffic_detail
WHERE website_activity_mst_date BETWEEN ... AND ...
    AND gd_sales_flag = TRUE
    AND session_id IS NOT NULL
```

**Improved Approach**:
```sql
-- Add more restrictive filters if business logic allows
FROM dev.website_prod.analytic_traffic_detail
WHERE website_activity_mst_date BETWEEN ... AND ...
    AND gd_sales_flag = TRUE
    AND session_id IS NOT NULL
    AND website_activity_exclusion_reason_desc IS NULL
    -- Add index/partition filters if available
    AND channel_grouping_name IS NOT NULL  -- if always needed
    -- Consider limiting to specific business units if applicable
    AND web_business_unit_name IN ('relevant', 'units', 'only')
```

**Impact**: **MEDIUM** - Reduces I/O and processing volume  
**Trade-offs**: Need to validate filters don't exclude valid data

---

### 1.6 Optimize Final Lookup Join
**Issue**: The final LEFT JOIN uses a subquery with GROUP BY that could be pre-materialized.

**Current Approach**:
```sql
LEFT JOIN (select itemtrackingcode as item_tracking_code, 
                  itcgrouping as itc_grouping 
           from dev.ba_corporate.wam_itc_site 
           group by 1,2) b 
```

**Improved Approach**:
```sql
-- Create a proper dimension table
CREATE TABLE IF NOT EXISTS dev.ba_corporate.dim_wam_itc_grouping
DISTSTYLE ALL
AS
SELECT DISTINCT 
    itemtrackingcode as item_tracking_code, 
    itcgrouping as itc_grouping 
FROM dev.ba_corporate.wam_itc_site;

-- Then use direct join
LEFT JOIN dev.ba_corporate.dim_wam_itc_grouping b 
    ON a.top_ranked_tracking_code = b.item_tracking_code
```

**Impact**: **MEDIUM** - Eliminates runtime aggregation  
**Trade-offs**: Requires table maintenance

---

### 1.7 Replace DELETE + INSERT with MERGE
**Issue**: Using DELETE followed by INSERT is less efficient than a MERGE/UPSERT pattern.

**Current Approach**:
```sql
DELETE FROM dev.ba_corporate.wam_site_performance1
WHERE website_date BETWEEN ... AND ...;

INSERT INTO dev.ba_corporate.wam_site_performance1
SELECT * from dev.ba_corporate.wam_site_daily;
```

**Improved Approach**:
```sql
-- For Redshift, use staging + transaction pattern
BEGIN TRANSACTION;

CREATE TEMP TABLE staging_data AS
SELECT * FROM dev.ba_corporate.wam_site_daily;

DELETE FROM dev.ba_corporate.wam_site_performance1
USING staging_data s
WHERE wam_site_performance1.website_date = s.website_date
  AND wam_site_performance1.top_ranked_tracking_code = s.top_ranked_tracking_code
  -- Add other key columns
;

INSERT INTO dev.ba_corporate.wam_site_performance1
SELECT * FROM staging_data;

END TRANSACTION;

-- Or consider partitioning by date and swapping partitions
```

**Impact**: **MEDIUM** - More efficient delete pattern, better transaction handling  
**Trade-offs**: Slightly more complex

---

## 2. Code Quality

### 2.1 Externalize Magic Numbers
**Issue**: Hardcoded values like `-16` days, specific filter values embedded in code.

**Current Approach**:
```sql
CURRENT_DATE - 16 AS min_processing_date
```

**Improved Approach**:
```sql
-- Create configuration table
CREATE TABLE IF NOT EXISTS dev.ba_corporate.wam_config (
    config_key VARCHAR(100),
    config_value VARCHAR(500),
    description VARCHAR(1000)
);

INSERT INTO dev.ba_corporate.wam_config VALUES
('LOOKBACK_DAYS', '16', 'Number of days to look back for incremental refresh'),
('PRODUCT_LINE', 'Websites and Marketing,Website Builder', 'Product lines to include');

-- Use in query
SELECT
    CURRENT_DATE - config_value::INT AS min_processing_date,
    CURRENT_DATE AS max_processing_date
FROM dev.ba_corporate.wam_config
WHERE config_key = 'LOOKBACK_DAYS';
```

**Impact**: **LOW** - Improves maintainability  
**Trade-offs**: Adds configuration management overhead

---

### 2.2 Standardize NULL Handling
**Issue**: Inconsistent use of 'Unknown', 'N/A', 'Not attributed' for NULL values.

**Current Approach**:
```sql
COALESCE(channel_grouping_name,'Unknown')
COALESCE(order_item_tracking_code,'N/A')
COALESCE(final_tracking_code,'Unknown')
COALESCE(top_ranked_tracking_code, 'Not attributed')
```

**Improved Approach**:
```sql
-- Define constants at top
-- Use consistent pattern: 'Unknown' for dimensions, 'None' for measures
COALESCE(channel_grouping_name, 'Unknown') AS channel_grouping_name,
COALESCE(order_item_tracking_code, 'Unknown') AS order_item_tracking_code,
COALESCE(final_tracking_code, 'Not Attributed') AS final_tracking_code

-- Or better: Keep NULLs and handle in reporting layer
-- This preserves data quality visibility
```

**Impact**: **LOW** - Improves consistency and data quality visibility  
**Trade-offs**: May require BI layer changes

---

### 2.3 Remove Commented Code
**Issue**: Commented out code creates confusion.

**Current Approach**:
```sql
--select * from date_process_range;
-- AND gcr_usd_amt >0
```

**Improved Approach**:
```sql
-- Remove commented code entirely
-- Use version control (Git) for history
```

**Impact**: **LOW** - Improves readability  
**Trade-offs**: None

---

### 2.4 Add Comprehensive Documentation
**Issue**: Complex business logic lacks explanation, especially the ITC attribution logic.

**Improved Approach**:
```sql
/*
Purpose: Daily incremental refresh of WAM site performance data
Author: [Team Name]
JIRA: HAT-3917
Last Modified: YYYY-MM-DD

Process Overview:
1. Define date range (16-day lookback for safety on 14-day WAD refresh)
2. Extract traffic data from analytic_traffic_detail
3. Extract product/GCR data from bill_line_traffic_ext
4. Join and deduplicate to prevent double-counting sessions with multiple products
5. Apply ITC attribution waterfall (order > payment > checkout > cart > click > impression)
6. Aggregate metrics by dimensional grain
7. Merge into target performance table

ITC Attribution Logic:
- Rank 1-95: ITCs with GCR > 0 (revenue-generating)
- Rank 96-253: ITCs with GCR = 0 (free tier, trials)
- Waterfall priority: order_itc > payment_attempt > begin_checkout > add_to_cart > click > impression
- If no match: 'Not attributed'

Performance Considerations:
- Processes approximately [X] million rows per day
- Runtime: ~[X] minutes
- Target SLA: Complete by [time]
*/
```

**Impact**: **LOW** - Improves maintainability and knowledge transfer  
**Trade-offs**: None

---

### 2.5 Improve Naming Conventions
**Issue**: Some names are unclear or inconsistent.

**Current Approach**:
```sql
base_product_data_raw
base_product_data  
base_data
top_ranked_extract
final_attribution
final_output
```

**Improved Approach**:
```sql
cte_traffic_base
cte_product_base
cte_traffic_product_joined
cte_itc_attributed
cte_deduplicated
cte_aggregated
```

**Impact**: **LOW** - Improves readability  
**Trade-offs**: None

---

## 3. Maintainability

### 3.1 Modularize ITC Attribution Logic
**Issue**: 253 hardcoded tracking codes make the query unmaintainable.

**Current Approach**: Giant CASE statement embedded in query

**Improved Approach**:
```sql
-- Create stored procedure or separate ETL step
CREATE OR REPLACE PROCEDURE sp_refresh_itc_rankings()
AS $$
BEGIN
    TRUNCATE TABLE dev.ba_corporate.wam_itc_rankings;
    
    INSERT INTO dev.ba_corporate.wam_itc_rankings
    SELECT 
        item_tracking_code,
        ROW_NUMBER() OVER (ORDER BY 
            CASE WHEN gcr_value > 0 THEN 0 ELSE 1 END,
            gcr_value DESC,
            unit_qty DESC
        ) AS rank_order,
        gcr_value,
        unit_qty
    FROM (
        -- Query to calculate current rankings from source data
        SELECT ...
    );
END;
$$ LANGUAGE plpgsql;

-- Call this weekly/monthly to refresh rankings
CALL sp_refresh_itc_rankings();
```

**Impact**: **HIGH** - Makes ITC ranking updates manageable  
**Trade-offs**: Requires process to refresh ranking table

---

### 3.2 Add Error Handling and Logging
**Issue**: No error handling or audit trail.

**Improved Approach**:
```sql
-- Create audit table
CREATE TABLE IF NOT EXISTS dev.ba_corporate.wam_etl_audit (
    run_id BIGINT IDENTITY(1,1),
    job_name VARCHAR(200),
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    status VARCHAR(50),
    rows_processed BIGINT,
    error_message VARCHAR(5000)
);

-- In main script
BEGIN TRANSACTION;

INSERT INTO dev.ba_corporate.wam_etl_audit (job_name, start_time, status)
VALUES ('site_daily_refresh', GETDATE(), 'RUNNING');

-- Get run_id
SET @run_id = LAST_INSERT_ID();

-- Main processing logic with exception handling
BEGIN
    -- Your query here
    
    -- Update audit on success
    UPDATE dev.ba_corporate.wam_etl_audit
    SET end_time = GETDATE(),
        status = 'SUCCESS',
        rows_processed = (SELECT COUNT(*) FROM dev.ba_corporate.wam_site_daily)
    WHERE run_id = @run_id;
    
EXCEPTION WHEN OTHERS THEN
    UPDATE dev.ba_corporate.wam_etl_audit
    SET end_time = GETDATE(),
        status = 'FAILED',
        error_message = SQLERRM
    WHERE run_id = @run_id;
    RAISE;
END;

END TRANSACTION;
```

**Impact**: **MEDIUM** - Improves observability and debugging  
**Trade-offs**: Slight performance overhead

---

### 3.3 Parameterize Date Range
**Issue**: Date range is hardcoded in the script.

**Current Approach**:
```sql
CURRENT_DATE - 16 AS min_processing_date
```

**Improved Approach**:
```sql
-- Accept parameters (implementation depends on orchestration tool)
-- Using variables:
DECLARE @start_date DATE = COALESCE(:start_date, CURRENT_DATE - 16);
DECLARE @end_date DATE = COALESCE(:end_date, CURRENT_DATE);

CREATE TEMP TABLE date_process_range AS
SELECT
    @start_date AS min_processing_date,
    @end_date AS max_processing_date;
```

**Impact**: **MEDIUM** - Enables backfills and custom date ranges  
**Trade-offs**: Requires parameter passing mechanism

---

### 3.4 Break Down Monolithic Query
**Issue**: Single massive query is hard to debug and maintain.

**Improved Approach**:
```sql
-- Step 1: Extract to staging tables
CREATE TABLE staging.traffic_extracted AS ...;
CREATE TABLE staging.product_extracted AS ...;

-- Step 2: Join and deduplicate
CREATE TABLE staging.traffic_product_joined AS ...;

-- Step 3: Apply attribution
CREATE TABLE staging.attributed_sessions AS ...;

-- Step 4: Aggregate
CREATE TABLE staging.aggregated_final AS ...;

-- Step 5: Load to target
-- This allows you to inspect intermediary results and pinpoint issues
```

**Impact**: **HIGH** - Much easier to debug and maintain  
**Trade-offs**: More storage for staging tables, but worth it

---

## 4. Data Quality

### 4.1 Add Data Validation Checks
**Issue**: No validation of input data quality or output expectations.

**Improved Approach**:
```sql
-- Pre-processing validation
CREATE TEMP TABLE validation_results AS
SELECT
    'Source row count' AS check_name,
    COUNT(*) AS check_value,
    100000 AS expected_min,  -- Adjust based on baseline
    NULL AS expected_max
FROM dev.website_prod.analytic_traffic_detail
WHERE website_activity_mst_date BETWEEN (SELECT min_processing_date FROM date_process_range) 
                                    AND (SELECT max_processing_date FROM date_process_range)
UNION ALL
SELECT
    'Null session_id count',
    COUNT(*),
    0,
    100  -- Tolerate some nulls but flag if excessive
FROM dev.website_prod.analytic_traffic_detail
WHERE website_activity_mst_date BETWEEN ...
  AND session_id IS NULL;

-- Check for anomalies
SELECT * FROM validation_results
WHERE check_value < expected_min 
   OR check_value > expected_max;

-- Post-processing validation
CREATE TEMP TABLE output_validation AS
SELECT
    website_date,
    COUNT(*) AS row_count,
    SUM(sessions) AS total_sessions,
    SUM(GCR) AS total_gcr
FROM dev.ba_corporate.wam_site_daily
GROUP BY website_date;

-- Flag anomalies (>50% deviation from average)
SELECT * FROM output_validation
WHERE total_sessions < (SELECT AVG(total_sessions) * 0.5 FROM output_validation)
   OR total_sessions > (SELECT AVG(total_sessions) * 1.5 FROM output_validation);
```

**Impact**: **HIGH** - Catches data quality issues early  
**Trade-offs**: Requires baseline establishment and ongoing monitoring

---

### 4.2 Improve String Delimiter Safety
**Issue**: Using `|~|` as delimiter assumes it never appears in data.

**Current Approach**:
```sql
'|~|' || COALESCE(item_tracking_code_payment_attempt_list, '') || '|~|'
```

**Improved Approach**:
```sql
-- Use a character guaranteed not to appear in tracking codes
-- Or better: use ARRAY data types if Redshift supports for your use case
-- Or use SPLIT_PART and iterate through lists properly

-- If using delimiter, document and validate:
-- Check for delimiter in source data
SELECT COUNT(*) 
FROM dev.website_prod.analytic_traffic_detail
WHERE item_tracking_code_payment_attempt_list LIKE '%|~|%'
   OR item_tracking_code_begin_checkout_list LIKE '%|~|%';
-- Should return 0, otherwise delimiter is unsafe
```

**Impact**: **MEDIUM** - Prevents rare but severe data corruption  
**Trade-offs**: None

---

### 4.3 Handle Edge Cases in Business Logic
**Issue**: Some business logic may have edge cases not handled.

**Example - Plan Type Mapping**:
```sql
-- Current: Uses CASE with specific values
CASE WHEN lower(product_pnl_subline_name) IN ('gocentral seo', 'gocentral marketing') 
     THEN 'Marketing'
     ...
     ELSE product_pnl_subline_name END plan_type
```

**Improved**:
```sql
-- Create reference table for mappings
CREATE TABLE dev.ba_corporate.ref_product_plan_mapping (
    product_pnl_subline_name VARCHAR(200),
    plan_type VARCHAR(100),
    effective_date DATE,
    expiration_date DATE
);

-- Use join for mapping
LEFT JOIN dev.ba_corporate.ref_product_plan_mapping pm
    ON LOWER(product_pnl_subline_name) = LOWER(pm.product_pnl_subline_name)
    AND website_activity_mst_date BETWEEN pm.effective_date AND pm.expiration_date

-- Log unmapped values
INSERT INTO dev.ba_corporate.data_quality_log
SELECT 
    CURRENT_TIMESTAMP,
    'Unmapped product_pnl_subline_name',
    product_pnl_subline_name,
    COUNT(*)
FROM base_product_data
WHERE plan_type IS NULL
GROUP BY product_pnl_subline_name;
```

**Impact**: **MEDIUM** - Improves data quality and maintainability  
**Trade-offs**: Requires reference table maintenance

---

### 4.4 Add NULL Safety in Joins
**Issue**: Potential for NULL values in join keys causing unexpected behavior.

**Current Approach**:
```sql
LEFT JOIN base_product_data b 
    ON a.website_activity_mst_date = b.website_activity_mst_date
    AND a.session_id = b.session_id
```

**Improved Approach**:
```sql
-- Add NULL checks in ON clause or WHERE clause
LEFT JOIN base_product_data b 
    ON a.website_activity_mst_date = b.website_activity_mst_date
    AND a.session_id = b.session_id
    AND a.session_id IS NOT NULL  -- Make explicit
    
-- Or validate before join
WHERE session_id IS NOT NULL
```

**Impact**: **LOW** - Prevents subtle bugs  
**Trade-offs**: None if session_id is already required

---

## 5. Scalability Concerns

### 5.1 Implement Incremental Processing Strategy
**Issue**: 16-day window may become a bottleneck as data volume grows.

**Current Approach**: Full reprocessing of 16-day window

**Improved Approach**:
```sql
-- Process only new/changed data
-- Add watermark table
CREATE TABLE dev.ba_corporate.wam_etl_watermark (
    table_name VARCHAR(200),
    last_processed_date DATE,
    last_update_timestamp TIMESTAMP
);

-- Track what's already processed
-- Only reprocess last 2 days (for late-arriving data)
-- Keep 16-day window for historical corrections if needed
CURRENT_DATE - 2 AS min_processing_date

-- Add logic to detect and reprocess specific dates if corrections needed
```

**Impact**: **HIGH** - Reduces processing time as data grows  
**Trade-offs**: More complex logic for handling late-arriving data

---

### 5.2 Implement Table Partitioning
**Issue**: No partitioning strategy mentioned for target tables.

**Improved Approach**:
```sql
-- Partition target table by date
CREATE TABLE dev.ba_corporate.wam_site_performance1 (
    website_date DATE,
    channel_grouping_name VARCHAR(100),
    -- ... other columns
)
DISTSTYLE KEY
DISTKEY(channel_grouping_name)
SORTKEY(website_date, top_ranked_tracking_code)
-- For Redshift: use date-based table names or external table partitioning
;

-- Or use separate tables by month and UNION ALL view
CREATE TABLE dev.ba_corporate.wam_site_performance1_202603 AS ...;
CREATE TABLE dev.ba_corporate.wam_site_performance1_202604 AS ...;

CREATE VIEW dev.ba_corporate.wam_site_performance1 AS
SELECT * FROM dev.ba_corporate.wam_site_performance1_202603
UNION ALL
SELECT * FROM dev.ba_corporate.wam_site_performance1_202604
...;
```

**Impact**: **HIGH** - Improves query performance on target table, enables efficient purging  
**Trade-offs**: Requires partition management process

---

### 5.3 Add Resource Management
**Issue**: Large queries can impact other workloads.

**Improved Approach**:
```sql
-- Use Redshift WLM (Workload Management)
-- Assign to appropriate queue

-- Set query group
SET query_group TO 'etl_large';

-- Or use query execution parameters
SET enable_result_cache_for_session TO off;  -- For ETL
SET statement_timeout TO 3600000;  -- 1 hour timeout

-- Consider breaking into smaller time chunks if needed
-- Process one day at a time if 16 days is too large
```

**Impact**: **MEDIUM** - Prevents resource contention  
**Trade-offs**: Requires WLM configuration

---

### 5.4 Optimize for Growing ITC List
**Issue**: Currently 253 ITCs; will likely grow over time.

**Improved Approach**:
```sql
-- Instead of CASE WHEN for each ITC:
-- Use ranking table with automatic updates
-- Limit to top N ITCs (e.g., top 100 by GCR)
-- Group long-tail ITCs into 'Other'

WITH itc_rankings_filtered AS (
    SELECT item_tracking_code, rank_order
    FROM dev.ba_corporate.wam_itc_rankings
    WHERE rank_order <= 100  -- Only top 100
)
-- This limits complexity as ITC list grows
```

**Impact**: **HIGH** - Prevents unbounded complexity growth  
**Trade-offs**: May lose granularity on long-tail ITCs (acceptable if low value)

---

### 5.5 Consider Materialized Views or Summary Tables
**Issue**: If this data is queried frequently, computing it daily may not be sufficient.

**Improved Approach**:
```sql
-- Create pre-aggregated summary tables for common queries
CREATE TABLE dev.ba_corporate.wam_site_performance_daily_summary AS
SELECT
    website_date,
    channel_grouping_name,
    device_category_name,
    SUM(sessions) AS total_sessions,
    SUM(gcr) AS total_gcr,
    SUM(wam_sessions) AS total_wam_sessions
FROM dev.ba_corporate.wam_site_performance1
GROUP BY 1, 2, 3;

-- Create for common time grains (weekly, monthly)
-- Refresh alongside detail table
```

**Impact**: **MEDIUM** - Improves query performance for common aggregations  
**Trade-offs**: Additional storage and processing overhead

---

## 6. Critical Bug Fixes

### 6.1 Fix ANALYZE Statement
**Issue**: ANALYZE statement references wrong table name.

**Current**:
```sql
analyze dev.ba_corporate.wam_site_performance;
```

**Fixed**:
```sql
ANALYZE dev.ba_corporate.wam_site_performance1;
```

**Impact**: **HIGH** - Query optimizer has stale statistics on target table  
**Trade-offs**: None

---

### 6.2 Remove Unnecessary Transaction Wrapper
**Issue**: Transaction wrapper around read-only temp table creation is not needed and may cause locks.

**Current**:
```sql
begin transaction;
DROP TABLE IF EXISTS dev.ba_corporate.wam_site_daily;
CREATE TABLE dev.ba_corporate.wam_site_daily
...
end transaction;
```

**Improved**:
```sql
-- Remove transaction wrapper
-- Or only wrap the actual data modification:
DROP TABLE IF EXISTS dev.ba_corporate.wam_site_daily;
CREATE TABLE dev.ba_corporate.wam_site_daily
...

-- Transaction only around target table modification:
BEGIN TRANSACTION;
DELETE FROM dev.ba_corporate.wam_site_performance1 WHERE ...;
INSERT INTO dev.ba_corporate.wam_site_performance1 SELECT ...;
ANALYZE dev.ba_corporate.wam_site_performance1;
END TRANSACTION;
```

**Impact**: **MEDIUM** - Reduces lock duration  
**Trade-offs**: Need to ensure temp table creation succeeds before modifying target

---

## Summary of Priority Improvements

### Must-Fix (High Impact, Critical)
1. **Replace 253-case LIKE pattern with lookup table join** - Massive performance improvement
2. **Fix ANALYZE statement** - Corrects bug
3. **Add distribution/sort keys to temp tables** - Significant join performance improvement
4. **Modularize ITC attribution logic** - Makes system maintainable

### Should-Fix (High Value)
5. **Add data validation checks** - Prevents data quality issues
6. **Break down monolithic query** - Improves debuggability
7. **Implement incremental processing** - Future-proofs for growth
8. **Add error handling and logging** - Improves observability

### Nice-to-Have (Low Effort, Good ROI)
9. **Standardize NULL handling** - Improves consistency
10. **Remove commented code** - Improves readability
11. **Add comprehensive documentation** - Knowledge transfer
12. **Parameterize date ranges** - Flexibility for backfills

### Future Considerations
13. **Table partitioning strategy** - For very large scale
14. **Materialized summary tables** - If query performance becomes an issue
15. **Resource management (WLM)** - If contention occurs