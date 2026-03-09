```markdown
# WAM Site Performance Query Documentation

## Overview

The WAM (Websites and Marketing) Site Performance query is a comprehensive data pipeline that tracks and attributes website traffic, user behavior, and revenue metrics for GoDaddy's Websites and Marketing product line. This query creates a daily aggregated table that combines traffic analytics with product purchase data, using a sophisticated item tracking code (ITC) ranking system to attribute sessions to marketing touchpoints.

**Primary Business Purpose**: Enable marketing teams to analyze the effectiveness of various marketing campaigns and user touchpoints by tracking which tracking codes drive traffic, engagement, and revenue for WAM products.

## JIRA Context

**Ticket**: [HAT-3917](https://jira.godaddy.com/browse/HAT-3917)  
**Issue**: Timeout/Performance optimization for WAM site performance tracking  
**Date Range**: 2026-01-01 to 2026-01-31 (configurable)

## Tables Used

### Source Tables

1. **`dev.website_prod.analytic_traffic_detail`**
   - Main traffic analytics table containing session-level website activity data
   - Provides: sessions, page views, device info, channel data, item tracking codes
   - Filtered by: GD sales flag, date range, exclusion reasons

2. **`dev.dna_approved.bill_line_traffic_ext`**
   - Billing and product purchase data with traffic attribution
   - Provides: product details, revenue (GCR), units sold, plan types
   - Filtered by: WAM products only, web purchases, new purchases only

3. **`dev.ba_corporate.wam_itc_site`**
   - Reference table for ITC (Item Tracking Code) groupings
   - Provides: business-friendly grouping names for tracking codes

### Output Table

- **`dev.ba_corporate.wam_site_performance`**
  - Daily aggregated performance metrics by tracking code and dimensions
  - Includes: traffic metrics, revenue, product details, attribution source

## Key Metrics

### Traffic Metrics

- **`sessions`**: Distinct count of session IDs
- **`session_cnt`**: Total session count (deduplicated using `traffic_row_flag`)
- **`pa_sessions`**: Page advance sessions (sessions with page progression)
- **`gcr_sessions`**: Sessions that resulted in GCR (revenue)

### Product Metrics

- **`wam_sessions`**: Sessions with at least one WAM product purchase (deduplicated to 1 per session)
- **`total_wam_units`**: Total quantity of WAM product units purchased
- **`GCR`**: Gross Customer Revenue in USD from WAM products

### Attribution Metrics

- **`top_ranked_tracking_code`**: The highest-priority ITC found in the session (or "Not attributed")
- **`source_field`**: Which ITC field contained the tracking code (order_itc, payment_attempt, begin_checkout, add_to_cart, click, impression)
- **`itc_grouping`**: Business-friendly grouping name from reference table

## Business Logic

### 1. Item Tracking Code (ITC) Ranking System

The query implements a **priority-based waterfall attribution model** with 253 ranked tracking codes:

- **Ranks 1-95**: Codes with GCR > 0, ordered by total revenue
  - Example: `upp_f2p_upgrade` (Rank 1, $37.5M GCR)
- **Ranks 96-253**: Codes with GCR = 0, ordered by unit quantity
  - These represent free trials and non-revenue touchpoints

**Attribution Logic**:
1. First checks `order_item_tracking_code` (direct order attribution)
2. If not found, searches through concatenated ITC fields in rank order
3. Returns first match found following the priority hierarchy
4. Falls back to "Not attributed" if no match

### 2. ITC Field Concatenation

The query searches across 5 ITC fields in user journey order:
- `item_tracking_code_payment_attempt_list` (closest to purchase)
- `item_tracking_code_begin_checkout_list`
- `item_tracking_code_add_to_cart_list`
- `item_tracking_code_click_list`
- `item_tracking_code_impression_list` (earliest touchpoint)

Fields are concatenated with `|~|` delimiter for pattern matching: `|~|{tracking_code}|~|`

### 3. Double-Counting Prevention

**Traffic Metrics Protection** (`traffic_row_flag`):
- When a session has multiple products, only the first row (by ITC) receives traffic credit
- Uses `ROW_NUMBER() OVER (PARTITION BY session_id, date ORDER BY item_tracking_code NULLS LAST) = 1`
- Ensures traffic metrics (sessions, page advances) aren't inflated

**Product Metrics Protection** (`wam_sessions`):
- Each session gets `wam_sessions=1` only once (first product row)
- Prevents double-counting of session-level conversion metrics
- Revenue and units still sum correctly across all products

### 4. Product Classification

**Plan Type Mapping**:
- Standard plan names (e.g., "Deluxe") mapped to simplified types (e.g., "Standard")
- Handles variations: GoCentral SEO/Marketing â†’ "Marketing", Economy â†’ "Basic", etc.

**Free vs. Paid Classification**:
- **Free**: GCR = $0
- **Free to Paid**: GCR > 0 and free trial conversion flag = True
- **Paid**: GCR > 0 and free trial conversion flag = False

## Data Flow

```
1. base_traffic_data (CTE)
   â†“ [Get all GD sales traffic with ITC fields]
   
2. base_product_data_raw (CTE)
   â†“ [Get WAM product purchases and revenue]
   
3. base_product_data (CTE)
   â†“ [Add wam_sessions deduplication flag]
   
4. base_data (CTE)
   â†“ [LEFT JOIN traffic + products, add traffic_row_flag]
   
5. top_ranked_extract (CTE)
   â†“ [Apply 253-code ranking logic with CASE WHEN]
   â†“ [Apply traffic_row_flag to metrics]
   
6. final_attribution (CTE)
   â†“ [Set final_tracking_code and determine source_field]
   
7. final_output (CTE)
   â†“ [GROUP BY and aggregate all metrics]
   
8. Final SELECT
   â†“ [LEFT JOIN with wam_itc_site for business groupings]
   â†“ [ORDER BY sessions DESC]
   
9. Output Table: dev.ba_corporate.wam_site_performance
```

## Filters & Conditions

### Traffic Data Filters (`base_traffic_data`)

```sql
WHERE website_activity_mst_date BETWEEN '2026-01-01' AND '2026-01-31'
  AND gd_sales_flag = TRUE                    -- GoDaddy sales traffic only
  AND session_id IS NOT NULL                  -- Valid sessions only
  AND website_activity_exclusion_reason_desc IS NULL  -- No exclusions
```

### Product Data Filters (`base_product_data_raw`)

```sql
WHERE website_activity_mst_date BETWEEN '2026-01-01' AND '2026-01-31'
  AND product_pnl_line_name IN ('Websites and Marketing', 'Website Builder')  -- WAM products only
  AND point_of_purchase_name = 'Web'          -- Web purchases (not phone/other)
  AND exclude_reason_month_end_desc IS NULL   -- No exclusions
  AND refund_flag = FALSE                     -- No refunds
  AND chargeback_flag = FALSE                 -- No chargebacks
  AND product_pnl_new_renewal_name = 'New Purchase'  -- New purchases only (not renewals)
```

### Date Range Configuration

- **Current Setting**: January 2026 (`'2026-01-01'` to `'2026-01-31'`)
- **To Update**: Change dates in both `base_traffic_data` and `base_product_data_raw` CTEs
- **Recommendation**: Use full month ranges for consistency with reporting periods

## Output Schema

| Column | Type | Description |
|--------|------|-------------|
| `website_date` | DATE | Date of website activity (MST timezone) |
| `channel_grouping_name` | VARCHAR | Marketing channel (Organic, Paid Search, Direct, etc.) |
| `device_category_name` | VARCHAR | Device type (Desktop, Mobile, Tablet) |
| `web_region_2_name` | VARCHAR | Geographic region of the session |
| `existing_customer_flag` | VARCHAR | Whether user is existing or new customer |
| `first_hit_content_group_2_name` | VARCHAR | Landing page content group |
| `top_ranked_tracking_code` | VARCHAR | Highest-priority ITC found (or "Not attributed") |
| `source_field` | VARCHAR | ITC source field (order_itc, payment_attempt, begin_checkout, add_to_cart, click, impression) |
| `web_focal_country_name` | VARCHAR | Primary country for the session |
| `page_path_list` | VARCHAR | Concatenated list of page paths visited |
| `web_business_unit_name` | VARCHAR | Business unit attribution |
| `order_item_tracking_code` | VARCHAR | ITC from order data (if available) |
| `product_term` | VARCHAR | Product term (e.g., "12 Month", "1 Year") |
| `plan_type` | VARCHAR | Simplified plan type (Basic, Standard, Premium, Commerce, etc.) |
| `free_or_paid` | VARCHAR | Free, Paid, or Free to Paid |
| `sessions` | INTEGER | Distinct session count |
| `session_cnt` | INTEGER | Total sessions (deduplicated) |
| `pa_sessions` | INTEGER | Page advance sessions |
| `gcr_sessions` | INTEGER | Sessions with revenue |
| `wam_sessions` | INTEGER | Sessions with WAM purchase (deduplicated) |
| `total_wam_units` | INTEGER | Total WAM product units sold |
| `GCR` | DECIMAL | Gross Customer Revenue (USD) |
| `itc_grouping` | VARCHAR | Business-friendly ITC grouping name |

### SORTKEY

- **Sortkey**: `website_date` (optimizes date-range queries)

## Dependencies

### Upstream Dependencies

1. **`dev.website_prod.analytic_traffic_detail`**
   - Refresh schedule: Daily
   - Critical fields: session_id, ITC lists, channel/device dimensions

2. **`dev.dna_approved.bill_line_traffic_ext`**
   - Refresh schedule: Daily
   - Critical fields: session_id, item_tracking_code, GCR, product dimensions

3. **`dev.ba_corporate.wam_itc_site`**
   - Type: Reference table
   - Update frequency: As needed for new ITC groupings

### Downstream Dependencies

- Business Intelligence dashboards tracking WAM marketing performance
- Attribution analysis reports
- Marketing ROI calculations
- Campaign effectiveness measurement

## Usage Examples

### Example 1: Run the Full Query

```sql
-- Execute the entire script to refresh the table
-- Runtime: ~2-5 minutes for 1 month of data
DROP TABLE IF EXISTS dev.ba_corporate.wam_site_performance;
CREATE TABLE dev.ba_corporate.wam_site_performance
SORTKEY (website_date)
AS
( ... full query ... );
```

### Example 2: Query Top Performing Tracking Codes

```sql
SELECT 
    top_ranked_tracking_code,
    itc_grouping,
    SUM(sessions) AS total_sessions,
    SUM(wam_sessions) AS total_wam_sessions,
    SUM(GCR) AS total_gcr,
    SUM(GCR) / NULLIF(SUM(wam_sessions), 0) AS gcr_per_session
FROM dev.ba_corporate.wam_site_performance
WHERE top_ranked_tracking_code <> 'Not attributed'
    AND website_date BETWEEN '2026-01-01' AND '2026-01-31'
GROUP BY 1, 2
ORDER BY total_gcr DESC
LIMIT 20;
```

### Example 3: Attribution Source Analysis

```sql
SELECT 
    source_field,
    COUNT(DISTINCT top_ranked_tracking_code) AS unique_codes,
    SUM(sessions) AS total_sessions,
    SUM(GCR) AS total_gcr
FROM dev.ba_corporate.wam_site_performance
WHERE top_ranked_tracking_code <> 'Not attributed'
    AND website_date BETWEEN '2026-01-01' AND '2026-01-31'
GROUP BY 1
ORDER BY total_gcr DESC;
```

### Example 4: Channel Performance by Plan Type

```sql
SELECT 
    channel_grouping_name,
    plan_type,
    free_or_paid,
    SUM(sessions) AS sessions,
    SUM(wam_sessions) AS conversions,
    SUM(GCR) AS revenue,
    ROUND(100.0 * SUM(wam_sessions) / NULLIF(SUM(sessions), 0), 2) AS conversion_rate
FROM dev.ba_corporate.wam_site_performance
WHERE website_date BETWEEN '2026-01-01' AND '2026-01-31'
    AND plan_type <> 'N/A'
GROUP BY 1, 2, 3
ORDER BY revenue DESC;
```

### Example 5: Daily Trend Analysis

```sql
SELECT 
    website_date,
    SUM(sessions) AS sessions,
    SUM(gcr_sessions) AS gcr_sessions,
    SUM(wam_sessions) AS wam_sessions,
    SUM(GCR) AS revenue,
    ROUND(SUM(GCR) / NULLIF(SUM(wam_sessions), 0), 2) AS avg_gcr_per_session
FROM dev.ba_corporate.wam_site_performance
WHERE website_date BETWEEN '2026-01-01' AND '2026-01-31'
GROUP BY 1
ORDER BY 1;
```

## Performance Considerations

1. **Date Partition**: Always filter by `website_date` to leverage the SORTKEY
2. **Tracking Code Filter**: Use `top_ranked_tracking_code <> 'Not attributed'` to focus on attributed traffic
3. **Aggregation Level**: Query already aggregates by key dimensions; avoid re-aggregating unnecessarily
4. **Index Usage**: The LEFT JOIN with `wam_itc_site` is efficient due to small reference table size

## Maintenance Notes

### Adding New Tracking Codes

To add a new tracking code to the ranking:
1. Determine appropriate rank based on GCR or quantity
2. Add new WHEN clause in `top_ranked_extract` CTE in rank order
3. Update `wam_itc_site` reference table with grouping
4. Re-run query to backfill historical data if needed

### Updating Date Ranges

Change dates in **two locations**:
1. `base_traffic_data` CTE WHERE clause
2. `base_product_data_raw` CTE WHERE clause

### Monitoring Data Quality

Key validation queries:
```sql
-- Check for attribution rate
SELECT 
    SUM(CASE WHEN top_ranked_tracking_code = 'Not attributed' THEN sessions ELSE 0 END) AS unattributed,
    SUM(sessions) AS total,
    ROUND(100.0 * SUM(CASE WHEN top_ranked_tracking_code = 'Not attributed' THEN sessions ELSE 0 END) / SUM(sessions), 2) AS pct_unattributed
FROM dev.ba_corporate.wam_site_performance;

-- Check for duplicate counting (should return 0 or small numbers)
SELECT 
    website_date,
    session_id,
    COUNT(*) AS dup_count
FROM dev.ba_corporate.wam_site_performance
GROUP BY 1, 2
HAVING COUNT(*) > 1;
```

## Change Log

| Date | Version | Changes | Author |
|------|---------|---------|--------|
| 2026-01 | 1.0 | Initial query creation for HAT-3917 | Analytics Team |

## Contact & Support

For questions or issues:
- **JIRA**: Create ticket in Analytics project
- **Slack**: #analytics-support
- **Documentation**: [Internal Wiki Link]
```