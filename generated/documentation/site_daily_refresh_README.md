# Site Daily Refresh Query - README

## Overview

The Site Daily Refresh Query is a comprehensive data pipeline that processes website traffic and product purchase data for Websites and Marketing (WAM) products. This query creates a unified view of user sessions, tracking code attribution, and product conversions across a rolling 16-day window.

**Business Purpose**: 
- Track WAM product performance across multiple dimensions (channel, device, geography, customer type)
- Attribute website sessions to specific tracking codes using a sophisticated ranking system
- Monitor key conversion metrics (sessions, page advances, GCR, product units)
- Support daily reporting and analytics for WAM business decisions

**Refresh Frequency**: Daily (supplements the WAD 14-day refresh process)

## JIRA Context

**Ticket**: [HAT-3917](https://jira.godaddy.com/browse/HAT-3917) - Site Daily Refresh Query

**Note**: Full JIRA context not available in provided materials. This query appears to be part of a daily data refresh initiative to ensure timely availability of WAM performance metrics.

## Tables Used

### Source Tables

| Table | Purpose | Key Columns |
|-------|---------|-------------|
| `dev.website_prod.analytic_traffic_detail` | Website session-level traffic data | session_id, website_activity_mst_date, channel_grouping_name, device_category_name, item_tracking_code lists |
| `dev.dna_approved.bill_line_traffic_ext` | Product purchase/billing data with traffic attribution | session_id, item_tracking_code, gcr_usd_amt, product_pnl_subline_name, product_term_num |
| `dev.ba_corporate.wam_itc_site` | ITC grouping/categorization lookup | itemtrackingcode, itcgrouping |

### Target Tables

| Table | Purpose |
|-------|---------|
| `dev.ba_corporate.wam_site_daily` | Staging table for current day's aggregated results |
| `dev.ba_corporate.wam_site_performance1` | Final persistent table storing historical daily data |

## Key Metrics

### Traffic Metrics

| Metric | Description | Calculation |
|--------|-------------|-------------|
| **sessions** | Distinct user sessions | COUNT(DISTINCT session_id) |
| **session_cnt** | Total session count (with deduplication flag) | SUM(session_cnt * traffic_row_flag) |
| **pa_sessions** | Page advance sessions (sessions with meaningful engagement) | SUM(page_advance_session_cnt * traffic_row_flag) |
| **gcr_sessions** | Sessions resulting in GCR (Gross Customer Receipts) | SUM(gcr_session_cnt * traffic_row_flag) |

### Product Metrics

| Metric | Description | Calculation |
|--------|-------------|-------------|
| **wam_sessions** | Sessions that purchased WAM products (deduplicated) | SUM(wam_sessions) - only 1 per session |
| **total_wam_units** | Total WAM product units purchased | SUM(product_unit_qty) from billing data |
| **GCR** | Gross Customer Receipts (revenue) | SUM(gcr_usd_amt) from billing data |

### Dimensions

- **website_date**: Date of website activity
- **channel_grouping_name**: Marketing channel (Organic, Paid, Direct, etc.)
- **device_category_name**: Device type (Desktop, Mobile, Tablet)
- **web_region_2_name**: Geographic region
- **existing_customer_flag**: New vs. existing customer status
- **first_hit_content_group_2_name**: Landing page content group
- **top_ranked_tracking_code**: Attributed ITC based on ranking hierarchy
- **source_field**: Which ITC field the attribution came from
- **product_term**: Product subscription term (e.g., "12 Month")
- **plan_type**: WAM plan tier (Basic, Standard, Premium, Commerce, etc.)
- **free_or_paid**: Product pricing classification

## Business Logic

### 1. Date Processing Window

```sql
CURRENT_DATE - 16 AS min_processing_date
CURRENT_DATE AS max_processing_date
```

- **16-day rolling window** ensures comprehensive coverage and handles late-arriving data
- Data for this range is **deleted and replaced** each run to maintain accuracy

### 2. Product Plan Type Classification

The query maps granular product sublines to standardized plan types:

| Source Product | Mapped Plan Type |
|----------------|------------------|
| gocentral seo, gocentral marketing | Marketing |
| commerce plus | Commerce Plus |
| deluxe | Standard |
| economy | Basic |
| paypal commerce, super premium | Commerce |
| premium, tier 1 premium | Premium |
| starter | Starter |

### 3. Free vs. Paid Classification

```sql
CASE 
  WHEN COALESCE(gcr_usd_amt, 0) = 0 THEN 'Free'
  WHEN gcr_usd_amt > 0 and product_free_trial_conversion_flag = 'True' THEN 'Free to Paid'
  WHEN gcr_usd_amt > 0 and product_free_trial_conversion_flag = 'False' THEN 'Paid'
END
```

Distinguishes between free trials, free-to-paid conversions, and direct paid purchases.

### 4. Deduplication Logic

**Problem**: When a session purchases multiple products, joins create multiple rows, causing double-counting of traffic metrics.

**Solution - Traffic Row Flag**:
```sql
CASE WHEN ROW_NUMBER() OVER (PARTITION BY session_id, website_activity_mst_date ORDER BY item_tracking_code NULLS LAST) = 1 
     THEN 1 ELSE 0 END as traffic_row_flag
```
- Ensures session_cnt, pa_sessions, and gcr_sessions are counted only once per session
- Applied in aggregation: `SUM(session_cnt * traffic_row_flag)`

**Solution - WAM Sessions Flag**:
```sql
CASE WHEN ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY item_tracking_code) = 1 
     THEN 1 ELSE 0 END as wam_sessions
```
- Ensures each session is counted as exactly one WAM session regardless of product count
- Prevents inflated session counts in product-level analysis

### 5. Tracking Code Attribution Hierarchy

The query implements a **253-rank waterfall attribution model**:

1. **Order ITC** (highest priority): Direct tracking code from completed orders
2. **Ranked ITCs by GCR** (ranks 1-95): ITCs sorted by historical revenue contribution
3. **Ranked ITCs by Quantity** (ranks 96-253): ITCs with zero GCR, sorted by volume
4. **Not attributed** (fallback): Sessions with no matching ITC

**Matching Logic**:
```sql
all_itc_combined LIKE '%|~|upp_f2p_upgrade|~|%'
```
- Concatenates all ITC fields with `|~|` separator for pattern matching
- Checks patterns in rank order using CASE WHEN logic
- Stops at first match (waterfall principle)

**Source Field Determination**:
After finding the ranked ITC, the query determines which field it came from:
- `order_itc`: From completed order
- `payment_attempt`: From payment attempt list
- `begin_checkout`: From checkout initiation list
- `add_to_cart`: From cart addition list
- `click`: From click event list
- `impression`: From impression list

### 6. ITC Grouping Lookup

Final join to `wam_itc_site` adds business-friendly ITC groupings for reporting:
```sql
LEFT JOIN (select itemtrackingcode, itcgrouping from dev.ba_corporate.wam_itc_site) b 
ON a.top_ranked_tracking_code = b.item_tracking_code
```

## Data Flow

```
1. DATE RANGE DEFINITION (date_process_range)
   â””â”€> Establishes 16-day processing window

2. TRAFFIC DATA EXTRACTION (base_traffic_data)
   â””â”€> Extract all GD sales sessions with ITC fields
   â””â”€> Concatenate ITC lists for pattern matching

3. PRODUCT DATA EXTRACTION (base_product_data_raw/base_product_data)
   â””â”€> Extract WAM product purchases
   â””â”€> Classify plan types and free/paid status
   â””â”€> Apply WAM session deduplication flag

4. JOIN TRAFFIC + PRODUCTS (base_data)
   â””â”€> LEFT JOIN traffic to products on session_id + date
   â””â”€> Apply traffic row flag for metric deduplication

5. ITC ATTRIBUTION (top_ranked_extract)
   â””â”€> Apply 253-rank waterfall logic to assign tracking code
   â””â”€> Preserve individual ITC lists for source determination

6. SOURCE IDENTIFICATION (final_attribution)
   â””â”€> Determine which ITC field contained the attributed code
   â””â”€> Coalesce to 'Not attributed' for unmatched sessions

7. AGGREGATION (final_output)
   â””â”€> Group by dimensions
   â””â”€> Sum traffic metrics (with flags) and product metrics
   â””â”€> Count distinct sessions

8. ITC GROUPING ENRICHMENT
   â””â”€> LEFT JOIN to add business-friendly ITC categories

9. PERSISTENCE
   â””â”€> CREATE dev.ba_corporate.wam_site_daily (staging)
   â””â”€> INSERT INTO dev.ba_corporate.wam_site_performance1 (final)
```

## Filters & Conditions

### Date Filters

| Applied To | Filter |
|------------|--------|
| Traffic data | `website_activity_mst_date BETWEEN (CURRENT_DATE - 16) AND CURRENT_DATE` |
| Product data | `website_activity_mst_date BETWEEN (CURRENT_DATE - 16) AND CURRENT_DATE` |
| Performance table cleanup | `DELETE WHERE website_date BETWEEN (CURRENT_DATE - 16) AND CURRENT_DATE` |

### Traffic Filters

```sql
WHERE gd_sales_flag = TRUE
  AND session_id IS NOT NULL
  AND website_activity_exclusion_reason_desc IS NULL
```

- **gd_sales_flag = TRUE**: Focus on GoDaddy sales-eligible traffic
- **session_id IS NOT NULL**: Exclude malformed sessions
- **No exclusion reason**: Remove bot traffic, internal traffic, etc.

### Product Filters

```sql
WHERE product_pnl_line_name IN ('Websites and Marketing', 'Website Builder')
  AND point_of_purchase_name = 'Web'
  AND exclude_reason_month_end_desc IS NULL
  AND refund_flag = FALSE
  AND chargeback_flag = FALSE
  AND product_pnl_new_renewal_name = 'New Purchase'
```

- **WAM products only**: Websites and Marketing / Website Builder
- **Web purchases**: Excludes phone, reseller channels
- **Valid transactions**: No refunds, chargebacks, or exclusions
- **New purchases only**: Excludes renewals

## Output Schema

### Dimension Columns

| Column | Type | Description | Default for Nulls |
|--------|------|-------------|-------------------|
| `website_date` | DATE | Date of website activity | - |
| `channel_grouping_name` | VARCHAR | Marketing channel | 'Unknown' |
| `device_category_name` | VARCHAR | Device type | 'Unknown' |
| `web_region_2_name` | VARCHAR | Geographic region | 'Unknown' |
| `existing_customer_flag` | VARCHAR | Customer status (New/Existing) | 'Unknown' |
| `first_hit_content_group_2_name` | VARCHAR | Landing page content group | 'Unknown' |
| `top_ranked_tracking_code` | VARCHAR | Attributed ITC from 253-rank hierarchy | 'Unknown' |
| `source_field` | VARCHAR | ITC source (order_itc, payment_attempt, etc.) | 'Unknown' |
| `web_focal_country_name` | VARCHAR | Focal country for session | 'Unknown' |
| `page_path_list` | VARCHAR | Pages visited in session | 'Unknown' |
| `web_business_unit_name` | VARCHAR | Business unit | 'Unknown' |
| `order_item_tracking_code` | VARCHAR | ITC from completed order | 'N/A' |
| `product_term` | VARCHAR | Subscription term (e.g., "12 Month") | 'N/A' |
| `plan_type` | VARCHAR | WAM plan tier (Basic, Standard, Premium, etc.) | 'N/A' |
| `free_or_paid` | VARCHAR | Free, Paid, or Free to Paid | 'N/A' |
| `itc_grouping` | VARCHAR | Business-friendly ITC category | NULL |

### Metric Columns

| Column | Type | Description |
|--------|------|-------------|
| `sessions` | INTEGER | Distinct session count |
| `session_cnt` | INTEGER | Total session count (deduplicated) |
| `pa_sessions` | INTEGER | Page advance sessions |
| `gcr_sessions` | INTEGER | Sessions with GCR |
| `wam_sessions` | INTEGER | Sessions with WAM purchases (deduplicated) |
| `total_wam_units` | INTEGER | Total WAM product units sold |
| `GCR` | DECIMAL | Total Gross Customer Receipts (USD) |

## Dependencies

### Upstream Dependencies

1. **dev.website_prod.analytic_traffic_detail**
   - Must be refreshed daily with previous day's traffic
   - Requires valid session_id and ITC fields

2. **dev.dna_approved.bill_line_traffic_ext**
   - Must contain billing data with traffic attribution
   - Requires product classification fields

3. **dev.ba_corporate.wam_itc_site**
   - ITC grouping lookup table
   - Should be updated as new ITCs are introduced

### Downstream Dependencies

- **Reporting dashboards** consuming `wam_site_performance1`
- **BI tools** querying daily WAM performance metrics
- **Business stakeholders** relying on WAM session and revenue data

### Schema Dependencies

- Table must exist: `dev.ba_corporate.wam_site_performance1`
- Table sortkey: `website_date` (for query performance)

## Usage Examples

### Running the Query

```sql
-- Execute the full script
-- Runtime: ~5-15 minutes depending on data volume

-- The query handles:
-- 1. Drops/recreates date range temp table
-- 2. Deletes existing data for date range
-- 3. Drops/recreates wam_site_daily staging table
-- 4. Processes and aggregates data
-- 5. Inserts into wam_site_performance1
-- 6. Analyzes table for optimizer
```

### Interpreting Results

**Example Row**:
```
website_date: 2026-03-09
channel_grouping_name: Organic Search
device_category_name: Desktop
top_ranked_tracking_code: slp_wsb_ft_nocc_config
source_field: click
plan_type: Standard
free_or_paid: Paid
sessions: 150
session_cnt: 150
pa_sessions: 120
gcr_sessions: 25
wam_sessions: 25
total_wam_units: 30
GCR: 2,500.00
```

**Interpretation**:
- 150 organic desktop sessions clicked the `slp_wsb_ft_nocc_config` tracking code
- 120 of those sessions advanced beyond the landing page (80% engagement)
- 25 sessions resulted in a WAM purchase (16.7% conversion)
- 30 total product units purchased (some multi-product orders)
- $2,500 in revenue generated

### Common Query Patterns

**Daily Performance Summary**:
```sql
SELECT 
    website_date,
    SUM(sessions) AS total_sessions,
    SUM(wam_sessions) AS total_wam_sessions,
    SUM(GCR) AS total_gcr,
    SUM(wam_sessions)::FLOAT / NULLIF(SUM(sessions), 0) AS conversion_rate
FROM dev.ba_corporate.wam_site_performance1
WHERE website_date >= CURRENT_DATE - 7
GROUP BY website_date
ORDER BY website_date DESC;
```

**Top Performing ITCs**:
```sql
SELECT 
    top_ranked_tracking_code,
    itc_grouping,
    SUM(sessions) AS total_sessions,
    SUM(GCR) AS total_gcr,
    SUM(GCR) / NULLIF(SUM(wam_sessions), 0) AS revenue_per_session
FROM dev.ba_corporate.wam_site_performance1
WHERE website_date >= CURRENT_DATE - 30
    AND top_ranked_tracking_code != 'Not attributed'
GROUP BY 1, 2
ORDER BY total_gcr DESC
LIMIT 20;
```

**Channel Performance by Plan Type**:
```sql
SELECT 
    channel_grouping_name,
    plan_type,
    SUM(wam_sessions) AS sessions,
    SUM(total_wam_units) AS units,
    SUM(GCR) AS revenue
FROM dev.ba_corporate.wam_site_performance1
WHERE website_date >= CURRENT_DATE - 30
    AND plan_type != 'N/A'
GROUP BY 1, 2
ORDER BY revenue DESC;
```

## Maintenance & Troubleshooting

### Expected Runtime
- **Normal**: 5-15 minutes
- **Large data volumes**: Up to 30 minutes

### Common Issues

1. **Long Runtime**: Check for missing indexes on `website_activity_mst_date` or `session_id`
2. **Duplicate Data**: Verify DELETE statement executed before INSERT
3. **Missing ITCs**: Update the 253-rank CASE statement as new ITCs are introduced
4. **Metric Discrepancies**: Verify `traffic_row_flag` and `wam_sessions` logic functioning correctly

### Monitoring

```sql
-- Check row counts and date coverage
SELECT 
    MIN(website_date) AS earliest_date,
    MAX(website_date) AS latest_date,
    COUNT(*) AS total_rows,
    SUM(sessions) AS total_sessions,
    SUM(GCR) AS total_gcr
FROM dev.ba_corporate.wam_site_performance1;

-- Verify no duplicates
SELECT website_date, COUNT(*) 
FROM dev.ba_corporate.wam_site_performance1
GROUP BY 1
HAVING COUNT(*) > 50000  -- Adjust threshold based on typical volumes
ORDER BY 1 DESC;
```

---

**Last Updated**: 2026-03-09  
**Query Version**: 1.0  
**Owner**: Business Analytics - Corporate Team