# cancel_rebuy.sql - Documentation

## Overview

This query analyzes customer rebuy behavior by identifying shoppers who cancelled their subscriptions and subsequently made new purchases within a 30-60 day window after cancellation. The analysis tracks rebuy patterns across different product hierarchy levels (product group, category, line, and version) to measure customer reactivation and product substitution behavior. The results are stored in the `ba_ecommerce.cancel_rebuy` table.

## JIRA Context

**Ticket**: [HAT-3923](https://godaddy-corp.atlassian.net/browse/HAT-3923)  
**Requirement**: Track and analyze customer rebuy patterns after subscription cancellations to understand reactivation behavior and product switching patterns.

## Tables Used

| Table | Description |
|-------|-------------|
| `dev.dna_approved.renewal_360` | Comprehensive renewal and subscription lifecycle table containing billing history, entitlement details, product hierarchies, and cancellation information for customer subscriptions |

## Key Metrics

### Rebuy Flags

| Metric | Description | Business Value |
|--------|-------------|----------------|
| `cancel_rebuy_flag` | Binary flag (1/0) indicating if shopper made ANY new purchase 30-60 days after cancellation | Measures overall customer reactivation rate |
| `cancel_rebuy_product_pnl_group_flag` | Flag indicating rebuy of same product PNL group | Tracks product group loyalty and reactivation within same category |
| `cancel_rebuy_product_pnl_category_flag` | Flag indicating rebuy of same product PNL subline | Measures category-level product retention |
| `cancel_rebuy_product_pnl_line_flag` | Flag indicating rebuy of same product PNL line | Tracks line-level product loyalty |
| `cancel_rebuy_product_pnl_version_flag` | Flag indicating rebuy of same product PNL version | Identifies exact product version repurchases |

### Dimensions

- **resource_id**: Unique identifier for the cancelled resource/subscription
- **product_family_name**: Product family classification
- **prior_bill_shopper_id**: Customer identifier
- **entitlement_cancel_mst_date**: Date when the entitlement was cancelled

## Business Logic

### CTE 1: Cancelled Subscriptions (`cte`)

Identifies baseline population of cancelled subscriptions with the following criteria:

- **Cancellation required**: `entitlement_cancel_mst_date IS NOT NULL`
- **Valid billing records**: Excludes records with `bill_exclude_reason_desc`
- **Non-domain products**: Excludes domain products (`prior_bill_product_pnl_group_name <> 'domains'`)
- **Primary products only**: `prior_bill_primary_product_flag = TRUE`
- **Time window**: Cancellations between 2024-01-01 and 60 days before current date (allows for 60-day lookforward window)
- **Product hierarchy captured**: Stores all PNL dimensions (group, category, line, subline, version) for matching

### CTE 2: New Purchases (`cte_2`)

Identifies new purchase events for potential rebuy matching:

- **New purchases only**: `prior_bill_product_pnl_new_renewal_name = 'New Purchase'`
- **First billing sequence**: `prior_bill_sequence_number = 1` (excludes subsequent billings)
- **Valid billing records**: Excludes records with `bill_exclude_reason_desc`
- **Non-domain products**: Excludes domain products
- **Primary products only**: `prior_bill_primary_product_flag = TRUE`
- **Time window**: Purchases between 2024-01-01 and yesterday

### Main Query: Rebuy Matching Logic

**Join Conditions**:
1. **Shopper match**: `cte.prior_bill_shopper_id = cte_2.prior_bill_shopper_id`
2. **Product family match**: `cte.product_family_name = cte_2.product_family_name`
3. **Temporal window**: New purchase occurs 30-60 days after cancellation
   - Lower bound: `DATEADD(day, 30, entitlement_cancel_mst_date)`
   - Upper bound: `DATEADD(day, 60, entitlement_cancel_mst_date)`

**Flag Calculations**:
- Uses `MAX()` aggregation with `CASE` statements to create binary flags
- Each flag checks if new purchase exists AND matches at specific product hierarchy level
- Hierarchy levels (from broad to specific): Group → Category → Line → Version
- Multiple purchases within window result in flag=1 if ANY match occurs

## Data Flow

```
┌─────────────────────────────────────┐
│  renewal_360 (Cancellations)       │
│  - Cancelled entitlements           │
│  - Non-domain, primary products     │
│  - 2024-01-01 to current_date-60    │
└──────────────┬──────────────────────┘
               │
               │ CTE 1 (cte)
               ▼
┌──────────────────────────────────────┐
│  Cancelled Subscriptions Dataset     │
│  - resource_id                       │
│  - shopper_id                        │
│  - product hierarchy                 │
│  - cancellation_date                 │
└──────────────┬───────────────────────┘
               │
               │ LEFT JOIN
               │ (shopper_id, product_family)
               │ + 30-60 day window
               ▼
┌──────────────────────────────────────┐     ┌─────────────────────────────┐
│  Rebuy Analysis                      │ ◄───│  renewal_360 (New Orders)   │
│  - Match on shopper + product family │     │  - New purchases only        │
│  - Calculate hierarchy match flags   │     │  - First sequence            │
│  - Aggregate with MAX()              │     │  - 2024-01-01 to yesterday   │
└──────────────┬───────────────────────┘     └─────────────────────────────┘
               │                                        CTE 2 (cte_2)
               ▼
┌──────────────────────────────────────┐
│  ba_ecommerce.cancel_rebuy           │
│  - One row per cancelled resource    │
│  - Rebuy flags at multiple levels    │
└──────────────────────────────────────┘
```

## Filters & Conditions

### CTE 1 - Cancelled Subscriptions

| Filter | Value | Rationale |
|--------|-------|-----------|
| Date Range | 2024-01-01 to current_date-60 | Ensures sufficient lookforward window for rebuy detection |
| Product Group | Exclude 'domains' | Focus on non-domain product rebuys |
| Primary Product Flag | TRUE | Analyze primary products only, exclude add-ons |
| Bill Exclusion | IS NULL | Include only valid billing records |
| Cancellation Date | IS NOT NULL | Must have actual cancellation event |

### CTE 2 - New Purchases

| Filter | Value | Rationale |
|--------|-------|-----------|
| Date Range | 2024-01-01 to current_date-1 | Cover full year plus current activity |
| New/Renewal Type | 'New Purchase' | Identify net-new purchases, not renewals |
| Sequence Number | 1 | First billing only, avoid duplicate counting |
| Product Group | Exclude 'domains' | Match CTE 1 exclusion criteria |
| Primary Product Flag | TRUE | Match CTE 1 scope |
| Bill Exclusion | IS NULL | Include only valid billing records |

### Join Temporal Window

**30-60 Day Window After Cancellation**: This window represents the "consideration period" where customers who cancelled might return. The 30-day lower bound allows cooling-off period, while 60-day upper bound captures medium-term reactivation.

## Output Schema

| Column | Data Type | Description | Example Use Case |
|--------|-----------|-------------|------------------|
| `resource_id` | STRING | Unique identifier of the cancelled resource | Track specific subscription cancellation-rebuy pairs |
| `product_family_name` | STRING | Product family classification | Segment analysis by product family |
| `prior_bill_shopper_id` | STRING | Customer/shopper unique identifier | Customer-level rebuy rate analysis |
| `entitlement_cancel_mst_date` | DATE | Date when entitlement was cancelled | Time-series analysis of rebuy behavior |
| `cancel_rebuy_flag` | INTEGER (0/1) | 1 if shopper made ANY new purchase in 30-60 day window | Overall rebuy rate: `SUM(flag)/COUNT(*)` |
| `cancel_rebuy_product_pnl_group_flag` | INTEGER (0/1) | 1 if rebuy was in same product PNL group | Group-level loyalty rate |
| `cancel_rebuy_product_pnl_category_flag` | INTEGER (0/1) | 1 if rebuy was in same product PNL subline/category | Category retention analysis |
| `cancel_rebuy_product_pnl_line_flag` | INTEGER (0/1) | 1 if rebuy was in same product PNL line | Line-level product retention |
| `cancel_rebuy_product_pnl_version_flag` | INTEGER (0/1) | 1 if rebuy was same product PNL version | Exact product repurchase rate |

**Key**: One row per cancelled resource. Flags use `MAX()` aggregation, so if multiple purchases occur in window, flag=1 if ANY match the criteria.

## Dependencies

### Upstream Dependencies
- **Table**: `dev.dna_approved.renewal_360`
  - Must contain current billing and entitlement data
  - Requires complete cancellation date population
  - PNL hierarchy fields must be populated

### Downstream Dependencies
- **Output Table**: `ba_ecommerce.cancel_rebuy`
  - Dropped and recreated on each run (not incremental)
  - May be consumed by reporting dashboards, BI tools, or downstream models
  - Schema must remain stable for dependent consumers

### Refresh Requirements
- **Data Freshness**: Query looks back to 2024-01-01 and forward 60 days from cancellation
- **Recommended Cadence**: Daily or weekly refresh depending on business needs
- **Runtime Considerations**: Full table scan and recreation on each execution

## Usage Examples

### Example 1: Calculate Overall Rebuy Rate

```sql
SELECT 
  COUNT(DISTINCT resource_id) AS total_cancellations,
  SUM(cancel_rebuy_flag) AS total_rebuys,
  SUM(cancel_rebuy_flag) * 100.0 / COUNT(DISTINCT resource_id) AS rebuy_rate_pct
FROM ba_ecommerce.cancel_rebuy;
```

**Expected Output**: Overall percentage of cancelled subscriptions that resulted in rebuy within 30-60 days

### Example 2: Rebuy Rate by Product Family

```sql
SELECT 
  product_family_name,
  COUNT(DISTINCT resource_id) AS cancellations,
  SUM(cancel_rebuy_flag) AS rebuys,
  SUM(cancel_rebuy_flag) * 100.0 / COUNT(DISTINCT resource_id) AS rebuy_rate_pct,
  SUM(cancel_rebuy_product_pnl_group_flag) AS same_group_rebuys,
  SUM(cancel_rebuy_product_pnl_version_flag) AS exact_product_rebuys
FROM ba_ecommerce.cancel_rebuy
GROUP BY product_family_name
ORDER BY cancellations DESC;
```

**Expected Output**: Rebuy metrics segmented by product family with loyalty indicators

### Example 3: Time Series Analysis

```sql
SELECT 
  DATE_TRUNC('month', entitlement_cancel_mst_date) AS cancel_month,
  COUNT(DISTINCT resource_id) AS cancellations,
  SUM(cancel_rebuy_flag) AS rebuys,
  SUM(cancel_rebuy_flag) * 100.0 / COUNT(DISTINCT resource_id) AS rebuy_rate_pct
FROM ba_ecommerce.cancel_rebuy
GROUP BY cancel_month
ORDER BY cancel_month;
```

**Expected Output**: Monthly trend of cancellation volumes and rebuy rates

### Example 4: Product Switching Analysis

```sql
SELECT 
  CASE 
    WHEN cancel_rebuy_product_pnl_version_flag = 1 THEN 'Exact Same Product'
    WHEN cancel_rebuy_product_pnl_line_flag = 1 THEN 'Same Line, Different Version'
    WHEN cancel_rebuy_product_pnl_group_flag = 1 THEN 'Same Group, Different Line'
    WHEN cancel_rebuy_flag = 1 THEN 'Different Group'
    ELSE 'No Rebuy'
  END AS rebuy_type,
  COUNT(*) AS count,
  COUNT(*) * 100.0 / SUM(COUNT(*)) OVER () AS pct_of_total
FROM ba_ecommerce.cancel_rebuy
GROUP BY rebuy_type
ORDER BY count DESC;
```

**Expected Output**: Distribution of rebuy behavior showing product loyalty vs. switching patterns

### Running the Query

```sql
-- Execute the full query to refresh the table
-- (Copy and run the entire cancel_rebuy.sql script)

-- Verify results
SELECT COUNT(*) AS total_records 
FROM ba_ecommerce.cancel_rebuy;

-- Check data quality
SELECT 
  MIN(entitlement_cancel_mst_date) AS earliest_cancel,
  MAX(entitlement_cancel_mst_date) AS latest_cancel,
  COUNT(DISTINCT prior_bill_shopper_id) AS unique_shoppers
FROM ba_ecommerce.cancel_rebuy;
```