# Cancel-Rebuy Analysis

## Overview

This query identifies customers who canceled their subscriptions and subsequently repurchased products within a 30-60 day window after cancellation. The analysis tracks rebuy behavior at multiple product hierarchy levels (product group, category, line, version) to understand customer return patterns and product loyalty. The results are materialized in the `ba_ecommerce.cancel_rebuy` table for downstream reporting and analysis.

**Business Purpose**: Enable product and retention teams to analyze win-back patterns, measure the effectiveness of re-engagement campaigns, and identify products with strong customer loyalty despite cancellations.

## JIRA Context

- **Ticket**: [HAT-3923](https://godaddy-corp.atlassian.net/browse/HAT-3923)
- **Requirements**: Detailed context not available in repository

## Tables Used

| Table | Description |
|-------|-------------|
| `dev.dna_approved.renewal_360` | Source table containing renewal, billing, and entitlement data including cancellation events, purchase history, and product hierarchy information |
| `ba_ecommerce.cancel_rebuy` | Output table (created/replaced by this query) containing cancel-rebuy analysis results |

## Key Metrics

### Flags and Dimensions

| Column | Type | Description |
|--------|------|-------------|
| `cancel_rebuy_flag` | Binary (0/1) | General rebuy indicator - set to 1 if customer made ANY new purchase within 30-60 days after cancellation |
| `cancel_rebuy_product_pnl_group_flag` | Binary (0/1) | Set to 1 if customer repurchased within the SAME product P&L group (e.g., Hosting, Security) |
| `cancel_rebuy_product_pnl_category_flag` | Binary (0/1) | Set to 1 if customer repurchased within the SAME product P&L subline (most granular categorization) |
| `cancel_rebuy_product_pnl_line_flag` | Binary (0/1) | Set to 1 if customer repurchased within the SAME product P&L line |
| `cancel_rebuy_product_pnl_version_flag` | Binary (0/1) | Set to 1 if customer repurchased the EXACT SAME product version |

### Dimensions

- `resource_id`: Unique identifier for the canceled resource/entitlement
- `product_family_name`: Product family grouping
- `prior_bill_shopper_id`: Customer identifier
- `entitlement_cancel_mst_date`: Date when the entitlement was canceled

## Business Logic

### CTE 1: Cancellation Events (`cte`)

Identifies qualifying cancellations with the following criteria:
- **Cancellation Requirement**: `entitlement_cancel_mst_date IS NOT NULL`
- **Valid Billing**: Excludes records with billing exclusions (`bill_exclude_reason_desc IS NULL`)
- **Product Scope**: Excludes Domains products (`lower(prior_bill_product_pnl_group_name) <> 'domains'`)
- **Primary Products Only**: `prior_bill_primary_product_flag = true`
- **Date Range**: Cancellations between '2024-01-01' and 60 days before current date
- **Note**: Migration-related cancellations are commented out but could be filtered

### CTE 2: New Purchase Events (`cte_2`)

Captures new purchase transactions with these filters:
- **New Purchase Only**: `prior_bill_product_pnl_new_renewal_name = 'New Purchase'`
- **First Billing**: `prior_bill_sequence_number = 1`
- **Valid Billing**: Excludes records with billing exclusions
- **Primary Products Only**: `prior_bill_primary_product_flag = true`
- **Product Scope**: Excludes Domains products
- **Date Range**: Purchases between '2024-01-01' and yesterday

### Main Query: Rebuy Detection

**Join Logic**:
- Matches on `prior_bill_shopper_id` (same customer) AND `product_family_name` (same product family)
- **Critical Time Window**: New purchase must occur between 30-60 days AFTER cancellation date
  ```sql
  cte_2.prior_bill_modified_mst_date BETWEEN 
    DATEADD(day, 30, cte.entitlement_cancel_mst_date) AND 
    DATEADD(day, 60, cte.entitlement_cancel_mst_date)
  ```

**Hierarchical Flag Logic**:
Each flag uses `MAX(CASE WHEN...)` to set binary indicators:
1. **General Rebuy**: Any new purchase by same customer in same product family within window
2. **Group-Level Rebuy**: Match on `prior_bill_product_pnl_group_name`
3. **Category-Level Rebuy**: Match on `prior_bill_product_pnl_subline_name`
4. **Line-Level Rebuy**: Match on `prior_bill_product_pnl_line_name`
5. **Version-Level Rebuy**: Match on `prior_bill_product_pnl_version_name` (exact product match)

## Data Flow

```
renewal_360 (Cancellations)           renewal_360 (New Purchases)
         ↓                                      ↓
    CTE (cte)                              CTE (cte_2)
  - Cancel date exists                   - New purchase flag
  - Exclude domains                      - Sequence = 1
  - Primary products                     - Exclude domains
  - 2024-01-01 to CD-60                 - 2024-01-01 to CD-1
         ↓                                      ↓
         └──────────── LEFT JOIN ──────────────┘
                   (shopper_id + product_family
                    + 30-60 day window)
                           ↓
                   GROUP BY resource
                           ↓
              MAX(CASE...) for each flag
                           ↓
            ba_ecommerce.cancel_rebuy
```

## Filters & Conditions

### Date Ranges

| Component | Date Filter | Rationale |
|-----------|-------------|-----------|
| Cancellations | `2024-01-01` to `CURRENT_DATE - 60` | 60-day lag ensures sufficient time for rebuy window to complete |
| New Purchases | `2024-01-01` to `CURRENT_DATE - 1` | Captures all purchases through yesterday |
| Rebuy Window | Cancel Date + 30 to Cancel Date + 60 | 30-day window starting 30 days after cancellation |

### Key Exclusions

- **Domains products**: Excluded from both cancellations and purchases
- **Billing exclusions**: Records with `bill_exclude_reason_desc` are filtered out
- **Non-primary products**: Only `prior_bill_primary_product_flag = true` included
- **Renewals**: Only new purchases considered (`sequence_number = 1`)

### Commented Logic

Migration-related cancellations are currently commented out but could be excluded:
```sql
-- LOWER(subscription_cancel_by_name) NOT LIKE '%migration%'
-- LOWER(subscription_cancel_by_name) NOT LIKE '%migr%'
-- LOWER(subscription_cancel_by_name) NOT LIKE '%transferaway%'
```

## Output Schema

| Column Name | Data Type | Description |
|-------------|-----------|-------------|
| `resource_id` | VARCHAR | Unique identifier for the canceled resource |
| `product_family_name` | VARCHAR | Product family grouping (e.g., Hosting, Security) |
| `prior_bill_shopper_id` | VARCHAR | Customer/shopper unique identifier |
| `entitlement_cancel_mst_date` | DATE | Date when the entitlement was canceled (MST timezone) |
| `cancel_rebuy_flag` | INTEGER | 1 = Customer repurchased any product in family within window; 0 = No rebuy |
| `cancel_rebuy_product_pnl_group_flag` | INTEGER | 1 = Repurchased same P&L group; 0 = Different group or no rebuy |
| `cancel_rebuy_product_pnl_category_flag` | INTEGER | 1 = Repurchased same P&L subline; 0 = Different subline or no rebuy |
| `cancel_rebuy_product_pnl_line_flag` | INTEGER | 1 = Repurchased same P&L line; 0 = Different line or no rebuy |
| `cancel_rebuy_product_pnl_version_flag` | INTEGER | 1 = Repurchased exact same version; 0 = Different version or no rebuy |

**Granularity**: One row per canceled resource

## Dependencies

### Upstream Dependencies
- **Table**: `dev.dna_approved.renewal_360`
  - Must contain cancellation and purchase history
  - Required columns: `resource_id`, `prior_bill_shopper_id`, `entitlement_cancel_mst_date`, `prior_bill_modified_mst_date`, product hierarchy columns, billing flags

### Downstream Dependencies
- Any dashboards, reports, or analyses consuming `ba_ecommerce.cancel_rebuy`
- Potential use cases: retention reporting, win-back campaign analysis, product loyalty metrics

### Schema Requirements
- Target schema `ba_ecommerce` must exist
- User must have CREATE TABLE privileges on `ba_ecommerce` schema

## Usage Examples

### Running the Query

```sql
-- Execute the full query (creates/replaces table)
-- Run in your SQL client connected to the appropriate data warehouse
-- Execution time: ~5-15 minutes depending on data volume
```

### Analyzing Results

**Example 1: Overall rebuy rate**
```sql
SELECT 
  COUNT(*) as total_cancellations,
  SUM(cancel_rebuy_flag) as total_rebuys,
  ROUND(100.0 * SUM(cancel_rebuy_flag) / COUNT(*), 2) as rebuy_rate_pct
FROM ba_ecommerce.cancel_rebuy;
```

**Example 2: Rebuy rate by product family**
```sql
SELECT 
  product_family_name,
  COUNT(*) as cancellations,
  SUM(cancel_rebuy_flag) as rebuys,
  ROUND(100.0 * SUM(cancel_rebuy_flag) / COUNT(*), 2) as rebuy_rate_pct,
  ROUND(100.0 * SUM(cancel_rebuy_product_pnl_version_flag) / NULLIF(SUM(cancel_rebuy_flag), 0), 2) as exact_version_loyalty_pct
FROM ba_ecommerce.cancel_rebuy
GROUP BY product_family_name
ORDER BY rebuys DESC;
```

**Example 3: Customer journey - same vs. different products**
```sql
SELECT 
  product_family_name,
  SUM(cancel_rebuy_flag) as total_rebuys,
  SUM(cancel_rebuy_product_pnl_version_flag) as same_version,
  SUM(cancel_rebuy_product_pnl_line_flag) as same_line,
  SUM(cancel_rebuy_product_pnl_group_flag) as same_group,
  SUM(CASE WHEN cancel_rebuy_flag = 1 AND cancel_rebuy_product_pnl_group_flag = 0 THEN 1 ELSE 0 END) as different_group
FROM ba_ecommerce.cancel_rebuy
GROUP BY product_family_name;
```

**Example 4: Monthly trend analysis**
```sql
SELECT 
  DATE_TRUNC('month', entitlement_cancel_mst_date) as cancel_month,
  COUNT(*) as cancellations,
  SUM(cancel_rebuy_flag) as rebuys,
  ROUND(100.0 * SUM(cancel_rebuy_flag) / COUNT(*), 2) as rebuy_rate_pct
FROM ba_ecommerce.cancel_rebuy
GROUP BY DATE_TRUNC('month', entitlement_cancel_mst_date)
ORDER BY cancel_month;
```

### Interpreting Results

- **Rebuy Rate**: Higher rates indicate strong product value or effective win-back strategies
- **Version Loyalty**: High `cancel_rebuy_product_pnl_version_flag` suggests product satisfaction despite cancellation
- **Cross-sell Patterns**: When `cancel_rebuy_flag = 1` but version/line flags = 0, customer switched products
- **Time Lag**: All rebuys occurred 30-60 days post-cancellation, indicating deliberate return behavior rather than immediate reconsideration