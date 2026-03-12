# Renewals Overview - SQL Query Documentation

## 1. Overview

This SQL query generates comprehensive renewal analytics for e-commerce products, providing multiple views of renewal performance across different time granularities (daily, weekly, monthly) and analytical bases (cash, cohort, renewal). The query implements a **Fixed Mix Adjustment** methodology to compare year-over-year renewal performance while controlling for product mix changes.

### Business Purpose
- Track renewal rates and revenue across product lines, customer segments, and geographic regions
- Enable apples-to-apples comparisons by fixing product mix to a baseline period (Dec 2024 - Feb 2025)
- Monitor key business drivers: customer type, pillar performance, auto-renewal adoption, and multi-product customer behavior
- Support financial forecasting and investor relations reporting with standardized metrics
- Identify trends in Google-migrated subscriptions and 2+ product customer segments

### Key Outputs
- **renewals_360_agg**: Base aggregation table with all renewal metrics
- **renewals_rate_mix_agg**: Summarized renewal rates with descriptive fields
- **renewal_fixed_rate_mix_adjusted_QS**: Fixed mix adjusted data for QuickSight reporting

---

## 2. JIRA Context

**Note**: No JIRA ticket information was provided in the query. Please update with:
- JIRA Ticket: [TICKET-NUMBER]
- Requirements Document: [Link]
- Business Owner: [Name/Team]
- Last Updated: Query includes fixed period Dec 2024 - Feb 2025

---

## 3. Tables Used

### Source Tables

| Table | Purpose | Key Fields |
|-------|---------|------------|
| `dev.ba_ecommerce.renewal_job_alerts` | Job execution monitoring | job_name, data_Expected_date, status |
| `ckp_analytic_share.finance360.dim_product_vw` | Finance product hierarchy | pf_id, fin_pnl_group_name, fin_pnl_category_name, fin_investor_relation_* |
| `dev.dna_approved.renewal_360` | Core renewal fact table | prior_bill_*, renewal_bill_*, expiry_qty, renewal_qty |
| `dev.dna_approved.two_plus_active_customer_history` | Historical multi-product customer flags | shopper_id, two_plus_customer_flag, product_category_count |
| `dev.dna_approved.two_plus_active_customer` | Current multi-product customer flags | shopper_id, two_plus_customer_flag |
| `dev.ba_dri.goog_migrations_final` | Google migration tracking | shopper_id, resource_id, google_migrated_subscription_flag |
| `dev.dna_approved.dim_geography` | Geographic dimension | country_code, report_region_3_name |
| `dev.bi_prod.dim_relative_date` | Relative date dimension | relative_date, relative_week, relative_month, period names |

### Output Tables

| Table | Purpose | Granularity |
|-------|---------|-------------|
| `dev.ba_ecommerce.renewals_360_agg` | Base renewal aggregation | Daily, by dimension combination |
| `ba_ecommerce.renewals_rate_mix_agg` | Descriptive renewal summary | Daily, with descriptive text fields |
| `ba_ecommerce.renewal_fixed_rate_mix_adjusted_QS` | Fixed mix adjusted data | Daily/Weekly/Monthly |
| `dev.ba_ecommerce.renewal_job_alerts` | Data quality monitoring | Job-level |

---

## 4. Key Metrics

### Expiry Metrics
- **expiry_qty**: Number of products/subscriptions expiring
- **potential_Receipt_price_usd_amt**: Expected revenue if all products renewed

### Renewal Metrics
- **renewal_qty**: Number of products/subscriptions renewed
- **ontime_renewal_qty**: Renewals occurring before or on expiration date
- **renewal_bill_gcr_usd_amt**: Gross Customer Revenue (GCR) in USD from renewals
- **renewal_bill_product_month_qty**: Total product months renewed (quantity × term length)
- **renewal_bill_receipt_price_usd_amt**: Receipt price (actual price paid) for renewals
- **renewal_bill_list_price_usd_amt**: List price (before discounts) for renewals
- **renewal_bill_cc_gcr_usd_amt**: Constant currency GCR (eliminates FX impact)

### Calculated Rates
- **Renewal Rate**: `renewal_qty / expiry_qty`
- **On-Time Renewal Rate**: `ontime_renewal_qty / expiry_qty`
- **Revenue Retention**: `renewal_bill_gcr_usd_amt / potential_Receipt_price_usd_amt`

### Fixed Mix Metrics
- **fixed_expiries**: Expiries from baseline period (Dec 2024 - Feb 2025)
- **fixed_renewal_qty**: Renewals from baseline period
- **fixed_gcr_amt**: GCR from baseline period
- **fixed_renewal_month_qty**: Product months from baseline period

---

## 5. Business Logic

### Analysis Types

The query generates three analytical views:

1. **Cohort Basis**: Analyzes renewals based on expiration date (`prior_bill_paid_through_mst_date`)
   - Groups all products expiring on the same date
   - Tracks whether they eventually renew (regardless of timing)

2. **Cash Basis**: Analyzes based on billing due date (`prior_bill_billing_due_mst_date`)
   - Aligns with cash collection timing
   - Matches financial reporting periods

3. **Renewal Basis**: Analyzes based on renewal transaction date (`renewal_bill_modified_mst_date`)
   - Shows when renewals actually occurred
   - Used for revenue recognition timing

### Customer Segmentation

**Pillar Classification**:
```sql
CASE 
    WHEN expected_pnl_international_independent_flag = true THEN 'International Independent'
    WHEN expected_pnl_us_independent_flag = true THEN 'US Independents'
    WHEN expected_pnl_investor_flag = true THEN 'Investors'
    WHEN expected_pnl_partner_flag = true THEN 'Partners'
    WHEN expected_pnl_commerce_flag = true THEN 'Commerce'
    ELSE 'Not Evaluated'
END
```

**Customer Type Logic**:
- Defaults to 'US Independent' if in United States and not otherwise classified
- Defaults to 'International Independent' if outside US and not otherwise classified
- Preserves explicit Partner/Investor classifications

**2+ Customer Segmentation**:
- Historical flag: Based on snapshot date matching the transaction date
- Current flag: Based on most recent customer status
- Product category count: Buckets customers by number of distinct product categories (2, 3, 4+)

### Google Migration Tracking

Two dimensions track Google-related migrations:
- **Google_migrated_shopper_flag**: Shopper-level migration status
- **Google_migrated_subscription_flag**: Subscription-level migration status (matches on resource_id, product_family, bill_id, bill_line_num)

Excludes shopper_id 10839228 from Google migration tracking (test account).

### Product Period Standardization

Normalizes product period names and quantities:
```sql
-- Period Name
CASE WHEN product_period_name = 'year' THEN 'Year' ELSE 'Month' END

-- Period Quantity
CASE 
    WHEN product_period_name = '6-month' THEN 6
    WHEN product_period_name = 'quarter' THEN 3
    ELSE product_period_qty 
END
```

### Finance Product Hierarchy Special Logic

**MS Office 365 Subline**:
Strips "MS Office 365" from the forecast group name to derive subline:
```sql
CASE 
    WHEN fin_pnl_line = 'MS Office 365' THEN 
        TRIM(REPLACE(pnl_forecast_group_name, 'MS Office 365', ''))
    ...
END
```

**Websites and Marketing**:
Defaults to 'GoCentral Website Paid' when subline is missing.

### Fixed Mix Adjustment Methodology

1. **Baseline Period**: Dec 1, 2024 - Feb 28, 2025
   - All dimension combinations from this period become the "fixed mix"

2. **Cross Join**: Each date in current/prior year × all dimension combinations
   - Creates complete matrix of date × dimensions
   - Ensures zero values for missing combinations

3. **Comparison**: 
   - Actual values populated where data exists
   - Fixed period values joined to every date
   - Enables calculation: Actual Performance on Fixed Mix vs. Prior Year on Fixed Mix

---

## 6. Data Flow

### Stage 1: Dimension Preparation
```
dim_prod ← dim_product_vw (finance hierarchy)
two_plus_cust ← two_plus_active_customer_history (filtered to date range)
cust360 ← goog_migrations_final (Google-migrated shoppers)
gsub360 ← goog_migrations_final (Google-migrated subscriptions)
```

### Stage 2: Base Data Creation

**Cohort Expirations** (`expirations_cohort_base_data`):
- Source: renewal_360
- Key Date: prior_bill_paid_through_mst_date
- Includes: expiry_qty, renewal_qty, ontime_renewal_qty, all revenue metrics
- Filters: Date range 2023-01-01 to current, exclude flagged records

**Cash Expirations** (`expirations_cash_base_data`):
- Source: renewal_360
- Key Date: prior_bill_billing_due_mst_date
- Includes: expiry_qty only (renewals joined separately)
- Special logic: Adjusts expiry_qty for free trial conversions

**Renewals** (`renewals_base_data`):
- Source: renewal_360
- Key Date: renewal_bill_modified_mst_date
- Includes: renewal_qty, all renewal revenue metrics
- Filter: renewal_bill_gcr_usd_amt <> 0

### Stage 3: Analysis-Type Specific Aggregation

**Cash Basis** (`tmp_cash_renewals_all`):
- Full outer join: expirations_cash_base_data + renewals_base_data
- Join on: billing_due_date + 40+ dimension fields
- Produces: Combined expiry and renewal metrics

**Cohort Basis** (`tmp_cohort_renewals_all`):
- Direct selection from expirations_cohort_base_data
- Key date: paid_through_date
- Includes on-time metrics and potential revenue

### Stage 4: Relative Date Integration

```
tmp_relative_dates ← dim_relative_date (filtered to current year, prior 2 years)
tmp_cash_final ← tmp_cash_renewals_all × tmp_relative_dates
tmp_cohort_renewal_final ← tmp_cohort_renewals_all × tmp_relative_dates
```

Inserts both into `renewals_360_agg`.

### Stage 5: Descriptive Aggregation

`renewals_rate_mix_agg`:
- Aggregates renewals_360_agg
- Adds descriptive text fields (e.g., 'Auto' vs 'Manual', 'Paid' vs 'Free')

### Stage 6: Fixed Mix Adjustment

For each granularity (Daily, Weekly, Monthly):

1. **Fixed Period**: Aggregate baseline period (Dec 2024 - Feb 2025)
2. **Actual Period**: Aggregate current/prior year data
3. **Cross Join**: All dates × all dimension combinations (from union of actual + fixed)
4. **Left Join**: Populate actual values where available, else 0
5. **Join Fixed**: Attach fixed period metrics to every row

Outputs to `renewal_fixed_rate_mix_adjusted_QS`.

### Stage 7: Data Quality Checks

Two validation checks inserted into `renewal_job_alerts`:
1. **Renewals Overview Agg**: Validates renewals_rate_mix_agg
2. **Renewals Fixed Mix Adjusted**: Validates renewal_fixed_rate_mix_adjusted_QS

Checks:
- Row count > 0
- Max date = current_date - 1 (PST timezone)
- Status: SUCCESS or FAILED

---

## 7. Filters & Conditions

### Date Ranges

| Analysis Type | Date Field | Range |
|---------------|------------|-------|
| Cohort Expirations | prior_bill_paid_through_mst_date | 2023-01-01 to current_date |
| Cash Expirations | prior_bill_billing_due_mst_date | 2023-01-01 to current_date |
| Renewals | renewal_bill_modified_mst_date | 2023-01-01 to current_date |
| Fixed Period | bill_mst_date | 2024-12-01 to 2025-02-28 |

### Key Exclusions

```sql
-- Renewal 360 base filters
WHERE bill_exclude_reason_desc IS NULL
AND bill_exclude_reason_desc IS NULL  -- repeated for different bases

-- Two Plus Customer filters
WHERE exclude_reason_desc IS NULL
AND source_type_enum = 'external'

-- Google Migration exclusions
WHERE prior_bill_shopper_id <> 10839228  -- test account

-- Renewal basis filter
WHERE renewal_bill_gcr_usd_amt <> 0  -- exclude zero-revenue renewals
```

### Relative Date Filters

```sql
-- Fixed mix adjustments use relative periods
WHERE relative_date_period_name IN ('Current Year', 'Prior Year (1)')
   OR relative_week_period_name IN ('Current Year', 'Prior Year (1)')
   OR relative_month_period_name IN ('Current Year', 'Prior Year (1)')

-- Weekly requires minimum 13 weeks of data
WHERE relative_week > (max_date_week - 13 * 7)

-- Daily requires all dates after minimum
WHERE relative_date > min_relative_renewal_date
```

---

## 8. Output Schema

### renewals_360_agg

| Column | Type | Description |
|--------|------|-------------|
| bill_mst_date | DATE | Transaction date (varies by analysis_type) |
| analysis_type | VARCHAR(20) | 'cash basis', 'cohort basis', or 'renewal basis' |
| pnl_pillar_name | VARCHAR(100) | Business pillar: International Independent, US Independents, Investors, Partners, Commerce |
| region_2_name | VARCHAR(100) | Geographic region level 2 |
| region_3_name | VARCHAR(100) | Geographic region level 3 |
| country_name | VARCHAR(100) | Country name |
| customer_type_name | VARCHAR(100) | Customer type classification |
| historical_auto_renewal_flag | BOOLEAN | Whether subscription was on auto-renewal |
| first_expiry_sequence_flag | BOOLEAN | TRUE if first expiration, FALSE if 2nd+ |
| product_pnl_group_name | VARCHAR(100) | Product P&L group |
| product_pnl_category_name | VARCHAR(100) | Product P&L category (e.g., Domain Registration) |
| product_pnl_line_name | VARCHAR(100) | Product P&L line |
| product_pnl_version_name | VARCHAR(100) | Product version |
| product_pnl_subline_name | VARCHAR(100) | Product subline |
| point_of_purchase_name | VARCHAR(100) | Sales channel |
| product_period_name | VARCHAR(100) | 'Year' or 'Month' |
| product_period_qty | INT | Number of billing periods |
| domain_bulk_pricing_flag | BOOLEAN | Bulk pricing indicator |
| renewal_timing_desc | VARCHAR(100) | Renewal timing classification |
| reseller_type_name | VARCHAR(100) | Reseller type if applicable |
| product_family_name | VARCHAR(100) | Product family |
| payable_bill_line_flag | BOOLEAN | Whether line is paid (vs. free) |
| bill_gcr_usd_amt_flag | BOOLEAN | Whether GCR > 0 |
| primary_product_flag | BOOLEAN | TRUE if primary product, FALSE if add-on |
| fin_pnl_group | VARCHAR(100) | Finance P&L group |
| fin_pnl_category | VARCHAR(100) | Finance P&L category |
| fin_pnl_line | VARCHAR(100) | Finance P&L line |
| fin_pnl_subline | VARCHAR(100) | Finance P&L subline (derived) |
| fin_investor_relation_class_name | VARCHAR(100) | Investor relations class |
| fin_investor_relation_subclass_name | VARCHAR(100) | Investor relations subclass |
| fin_investor_relation_segment_name | VARCHAR(100) | Investor relations segment |
| renewal_month | DATE | Month of renewal transaction |
| two_plus_hist_flag | BOOLEAN | Historical 2+ product customer status |
| two_plus_current_flag | BOOLEAN | Current 2+ product customer status |
| Google_migrated_shopper_flag | VARCHAR(100) | 'Google Migrated Shopper' or 'Other' |
| Google_migrated_subscription_flag | VARCHAR(100) | 'Google Migrated Subscription' or 'Other' |
| customer_paid_product_category | VARCHAR(100) | '4 + Products', '3 Products', '2 Products', or 'Not 2+ Product' |
| expiry_qty | INT | Number of expirations |
| renewal_qty | INT | Number of renewals |
| ontime_renewal_qty | INT | Number of on-time renewals (cohort basis only) |
| renewal_bill_gcr_usd_amt | DECIMAL(38,10) | Gross customer revenue (USD) |
| renewal_bill_product_month_qty | INT | Total product months |
| renewal_bill_receipt_price_usd_amt | DECIMAL(38,10) | Receipt price (USD) |
| ontime_renewal_receipt_price_amt | DECIMAL(38,10) | Receipt price for on-time renewals |
| potential_receipt_price_usd_amt | DECIMAL(38,10) | Potential revenue if all renewed |
| renewal_bill_list_price_usd_amt | DECIMAL(38,10) | List price before discounts |
| renewal_bill_cc_gcr_usd_amt | DECIMAL(38,10) | Constant currency GCR |
| relative_date | DATE | Date relative to max_date |
| relative_week | DATE | Week start date relative to max_date |
| relative_month | DATE | Month start date relative to max_date |
| relative_date_period_name | VARCHAR(25) | 'Current Year', 'Prior Year (1)', etc. |
| relative_week_period_name | VARCHAR(25) | Week-based period name |
| relative_month_period_name | VARCHAR(25) | Month-based period name |
| calendar_date | DATE | Actual calendar date |
| max_date | DATE | As-of date for analysis |
| anchor_date_week | DATE | Week anchor date |
| anchor_date_month | DATE | Month anchor date |
| max_date_week | DATE | Max date at week level |
| max_date_month | DATE | Max date at month level |

### renewals_rate_mix_agg

All columns from renewals_360_agg, plus:

| Column | Type | Description |
|--------|------|-------------|
| first_expiry_seq_desc | VARCHAR | '1st Expiry' or '2nd-Nth Expiry' |
| domain_bulk_pricing_desc | VARCHAR | 'Bulk' or 'Non-Bulk' |
| payable_bill_line_desc | VARCHAR | 'Paid' or 'Free' |
| historical_auto_renewal_desc | VARCHAR | 'Auto' or 'Manual' |
| primary_product_flag_desc | VARCHAR | 'Primary' or 'Add-on' |
| bill_gcr_usd_amt_desc | VARCHAR | 'Paid' or 'Free' |
| two_plus_current_desc | VARCHAR | 'True' or 'False' |
| two_plus_hist_desc | VARCHAR | 'True' or 'False' |

### renewal_fixed_rate_mix_adjusted_QS

| Column | Type | Description |
|--------|------|-------------|
| as_of_date | DATE | Report date (day/week/month start) |
| relative_date / relative_week / relative_month | DATE | Relative date dimension |
| relative_*_period_name | VARCHAR | Period classification |
| analysis_type | VARCHAR(20) | Basis type |
| ... | ... | All dimension columns (region, customer, product, etc.) |
| renewal_bill_gcr_usd_amt | DECIMAL | Actual renewal GCR |
| renewal_bill_product_month_qty | INT | Actual product months |
| renewal_qty | INT | Actual renewal quantity |
| expiry_qty | INT | Actual expiry quantity |
| fixed_gcr_amt | DECIMAL | Baseline period GCR |
| fixed_renewal_month_qty | INT | Baseline period product months |
| fixed_renewal_qty | INT | Baseline period renewal quantity |
| fixed_expiries | INT | Baseline period expiries |
| date_granularity | VARCHAR | 'Daily', 'Weekly', or 'Monthly' |
| Fixed_mix_period | VARCHAR | 'Dec24-Feb25' |

### renewal_job_alerts

| Column | Type | Description |
|--------|------|-------------|
| job_name / dataset_name | VARCHAR | Job identifier |
| max_date | DATE | Latest date in dataset |
| data_Expected_date | DATE | Expected latest date (current_date - 1) |
| run_ts | TIMESTAMP | Execution timestamp |
| status | VARCHAR | 'SUCCESS' or 'FAILED' |
| row_count | INT | Total rows in dataset |
| error_message | VARCHAR | Error description if failed |

---

## 9. Dependencies

### Upstream Dependencies

**Critical Data Sources**:
- `dev.dna_approved.renewal_360`: Core renewal fact table (must be refreshed before this query)
- `dev.dna_approved.two_plus_active_customer`: Current customer segmentation
- `dev.dna_approved.two_plus_active_customer_history`: Historical customer segmentation
- `ckp_analytic_share.finance360.dim_product_vw`: Finance product hierarchy
- `dev.bi_prod.dim_relative_date`: Relative date dimension (must include current date)

**Reference Data**:
- `dev.dna_approved.dim_geography`: Geographic mappings
- `dev.ba_dri.goog_migrations_final`: Google migration status

### Downstream Dependencies

**Consuming Systems**:
- QuickSight dashboards reading from `renewal_fixed_rate_mix_adjusted_QS`
- Financial reporting using `renewals_360_agg` and `renewals_rate_mix_agg`
- Data quality monitoring via `renewal_job_alerts`

### Refresh Schedule

Based on the query logic:
- Expected to run daily
- Target completion: Before start of business day (PST)
- Validates data through `current_date - 1`
- Updates job alert flags on completion

---

## 10. Usage Examples

### Running the Query

```sql
-- Execute the full query
-- Note: This is a long-running query (~15-30 minutes depending on data volume)
-- Ensure sufficient warehouse resources

-- The query will:
-- 1. Update job alert status at start
-- 2. Process all temporary tables
-- 3. Create/truncate final output tables
-- 4. Validate results and update job alerts
```

### Querying Results

**Example 1: Year-over-Year Renewal Rate by Product Line (Cash Basis)**
```sql
SELECT 
    product_pnl_line_name,
    relative_date_period_name,
    SUM(expiry_qty) AS expiries,
    SUM(renewal_qty) AS renewals,
    ROUND(SUM(renewal_qty)::DECIMAL / NULLIF(SUM(expiry_qty), 0), 4) AS renewal_rate
FROM ba_ecommerce.renewals_rate_mix_agg
WHERE analysis_type = 'Cash Basis'
    AND relative_date_period_name IN ('Current Year', 'Prior Year (1)')
    AND calendar_date >= '2025-01-01'
GROUP BY 1, 2
ORDER BY 1, 2;
```

**Example 2: Fixed Mix Adjusted Performance (Daily)**
```sql
SELECT 
    as_of_date,
    relative_date_period_name,
    fin_pnl_line,
    -- Actual renewal rate on current mix
    SUM(renewal_qty)::DECIMAL / NULLIF(SUM(expiry_qty), 0) AS actual_renewal_rate,
    -- Renewal rate on fixed mix (Dec24-Feb25)
    SUM(renewal_qty)::DECIMAL / NULLIF(SUM(fixed_expiries), 0) AS fixed_mix_renewal_rate
FROM ba_ecommerce.renewal_fixed_rate_mix_adjusted_QS
WHERE date_granularity = 'Daily'
    AND analysis_type = 'Cash Basis'
GROUP BY 1, 2, 3
ORDER BY 1, 3;
```

**Example 3: Google Migration Impact**
```sql
SELECT 
    Google_migrated_subscription_flag,
    relative_month_period_name,
    SUM(renewal_bill_gcr_usd_amt) / 1000000 AS renewal_gcr_mm,
    SUM(renewal_qty) AS renewal_count,
    AVG(renewal_bill_gcr_usd_amt / NULLIF(renewal_qty, 0)) AS avg_renewal_price
FROM ba_ecommerce.renewals_rate_mix_agg
WHERE analysis_type = 'Cash Basis'
    AND relative_month_period_name = 'Current Year'
GROUP BY 1, 2
ORDER BY 2, 1;
```

**Example 4: 2+ Product Customer Renewal Performance**
```sql
SELECT 
    customer_paid_product_category,
    two_plus_current_desc,
    product_pnl_category_name,
    SUM(renewal_bill_gcr_usd_amt) AS renewal_gcr,
    SUM(renewal_qty)::DECIMAL / NULLIF(SUM(expiry_qty), 0) AS renewal_rate
FROM ba_ecommerce.renewals_rate_mix_agg
WHERE analysis_type = 'Cohort Basis'
    AND relative_date_period_name = 'Current Year'
    AND calendar_date >= CURRENT_DATE - 90
GROUP BY 1, 2, 3
ORDER BY 4 DESC;
```

**Example 5: Data Quality Check**
```sql
SELECT 
    dataset_name,
    max_date,
    data_expected_date,
    status,
    error_message,
    run_ts
FROM dev.ba_ecommerce.renewal_job_alerts
WHERE job_name IN ('Renewals Overview Agg', 'Renewals Fixed Mix Adjusted')
ORDER BY run_ts DESC
LIMIT 10;
```

### Interpreting Results

**Renewal Rate Calculation**:
- Cohort Basis: Includes all eventual renewals, regardless of timing
- Cash Basis: Matches renewals to expirations by billing due date (may show different timing)
- On-Time Rate: Only available in cohort basis

**Fixed Mix Interpretation**:
- Compare `actual` metrics to `fixed_*` metrics
- If actual > fixed: Performance improved beyond mix shift
- If actual < fixed: Mix shift explains performance difference

**Revenue Metrics**:
- GCR: Gross Customer Revenue (standard metric)
- Receipt Price: Actual price customer paid (after discounts)
- List Price: Price before discounts
- Constant Currency: Removes FX impact for like-for-like comparison

---

## Notes

- **Timezone**: All date comparisons use Pacific Time (America/Los_Angeles)
- **Performance**: Query creates multiple large temporary tables; ensure adequate Redshift resources
- **Data Lag**: Designed to run on D+1 basis (processes through yesterday)
- **Fixed Period**: Hardcoded to Dec 2024 - Feb 2025; update `WHERE bill_mst_date BETWEEN '2024-12-01' AND '2025-02-28'` to change baseline