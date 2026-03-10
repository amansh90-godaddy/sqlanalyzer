-- JIRA: HAT-3917 - WAM Site Performance Tracking
DROP TABLE IF EXISTS dev.ba_corporate.wam_site_performance;
CREATE TABLE dev.ba_corporate.wam_site_performance
-- create or replace view dna_sandbox.wam_site_marketing_v1

SORTKEY (website_date)
as
(

-- ================================================================================
-- TOP-RANKED TRACKING CODE LOGIC (Optimized)
-- Only includes tracking codes with GCR > 0 (Ranks 1-53)
-- Falls back to last value from ITC fields if no ranked code found
-- ================================================================================
WITH base_traffic_data AS (
    SELECT
        session_id,
        website_activity_mst_date,
        channel_grouping_name,
        device_category_name,
        web_region_2_name,
        market_site_country_name,
        web_customer_state_name AS existing_customer_flag,
        session_cnt,
        new_gcr_session_cnt as gcr_session_cnt,
        page_advance_session_cnt,
        web_focal_country_name,
        page_path_list,
        web_business_unit_name,
        first_hit_content_group_2_name,
        order_item_tracking_code_list,
        item_tracking_code_payment_attempt_list,
        item_tracking_code_begin_checkout_list,
        item_tracking_code_add_to_cart_list,
        item_tracking_code_click_list,
        item_tracking_code_impression_list,
        -- Concatenate all ITC fields with separator for pattern matching
        -- '|~|' || COALESCE(order_item_tracking_code_list, '') || '|~|' ||
        '|~|' || COALESCE(item_tracking_code_payment_attempt_list, '') || '|~|' ||
        COALESCE(item_tracking_code_begin_checkout_list, '') || '|~|' ||
        COALESCE(item_tracking_code_add_to_cart_list, '') || '|~|' ||
        COALESCE(item_tracking_code_click_list, '') || '|~|' ||
        COALESCE(item_tracking_code_impression_list, '') || '|~|' AS all_itc_combined,
        -- Keep individual fields for source determination and fallback
        -- '|~|' || COALESCE(order_item_tracking_code_list, '') || '|~|' AS order_itc_search,
        '|~|' || COALESCE(item_tracking_code_payment_attempt_list, '') || '|~|' AS payment_attempt_search,
        '|~|' || COALESCE(item_tracking_code_begin_checkout_list, '') || '|~|' AS begin_checkout_search,
        '|~|' || COALESCE(item_tracking_code_add_to_cart_list, '') || '|~|' AS add_to_cart_search,
        '|~|' || COALESCE(item_tracking_code_click_list, '') || '|~|' AS click_search,
        '|~|' || COALESCE(item_tracking_code_impression_list, '') || '|~|' AS impression_search
    FROM dev.website_prod.analytic_traffic_detail
    WHERE website_activity_mst_date BETWEEN '2026-01-01' AND '2026-01-31'
        AND gd_sales_flag = TRUE
        AND session_id IS NOT NULL
        AND website_activity_exclusion_reason_desc IS NULL
),

base_product_data_raw as (
SELECT 
  website_activity_mst_date,
  session_id,
  item_tracking_code,
  product_term_num::VARCHAR || ' ' || product_term_unit_desc AS product_term,
  CASE WHEN lower(product_pnl_subline_name) IN ('gocentral seo', 'gocentral marketing') then 'Marketing'
      WHEN lower(product_pnl_subline_name) IN ('commerce plus') then 'Commerce Plus'
      WHEN lower(product_pnl_subline_name) IN ('deluxe') then 'Standard'
      WHEN lower(product_pnl_subline_name) IN ('economy') then 'Basic'
      WHEN lower(product_pnl_subline_name) IN ('paypal commerce', 'super premium') then 'Commerce'
      WHEN lower(product_pnl_subline_name) IN ('premium', 'tier 1 premium') then 'Premium'
      WHEN lower(product_pnl_subline_name) IN ('starter') then 'Starter'
      ELSE product_pnl_subline_name END plan_type,
  CASE 
    WHEN COALESCE(gcr_usd_amt, 0) = 0 THEN 'Free'
    WHEN gcr_usd_amt > 0 and product_free_trial_conversion_flag = 'True' THEN 'Free to Paid'
    WHEN gcr_usd_amt > 0 and product_free_trial_conversion_flag = 'False' THEN 'Paid'
  END as free_or_paid,
  sum(product_unit_qty) as total_wam_units,
  sum(gcr_usd_amt) as GCR
  FROM dev.dna_approved.bill_line_traffic_ext 
WHERE website_activity_mst_date BETWEEN '2026-01-01' AND '2026-01-31'
  AND bill_line_traffic_ext.product_pnl_line_name in ('Websites and Marketing', 'Website Builder')  -- WAM filter
  AND point_of_purchase_name = 'Web'
  AND exclude_reason_month_end_desc is NULL
  AND refund_flag = false
  AND chargeback_flag = false
  -- AND gcr_usd_amt >0
  AND product_pnl_new_renewal_name = 'New Purchase'
GROUP BY 1,2,3,4,5,6
),

-- Assign wam_sessions=1 to only ONE row per session (first by item_tracking_code)
base_product_data as (
SELECT 
  *,
  CASE WHEN ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY item_tracking_code) = 1 
       THEN 1 ELSE 0 END as wam_sessions
FROM base_product_data_raw
),

base_data as (
select 
    a.*, 
    b.item_tracking_code as order_item_tracking_code, 
    b.product_term, 
    b.plan_type,
    b.free_or_paid, 
    b.wam_sessions, 
    b.total_wam_units, 
    b.GCR as WAM_gcr,
    -- Flag to prevent double-counting traffic metrics when session has multiple products
    -- Partition by BOTH session_id AND date to handle edge cases
    CASE WHEN ROW_NUMBER() OVER (PARTITION BY a.session_id, a.website_activity_mst_date ORDER BY b.item_tracking_code NULLS LAST) = 1 
         THEN 1 ELSE 0 END as traffic_row_flag
from base_traffic_data a 
LEFT JOIN base_product_data b 
    ON a.website_activity_mst_date = b.website_activity_mst_date
    AND a.session_id = b.session_id
),

-- Find top-ranked tracking code using CASE WHEN in rank order (GCR > 0 only: Ranks 1-53)
top_ranked_extract AS (
    SELECT
        session_id,
        website_activity_mst_date,
        channel_grouping_name,
        device_category_name,
        web_region_2_name,
        existing_customer_flag,
        -- Apply traffic_row_flag to prevent double-counting traffic metrics
        session_cnt * traffic_row_flag as session_cnt,
        page_advance_session_cnt * traffic_row_flag as page_advance_session_cnt,
        gcr_session_cnt * traffic_row_flag as gcr_session_cnt,
        first_hit_content_group_2_name,
        web_focal_country_name,
        page_path_list,
        web_business_unit_name,
        -- Product columns from join
        order_item_tracking_code,
        product_term,
        plan_type,
        free_or_paid,
        wam_sessions,
        total_wam_units,
        WAM_gcr,
        -- ITC list columns
        item_tracking_code_payment_attempt_list,
        item_tracking_code_begin_checkout_list,
        item_tracking_code_add_to_cart_list,
        item_tracking_code_click_list,
        item_tracking_code_impression_list,
        -- Check for ALL tracking codes in rank order (based on TopITCs.csv - 94 codes with GCR > 0, 158 codes with GCR = 0)
        CASE
            WHEN order_item_tracking_code is NOT NULL THEN order_item_tracking_code
            -- GCR > 0 codes (Ranks 1-95)
            WHEN all_itc_combined LIKE '%|~|upp_f2p_upgrade|~|%' THEN 'upp_f2p_upgrade'  -- Rank 1, GCR: 37,541,749.14
            WHEN all_itc_combined LIKE '%|~|slp_wsb_ft_nocc_config|~|%' THEN 'slp_wsb_ft_nocc_config'  -- Rank 2, GCR: 1,597,432.08
            WHEN all_itc_combined LIKE '%|~|cart_xsell_carousel|~|%' THEN 'cart_xsell_carousel'  -- Rank 3, GCR: 1,225,591.67
            WHEN all_itc_combined LIKE '%|~|slp_wsb_ft_nocc|~|%' THEN 'slp_wsb_ft_nocc'  -- Rank 4, GCR: 579,456.77
            WHEN all_itc_combined LIKE '%|~|upp_p2p_upgrade_downgrade|~|%' THEN 'upp_p2p_upgrade_downgrade'  -- Rank 5, GCR: 480,265.89
            WHEN all_itc_combined LIKE '%|~|mgr_slp_wsb_ft_nocc_config|~|%' THEN 'mgr_slp_wsb_ft_nocc_config'  -- Rank 6, GCR: 446,238.85
            WHEN all_itc_combined LIKE '%|~|cart_xsell_single_card|~|%' THEN 'cart_xsell_single_card'  -- Rank 7, GCR: 312,170.53
            WHEN all_itc_combined LIKE '%|~|notifications_bell|~|%' THEN 'notifications_bell'  -- Rank 8, GCR: 231,300.49
            WHEN all_itc_combined LIKE '%|~|cart_xsells_inline|~|%' THEN 'cart_xsells_inline'  -- Rank 9, GCR: 217,177.98
            WHEN all_itc_combined LIKE '%|~|mgr_shared_shopping_service|~|%' THEN 'mgr_shared_shopping_service'  -- Rank 10, GCR: 209,789.51
            WHEN all_itc_combined LIKE '%|~|123reg_slp_wsb|~|%' THEN '123reg_slp_wsb'  -- Rank 11, GCR: 206,694.45
            WHEN all_itc_combined LIKE '%|~|single_product_renewal|~|%' THEN 'single_product_renewal'  -- Rank 12, GCR: 176,007.54
            WHEN all_itc_combined LIKE '%|~|mgr_slp_wsb_ft_nocc|~|%' THEN 'mgr_slp_wsb_ft_nocc'  -- Rank 13, GCR: 156,937.82
            WHEN all_itc_combined LIKE '%|~|dpp_precheck|~|%' THEN 'dpp_precheck'  -- Rank 14, GCR: 151,038.91
            WHEN all_itc_combined LIKE '%|~|slp_rstdstore|~|%' THEN 'slp_rstdstore'  -- Rank 15, GCR: 113,654.34
            WHEN all_itc_combined LIKE '%|~|mgr_slp_sapi_config|~|%' THEN 'mgr_slp_sapi_config'  -- Rank 16, GCR: 111,009.54
            WHEN all_itc_combined LIKE '%|~|slp_sapi_config|~|%' THEN 'slp_sapi_config'  -- Rank 17, GCR: 85,830.36
            WHEN all_itc_combined LIKE '%|~|plp_essentials_bundle|~|%' THEN 'plp_essentials_bundle'  -- Rank 18, GCR: 81,749.11
            WHEN all_itc_combined LIKE '%|~|account_myrenewals_single|~|%' THEN 'account_myrenewals_single'  -- Rank 19, GCR: 75,720.70
            WHEN all_itc_combined LIKE '%|~|slp_subs_pricing|~|%' THEN 'slp_subs_pricing'  -- Rank 20, GCR: 69,201.07
            WHEN all_itc_combined LIKE '%|~|mgr_misc-purchase|~|%' THEN 'mgr_misc-purchase'  -- Rank 21, GCR: 68,478.35
            WHEN all_itc_combined LIKE '%|~|mya_acctsettings_subscriptions_multiselect|~|%' THEN 'mya_acctsettings_subscriptions_multiselect'  -- Rank 22, GCR: 54,940.68
            WHEN all_itc_combined LIKE '%|~|account_myrenewals_jtbd|~|%' THEN 'account_myrenewals_jtbd'  -- Rank 23, GCR: 47,767.95
            WHEN all_itc_combined LIKE '%|~|dpp_config1|~|%' THEN 'dpp_config1'  -- Rank 24, GCR: 40,592.88
            WHEN all_itc_combined LIKE '%|~|shared_shopping_service|~|%' THEN 'shared_shopping_service'  -- Rank 25, GCR: 30,418.43
            WHEN all_itc_combined LIKE '%|~|upp_renewals|~|%' THEN 'upp_renewals'  -- Rank 26, GCR: 22,512.28
            WHEN all_itc_combined LIKE '%|~|plp_ecommerce_bundle|~|%' THEN 'plp_ecommerce_bundle'  -- Rank 27, GCR: 21,597.90
            WHEN all_itc_combined LIKE '%|~|dcc_manage_website_activation|~|%' THEN 'dcc_manage_website_activation'  -- Rank 28, GCR: 19,582.38
            WHEN all_itc_combined LIKE '%|~|empty_cart_xsell_carousel|~|%' THEN 'empty_cart_xsell_carousel'  -- Rank 29, GCR: 18,849.01
            WHEN all_itc_combined LIKE '%|~|slp_wsb_ft_nocc_test&itc=wsb_test_config|~|%' THEN 'slp_wsb_ft_nocc_test&itc=wsb_test_config'  -- Rank 30, GCR: 17,591.74
            WHEN all_itc_combined LIKE '%|~|slp_wsb_ft_nocc&ref=slp_trusted|~|%' THEN 'slp_wsb_ft_nocc&ref=slp_trusted'  -- Rank 31, GCR: 15,158.91
            WHEN all_itc_combined LIKE '%|~|slp_wsb_ft_nocc&ref=slp_trusted_config|~|%' THEN 'slp_wsb_ft_nocc&ref=slp_trusted_config'  -- Rank 32, GCR: 13,382.26
            WHEN all_itc_combined LIKE '%|~|slp_wds_start_plan|~|%' THEN 'slp_wds_start_plan'  -- Rank 33, GCR: 12,374.83
            WHEN all_itc_combined LIKE '%|~|slp_linkinbio|~|%' THEN 'slp_linkinbio'  -- Rank 34, GCR: 12,034.62
            WHEN all_itc_combined LIKE '%|~|upp_start_upp_start|~|%' THEN 'upp_start_upp_start'  -- Rank 35, GCR: 10,913.78
            WHEN all_itc_combined LIKE '%|~|mui_wsb|~|%' THEN 'mui_wsb'  -- Rank 36, GCR: 10,300.99
            WHEN all_itc_combined LIKE '%|~|app_vnext_free_trial_expired_renewal|~|%' THEN 'app_vnext_free_trial_expired_renewal'  -- Rank 37, GCR: 9,563.12
            WHEN all_itc_combined LIKE '%|~|plp_starter_bundle|~|%' THEN 'plp_starter_bundle'  -- Rank 38, GCR: 8,149.98
            WHEN all_itc_combined LIKE '%|~|mgr_slp_webdesign_simple_site|~|%' THEN 'mgr_slp_webdesign_simple_site'  -- Rank 39, GCR: 8,031.50
            WHEN all_itc_combined LIKE '%|~|mgr_123reg_slp_wsb|~|%' THEN 'mgr_123reg_slp_wsb'  -- Rank 40, GCR: 7,942.20
            WHEN all_itc_combined LIKE '%|~|mgr_slp_marketing_suite|~|%' THEN 'mgr_slp_marketing_suite'  -- Rank 41, GCR: 7,331.05
            WHEN all_itc_combined LIKE '%|~|slp_ols_config|~|%' THEN 'slp_ols_config'  -- Rank 42, GCR: 6,810.19
            WHEN all_itc_combined LIKE '%|~|mgr_slp_rstdstore|~|%' THEN 'mgr_slp_rstdstore'  -- Rank 43, GCR: 6,377.74
            WHEN all_itc_combined LIKE '%|~|slp_boost6|~|%' THEN 'slp_boost6'  -- Rank 44, GCR: 5,578.72
            WHEN all_itc_combined LIKE '%|~|slp_wsb_ft_nocc_test&itc=wsb_test|~|%' THEN 'slp_wsb_ft_nocc_test&itc=wsb_test'  -- Rank 45, GCR: 5,279.94
            WHEN all_itc_combined LIKE '%|~|mgr_slp_webdesign_simple_site_config|~|%' THEN 'mgr_slp_webdesign_simple_site_config'  -- Rank 46, GCR: 4,961.33
            WHEN all_itc_combined LIKE '%|~|cart_xsell_multioffer_card|~|%' THEN 'cart_xsell_multioffer_card'  -- Rank 47, GCR: 4,847.28
            WHEN all_itc_combined LIKE '%|~|wp_client_card|~|%' THEN 'wp_client_card'  -- Rank 48, GCR: 4,510.64
            WHEN all_itc_combined LIKE '%|~|mgrzed0ov|~|%' THEN 'mgrzed0ov'  -- Rank 49, GCR: 4,378.04
            WHEN all_itc_combined LIKE '%|~|digital_marketing_suite_deluxe|~|%' THEN 'digital_marketing_suite_deluxe'  -- Rank 50, GCR: 3,537.58
            WHEN all_itc_combined LIKE '%|~|slp_managed_woocommerce|~|%' THEN 'slp_managed_woocommerce'  -- Rank 51, GCR: 3,149.04
            WHEN all_itc_combined LIKE '%|~|slp_ols|~|%' THEN 'slp_ols'  -- Rank 52, GCR: 2,551.32
            WHEN all_itc_combined LIKE '%|~|slp_rstore|~|%' THEN 'slp_rstore'  -- Rank 53, GCR: 2,463.63
            WHEN all_itc_combined LIKE '%|~|mgr_cs_bdgt_cal|~|%' THEN 'mgr_cs_bdgt_cal'  -- Rank 54, GCR: 2,318.04
            WHEN all_itc_combined LIKE '%|~|showinbio_lp|~|%' THEN 'showinbio_lp'  -- Rank 55, GCR: 2,273.37
            WHEN all_itc_combined LIKE '%|~|misc-purchase|~|%' THEN 'misc-purchase'  -- Rank 56, GCR: 2,186.65
            WHEN all_itc_combined LIKE '%|~|wp_shared_list|~|%' THEN 'wp_shared_list'  -- Rank 57, GCR: 1,609.34
            WHEN all_itc_combined LIKE '%|~|single_plan_upgrade_upp_d2p_new_purchase_conversat|~|%' THEN 'single_plan_upgrade_upp_d2p_new_purchase_conversat'  -- Rank 58, GCR: 1,576.10
            WHEN all_itc_combined LIKE '%|~|ios_studio_app_showinbio|~|%' THEN 'ios_studio_app_showinbio'  -- Rank 59, GCR: 1,435.36
            WHEN all_itc_combined LIKE '%|~|wp_pro_card|~|%' THEN 'wp_pro_card'  -- Rank 60, GCR: 1,364.64
            WHEN all_itc_combined LIKE '%|~|mgr_slp_wds_start_plan|~|%' THEN 'mgr_slp_wds_start_plan'  -- Rank 61, GCR: 1,252.49
            WHEN all_itc_combined LIKE '%|~|digital_marketing_suite_ultimate|~|%' THEN 'digital_marketing_suite_ultimate'  -- Rank 62, GCR: 1,023.64
            WHEN all_itc_combined LIKE '%|~|dcc_activation_email|~|%' THEN 'dcc_activation_email'  -- Rank 63, GCR: 1,001.12
            WHEN all_itc_combined LIKE '%|~|account_myrenewals_bulk|~|%' THEN 'account_myrenewals_bulk'  -- Rank 64, GCR: 939.75
            WHEN all_itc_combined LIKE '%|~|mya_acctsettings_billing_upgrade|~|%' THEN 'mya_acctsettings_billing_upgrade'  -- Rank 65, GCR: 920.71
            WHEN all_itc_combined LIKE '%|~|mgr_slp_wsb_ft_nocc_test&itc=wsb_test|~|%' THEN 'mgr_slp_wsb_ft_nocc_test&itc=wsb_test'  -- Rank 66, GCR: 773.30
            WHEN all_itc_combined LIKE '%|~|android_studio_app_showinbio|~|%' THEN 'android_studio_app_showinbio'  -- Rank 67, GCR: 651.97
            WHEN all_itc_combined LIKE '%|~|dpp_absol1.primary_exact|~|%' THEN 'dpp_absol1.primary_exact'  -- Rank 68, GCR: 638.92
            WHEN all_itc_combined LIKE '%|~|mgr_slp_wsb_ft_nocc&ref=slp_trusted|~|%' THEN 'mgr_slp_wsb_ft_nocc&ref=slp_trusted'  -- Rank 69, GCR: 632.18
            WHEN all_itc_combined LIKE '%|~|mya_acctsettings_myrenewals|~|%' THEN 'mya_acctsettings_myrenewals'  -- Rank 70, GCR: 590.05
            WHEN all_itc_combined LIKE '%|~|digital_marketing_suite_essentials|~|%' THEN 'digital_marketing_suite_essentials'  -- Rank 71, GCR: 588.07
            WHEN all_itc_combined LIKE '%|~|mgr_slp_wsb_ft_nocc&ref=slp_trusted_config|~|%' THEN 'mgr_slp_wsb_ft_nocc&ref=slp_trusted_config'  -- Rank 72, GCR: 578.77
            WHEN all_itc_combined LIKE '%|~|app_wsb_ptobp_upgrade|~|%' THEN 'app_wsb_ptobp_upgrade'  -- Rank 73, GCR: 535.98
            WHEN all_itc_combined LIKE '%|~|dlp_mena_digital_kit|~|%' THEN 'dlp_mena_digital_kit'  -- Rank 74, GCR: 385.48
            WHEN all_itc_combined LIKE '%|~|slp_wsb_bmat_annual_monthly|~|%' THEN 'slp_wsb_bmat_annual_monthly'  -- Rank 75, GCR: 380.78
            WHEN all_itc_combined LIKE '%|~|mgr_slp_ols_config|~|%' THEN 'mgr_slp_ols_config'  -- Rank 76, GCR: 357.57
            WHEN all_itc_combined LIKE '%|~|app_wsb_ptob_upgrade|~|%' THEN 'app_wsb_ptob_upgrade'  -- Rank 77, GCR: 331.09
            WHEN all_itc_combined LIKE '%|~|app_wsb_btobp_upgrade|~|%' THEN 'app_wsb_btobp_upgrade'  -- Rank 78, GCR: 309.34
            WHEN all_itc_combined LIKE '%|~|slp_wsb_bmat_annual|~|%' THEN 'slp_wsb_bmat_annual'  -- Rank 79, GCR: 287.65
            WHEN all_itc_combined LIKE '%|~|transferin_dcc_searchdomain|~|%' THEN 'transferin_dcc_searchdomain'  -- Rank 80, GCR: 279.71
            WHEN all_itc_combined LIKE '%|~|mgr_slp_ols|~|%' THEN 'mgr_slp_ols'  -- Rank 81, GCR: 213.81
            WHEN all_itc_combined LIKE '%|~|slp-boost|~|%' THEN 'slp-boost'  -- Rank 82, GCR: 201.58
            WHEN all_itc_combined LIKE '%|~|mgr_slp_boost6|~|%' THEN 'mgr_slp_boost6'  -- Rank 83, GCR: 192.79
            WHEN all_itc_combined LIKE '%|~|dpp_absol1.unavailable_organicspin|~|%' THEN 'dpp_absol1.unavailable_organicspin'  -- Rank 84, GCR: 103.20
            WHEN all_itc_combined LIKE '%|~|mgr_slp_rstore|~|%' THEN 'mgr_slp_rstore'  -- Rank 85, GCR: 95.88
            WHEN all_itc_combined LIKE '%|~|mgr_showinbio_lp|~|%' THEN 'mgr_showinbio_lp'  -- Rank 86, GCR: 45.24
            WHEN all_itc_combined LIKE '%|~|app_vnext_in_app_upgrade|~|%' THEN 'app_vnext_in_app_upgrade'  -- Rank 87, GCR: 45.05
            WHEN all_itc_combined LIKE '%|~|mgr_slp_wst_3|~|%' THEN 'mgr_slp_wst_3'  -- Rank 88, GCR: 34.99
            WHEN all_itc_combined LIKE '%|~|madmimi_marketing_suite_ultimate|~|%' THEN 'madmimi_marketing_suite_ultimate'  -- Rank 89, GCR: 34.92
            WHEN all_itc_combined LIKE '%|~|test-itc|~|%' THEN 'test-itc'  -- Rank 90, GCR: 27.48
            WHEN all_itc_combined LIKE '%|~|madmimi_marketing_suite_essentials|~|%' THEN 'madmimi_marketing_suite_essentials'  -- Rank 91, GCR: 23.24
            WHEN all_itc_combined LIKE '%|~|dpp_absol1.aftermarket_organicspin|~|%' THEN 'dpp_absol1.aftermarket_organicspin'  -- Rank 92, GCR: 10.18
            WHEN all_itc_combined LIKE '%|~|dpp_absol1|~|%' THEN 'dpp_absol1'  -- Rank 93, GCR: 10.18
            WHEN all_itc_combined LIKE '%|~|mgr_dcc_manage_website_activation|~|%' THEN 'mgr_dcc_manage_website_activation'  -- Rank 94, GCR: 5.99
            WHEN all_itc_combined LIKE '%|~|dcc_dns_management_use_my_domain_airo|~|%' THEN 'dcc_dns_management_use_my_domain_airo'  -- Rank 95, GCR: 4.99
            -- GCR = 0 codes (Ranks 96-253) - Ranked by Unit Qty
            WHEN all_itc_combined LIKE '%|~|dpp_bundling_is|~|%' THEN 'dpp_bundling_is'  -- Rank 96, Qty: 7,050,351
            WHEN all_itc_combined LIKE '%|~|vh_bundling_ai|~|%' THEN 'vh_bundling_ai'  -- Rank 97, Qty: 4,117,522
            WHEN all_itc_combined LIKE '%|~|vh_hosted_bundling_ai|~|%' THEN 'vh_hosted_bundling_ai'  -- Rank 98, Qty: 1,675,894
            WHEN all_itc_combined LIKE '%|~|slp_wsb_ft_getstarted_plans_nocc|~|%' THEN 'slp_wsb_ft_getstarted_plans_nocc'  -- Rank 99, Qty: 1,423,016
            WHEN all_itc_combined LIKE '%|~|myproducts-venture-tiles-start-new|~|%' THEN 'myproducts-venture-tiles-start-new'  -- Rank 100, Qty: 551,134
            WHEN all_itc_combined LIKE '%|~|myp_vt_bundling_ai|~|%' THEN 'myp_vt_bundling_ai'  -- Rank 101, Qty: 542,105
            WHEN all_itc_combined LIKE '%|~|slp_gocentral_themes_homepage|~|%' THEN 'slp_gocentral_themes_homepage'  -- Rank 102, Qty: 120,392
            WHEN all_itc_combined LIKE '%|~|dcc_usemydomain_airo|~|%' THEN 'dcc_usemydomain_airo'  -- Rank 103, Qty: 118,121
            WHEN all_itc_combined LIKE '%|~|mya_domain_manager_website_cart|~|%' THEN 'mya_domain_manager_website_cart'  -- Rank 104, Qty: 117,844
            WHEN all_itc_combined LIKE '%|~|recore_wsb|~|%' THEN 'recore_wsb'  -- Rank 105, Qty: 71,402
            WHEN all_itc_combined LIKE '%|~|mya_vh_buildwebsite_website|~|%' THEN 'mya_vh_buildwebsite_website'  -- Rank 106, Qty: 54,617
            WHEN all_itc_combined LIKE '%|~|dcc_portfolio_settings_website|~|%' THEN 'dcc_portfolio_settings_website'  -- Rank 107, Qty: 44,073
            WHEN all_itc_combined LIKE '%|~|hp_wsb|~|%' THEN 'hp_wsb'  -- Rank 108, Qty: 43,512
            WHEN all_itc_combined LIKE '%|~|slp_onlinestore_ft_nocc|~|%' THEN 'slp_onlinestore_ft_nocc'  -- Rank 109, Qty: 39,047
            WHEN all_itc_combined LIKE '%|~|dlp_website|~|%' THEN 'dlp_website'  -- Rank 110, Qty: 27,814
            WHEN all_itc_combined LIKE '%|~|dpp_gc_merchandise|~|%' THEN 'dpp_gc_merchandise'  -- Rank 111, Qty: 21,730
            WHEN all_itc_combined LIKE '%|~|slp_wsb_ft_websites_category_wsb|~|%' THEN 'slp_wsb_ft_websites_category_wsb'  -- Rank 112, Qty: 17,228
            WHEN all_itc_combined LIKE '%|~|mobile_ios_studio_app_sitemaker_primer|~|%' THEN 'mobile_ios_studio_app_sitemaker_primer'  -- Rank 113, Qty: 16,165
            WHEN all_itc_combined LIKE '%|~|mobile_android_studio_app_sitemaker_primer|~|%' THEN 'mobile_android_studio_app_sitemaker_primer'  -- Rank 114, Qty: 14,421
            WHEN all_itc_combined LIKE '%|~|mya_vh_buildwebsite_comingsoon|~|%' THEN 'mya_vh_buildwebsite_comingsoon'  -- Rank 115, Qty: 14,236
            WHEN all_itc_combined LIKE '%|~|slp_wsb_ft_features|~|%' THEN 'slp_wsb_ft_features'  -- Rank 116, Qty: 13,392
            WHEN all_itc_combined LIKE '%|~|mya_vh_buildwebsite_with_logo|~|%' THEN 'mya_vh_buildwebsite_with_logo'  -- Rank 117, Qty: 11,304
            WHEN all_itc_combined LIKE '%|~|dlp_prospect_brand|~|%' THEN 'dlp_prospect_brand'  -- Rank 118, Qty: 9,037
            WHEN all_itc_combined LIKE '%|~|dcc_dns_management_recore|~|%' THEN 'dcc_dns_management_recore'  -- Rank 119, Qty: 8,587
            WHEN all_itc_combined LIKE '%|~|airohq-non-domain-website|~|%' THEN 'airohq-non-domain-website'  -- Rank 120, Qty: 7,524
            WHEN all_itc_combined LIKE '%|~|dcc_usemydomain_settings|~|%' THEN 'dcc_usemydomain_settings'  -- Rank 121, Qty: 6,654
            WHEN all_itc_combined LIKE '%|~|ios-gd-mobile|~|%' THEN 'ios-gd-mobile'  -- Rank 122, Qty: 5,563
            WHEN all_itc_combined LIKE '%|~|aap_ai_onboarding_no_domain|~|%' THEN 'aap_ai_onboarding_no_domain'  -- Rank 123, Qty: 5,427
            WHEN all_itc_combined LIKE '%|~|hp_commerce_sell_anywhere|~|%' THEN 'hp_commerce_sell_anywhere'  -- Rank 124, Qty: 4,395
            WHEN all_itc_combined LIKE '%|~|slp_wsb_ft_getstarted_pp_nocc|~|%' THEN 'slp_wsb_ft_getstarted_pp_nocc'  -- Rank 125, Qty: 3,682
            WHEN all_itc_combined LIKE '%|~|ios_studio_app_createshelf_sites|~|%' THEN 'ios_studio_app_createshelf_sites'  -- Rank 126, Qty: 2,811
            WHEN all_itc_combined LIKE '%|~|recore_mvp|~|%' THEN 'recore_mvp'  -- Rank 127, Qty: 2,602
            WHEN all_itc_combined LIKE '%|~|slp_wsb_ft_websites_category_ols|~|%' THEN 'slp_wsb_ft_websites_category_ols'  -- Rank 128, Qty: 2,340
            WHEN all_itc_combined LIKE '%|~|mya_vh_buildwebsite_commerce|~|%' THEN 'mya_vh_buildwebsite_commerce'  -- Rank 129, Qty: 2,108
            WHEN all_itc_combined LIKE '%|~|myp_venture_tiles|~|%' THEN 'myp_venture_tiles'  -- Rank 130, Qty: 1,802
            WHEN all_itc_combined LIKE '%|~|mya_vh_buildwebsite_marketing|~|%' THEN 'mya_vh_buildwebsite_marketing'  -- Rank 131, Qty: 1,624
            WHEN all_itc_combined LIKE '%|~|mya_vh_buildwebsite|~|%' THEN 'mya_vh_buildwebsite'  -- Rank 132, Qty: 1,539
            WHEN all_itc_combined LIKE '%|~|android_studio_app_createshelf_sites|~|%' THEN 'android_studio_app_createshelf_sites'  -- Rank 133, Qty: 1,437
            WHEN all_itc_combined LIKE '%|~|hp_usindependents|~|%' THEN 'hp_usindependents'  -- Rank 134, Qty: 1,427
            WHEN all_itc_combined LIKE '%|~|ios_studio_auto_creation|~|%' THEN 'ios_studio_auto_creation'  -- Rank 135, Qty: 1,176
            WHEN all_itc_combined LIKE '%|~|android-gd-mobile|~|%' THEN 'android-gd-mobile'  -- Rank 136, Qty: 1,135
            WHEN all_itc_combined LIKE '%|~|ios_studio_app_your_domains_inline_action|~|%' THEN 'ios_studio_app_your_domains_inline_action'  -- Rank 137, Qty: 1,130
            WHEN all_itc_combined LIKE '%|~|launch_ai_needs_onboarding|~|%' THEN 'launch_ai_needs_onboarding'  -- Rank 138, Qty: 999
            WHEN all_itc_combined LIKE '%|~|studio_ios_shiny_tile_site_creation|~|%' THEN 'studio_ios_shiny_tile_site_creation'  -- Rank 139, Qty: 905
            WHEN all_itc_combined LIKE '%|~|app_commercehome_onlinestore_freemat|~|%' THEN 'app_commercehome_onlinestore_freemat'  -- Rank 140, Qty: 900
            WHEN all_itc_combined LIKE '%|~|android_studio_auto_creation|~|%' THEN 'android_studio_auto_creation'  -- Rank 141, Qty: 861
            WHEN all_itc_combined LIKE '%|~|hp_recore_comingsoon|~|%' THEN 'hp_recore_comingsoon'  -- Rank 142, Qty: 830
            WHEN all_itc_combined LIKE '%|~|android_studio_app_your_domains_inline_action|~|%' THEN 'android_studio_app_your_domains_inline_action'  -- Rank 143, Qty: 820
            WHEN all_itc_combined LIKE '%|~|cart_carousel_inline_modal|~|%' THEN 'cart_carousel_inline_modal'  -- Rank 144, Qty: 705
            WHEN all_itc_combined LIKE '%|~|mobile_ios_studio_app_sitemaker_primer_tmpl|~|%' THEN 'mobile_ios_studio_app_sitemaker_primer_tmpl'  -- Rank 145, Qty: 703
            WHEN all_itc_combined LIKE '%|~|slp_marketplaces|~|%' THEN 'slp_marketplaces'  -- Rank 146, Qty: 578
            WHEN all_itc_combined LIKE '%|~|slp_best_website_builders_comparison|~|%' THEN 'slp_best_website_builders_comparison'  -- Rank 147, Qty: 545
            WHEN all_itc_combined LIKE '%|~|campaign_wpp_airo_existing_domain|~|%' THEN 'campaign_wpp_airo_existing_domain'  -- Rank 148, Qty: 524
            WHEN all_itc_combined LIKE '%|~|dlp_website_builder_paypal|~|%' THEN 'dlp_website_builder_paypal'  -- Rank 149, Qty: 449
            WHEN all_itc_combined LIKE '%|~|ai_onboarding_no_domain_upp|~|%' THEN 'ai_onboarding_no_domain_upp'  -- Rank 150, Qty: 433
            WHEN all_itc_combined LIKE '%|~|slp_recore_wsb_comingsoon|~|%' THEN 'slp_recore_wsb_comingsoon'  -- Rank 151, Qty: 404
            WHEN all_itc_combined LIKE '%|~|ai_onboarding_no_domain|~|%' THEN 'ai_onboarding_no_domain'  -- Rank 152, Qty: 402
            WHEN all_itc_combined LIKE '%|~|hp_recore_wsb|~|%' THEN 'hp_recore_wsb'  -- Rank 153, Qty: 366
            WHEN all_itc_combined LIKE '%|~|mya_vh_buildwebsite_marketing_emm|~|%' THEN 'mya_vh_buildwebsite_marketing_emm'  -- Rank 154, Qty: 332
            WHEN all_itc_combined LIKE '%|~|mya_vh_buildwebsite_previewmodal|~|%' THEN 'mya_vh_buildwebsite_previewmodal'  -- Rank 155, Qty: 325
            WHEN all_itc_combined LIKE '%|~|dlp_website_builder_ols|~|%' THEN 'dlp_website_builder_ols'  -- Rank 156, Qty: 281
            WHEN all_itc_combined LIKE '%|~|upp_d2p_marketing_onboarding|~|%' THEN 'upp_d2p_marketing_onboarding'  -- Rank 157, Qty: 247
            WHEN all_itc_combined LIKE '%|~|dcc_wacampaign|~|%' THEN 'dcc_wacampaign'  -- Rank 158, Qty: 204
            WHEN all_itc_combined LIKE '%|~|mya_vh_buildwebsite_marketing_digital_ads|~|%' THEN 'mya_vh_buildwebsite_marketing_digital_ads'  -- Rank 159, Qty: 203
            WHEN all_itc_combined LIKE '%|~|madmimi_marketing_suite_deluxe|~|%' THEN 'madmimi_marketing_suite_deluxe'  -- Rank 160, Qty: 163
            WHEN all_itc_combined LIKE '%|~|dlp_gocentral|~|%' THEN 'dlp_gocentral'  -- Rank 161, Qty: 160
            WHEN all_itc_combined LIKE '%|~|dlp_online_store_google|~|%' THEN 'dlp_online_store_google'  -- Rank 162, Qty: 155
            WHEN all_itc_combined LIKE '%|~|slp_commerce_sell_anywhere|~|%' THEN 'slp_commerce_sell_anywhere'  -- Rank 163, Qty: 147
            WHEN all_itc_combined LIKE '%|~|slp_recore_marketingsuite|~|%' THEN 'slp_recore_marketingsuite'  -- Rank 164, Qty: 130
            WHEN all_itc_combined LIKE '%|~|dlp_online_store_facebook|~|%' THEN 'dlp_online_store_facebook'  -- Rank 165, Qty: 113
            WHEN all_itc_combined LIKE '%|~|mya_vh_buildwebsite_previewModal|~|%' THEN 'mya_vh_buildwebsite_previewModal'  -- Rank 166, Qty: 108
            WHEN all_itc_combined LIKE '%|~|slp_godaddy_payments|~|%' THEN 'slp_godaddy_payments'  -- Rank 167, Qty: 99
            WHEN all_itc_combined LIKE '%|~|slp_recore_ols|~|%' THEN 'slp_recore_ols'  -- Rank 168, Qty: 98
            WHEN all_itc_combined LIKE '%|~|hp_recore_marketingsuite|~|%' THEN 'hp_recore_marketingsuite'  -- Rank 169, Qty: 66
            WHEN all_itc_combined LIKE '%|~|dlp_website_builder|~|%' THEN 'dlp_website_builder'  -- Rank 170, Qty: 62
            WHEN all_itc_combined LIKE '%|~|slp_best_ecommerce_website_builders_comparison|~|%' THEN 'slp_best_ecommerce_website_builders_comparison'  -- Rank 171, Qty: 51
            WHEN all_itc_combined LIKE '%|~|mya_acctsettings_subscriptions_updowngrade_bundle|~|%' THEN 'mya_acctsettings_subscriptions_updowngrade_bundle'  -- Rank 172, Qty: 50
            WHEN all_itc_combined LIKE '%|~|dcc_emailcampaign|~|%' THEN 'dcc_emailcampaign'  -- Rank 173, Qty: 43
            WHEN all_itc_combined LIKE '%|~|ops_tool_basketcase|~|%' THEN 'ops_tool_basketcase'  -- Rank 174, Qty: 41
            WHEN all_itc_combined LIKE '%|~|mgr_dpp_config1|~|%' THEN 'mgr_dpp_config1'  -- Rank 175, Qty: 36
            WHEN all_itc_combined LIKE '%|~|madmimi_marketing_suite_premier|~|%' THEN 'madmimi_marketing_suite_premier'  -- Rank 176, Qty: 36
            WHEN all_itc_combined LIKE '%|~|mya_vh_buildwebsite_social_rec|~|%' THEN 'mya_vh_buildwebsite_social_rec'  -- Rank 177, Qty: 25
            WHEN all_itc_combined LIKE '%|~|slp_marketing_suite|~|%' THEN 'slp_marketing_suite'  -- Rank 178, Qty: 25
            WHEN all_itc_combined LIKE '%|~|dpp|~|%' THEN 'dpp'  -- Rank 179, Qty: 20
            WHEN all_itc_combined LIKE '%|~|vh_wrp_edit|~|%' THEN 'vh_wrp_edit'  -- Rank 180, Qty: 17
            WHEN all_itc_combined LIKE '%|~|app_v7_start_free_vnext_banner|~|%' THEN 'app_v7_start_free_vnext_banner'  -- Rank 181, Qty: 17
            WHEN all_itc_combined LIKE '%|~|app_commercechannels_onlinestore_freemat|~|%' THEN 'app_commercechannels_onlinestore_freemat'  -- Rank 182, Qty: 14
            WHEN all_itc_combined LIKE '%|~|slp_redeem|~|%' THEN 'slp_redeem'  -- Rank 183, Qty: 14
            WHEN all_itc_combined LIKE '%|~|slp_wsb_ft_getstarted_plans_nocc&listingid=wsb-vne|~|%' THEN 'slp_wsb_ft_getstarted_plans_nocc&listingid=wsb-vne'  -- Rank 184, Qty: 10
            WHEN all_itc_combined LIKE '%|~|vh_wrp_edit_|~|%' THEN 'vh_wrp_edit_'  -- Rank 185, Qty: 8
            WHEN all_itc_combined LIKE '%|~|parkedpage_gocentral|~|%' THEN 'parkedpage_gocentral'  -- Rank 186, Qty: 7
            WHEN all_itc_combined LIKE '%|~|conversational_onboard_with_airo|~|%' THEN 'conversational_onboard_with_airo'  -- Rank 187, Qty: 7
            WHEN all_itc_combined LIKE '%|~|studio-lib|~|%' THEN 'studio-lib'  -- Rank 188, Qty: 7
            WHEN all_itc_combined LIKE '%|~|hp_recore_ols|~|%' THEN 'hp_recore_ols'  -- Rank 189, Qty: 7
            WHEN all_itc_combined LIKE '%|~|app_vnext_in_app_subscription_upgrade|~|%' THEN 'app_vnext_in_app_subscription_upgrade'  -- Rank 190, Qty: 6
            WHEN all_itc_combined LIKE '%|~|vh_edit|~|%' THEN 'vh_edit'  -- Rank 191, Qty: 6
            WHEN all_itc_combined LIKE '%|~|ai-onboarding-mwp|~|%' THEN 'ai-onboarding-mwp'  -- Rank 192, Qty: 6
            WHEN all_itc_combined LIKE '%|~|slp_gocentral_themes_verticals|~|%' THEN 'slp_gocentral_themes_verticals'  -- Rank 193, Qty: 5
            WHEN all_itc_combined LIKE '%|~|slp_wsb_ft_getstarted_pla|~|%' THEN 'slp_wsb_ft_getstarted_pla'  -- Rank 194, Qty: 5
            WHEN all_itc_combined LIKE '%|~|dpp_wsb_receipt|~|%' THEN 'dpp_wsb_receipt'  -- Rank 195, Qty: 5
            WHEN all_itc_combined LIKE '%|~|website_reset|~|%' THEN 'website_reset'  -- Rank 196, Qty: 4
            WHEN all_itc_combined LIKE '%|~|basic|~|%' THEN 'basic'  -- Rank 197, Qty: 3
            WHEN all_itc_combined LIKE '%|~|dpp_absol1.smartdefault_organicspin|~|%' THEN 'dpp_absol1.smartdefault_organicspin'  -- Rank 198, Qty: 3
            WHEN all_itc_combined LIKE '%|~|dpp_wam_optout|~|%' THEN 'dpp_wam_optout'  -- Rank 199, Qty: 3
            WHEN all_itc_combined LIKE '%|~|vh_venture_tiles|~|%' THEN 'vh_venture_tiles'  -- Rank 200, Qty: 3
            WHEN all_itc_combined LIKE '%|~|bizbox|~|%' THEN 'bizbox'  -- Rank 201, Qty: 3
            WHEN all_itc_combined LIKE '%|~|myproducts-venture-tiles-start-new&listingid=wsb-v|~|%' THEN 'myproducts-venture-tiles-start-new&listingid=wsb-v'  -- Rank 202, Qty: 3
            WHEN all_itc_combined LIKE '%|~|test_editor|~|%' THEN 'test_editor'  -- Rank 203, Qty: 3
            WHEN all_itc_combined LIKE '%|~|vh_edit_comingsoo|~|%' THEN 'vh_edit_comingsoo'  -- Rank 204, Qty: 2
            WHEN all_itc_combined LIKE '%|~|dpp_absol1.primary_alternate|~|%' THEN 'dpp_absol1.primary_alternate'  -- Rank 205, Qty: 2
            WHEN all_itc_combined LIKE '%|~|mya_acctsettings_products_updowngrade_bundle|~|%' THEN 'mya_acctsettings_products_updowngrade_bundle'  -- Rank 206, Qty: 2
            WHEN all_itc_combined LIKE '%|~|vh_edit_|~|%' THEN 'vh_edit_'  -- Rank 207, Qty: 2
            WHEN all_itc_combined LIKE '%|~|vh|~|%' THEN 'vh'  -- Rank 208, Qty: 2
            WHEN all_itc_combined LIKE '%|~|dlp_email_professional|~|%' THEN 'dlp_email_professional'  -- Rank 209, Qty: 2
            WHEN all_itc_combined LIKE '%|~|slp_wsb_ft_getstarted_plans|~|%' THEN 'slp_wsb_ft_getstarted_plans'  -- Rank 210, Qty: 2
            WHEN all_itc_combined LIKE '%|~|slp_wsb_ft_getstarted_plans_nocc%26listingid%3dwsb|~|%' THEN 'slp_wsb_ft_getstarted_plans_nocc%26listingid%3dwsb'  -- Rank 211, Qty: 2
            WHEN all_itc_combined LIKE '%|~|yh_wrp_edit_comingsoom|~|%' THEN 'yh_wrp_edit_comingsoom'  -- Rank 212, Qty: 2
            WHEN all_itc_combined LIKE '%|~|studio_ios_coming_soon_site|~|%' THEN 'studio_ios_coming_soon_site'  -- Rank 213, Qty: 2
            WHEN all_itc_combined LIKE '%|~|launchaineeds_onboarding|~|%' THEN 'launchaineeds_onboarding'  -- Rank 214, Qty: 2
            WHEN all_itc_combined LIKE '%|~|dcc_wam_dppoffer|~|%' THEN 'dcc_wam_dppoffer'  -- Rank 215, Qty: 2
            WHEN all_itc_combined LIKE '%|~|dpp_absol1.ai_search|~|%' THEN 'dpp_absol1.ai_search'  -- Rank 216, Qty: 2
            WHEN all_itc_combined LIKE '%|~|slp_wsb_ft_getstarted_plans_|~|%' THEN 'slp_wsb_ft_getstarted_plans_'  -- Rank 217, Qty: 1
            WHEN all_itc_combined LIKE '%|~|mya_myproducts_-manage_websiteandmarketing|~|%' THEN 'mya_myproducts_-manage_websiteandmarketing'  -- Rank 218, Qty: 1
            WHEN all_itc_combined LIKE '%|~|vh_delete_comingsoon|~|%' THEN 'vh_delete_comingsoon'  -- Rank 219, Qty: 1
            WHEN all_itc_combined LIKE '%|~|vh_wrp_edit_categories|~|%' THEN 'vh_wrp_edit_categories'  -- Rank 220, Qty: 1
            WHEN all_itc_combined LIKE '%|~|vh_wrp_edit_comin…|~|%' THEN 'vh_wrp_edit_comin…'  -- Rank 221, Qty: 1
            WHEN all_itc_combined LIKE '%|~|vh_wrp_edit_accountid=14a5b586-cf88-11f0-95e4-008c|~|%' THEN 'vh_wrp_edit_accountid=14a5b586-cf88-11f0-95e4-008c'  -- Rank 222, Qty: 1
            WHEN all_itc_combined LIKE '%|~|vh_edit_comingsoon4|~|%' THEN 'vh_edit_comingsoon4'  -- Rank 223, Qty: 1
            WHEN all_itc_combined LIKE '%|~|app_vnext_free_trial_banner_email_renewal|~|%' THEN 'app_vnext_free_trial_banner_email_renewal'  -- Rank 224, Qty: 1
            WHEN all_itc_combined LIKE '%|~|domain_manager_website_cart|~|%' THEN 'domain_manager_website_cart'  -- Rank 225, Qty: 1
            WHEN all_itc_combined LIKE '%|~|hp_w|~|%' THEN 'hp_w'  -- Rank 226, Qty: 1
            WHEN all_itc_combined LIKE '%|~|vhwrpeditcomingsoon|~|%' THEN 'vhwrpeditcomingsoon'  -- Rank 227, Qty: 1
            WHEN all_itc_combined LIKE '%|~|anon-placeholder|~|%' THEN 'anon-placeholder'  -- Rank 228, Qty: 1
            WHEN all_itc_combined LIKE '%|~|edite=wsb|~|%' THEN 'edite=wsb'  -- Rank 229, Qty: 1
            WHEN all_itc_combined LIKE '%|~|vh_bundling_aigodaddy|~|%' THEN 'vh_bundling_aigodaddy'  -- Rank 230, Qty: 1
            WHEN all_itc_combined LIKE '%|~|vh_edit_parked|~|%' THEN 'vh_edit_parked'  -- Rank 231, Qty: 1
            WHEN all_itc_combined LIKE '%|~|vh_edit_comingsoongodaddy|~|%' THEN 'vh_edit_comingsoongodaddy'  -- Rank 232, Qty: 1
            WHEN all_itc_combined LIKE '%|~|vh_bundling_godaddy.comai|~|%' THEN 'vh_bundling_godaddy.comai'  -- Rank 233, Qty: 1
            WHEN all_itc_combined LIKE '%|~|dlp_wsb_verticals_real_estate_ft_nocc|~|%' THEN 'dlp_wsb_verticals_real_estate_ft_nocc'  -- Rank 234, Qty: 1
            WHEN all_itc_combined LIKE '%|~|mya_myproducts_wam_accordion|~|%' THEN 'mya_myproducts_wam_accordion'  -- Rank 235, Qty: 1
            WHEN all_itc_combined LIKE '%|~|slp_dm_3mft_email|~|%' THEN 'slp_dm_3mft_email'  -- Rank 236, Qty: 1
            WHEN all_itc_combined LIKE '%|~|recore_wsb&amp;listingid=wsb-vnext-intl-freemat-1|~|%' THEN 'recore_wsb&amp;listingid=wsb-vnext-intl-freemat-1'  -- Rank 237, Qty: 1
            WHEN all_itc_combined LIKE '%|~|slp_365_category_config|~|%' THEN 'slp_365_category_config'  -- Rank 238, Qty: 1
            WHEN all_itc_combined LIKE '%|~|vh_wrp_edit_accountid=47b4a249-8715-11f0-95c9-008c|~|%' THEN 'vh_wrp_edit_accountid=47b4a249-8715-11f0-95c9-008c'  -- Rank 239, Qty: 1
            WHEN all_itc_combined LIKE '%|~|marketing_onboarding|~|%' THEN 'marketing_onboarding'  -- Rank 240, Qty: 1
            WHEN all_itc_combined LIKE '%|~|dlp_gocentral_website_builder|~|%' THEN 'dlp_gocentral_website_builder'  -- Rank 241, Qty: 1
            WHEN all_itc_combined LIKE '%|~|dlp_onlinestore_ft_nocc|~|%' THEN 'dlp_onlinestore_ft_nocc'  -- Rank 242, Qty: 1
            WHEN all_itc_combined LIKE '%|~|vh_wrp_edit=9aba261b-7418-11f0-8699-7cd30acd3d0c|~|%' THEN 'vh_wrp_edit=9aba261b-7418-11f0-8699-7cd30acd3d0c'  -- Rank 243, Qty: 1
            WHEN all_itc_combined LIKE '%|~|edit|~|%' THEN 'edit'  -- Rank 244, Qty: 1
            WHEN all_itc_combined LIKE '%|~|dlp_usoybo|~|%' THEN 'dlp_usoybo'  -- Rank 245, Qty: 1
            WHEN all_itc_combined LIKE '%|~|launch_ai_needs_onboarding.|~|%' THEN 'launch_ai_needs_onboarding.'  -- Rank 246, Qty: 1
            WHEN all_itc_combined LIKE '%|~|dpp_absol1.favorites|~|%' THEN 'dpp_absol1.favorites'  -- Rank 247, Qty: 1
            WHEN all_itc_combined LIKE '%|~|vh_unpublish_comingsoon|~|%' THEN 'vh_unpublish_comingsoon'  -- Rank 248, Qty: 1
            WHEN all_itc_combined LIKE '%|~|slpwsbftgetstartedplans_nocc|~|%' THEN 'slpwsbftgetstartedplans_nocc'  -- Rank 249, Qty: 1
            WHEN all_itc_combined LIKE '%|~|vh_bundling_a|~|%' THEN 'vh_bundling_a'  -- Rank 250, Qty: 1
            WHEN all_itc_combined LIKE '%|~|vh_wrp|~|%' THEN 'vh_wrp'  -- Rank 251, Qty: 1
            WHEN all_itc_combined LIKE '%|~|vh_wrp_edit_=a7fb6c9e-978e-11f0-95ca-7cd30aca43de|~|%' THEN 'vh_wrp_edit_=a7fb6c9e-978e-11f0-95ca-7cd30aca43de'  -- Rank 252, Qty: 1
            WHEN all_itc_combined LIKE '%|~|email_channel|~|%' THEN 'email_channel'  -- Rank 253, Qty: 1
            ELSE NULL
        END AS top_ranked_tracking_code,
        payment_attempt_search,
        begin_checkout_search,
        add_to_cart_search,
        click_search,
        impression_search
    FROM base_data
),

-- Assign final tracking code (no fallback - either ranked or Not attributed)
final_attribution AS (
    SELECT
        session_id,
        website_activity_mst_date,
        channel_grouping_name,
        device_category_name,
        web_region_2_name,
        existing_customer_flag,
        session_cnt,
        page_advance_session_cnt,
        gcr_session_cnt,
        first_hit_content_group_2_name,
        web_focal_country_name,
        page_path_list,
        web_business_unit_name,
        -- Product columns
        order_item_tracking_code,
        product_term,
        plan_type,
        free_or_paid,
        wam_sessions,
        total_wam_units,
        WAM_gcr,
        -- If no ranked code found, return 'Not attributed'
        COALESCE(top_ranked_tracking_code, 'Not attributed') AS final_tracking_code,
        -- Determine source field (NULL if not attributed)
        CASE
            WHEN order_item_tracking_code IS NOT NULL THEN 'order_itc'
            WHEN top_ranked_tracking_code IS NOT NULL AND payment_attempt_search LIKE '%' || top_ranked_tracking_code || '%' THEN 'payment_attempt'
            WHEN top_ranked_tracking_code IS NOT NULL AND begin_checkout_search LIKE '%' || top_ranked_tracking_code || '%' THEN 'begin_checkout'
            WHEN top_ranked_tracking_code IS NOT NULL AND add_to_cart_search LIKE '%' || top_ranked_tracking_code || '%' THEN 'add_to_cart'
            WHEN top_ranked_tracking_code IS NOT NULL AND click_search LIKE '%' || top_ranked_tracking_code || '%' THEN 'click'
            WHEN top_ranked_tracking_code IS NOT NULL AND impression_search LIKE '%' || top_ranked_tracking_code || '%' THEN 'impression'
            ELSE NULL
        END AS source_field
    FROM top_ranked_extract
),
-- Final aggregation with joined traffic + product data
final_output AS (
    SELECT
        website_activity_mst_date AS website_date,
        COALESCE(channel_grouping_name,'Unknown') AS channel_grouping_name,
        COALESCE(device_category_name,'Unknown')  AS device_category_name,
        COALESCE(web_region_2_name,'Unknown')     AS web_region_2_name,
        COALESCE(existing_customer_flag,'Unknown') AS existing_customer_flag,
        COALESCE(first_hit_content_group_2_name,'Unknown') AS first_hit_content_group_2_name,
        COALESCE(final_tracking_code,'Unknown') AS top_ranked_tracking_code,
        COALESCE(source_field,'Unknown') AS source_field,
        COALESCE(web_focal_country_name,'Unknown') AS web_focal_country_name,
        COALESCE(page_path_list,'Unknown') AS page_path_list,
        COALESCE(web_business_unit_name,'Unknown') AS web_business_unit_name,
        -- Product dimensions (from joined data)
        COALESCE(order_item_tracking_code,'N/A') AS order_item_tracking_code,
        COALESCE(product_term,'N/A') AS product_term,
        COALESCE(plan_type,'N/A') AS plan_type,
        COALESCE(free_or_paid,'N/A') AS free_or_paid,
        -- Traffic metrics
        COUNT(DISTINCT session_id) AS sessions,
        SUM(session_cnt) AS session_cnt,
        SUM(page_advance_session_cnt) AS pa_sessions,
        SUM(gcr_session_cnt) AS gcr_sessions,
        -- Product metrics (from joined data)
        SUM(wam_sessions) AS wam_sessions,
        SUM(total_wam_units) AS total_wam_units,
        SUM(WAM_gcr) AS GCR
    FROM final_attribution
    GROUP BY
        website_activity_mst_date,
        channel_grouping_name,
        device_category_name,
        web_region_2_name,
        existing_customer_flag,
        first_hit_content_group_2_name,
        web_focal_country_name,
        page_path_list,
        web_business_unit_name,
        final_tracking_code,
        source_field,
        order_item_tracking_code,
        product_term,
        plan_type,
        free_or_paid
)

SELECT a.*, itc_grouping FROM final_output a left join (select itemtrackingcode as item_tracking_code, itcgrouping as itc_grouping from dev.ba_corporate.wam_itc_site group by 1,2) b on 
a.top_ranked_tracking_code = b.item_tracking_code
ORDER BY sessions DESC)


-- select * from dna_sandbox.wam_site_marketing_v1 where top_ranked_tracking_code <> 'Not attributed'-- Last updated: 2026-03-09
