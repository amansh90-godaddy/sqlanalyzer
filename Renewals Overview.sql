
UPDATE dev.ba_ecommerce.renewal_job_alerts
SET data_Expected_date = (CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', GETDATE())::date - 1)
WHERE job_name in (  'Renewals Fixed Mix Adjusted',  'Renewals Overview Agg');







drop table if exists dim_prod;
create temp table dim_prod as 

SELECT * FROM ckp_analytic_share.finance360.dim_product_vw
;





--Getting  2 plust customer activity from history tables



Drop table if exists two_plus_cust;
create temp table two_plus_cust as 
Select distinct shopper_id, snap_start_mst_date, snap_end_mst_date, two_plus_customer_flag, product_category_count  

from dev.dna_approved.two_plus_active_customer_history
where snap_end_mst_date between cast('2023-01-01' as date) and    cast( current_date  as date)
and exclude_reason_desc IS NULL
and source_type_enum='external';





Drop table if exists cust360;
create temp table CUST360 as
SELECT distinct shopper_id FROM dev.ba_dri.goog_migrations_final
WHERE google_migrated_subscription_flag='Google Subscription';



Drop table if exists gsub360;
create temp table gsub360 as
SELECT distinct resource_id, product_family_name, bill_id ,bill_line_num  FROM dev.ba_dri.goog_migrations_final
WHERE google_migrated_subscription_flag='Google Subscription';


----Non Finance Cash & Cohort basis 

  
--getting base data based on expiration date for cohort basis and derive/join any product specific fields



---Cohort expirations 

drop table if exists expirations_cohort_base_data;
create temp table expirations_cohort_base_data 
distkey(prior_bill_paid_through_mst_date)
sortkey(prior_bill_paid_through_mst_date)
as
select 

	r.prior_bill_paid_through_mst_date
,   'cohort basis' as analysis_type
,	r.prior_bill_pnl_international_independent_flag
,	r.prior_bill_pnl_us_independent_flag
,	r.prior_bill_pnl_investor_flag
,	r.prior_bill_pnl_partner_flag
,   case when r.expected_pnl_international_independent_flag = true then 'International Independent'
        when r.expected_pnl_us_independent_flag = true then 'US Independents'
        when r.expected_pnl_investor_flag = true then 'Investors'
        when r.expected_pnl_partner_flag = true then 'Partners'
        when r.expected_pnl_commerce_flag=true then 'Commerce'
        
       else 'Not Evaluated'
        end as pillar_name
, case when  (expected_customer_type_name = 'US Independent' or expected_customer_type_name = 'Partner' ) and r.prior_bill_region_2_name is null  then  	'United States' 
 when prior_bill_region_2_name is null then 'Rest of World (RoW)'
 else prior_bill_region_2_name end as prior_bill_region_2_name
,	case when expected_customer_type_name is null  and prior_bill_region_2_name='United States' then 'US Independent' 
     when   expected_customer_type_name is null  and prior_bill_region_2_name<>'United States' then 'International Independent' 
     else expected_customer_type_name end as prior_bill_customer_type_name
,	r.historical_auto_renewal_flag
,	case when prior_bill_sequence_number=1 then true else false end as first_expiry_sequence_flag
,	r.prior_bill_product_pnl_group_name
,	r.prior_bill_product_pnl_category_name 
,	r.prior_bill_product_pnl_line_name 
,	r.prior_bill_product_pnl_version_name 
,	r.prior_bill_product_pnl_subline_name
,r.prior_bill_point_of_purchase_name
,r.prior_bill_country_name
,g.report_region_3_name as prior_bill_region_3_name
, CASE
     WHEN r.prior_bill_product_period_name = 'year' THEN 'Year'
     ELSE 'Month' END AS prior_bill_product_period_name
, CASE
     WHEN r.prior_bill_product_period_name = '6-month' THEN 6
     WHEN r.prior_bill_product_period_name = 'quarter' THEN 3
     ELSE r.prior_bill_product_period_qty END AS prior_bill_product_period_qty
,	r.prior_bill_trxn_currency_code
,	r.prior_free_receipt_price_flag
,	r.prior_bill_domain_bulk_pricing_flag
,	r.renewal_timing_desc
,	r.prior_bill_reseller_type_name
,	r.prior_bill_refund_flag as refund_flag
,	r.product_family_name
,	r.prior_bill_product_pnl_new_renewal_name
,	r.prior_payable_bill_line_flag
,	case when r.prior_bill_gcr_usd_amt >0 then True else False end prior_bill_gcr_usd_amt_flag
, r.prior_bill_primary_product_flag
-- finance product hierarchy 
,	fdp.fin_pnl_group_name as fin_pnl_group
,	fdp.fin_pnl_category_name as fin_pnl_category
,	fdp.fin_pnl_line_name as fin_pnl_line
,	fdp.fin_pnl_subline_name as fin_pnl_subline
, fdp.pnl_forecast_group_name
, fdp.fin_investor_relation_class_name

,fdp.fin_investor_relation_subclass_name

,fdp.fin_investor_relation_segment_name
-- domain renewal rate dimensions 
,date_trunc('month',renewal_bill_modified_mst_date) as renewal_month
--customer 2 plus flags
,cust.two_plus_customer_flag as two_plus_hist_Flag
, cust_2.two_plus_customer_flag as two_plus_current_flag
, case when c.shopper_id is not null then 'Google Migrated Shopper' else 'Other' end as Google_migrated_shopper_flag
,case when s.resource_id is not null then 'Google Migrated Subscription' else 'Other' end as Google_migrated_subscription_flag
,case when cust.product_category_count >=4 then '4 + Products' when cust.product_category_count=3 then '3 Products' when cust.product_category_count=2 then '2 Products' else 'Not 2+ Product' end as customer_paid_product_category
,	sum(r.expiry_qty) as expiry_qty
,	sum( case when r.renewal_bill_product_pnl_new_renewal_name='Renewal' then renewal_qty else 0 end ) as renewal_qty
,	sum( case when r.renewal_bill_product_pnl_new_renewal_name='Renewal' and on_time_renewal_flag=True then renewal_qty else 0 end ) as Ontime_renewal_qty
,	sum(case when r.renewal_bill_product_pnl_new_renewal_name='Renewal' then renewal_bill_gcr_usd_amt else 0 end) as renewal_bill_gcr_usd_amt
,	sum(case when r.renewal_bill_product_pnl_new_renewal_name='Renewal' then renewal_bill_product_month_qty else 0  end) as renewal_bill_product_month_qty
,sum(case when r.renewal_bill_product_pnl_new_renewal_name='Renewal' then renewal_bill_receipt_price_usd_amt else 0 end) as renewal_bill_receipt_price_usd_amt
,sum(case when r.renewal_bill_product_pnl_new_renewal_name='Renewal' and on_time_renewal_flag=true then renewal_bill_receipt_price_usd_amt else 0 end) as ontime_renewal_bill_receipt_price_usd_amt
,sum(expected_receipt_price_usd_amt) as potential_Receipt_price_usd_amt
,sum(case when r.renewal_bill_product_pnl_new_renewal_name='Renewal'  then renewal_bill_list_price_usd_amt else 0 end) as renewal_bill_list_price_usd_amt
,sum(case when r.renewal_bill_product_pnl_new_renewal_name='Renewal'  then renewal_bill_gcr_constant_currency_amt else 0 end) as renewal_bill_gcr_constant_currency_amt
from dev.dna_approved.renewal_360 r 

LEFT JOIN dim_prod fdp
  ON r.prior_bill_pf_id = fdp.pf_id	
  
  left join dev.dna_approved.dim_geography g
  on r.prior_bill_country_code= g.country_code
  
  left join two_plus_cust cust
  
  on r.prior_bill_paid_through_mst_date= cust.snap_end_mst_date 
  and r.prior_bill_shopper_id= cust.shopper_id 

  

  
    left join  dna_approved.two_plus_active_customer   cust_2
  
 --- on cast(current_Date -1 as date) = cust_2.snap_end_mst_date 
 on  r.prior_bill_shopper_id= cust_2.shopper_id 
 and source_type_enum = 'external'
 
 left join cust360 c 
 on prior_bill_shopper_id=c.shopper_id
 and prior_bill_shopper_id <>10839228
 
 
  
 left join gsub360 s
 on r.resource_id = s.resource_id
 and r.product_family_name=s.product_family_name
 and r.prior_bill_id= s.bill_id
 and r.prior_bill_line_num=s.bill_line_num

  
  

where 
	1=1
	and r.prior_bill_paid_through_mst_date between cast('2023-01-01' as date) and    cast( current_date  as date)
--	and r.product_pnl_category_name ='Domain Registration'
	and r.bill_exclude_reason_desc is null 

group by 
	1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46
;
--analyze expirations_base_data;






---cash expirations 



drop table if exists expirations_cash_base_data;
create temp table expirations_cash_base_data 
distkey(prior_bill_billing_due_mst_date)
sortkey(prior_bill_billing_due_mst_date)
as
select 
	r.prior_bill_billing_due_mst_date
,   'cash basis' as analysis_type
,	r.prior_bill_pnl_international_independent_flag
,	r.prior_bill_pnl_us_independent_flag
,	r.prior_bill_pnl_investor_flag
,	r.prior_bill_pnl_partner_flag
,   case when r.expected_pnl_international_independent_flag = true then 'International Independent'
        when r.expected_pnl_us_independent_flag = true then 'US Independents'
        when r.expected_pnl_investor_flag = true then 'Investors'
        when r.expected_pnl_partner_flag = true then 'Partners'
        when r.expected_pnl_commerce_flag=true then 'Commerce'
        
       else 'Not Evaluated'
        end as pillar_name
 --case when  (expected_customer_type_name = 'US Independent' or expected_customer_type_name = 'Partner' ) and prior_bill_region_2_name is null  then  	'United --States' 
 --when prior_bill_region_2_name is null then 'Rest of World (RoW)'
 --else prior_bill_region_2_name end as 
 , case when  (expected_customer_type_name = 'US Independent' or expected_customer_type_name = 'Partner' ) and r.prior_bill_region_2_name is null  then  	'United States' 
 when prior_bill_region_2_name is null then 'Rest of World (RoW)'
 else prior_bill_region_2_name end as prior_bill_region_2_name
,	case when expected_customer_type_name is null  and prior_bill_region_2_name='United States' then 'US Independent' 
     when   expected_customer_type_name is null  and prior_bill_region_2_name<>'United States' then 'International Independent' 
     else expected_customer_type_name end as prior_bill_customer_type_name
,	r.historical_auto_renewal_flag
,	case when prior_bill_sequence_number=1 then true else false end as first_expiry_sequence_flag
,	r.prior_bill_product_pnl_group_name
,	r.prior_bill_product_pnl_category_name 
,	r.prior_bill_product_pnl_line_name 
,	r.prior_bill_product_pnl_version_name 
,	r.prior_bill_product_pnl_subline_name
,r.prior_bill_point_of_purchase_name
,r.prior_bill_country_name
,g.report_region_3_name as prior_bill_region_3_name
, CASE
     WHEN r.prior_bill_product_period_name = 'year' THEN 'Year'
     ELSE 'Month' END AS prior_bill_product_period_name
, CASE
     WHEN r.prior_bill_product_period_name = '6-month' THEN 6
     WHEN r.prior_bill_product_period_name = 'quarter' THEN 3
     ELSE r.prior_bill_product_period_qty END AS prior_bill_product_period_qty
,	r.prior_bill_trxn_currency_code
,	r.prior_free_receipt_price_flag
,	r.prior_bill_domain_bulk_pricing_flag
,	r.renewal_timing_monthly_desc as renewal_timing_desc
,	r.prior_bill_reseller_type_name
,	r.prior_bill_refund_flag as refund_flag
,	r.product_family_name
,	r.prior_bill_product_pnl_new_renewal_name
,	r.prior_payable_bill_line_flag
,	case when r.prior_bill_gcr_usd_amt >0 then True else False end prior_bill_gcr_usd_amt_flag
, r.prior_bill_primary_product_flag
-- finance product hierarchy 
,	fdp.fin_pnl_group_name as fin_pnl_group
,	fdp.fin_pnl_category_name as fin_pnl_category
,	fdp.fin_pnl_line_name as fin_pnl_line
,	fdp.fin_pnl_subline_name as fin_pnl_subline
, fdp.fin_investor_relation_class_name

,fdp.fin_investor_relation_subclass_name

,fdp.fin_investor_relation_segment_name
, fdp.pnl_forecast_group_name
,date_trunc('month',renewal_bill_modified_mst_date) as renewal_month
--customer 2 plus flags
,cust.two_plus_customer_flag as two_plus_hist_Flag
, cust_2.two_plus_customer_flag as two_plus_current_flag

, case when c.shopper_id is not null then 'Google Migrated Shopper' else 'Other' end as Google_migrated_shopper_flag
 ,case when s.resource_id is not null then 'Google Migrated Subscription' else 'Other' end as Google_migrated_subscription_flag
 ,case when cust.product_category_count >=4 then '4 + Products' when cust.product_category_count=3 then '3 Products' when cust.product_category_count=2 then '2 Products' else 'Not 2+ Product' end as customer_paid_product_category
,	sum(case when prior_entitlement_bill_type='free trial conversion to paid'  then r.original_expiry_qty
           when prior_bill_product_pnl_line_name = 'Websites and Marketing' and  prior_payable_bill_line_flag<>true then 0 else r.expiry_qty end) as expiry_qty
--,	sum( case when r.renewal_bill_product_pnl_new_renewal_name='Renewal' then renewal_qty else 0 end ) as renewal_qty
--,	sum(case when r.renewal_bill_product_pnl_new_renewal_name='Renewal' then renewal_bill_gcr_usd_amt else 0 end) as renewal_bill_gcr_usd_amt
--,	sum(case when r.renewal_bill_product_pnl_new_renewal_name='Renewal' then renewal_bill_receipt_price_usd_amt else 0 end) as renewal_bill_receipt_price_usd_amt
--,	sum(case when r.renewal_bill_product_pnl_new_renewal_name='Renewal' then renewal_bill_product_month_qty else 0  end) as renewal_bill_product_month_qty



from dev.dna_approved.renewal_360 r 

LEFT JOIN dim_prod fdp
  ON r.prior_bill_pf_id = fdp.pf_id	
  
    left join dev.dna_approved.dim_geography g
  on r.prior_bill_country_code= g.country_code
  
   left join two_plus_cust cust
  
  on r.prior_bill_billing_due_mst_date= cust.snap_end_mst_date 
  and r.prior_bill_shopper_id= cust.shopper_id 

  

  
    left join  dna_approved.two_plus_active_customer   cust_2
  
 --- on cast(current_Date -1 as date) = cust_2.snap_end_mst_date 
 on  r.prior_bill_shopper_id= cust_2.shopper_id 
 and source_type_enum = 'external'
  
 left join cust360 c 
 on prior_bill_shopper_id=c.shopper_id
 and   prior_bill_shopper_id <>10839228
 
  
 left join gsub360 s
 on r.resource_id = s.resource_id
 and r.product_family_name=s.product_family_name
 and r.prior_bill_id= s.bill_id
 and r.prior_bill_line_num=s.bill_line_num

  
  
where 
	1=1
	and r.prior_bill_billing_due_mst_date between cast('2023-01-01' as date) and    cast( current_date  as date)
--	and r.product_pnl_category_name ='Domain Registration'
	and r.bill_exclude_reason_desc is null 
--	and r.prior_bill_point_of_purchase_name<>'Third Party App Stores'
--	and (prior_bill_product_pnl_category_name='Domain Registration' or prior_bill_primary_product_flag=True)

group by 
	1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46
;

  













----getting data based on renewal date for cash basis along with any product specific fields/dimensions

drop table if exists renewals_base_data;
create temp table renewals_base_data 
distkey(renewal_bill_modified_mst_date)
sortkey(renewal_bill_modified_mst_date)
as
select 
	r.renewal_bill_modified_mst_Date
,	'renewals basis' as analysis_type
,	r.renewal_bill_pnl_international_independent_flag
,	r.renewal_bill_pnl_us_independent_flag
,	r.renewal_bill_pnl_investor_flag
,	r.renewal_bill_pnl_partner_flag
,  case when r.renewal_bill_pnl_international_independent_flag = true then 'International Independent'
        when r.renewal_bill_pnl_us_independent_flag = true then 'US Independents'
        when r.renewal_bill_pnl_investor_flag = true then 'Investors'
        when r.renewal_bill_pnl_partner_flag = true then 'Partners'
        when r.renewal_bill_pnl_commerce_flag=true then 'Commerce'
        else 'Not Evaluated'
        end as pillar_name
,case when  (renewal_bill_customer_type_name = 'US Independent' or renewal_bill_customer_type_name = 'Partner' ) and r.renewal_bill_report_region_2_name is null  then  	'United States' 
 when r.renewal_bill_report_region_2_name is null then 'Rest of World (RoW)'
 else r.renewal_bill_report_region_2_name end as renewal_bill_report_region_2_name

,	case when (renewal_bill_customer_type_name is null or renewal_bill_customer_type_name='Not Evaluated')   and renewal_bill_report_region_2_name='United States' then 'US Independent' 
     when   (renewal_bill_customer_type_name is null or renewal_bill_customer_type_name='Not Evaluated')  and renewal_bill_report_region_2_name<>'United States' then 'International Independent'
     else r.renewal_bill_customer_type_name end as renewal_bill_customer_type_name
,	r.historical_auto_renewal_flag
,	case when prior_bill_sequence_number=1 then true else false end as first_expiry_sequence_flag
,	r.renewal_bill_product_pnl_group_name
,	r.renewal_bill_product_pnl_category_name 
,	r.renewal_bill_product_pnl_line_name 
,	r.renewal_bill_product_pnl_version_name 
,	r.renewal_bill_product_pnl_subline_name
,r.renewal_bill_point_of_purchase_name
,r.renewal_bill_country_name
,g.report_region_3_name as renewal_bill_region_3_name
, case
     when renewal_bill_product_period_name = 'year' then 'Year'
     else 'Month' 
     end AS renewal_bill_product_period_name
, case
     when r.renewal_bill_product_period_name = '6-month' then 6
     when r.renewal_bill_product_period_name = 'quarter' then 3
     else r.renewal_bill_product_period_qty 
     end AS renewal_bill_product_period_qty
,	r.renewal_bill_trxn_currency_code
,	r.prior_free_receipt_price_flag -- same as expiration code 
,	r.renewal_bill_domain_bulk_pricing_flag
,	r.renewal_timing_monthly_desc as renewal_timing_desc
,	r.renewal_bill_reseller_type_name
,	r.renewal_bill_refund_flag as refund_flag
,	r.product_family_name
,	r.renewal_bill_product_pnl_new_renewal_name
,	r.renewal_payable_bill_line_flag
,	case when r.renewal_bill_gcr_usd_amt>0 then  True 
     else False 
     end as renewal_bill_gcr_usd_amt_flag
     , r.renewal_bill_primary_product_flag
-- finance product hierarchy 
,	fdp.fin_pnl_group_name as fin_pnl_group
,	fdp.fin_pnl_category_name as fin_pnl_category
,	fdp.fin_pnl_line_name as fin_pnl_line
,	fdp.fin_pnl_subline_name as fin_pnl_subline
, fdp.pnl_forecast_group_name
, fdp.fin_investor_relation_class_name

,fdp.fin_investor_relation_subclass_name

,fdp.fin_investor_relation_segment_name
-- domain renewal rate dimensions 
,date_trunc('month',renewal_bill_modified_mst_date) as renewal_month
,cust.two_plus_customer_flag as two_plus_hist_Flag
, cust_2.two_plus_customer_flag as two_plus_current_flag
, case when c.shopper_id is not null then 'Google Migrated Shopper' else 'Other' end as Google_migrated_shopper_flag
,case when s.resource_id is not null then 'Google Migrated Subscription' else 'Other' end as Google_migrated_subscription_flag
,case when cust.product_category_count >=4 then '4 + Products' when cust.product_category_count=3 then '3 Products' when cust.product_category_count=2 then '2 Products' else 'Not 2+ Product' end as customer_paid_product_category
,	sum( case when renewal_bill_product_pnl_new_renewal_name='Renewal' then renewal_qty else 0 end ) as renewal_qty
,	sum(case when renewal_bill_product_pnl_new_renewal_name='Renewal' then renewal_bill_gcr_usd_amt else 0 end) as renewal_bill_gcr_usd_amt
,	sum(case when renewal_bill_product_pnl_new_renewal_name='Renewal' then renewal_bill_receipt_price_usd_amt else 0 end) as renewal_bill_receipt_price_usd_amt
,	sum(case when renewal_bill_product_pnl_new_renewal_name='Renewal' then renewal_bill_product_month_qty else 0  end) as renewal_bill_product_month_qty
,sum(case when renewal_bill_product_pnl_new_renewal_name='Renewal'  then renewal_bill_list_price_usd_amt else 0 end) as renewal_bill_list_price_usd_amt
,sum(case when r.renewal_bill_product_pnl_new_renewal_name='Renewal'  then renewal_bill_gcr_constant_currency_amt else 0 end) as renewal_bill_gcr_constant_currency_amt

--,	sum(renewal_bill_gcr_constant_currency_amt) as renewal_bill_gcr_constant_currency_amt
from dev.dna_approved.renewal_360 r 

left join dim_prod fdp
  on r.renewal_bill_pf_id = fdp.pf_id	
  
    left join dev.dna_approved.dim_geography g
  on r.renewal_bill_country_code= g.country_code
  
    left join two_plus_cust cust
  
  on r.renewal_bill_modified_mst_Date= cust.snap_end_mst_date 
  and r.renewal_bill_shopper_id= cust.shopper_id 

  

  
    left join  dna_approved.two_plus_active_customer   cust_2
  
 --- on cast(current_Date -1 as date) = cust_2.snap_end_mst_date 
 on  r.renewal_bill_shopper_id= cust_2.shopper_id 
 and source_type_enum = 'external'
  left join cust360 c 
 on renewal_bill_shopper_id=c.shopper_id
  and prior_bill_shopper_id <>10839228
 
  
 left join gsub360 s
 on r.resource_id = s.resource_id
 and r.product_family_name=s.product_family_name
 and r.renewal_bill_id= s.bill_id
 and r.renewal_bill_line_num=s.bill_line_num

 
 
  
where 
	1=1
	and r.renewal_bill_modified_mst_date between cast('2023-01-01' as date) and  cast( current_date  as date)
	and r.renewal_bill_exclude_reason_desc is null 
	and renewal_bill_gcr_usd_amt<>0
	--	and (renewal_bill_product_pnl_category_name='Domain Registration' or renewal_bill_primary_product_flag=true)

group by 
	1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46
;
--analyze tmp_renewals;


---Creating domain specific cash dataset


--cohort basis 

--drop table if exists expirations_cohort_base_domains;
--create temp table expirations_cohort_base_domains
--distkey(prior_bill_paid_through_mst_date)
--sortkey(prior_bill_paid_through_mst_date)
--as 
--Select * from expirations_cohort_base_data
--where prior_bill_product_pnl_category_name ='Domain Registration';





-------creating cash and cohort basis domain specific dataset 


--truncate table dev.dna_sandbox.domains_renewals_agg;

--DROP TABLE IF EXISTS dev.dna_sandbox.domains_renewals_agg;
--CREATE TABLE dev.dna_sandbox.domains_renewals_agg (
 --   bill_mst_date DATE,
 --   analysis_type VARCHAR(20),
 --   pnl_international_independent_flag BOOLEAN,
 --   pnl_us_independent_flag BOOLEAN,
 --   pnl_investor_flag BOOLEAN,
 --   pnl_partner_flag BOOLEAN,
 --   pnl_pillar_name VARCHAR(100),
 --   region_2_name VARCHAR(100),
 --   customer_type_name VARCHAR(100),
 --   historical_auto_renewal_flag BOOLEAN,
  --  first_expiry_sequence_flag BOOLEAN,
  --  product_pnl_group_name VARCHAR(100),
  --  product_pnl_category_name VARCHAR(100),
  --  product_pnl_line_name VARCHAR(100),
  --  product_pnl_version_name VARCHAR(100),
  --  product_pnl_subline_name VARCHAR(100),
  --  point_of_purchase_name VARCHAR(100),
  --  product_period_name VARCHAR(100),
  --  product_period_qty int,
  --  trxn_currency_code VARCHAR(10),
  --  free_receipt_price_flag BOOLEAN,
  --  domain_bulk_pricing_flag BOOLEAN,
  --  renewal_timing_desc VARCHAR(100),
  --  reseller_type_name VARCHAR(100),
  --  refund_flag BOOLEAN,
  --  product_family_name VARCHAR(100),
  --  product_pnl_new_renewal_name VARCHAR(100),
  --  payable_bill_line_flag BOOLEAN,
  --  bill_gcr_usd_amt_flag BOOLEAN,
  --  fin_pnl_group VARCHAR(100),
  --  fin_pnl_category VARCHAR(100),
  --  fin_pnl_line VARCHAR(100),
  --  fin_pnl_subline VARCHAR(100),
  --  adslot_flag VARCHAR(20),
  --  search_premium_flag VARCHAR(20),
  --  hosting_detected_flag BOOLEAN,
  --  ssl_detected_flag BOOLEAN,
  --  email_detected_flag BOOLEAN,
  ---  godaddy_provided_hosting_flag BOOLEAN,
  --  godaddy_provided_ssl_flag BOOLEAN,
  --  godaddy_provided_email_flag BOOLEAN,
  --  domain_activation_flag BOOLEAN,
  --  forward_detected_flag BOOLEAN,
   -- lfs_detected_flag BOOLEAN,
   -- prior_bill_premium_flag VARCHAR(20),
   -- prior_bill_ddc_customer_flag BOOLEAN,
   -- prior_bill_isc_viral_flag BOOLEAN,
  --  activation_triggers VARCHAR(150),
  --  expiry_qty int,
  --  renewal_qty int,
  --  renewal_bill_gcr_usd_amt decimal (38,10),
--    renewal_bill_gcr_constant_currency_amt decimal (38,10),
 --   renewal_bill_product_month_qty int
--)
--DISTKEY(bill_mst_date)
--SORTKEY(bill_mst_date,analysis_type);





--insert into dev.dna_sandbox.domains_renewals_agg select * from tmp_cash_domains_renewals_stg;
--insert into dev.dna_sandbox.domains_renewals_agg select * from expirations_cohort_base_domains ;




--- creating   renewal 360 agg  dataset including all products with key dims





drop table if exists tmp_cash_expirations_all;
create temp table  tmp_cash_expirations_all
distkey(prior_bill_billing_due_mst_date)
sortkey(prior_bill_billing_due_mst_date)
as 
SELECT 
  e.prior_bill_billing_due_mst_date
,	'cash basis' as analysis_type
--, 	COALESCE(e.prior_bill_pnl_international_independent_flag, r.renewal_bill_pnl_international_independent_flag) AS pnl_international_independent_flag
--, 	COALESCE(e.prior_bill_pnl_us_independent_flag, r.renewal_bill_pnl_us_independent_flag) AS pnl_us_independent_flag
--, 	COALESCE(e.prior_bill_pnl_investor_flag, r.renewal_bill_pnl_investor_flag) AS pnl_investor_flag
--, 	COALESCE(e.prior_bill_pnl_partner_flag, r.renewal_bill_pnl_partner_flag) AS pnl_partner_flag
, 	e.pillar_name
, 	e.prior_bill_region_2_name
,e.prior_bill_region_3_name
, e.prior_bill_country_name

, 	e.prior_bill_customer_type_name
, 	e.historical_auto_renewal_flag
, 	e.first_expiry_sequence_flag
,   e.prior_bill_product_pnl_group_name
,	  e.prior_bill_product_pnl_category_name
, 	e.prior_bill_product_pnl_line_name
, 	e.prior_bill_product_pnl_version_name
,   e.prior_bill_product_pnl_subline_name
,e.prior_bill_point_of_purchase_name
,   e.prior_bill_product_period_name
, 	e.prior_bill_product_period_qty 
--, 	COALESCE(e.prior_bill_trxn_currency_code, r.renewal_bill_trxn_currency_code) AS trxn_currency_code
--, 	COALESCE(e.prior_free_receipt_price_flag, r.prior_free_receipt_price_flag) AS free_receipt_price_flag
, e.prior_bill_domain_bulk_pricing_flag
, e.renewal_timing_desc
, 	e.prior_bill_reseller_type_name
--, 	COALESCE(e.refund_flag, r.refund_flag) AS refund_flag
, 	e.product_family_name
--, 	COALESCE(e.prior_bill_product_pnl_new_renewal_name, r.renewal_bill_product_pnl_new_renewal_name) AS product_pnl_new_renewal_name
, e.prior_payable_bill_line_flag 
, e.prior_bill_gcr_usd_amt_flag  -- change for this coalesce 
,e.prior_bill_primary_product_flag
-- finance specific fields 
,	e.fin_pnl_group 
,	e.fin_pnl_category
,	e.fin_pnl_line

, COALESCE(
  CASE 
    WHEN fin_pnl_line = 'MS Office 365' THEN 
      CASE 
        WHEN NULLIF(TRIM(REPLACE(pnl_forecast_group_name, 'MS Office 365', '')), '') IS NULL THEN NULL
        ELSE TRIM(REPLACE(pnl_forecast_group_name, 'MS Office 365', '')) END
    WHEN fin_pnl_line = 'Websites and Marketing' THEN 'GoCentral Website Paid'
    ELSE fin_pnl_subline


  END,
  fin_pnl_subline
) as fin_pnl_subline
, e.fin_investor_relation_class_name

,e.fin_investor_relation_subclass_name

,e.fin_investor_relation_segment_name
---- drivers
, e.renewal_month
,e.two_plus_hist_Flag
,e.two_plus_current_flag
,e.Google_migrated_shopper_flag
,e.Google_migrated_subscription_flag
,e.customer_paid_product_category
, 	sum(e.expiry_qty) as expiry_qty

from expirations_cash_base_data e

GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37;



drop table if exists tmp_renewals_all;
create temp table tmp_renewals_all 
distkey(renewal_bill_modified_mst_date)
sortkey(renewal_bill_modified_mst_date)
as 
SELECT 
  e.renewal_bill_modified_mst_date
,	'renewal basis' as analysis_type
--, 	COALESCE(e.prior_bill_pnl_international_independent_flag, r.renewal_bill_pnl_international_independent_flag) AS pnl_international_independent_flag
--, 	COALESCE(e.prior_bill_pnl_us_independent_flag, r.renewal_bill_pnl_us_independent_flag) AS pnl_us_independent_flag
--, 	COALESCE(e.prior_bill_pnl_investor_flag, r.renewal_bill_pnl_investor_flag) AS pnl_investor_flag
--, 	COALESCE(e.prior_bill_pnl_partner_flag, r.renewal_bill_pnl_partner_flag) AS pnl_partner_flag
, 	e.pillar_name
, 	e.renewal_bill_report_region_2_name
,e.renewal_bill_region_3_name
,e.renewal_bill_country_name
, 	e.renewal_bill_customer_type_name
, 	e.historical_auto_renewal_flag
, 	e.first_expiry_sequence_flag
,   e.renewal_bill_product_pnl_group_name
,	  e.renewal_bill_product_pnl_category_name
, 	e.renewal_bill_product_pnl_line_name
, 	e.renewal_bill_product_pnl_version_name
,   e.renewal_bill_product_pnl_subline_name
,   e.renewal_bill_point_of_purchase_name
,   e.renewal_bill_product_period_name
, 	e.renewal_bill_product_period_qty 
,   e.renewal_bill_domain_bulk_pricing_flag
, e.renewal_timing_desc
, 	e.renewal_bill_reseller_type_name
, 	e.product_family_name
,   e.renewal_payable_bill_line_flag 
,   e.renewal_bill_gcr_usd_amt_flag 
,   e.renewal_bill_primary_product_flag

-- finance specific fields 
,	  e.fin_pnl_group
,	  e.fin_pnl_category
,   e.fin_pnl_line

, COALESCE(
  CASE 
    WHEN fin_pnl_line = 'MS Office 365' THEN 
      CASE 
        WHEN NULLIF(TRIM(REPLACE(pnl_forecast_group_name, 'MS Office 365', '')), '') IS NULL THEN NULL
        ELSE TRIM(REPLACE(pnl_forecast_group_name, 'MS Office 365', '')) END
    WHEN fin_pnl_line = 'Websites and Marketing' THEN 'GoCentral Website Paid'
    ELSE fin_pnl_subline


  END,
  fin_pnl_subline
) as fin_pnl_subline
, e.fin_investor_relation_class_name

,e.fin_investor_relation_subclass_name

,e.fin_investor_relation_segment_name
---- drivers
, renewal_month
,e.two_plus_hist_Flag
,e.two_plus_current_flag
,e.Google_migrated_shopper_flag
,e.Google_migrated_subscription_flag
,e.customer_paid_product_category
,	  sum(e.renewal_qty) as renewal_qty
, 	sum(e.renewal_bill_gcr_usd_amt) as renewal_bill_gcr_usd_amt
,	  sum(e.renewal_bill_product_month_qty) as renewal_bill_product_month_qty
,sum(e.renewal_bill_receipt_price_usd_amt) AS renewal_bill_receipt_price_usd_amt
,sum( e.renewal_bill_list_price_usd_amt ) as renewal_bill_list_price_usd_amt
,sum(e.renewal_bill_gcr_constant_currency_amt ) as renewal_bill_gcr_constant_currency_amt
from renewals_base_data e
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37;




-----creating cash and cohort basis renewals aggregate all products (with key dimensions)



drop table if exists tmp_cash_renewals_all;

create temp table tmp_cash_renewals_all
distkey(bill_mst_date)
sortkey(bill_mst_date)
as 
 
SELECT 
  COALESCE(e.prior_bill_billing_due_mst_Date, r.renewal_bill_modified_mst_Date) AS bill_mst_date
,	'cash basis' as analysis_type
--,COALESCE(e.prior_bill_pnl_international_independent_flag, r.renewal_bill_pnl_international_independent_flag) AS pnl_international_independent_flag
--,COALESCE(e.prior_bill_pnl_us_independent_flag, r.renewal_bill_pnl_us_independent_flag) AS pnl_us_independent_flag
--,COALESCE(e.prior_bill_pnl_investor_flag, r.renewal_bill_pnl_investor_flag) AS pnl_investor_flag
--,COALESCE(e.prior_bill_pnl_partner_flag, r.renewal_bill_pnl_partner_flag) AS pnl_partner_flag
, 	COALESCE(e.pillar_name, r.pillar_name) AS pnl_pillar_name
, 	COALESCE(e.prior_bill_region_2_name, r.renewal_bill_report_region_2_name) AS region_2_name
,	COALESCE(e.prior_bill_region_3_name, r.renewal_bill_region_3_name) AS region_3_name
,coalesce(e.prior_bill_country_name, r.renewal_bill_country_name) as country_name
, 	COALESCE(e.prior_bill_customer_type_name, r.renewal_bill_customer_type_name) AS customer_type_name
, 	COALESCE(e.historical_auto_renewal_flag, r.historical_auto_renewal_flag) AS historical_auto_renewal_flag
, 	COALESCE(e.first_expiry_sequence_flag, r.first_expiry_sequence_flag) AS first_expiry_sequence_flag
, 	COALESCE(e.prior_bill_product_pnl_group_name, r.renewal_bill_product_pnl_group_name) AS product_pnl_group_name
,	  COALESCE(e.prior_bill_product_pnl_category_name,r.renewal_bill_product_pnl_category_name) as product_pnl_category_name
, 	COALESCE(e.prior_bill_product_pnl_line_name, r.renewal_bill_product_pnl_line_name) AS product_pnl_line_name
, 	COALESCE(e.prior_bill_product_pnl_version_name, r.renewal_bill_product_pnl_version_name) AS product_pnl_version_name
, 	COALESCE(e.prior_bill_product_pnl_subline_name, r.renewal_bill_product_pnl_subline_name) AS product_pnl_subline_name
, 	COALESCE(e.prior_bill_point_of_purchase_name, r.renewal_bill_point_of_purchase_name) AS point_of_purchase_name
, 	COALESCE(e.prior_bill_product_period_name, r.renewal_bill_product_period_name) AS product_period_name
, 	COALESCE(e.prior_bill_product_period_qty, r.renewal_bill_product_period_qty) AS product_period_qty
--, COALESCE(e.prior_bill_trxn_currency_code, r.renewal_bill_trxn_currency_code) AS trxn_currency_code
--, COALESCE(e.prior_free_receipt_price_flag, r.prior_free_receipt_price_flag) AS free_receipt_price_flag
, 	COALESCE(e.prior_bill_domain_bulk_pricing_flag, r.renewal_bill_domain_bulk_pricing_flag) AS domain_bulk_pricing_flag
, COALESCE(e.renewal_timing_desc, r.renewal_timing_desc) AS renewal_timing_desc
, 	COALESCE(e.prior_bill_reseller_type_name, r.renewal_bill_reseller_type_name) AS reseller_type_name
--, COALESCE(e.refund_flag, r.refund_flag) AS refund_flag
, 	COALESCE(e.product_family_name, r.product_family_name) AS product_family_name
--, COALESCE(e.prior_bill_product_pnl_new_renewal_name, r.renewal_bill_product_pnl_new_renewal_name) AS product_pnl_new_renewal_name
, 	COALESCE(e.prior_payable_bill_line_flag, r.renewal_payable_bill_line_flag) AS payable_bill_line_flag
, 	coalesce(e.prior_bill_gcr_usd_amt_flag,r.renewal_bill_gcr_usd_amt_flag) as bill_gcr_usd_amt_flag -- change for this coalesce 
, 	coalesce(e.prior_bill_primary_product_flag,r.renewal_bill_primary_product_flag) as primary_product_flag
-- finance specific fields 
,	coalesce(e.fin_pnl_group,r.fin_pnl_group) as fin_pnl_group
,	coalesce(e.fin_pnl_category,r.fin_pnl_category) as fin_pnl_category
,	coalesce(e.fin_pnl_line,r.fin_pnl_line) as fin_pnl_line
,	coalesce(e.fin_pnl_subline,r.fin_pnl_subline) as fin_pnl_subline
,	coalesce(e.fin_investor_relation_class_name,r.fin_investor_relation_class_name) as fin_investor_relation_class_name
,	coalesce(e.fin_investor_relation_subclass_name,r.fin_investor_relation_subclass_name) as fin_investor_relation_subclass_name
,	coalesce(e.fin_investor_relation_segment_name,r.fin_investor_relation_segment_name) as fin_investor_relation_segment_name

--,	coalesce(e.pnl_forecast_group_name,r.pnl_forecast_group_name) as pnl_forecast_group_name
,coalesce(e.renewal_month,null) as renewal_month

---- drivers
, coalesce(e.two_plus_hist_Flag,r.two_plus_hist_Flag ) as two_plus_hist_flag
,coalesce(e.two_plus_current_flag, r.two_plus_current_flag) as two_plus_current_flag
,coalesce(e.Google_migrated_shopper_flag,r.Google_migrated_shopper_flag) as Google_migrated_shopper_flag
,coalesce(e.Google_migrated_subscription_flag,r.Google_migrated_subscription_flag) as Google_migrated_subscription_flag
,coalesce(e.customer_paid_product_category ,r.customer_paid_product_category ) as customer_paid_product_category 

, 	sum(e.expiry_qty) as expiry_qty
,	  sum(r.renewal_qty) as renewal_qty
, 0  as ontime_renewal_qty
, 	sum(r.renewal_bill_gcr_usd_amt) as renewal_bill_gcr_usd_amt
--,sum(r.renewal_bill_gcr_constant_currency_amt) as renewal_bill_gcr_constant_currency_amt
,	  sum(r.renewal_bill_product_month_qty) as renewal_bill_product_month_qty
, sum(r.renewal_bill_receipt_price_usd_Amt) as renewal_bill_receipt_price_usd_Amt
, 0 as ontime_renewal_receipt_price_Amt
, 0 as potential_Receipt_price_usd_Amt
,sum( r.renewal_bill_list_price_usd_amt) as renewal_bill_list_price_usd_amt
,sum(r.renewal_bill_gcr_constant_currency_amt ) as renewal_bill_gcr_constant_currency_amt
from tmp_cash_expirations_all e
FULL OUTER JOIN 
tmp_renewals_all r
ON e.prior_bill_billing_due_mst_Date = r.renewal_bill_modified_mst_Date
--AND e.prior_bill_pnl_international_independent_flag = r.renewal_bill_pnl_international_independent_flag
--AND e.prior_bill_pnl_us_independent_flag = r.renewal_bill_pnl_us_independent_flag
--AND e.prior_bill_pnl_investor_flag = r.renewal_bill_pnl_investor_flag
--AND e.prior_bill_pnl_partner_flag = r.renewal_bill_pnl_partner_flag

AND e.pillar_name = r.pillar_name
AND e.prior_bill_region_2_name = r.renewal_bill_report_region_2_name
and e.prior_bill_region_3_name=r.renewal_bill_region_3_name
and e.prior_bill_country_name=r.renewal_Bill_country_name
AND e.prior_bill_customer_type_name = r.renewal_bill_customer_type_name
AND e.historical_auto_renewal_flag = r.historical_auto_renewal_flag
AND e.first_expiry_sequence_flag = r.first_expiry_sequence_flag
AND e.prior_bill_product_pnl_group_name = r.renewal_bill_product_pnl_group_name
and e.prior_bill_product_pnl_category_name = r.renewal_bill_product_pnl_category_name
AND e.prior_bill_product_pnl_line_name = r.renewal_bill_product_pnl_line_name
AND e.prior_bill_product_pnl_version_name = r.renewal_bill_product_pnl_version_name
AND e.prior_bill_product_pnl_subline_name = r.renewal_bill_product_pnl_subline_name
and e.prior_bill_point_of_purchase_name=r.renewal_Bill_point_of_purchase_name
AND e.prior_bill_product_period_name = r.renewal_bill_product_period_name
AND e.prior_bill_product_period_qty = r.renewal_bill_product_period_qty
--AND e.prior_bill_trxn_currency_code = r.renewal_bill_trxn_currency_code
--AND e.prior_free_receipt_price_flag = r.prior_free_receipt_price_flag
AND e.prior_bill_domain_bulk_pricing_flag = r.renewal_bill_domain_bulk_pricing_flag
AND e.renewal_timing_desc = r.renewal_timing_desc
AND e.prior_bill_reseller_type_name = r.renewal_bill_reseller_type_name
--AND e.refund_flag = r.refund_flag
AND e.product_family_name = r.product_family_name
--AND e.prior_bill_product_pnl_new_renewal_name = r.renewal_bill_product_pnl_new_renewal_name
AND e.prior_payable_bill_line_flag = r.renewal_payable_bill_line_flag
AND e.prior_bill_gcr_usd_amt_flag = r.renewal_bill_gcr_usd_amt_flag
and e.prior_bill_primary_product_flag=r.renewal_bill_primary_product_flag
-- finance specific joins
and e.fin_pnl_group = r.fin_pnl_group
and e.fin_pnl_category = r.fin_pnl_category
and e.fin_pnl_line = r.fin_pnl_line 
and e.fin_pnl_subline = r.fin_pnl_subline
and e.fin_investor_relation_class_name=r.fin_investor_relation_class_name
and e.fin_investor_relation_subclass_name=r.fin_investor_relation_subclass_name
and e.fin_investor_relation_segment_name=r.fin_investor_relation_segment_name
and e.renewal_month=r.renewal_month
and e.two_plus_hist_flag=r.two_plus_hist_flag
and e.two_plus_current_flag=r.two_plus_current_flag
--and e.pnl_forecast_group_name=r.pnl_forecast_group_name
and e.Google_migrated_subscription_flag=r.google_migrated_subscription_flag
and e.Google_migrated_shopper_flag=r.Google_migrated_shopper_flag
and e.customer_paid_product_category=r.customer_paid_product_category
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37;


--cohort basis dataset for renewal agg
   
drop table if exists tmp_cohort_renewals_all;

create temp table tmp_cohort_renewals_all
distkey(bill_mst_date)
sortkey(bill_mst_date)
as 
select 
   e.prior_bill_paid_through_mst_date AS bill_mst_date
,	'cohort basis' as analysis_type
, e.pillar_name AS pnl_pillar_name
, e.prior_bill_region_2_name AS region_2_name
,e.prior_bill_region_3_name as region_3_name
,e.prior_bill_country_name as country_name
, e.prior_bill_customer_type_name AS customer_type_name
, e.historical_auto_renewal_flag AS historical_auto_renewal_flag
, e.first_expiry_sequence_flag AS first_expiry_sequence_flag
, e.prior_bill_product_pnl_group_name AS product_pnl_group_name
,	e.prior_bill_product_pnl_category_name as product_pnl_category_name
, e.prior_bill_product_pnl_line_name AS product_pnl_line_name
, e.prior_bill_product_pnl_version_name  AS product_pnl_version_name
, e.prior_bill_product_pnl_subline_name AS product_pnl_subline_name
, e.prior_bill_point_of_purchase_name as point_of_purchase_name
, e.prior_bill_product_period_name AS product_period_name
, e.prior_bill_product_period_qty AS product_period_qty

--, 	COALESCE(e.prior_bill_trxn_currency_code, r.renewal_bill_trxn_currency_code) AS trxn_currency_code
--, 	COALESCE(e.prior_free_receipt_price_flag, r.prior_free_receipt_price_flag) AS free_receipt_price_flag
, e.prior_bill_domain_bulk_pricing_flag AS domain_bulk_pricing_flag
, e.renewal_timing_desc
, e.prior_bill_reseller_type_name AS reseller_type_name
--, e.refund_flag AS refund_flag
, e.product_family_name AS product_family_name
--, e.prior_bill_product_pnl_new_renewal_name AS product_pnl_new_renewal_name
, e.prior_payable_bill_line_flag  AS payable_bill_line_flag
, e.prior_bill_gcr_usd_amt_flag as bill_gcr_usd_amt_flag -- change for this coalesce 
,e.prior_bill_primary_product_flag as primary_product_flag
-- finance specific fields 
,	e.fin_pnl_group as fin_pnl_group
,	e.fin_pnl_category as fin_pnl_category
,	e.fin_pnl_line as fin_pnl_line
, COALESCE(
  CASE 
    WHEN fin_pnl_line = 'MS Office 365' THEN 
      CASE 
        WHEN NULLIF(TRIM(REPLACE(pnl_forecast_group_name, 'MS Office 365', '')), '') IS NULL THEN NULL
        ELSE TRIM(REPLACE(pnl_forecast_group_name, 'MS Office 365', '')) END
    WHEN fin_pnl_line = 'Websites and Marketing' THEN 'GoCentral Website Paid'
    ELSE fin_pnl_subline


  END,
  fin_pnl_subline
) as fin_pnl_subline
, e.fin_investor_relation_class_name

,e.fin_investor_relation_subclass_name

,e.fin_investor_relation_segment_name
----
,e.renewal_month 
,e.two_plus_hist_flag
,e.two_plus_current_flag
,e.Google_migrated_shopper_flag
,e.Google_migrated_subscription_flag
,e.customer_paid_product_category
,	sum(e.expiry_qty) as expiry_qty
,	sum(e.renewal_qty) as renewal_qty
,sum(e.Ontime_renewal_qty) as Ontime_renewal_qty

,	sum(e.renewal_bill_gcr_usd_amt) as renewal_bill_gcr_usd_amt
--,	sum(r.renewal_bill_gcr_constant_currency_amt) as renewal_bill_gcr_constant_currency_amt
,	sum(e.renewal_bill_product_month_qty) as renewal_bill_product_month_qty
,sum(e.renewal_bill_receipt_price_usd_amt) as renewal_bill_receipt_price_usd_Amt
,sum(e.ontime_renewal_bill_receipt_price_usd_amt) as ontime_renewal_receipt_price_Amt
,	sum(e.potential_Receipt_price_usd_amt) as potential_receipt_price_usd_amt
,sum(e.renewal_bill_list_price_usd_amt) as renewal_bill_list_price_usd_amt
,sum(e.renewal_bill_gcr_constant_currency_amt ) as renewal_bill_gcr_constant_currency_amt
from expirations_cohort_base_data e
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37;




-----get calendar dates


drop table if exists tmp_relative_dates;
create temp table tmp_relative_dates as
select 
	drd.relative_date
,	drd.relative_week
,	drd.relative_month
,	drd.relative_date_period_name
,	drd.relative_week_period_name
,	drd.relative_month_period_name
,	calendar_date
,	max_date
,   CASE WHEN DATE_PART(dow, max_date) IN (0,6) THEN DATE(DATEADD(day, -1, DATEADD(week, 1, DATE_TRUNC('week', max_date)))) ELSE DATE(DATEADD(day, -1, DATE_TRUNC('week', max_date))) END AS anchor_date_week
,   CASE WHEN max_date = LAST_DAY(max_date) THEN DATE(DATEADD(day, 1, max_date)) ELSE DATE(DATE_TRUNC('month', max_date)) END AS anchor_date_month
 , max_date - ((EXTRACT(DOW FROM max_date) + 1) % 7) as max_date_week
,    CASE WHEN max_date = LAST_DAY(max_date) THEN DATE(DATEADD(day, 1, max_date)) ELSE DATE(DATE_TRUNC('month', max_date)) END AS max_date_month
 

from dev.bi_prod.dim_relative_date drd 
where 
	1=1
	and drd.max_date = current_date - 1
	 and (
            relative_date_period_name in ('Current Year', 'Prior Year (1)', 'Prior Year (2)') or 
            relative_week_period_name in ('Current Year', 'Prior Year (1)', 'Prior Year (2)') or 
            relative_month_period_name in ('Current Year', 'Prior Year (1)', 'Prior Year (2)')
            )
;


--analyze  tmp_relative_dates;

drop table if exists tmp_cohort_renewal_final;
create temp table tmp_cohort_renewal_final as 
select 
a.*,
b.*
from tmp_cohort_renewals_all a 
inner join tmp_relative_dates b 
	on a.bill_mst_date = b.calendar_date
;
--analyze tmp_cohort_renewal_final;

drop table if exists tmp_cash_final;
create temp table tmp_cash_final as 
select 
a.*,
b.*
from tmp_cash_renewals_all a 
inner join tmp_relative_dates b 
	on a.bill_mst_date = b.calendar_date
;
--analyze tmp_cohort_final;


--truncate table dev.dna_sandbox.renewals_360_agg;

DROP TABLE IF EXISTS dev.ba_ecommerce.renewals_360_agg ;
CREATE TABLE dev.ba_ecommerce.renewals_360_agg  (
    bill_mst_date DATE,
    analysis_type VARCHAR(20),
 --   pnl_international_independent_flag BOOLEAN,
 --   pnl_us_independent_flag BOOLEAN,
 --   pnl_investor_flag BOOLEAN,
  --  pnl_partner_flag BOOLEAN,
    pnl_pillar_name VARCHAR(100),
    region_2_name VARCHAR(100),
    region_3_name   VARCHAR(100),
    country_name VARCHAR(100),
    customer_type_name VARCHAR(100),
    historical_auto_renewal_flag BOOLEAN,
    first_expiry_sequence_flag BOOLEAN,
    product_pnl_group_name VARCHAR(100),
    product_pnl_category_name VARCHAR(100),
    product_pnl_line_name VARCHAR(100),
    product_pnl_version_name VARCHAR(100),
    product_pnl_subline_name VARCHAR(100),
     point_of_purchase_name VARCHAR(100),
    product_period_name VARCHAR(100),
    product_period_qty int,
  --  trxn_currency_code VARCHAR(10),
  --  free_receipt_price_flag BOOLEAN,
    domain_bulk_pricing_flag BOOLEAN,
  renewal_timing_desc VARCHAR(100),
    reseller_type_name VARCHAR(100),
 --   refund_flag BOOLEAN,
    product_family_name VARCHAR(100),
--    product_pnl_new_renewal_name VARCHAR(100),
    payable_bill_line_flag BOOLEAN,
    bill_gcr_usd_amt_flag BOOLEAN,
    primary_product_flag BOOLEAN,
    fin_pnl_group VARCHAR(100),
    fin_pnl_category VARCHAR(100),
    fin_pnl_line VARCHAR(100),
    fin_pnl_subline VARCHAR(100),
  fin_investor_relation_class_name VARCHAR(100),

   fin_investor_relation_subclass_name VARCHAR(100),

    fin_investor_relation_segment_name VARCHAR(100),
    renewal_month DATE,
    two_plus_hist_flag BOOLEAN, 
    two_plus_current_flag BOOLEAN,
    Google_migrated_shopper_flag VARCHAR(100),
    Google_migrated_subscription_flag VARCHAR(100),
    customer_paid_product_category VARCHAR(100),
  
    expiry_qty int,
    renewal_qty int,
    ontime_renewal_qty int,
    renewal_bill_gcr_usd_amt decimal (38,10),
 --   renewal_bill_gcr_constant_currency_amt decimal (38,10),
    renewal_bill_product_month_qty int,
    renewal_bill_receipt_price_usd_amt decimal (38,10),
    ontime_renewal_receipt_price_amt decimal (38,10),
    potential_receipt_price_usd_amt decimal (38,10),
     renewal_bill_list_price_usd_amt decimal (38,10),
       renewal_bill_cc_gcr_usd_amt decimal (38,10),
 relative_date date ,
    relative_week date,
    relative_month date ,
    relative_date_period_name character varying(25) ,
    relative_week_period_name character varying(25) ,
    relative_month_period_name character varying(25) ,
    calendar_date date ,
    max_date date ,
    anchor_date_week date ,
    anchor_date_month date ,
    max_date_week date,
    max_date_month date
)
DISTKEY(bill_mst_date)
SORTKEY(bill_mst_date,analysis_type);





insert into dev.ba_ecommerce.renewals_360_agg  select * from tmp_cash_final;
insert into dev.ba_ecommerce.renewals_360_agg  select * from tmp_cohort_renewal_final ;



drop table if exists ba_ecommerce.renewals_rate_mix_agg;
create table ba_ecommerce.renewals_rate_mix_agg
as
Select 
bill_mst_date	,
relative_date	,
relative_week	,
relative_month	,
relative_date_period_name	,
relative_week_period_name	,
relative_month_period_name	,
calendar_date	,
max_date	,
 max_Date_Week,
 Max_Date_Month,
anchor_date_week	,
anchor_date_month	,
renewal_month,


Initcap(trim(analysis_type)) as analysis_type	,
pnl_pillar_name	,
region_2_name	,
region_3_name,
country_name,
customer_type_name	,
historical_auto_renewal_flag	,
first_expiry_sequence_flag	,
product_pnl_group_name	,
product_pnl_category_name	,
product_pnl_line_name	,
product_pnl_version_name	,
product_pnl_subline_name	,
point_of_purchase_name	,
product_period_name	,
product_period_qty	,
domain_bulk_pricing_flag	,
reseller_type_name	,
product_family_name	,
payable_bill_line_flag	,
bill_gcr_usd_amt_flag	,
primary_product_flag,
fin_pnl_group,
fin_pnl_category,
fin_pnl_line,
fin_pnl_subline,
fin_investor_relation_class_name

,fin_investor_relation_subclass_name

,fin_investor_relation_segment_name

,case when first_expiry_sequence_flag=true then '1st Expiry' else '2nd-Nth Expiry' end as first_expiry_seq_desc 	,
case when domain_bulk_pricing_flag=true then 'Bulk' else 'Non-Bulk' end as domain_bulk_pricing_desc,
case when payable_bill_line_flag=true then 'Paid' else 'Free' end as payable_bill_line_desc,
case when historical_auto_renewal_flag=true then 'Auto' else 'Manual' end as historical_auto_renewal_desc,
Case when primary_product_flag=true then 'Primary' else 'Add-on' end as primary_product_flag_desc,
case when bill_gcr_usd_amt_flag=true then 'Paid' else 'Free' end as bill_gcr_usd_amt_desc,

case when two_plus_current_flag=true then 'True' else 'False' end as   two_plus_current_desc,
case when two_plus_hist_flag=true then 'True' else 'False' end as  two_plus_hist_desc,


renewal_timing_desc,
Google_migrated_shopper_flag,
Google_migrated_subscription_flag,
customer_paid_product_category ,
sum(expiry_qty)	 as expiry_qty,
sum(renewal_qty) as renewal_qty	,
sum(renewal_bill_gcr_usd_amt) as renewal_Bill_gcr_usd_amt	,
sum(renewal_bill_product_month_qty) as renewal_bill_product_month_qty	,
sum(renewal_bill_receipt_price_usd_amt) as renewal_bill_receipt_price_usd_amt,
sum(ontime_renewal_qty) as ontime_renewal_qty,
sum(potential_receipt_price_usd_amt) as potential_Receipt_price_Amt,
sum(ontime_renewal_receipt_price_amt) as ontime_renewal_receipt_price_amt,
sum(renewal_bill_list_price_usd_amt) as renewal_bill_list_price_usd_amt,
sum( renewal_bill_cc_gcr_usd_amt) as  renewal_bill_cc_gcr_usd_amt 



from dev.ba_ecommerce.renewals_360_agg  
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55;


-----End of Renewal overview

---Insert into check table 



INSERT INTO dev.ba_ecommerce.renewal_job_alerts 



SELECT
    'Renewals Overview Agg' AS dataset_name,
        MAX(bill_mst_Date) AS max_date,
         (CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', GETDATE())::date - 1)  as data_expected_date,
    GETDATE() AS run_ts,

 
    CASE
        WHEN COUNT(*) = 0 THEN 'FAILED'
        WHEN MAX(bill_mst_date) <   (CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', GETDATE())::date - 1)  THEN 'FAILED'
        ELSE 'SUCCESS'
    END AS status,
    
  COUNT(*) AS row_count,
    CASE
        WHEN COUNT(*) = 0 THEN 'No rows found in dataset'
        WHEN MAX(bill_mst_date) <  (CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', GETDATE())::date - 1)  THEN 'Max date is stale'
        ELSE NULL END AS error_message
        
    from  ba_ecommerce.renewals_rate_mix_agg
 ;

----Beginning of Rate Mix adjusted dashboard





            
DROP TABLE IF EXISTS renewal_max_dates;
        
        create TEMP table renewal_max_dates as
    select 
     max(max_date) as as_of_Date,
        min(bill_mst_date)  as min_relative_renewal_date,
        max(max_date_week) - 13 * 7 as min_relative_renewal_week
    from dev.ba_ecommerce.renewals_360_agg rr
    where (relative_date_period_name in ('Current Year', 'Prior Year (1)') or 
                relative_week_period_name in ('Current Year', 'Prior Year (1)'));
         
         
         
         
--Fixed period data 





drop table if exists fixed_period_mix_renewals ;
create temp table fixed_period_mix_renewals
distkey(product_pnl_version_name)
sortkey(product_pnl_version_name)


as 
select 
analysis_type,
 customer_type_name,
 pnl_pillar_name,
region_2_name,
 product_family_name,
 product_pnl_category_name,
product_pnl_line_name,
 product_pnl_version_name,
-- product_pnl_subline_name,
payable_bill_line_flag,
domain_bulk_pricing_flag,
 first_Expiry_sequence_flag,
 product_period_name,
 product_period_qty,

fin_pnl_category,
fin_pnl_line,
fin_pnl_subline,
fin_investor_relation_class_name

,fin_investor_relation_subclass_name

,fin_investor_relation_segment_name
 

 ,
 sum(renewal_bill_gcr_usd_amt) as fixed_gcr_amt,
 sum(renewal_bill_product_month_qty) as fixed_renewal_month_qty,
  sum (renewal_qty) as fixed_renewal_qty,
sum(expiry_qty) AS fixed_Expiries
from dev.ba_ecommerce.renewals_360_agg
where bill_mst_date BETWEEN '2024-12-01' AND '2025-02-28'
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19;




----Actuals Daily DATA


Drop table if exists actual_daily_period_renewals ;

create temp table actual_daily_period_renewals 
distkey(product_pnl_version_name)
sortkey(as_of_date)
 as 
select      
                     
  date(bill_mst_date) as as_of_date,
    relative_date,
    relative_date_period_name,
   analysis_type,
 customer_type_name,
 pnl_pillar_name,
region_2_name,
 product_family_name,
 product_pnl_category_name,
product_pnl_line_name,
 product_pnl_version_name,
-- product_pnl_subline_name,
payable_bill_line_flag,
domain_bulk_pricing_flag,
 first_Expiry_sequence_flag,
 product_period_name,
 product_period_qty,

fin_pnl_category,
fin_pnl_line,
fin_pnl_subline,
fin_investor_relation_class_name

,fin_investor_relation_subclass_name

,fin_investor_relation_segment_name

 ,sum(renewal_bill_gcr_usd_amt) as renewal_bill_gcr_usd_amt,
 sum(renewal_bill_product_month_qty) as renewal_bill_product_month_qty,
  sum (renewal_qty) as renewal_qty,
sum(expiry_qty) AS expiry_qty
    
    from dev.ba_ecommerce.renewals_360_agg  a
    cross join renewal_max_dates b 
    where relative_date>min_relative_renewal_date
    and  relative_date_period_name in ('Current Year', 'Prior Year (1)') 
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22;
    
    
    
    
    
    
    
    
    
    


DROP TABLE IF EXISTS fixed_mix_daily_actual_renewals;


create temporary table fixed_mix_daily_actual_renewals as 

with cte_distinct_dates_Daily as 

(
select distinct 
as_of_date
,relative_date
,relative_date_period_name
from actual_daily_period_renewals

)
, cte_Distinct_dims_Daily as 

(select distinct 
analysis_type,
region_2_name,
product_family_name,
product_pnl_category_name,
product_pnl_line_name,
product_pnl_version_name,
--product_pnl_subline_name,
payable_bill_line_flag,
domain_bulk_pricing_flag,
product_period_name,
product_period_qty,
first_expiry_sequence_flag,
customer_Type_name,
pnl_pillar_name,

fin_pnl_category,
fin_pnl_line,
fin_pnl_subline,
fin_investor_relation_class_name

,fin_investor_relation_subclass_name

,fin_investor_relation_segment_name
from 
actual_daily_period_renewals
union
select distinct 
analysis_type,
region_2_name,
product_family_name,
product_pnl_category_name,
product_pnl_line_name,
product_pnl_version_name,
--product_pnl_subline_name,
payable_bill_line_flag,
domain_bulk_pricing_flag,
product_period_name,
product_period_qty,
first_expiry_sequence_flag,
customer_Type_name,
pnl_pillar_name,

fin_pnl_category,
fin_pnl_line,
fin_pnl_subline,
fin_investor_relation_class_name

,fin_investor_relation_subclass_name

,fin_investor_relation_segment_name
from fixed_period_mix_renewals
)


  , cte_cross_daily AS
    (
        SELECT *
        FROM
            cte_distinct_dates_daily
            CROSS JOIN
                cte_distinct_dims_daily
    )
  , cte_full_results_daily AS
   (
   
   select 
   dd.as_of_date
   ,dd.relative_date
,dd.relative_date_period_name
 ,dd.analysis_type
,dd.region_2_name
,dd.product_family_name
,dd.product_pnl_category_name
,dd.product_pnl_line_name
,dd.product_pnl_version_name
--product_pnl_subline_name,
,dd.payable_bill_line_flag
,dd.domain_bulk_pricing_flag
,dd.product_period_name
,dd.product_period_qty
,dd.first_expiry_sequence_flag
,dd.customer_Type_name
,dd.pnl_pillar_name
,dd.fin_pnl_category,
dd.fin_pnl_line,
dd.fin_pnl_subline,
dd.fin_investor_relation_class_name

,dd.fin_investor_relation_subclass_name

,dd.fin_investor_relation_segment_name

,coalesce(renewal_bill_gcr_usd_amt,0) as renewal_bill_gcr_usd_amt
, coalesce(renewal_bill_product_month_qty,0) as renewal_bill_product_month_qty
 , coalesce (renewal_qty,0) as renewal_qty
,coalesce(expiry_qty,0) AS expiry_qty
    

   
  FROM
            cte_cross_daily dd
            LEFT OUTER JOIN actual_daily_period_renewals t
            
          on dd.as_of_Date=t.as_of_Date
    and dd.analysis_type=t.analysis_type
AND dd.region_2_name=t.region_2_name
and dd.product_family_name=t.product_family_name
AND dd.product_pnl_category_name=t.product_pnl_category_name
AND dd.product_pnl_line_name=t.product_pnl_line_name
AND dd.product_pnl_version_name=t.product_pnl_version_name
--AND dd.product_pnl_subline_name=t.product_pnl_subline_name
AND dd.payable_bill_line_flag=t.payable_bill_line_flag
AND dd.domain_bulk_pricing_flag=t.domain_bulk_pricing_flag
and dd.product_period_name=t.product_period_name
and dd.product_period_qty=t.product_period_qty
and dd.first_expiry_sequence_flag=t.first_expiry_sequence_flag
and dd.customer_type_name=t.customer_type_name
and dd.pnl_pillar_name=t.pnl_pillar_name
and dd.fin_pnl_category=t.fin_pnl_category
and dd.fin_pnl_line=t.fin_pnl_line
and dd.fin_pnl_subline=t.fin_pnl_subline
and dd.fin_investor_relation_class_name=t.fin_investor_relation_class_name
and dd.fin_investor_relation_subclass_name=t.fin_investor_relation_subclass_name
and dd.fin_investor_relation_segment_name=t.fin_investor_relation_segment_name
where t.as_of_Date is null
   union all
   
select 
   dd.as_of_date
   ,dd.relative_date
,dd.relative_date_period_name
, dd.analysis_type
,dd.region_2_name
,dd.product_family_name
,dd.product_pnl_category_name
,dd.product_pnl_line_name
,dd.product_pnl_version_name
--,dd.product_pnl_subline_name
,dd.payable_bill_line_flag
,dd.domain_bulk_pricing_flag
,dd.product_period_name
,dd.product_period_qty
,dd.first_expiry_sequence_flag
,dd.customer_Type_name
,dd.pnl_pillar_name
,dd.fin_pnl_category,
dd.fin_pnl_line,
dd.fin_pnl_subline,
dd.fin_investor_relation_class_name

,dd.fin_investor_relation_subclass_name

,dd.fin_investor_relation_segment_name

,coalesce(renewal_bill_gcr_usd_amt,0) as renewal_bill_gcr_usd_amt
, coalesce(renewal_bill_product_month_qty,0) as renewal_bill_product_month_qty
 , coalesce (renewal_qty,0) as renewal_qty
,coalesce(expiry_qty,0) AS expiry_qty
from 
  actual_daily_period_renewals dd
  )
  , cte_final_Results_daily as 
  
  (select
  
     dd.as_of_date
    ,dd.relative_date
   
,dd.relative_date_period_name
 ,dd.analysis_type
,dd.region_2_name
,dd.product_family_name
,dd.product_pnl_category_name
,dd.product_pnl_line_name
,dd.product_pnl_version_name
--,dd.product_pnl_subline_name
,dd.payable_bill_line_flag
,dd.domain_bulk_pricing_flag
,dd.product_period_name
,dd.product_period_qty
,dd.first_expiry_sequence_flag
,dd.customer_Type_name
,dd.pnl_pillar_name
,dd.fin_pnl_category,
dd.fin_pnl_line,
dd.fin_pnl_subline,
dd.fin_investor_relation_class_name

,dd.fin_investor_relation_subclass_name

,dd.fin_investor_relation_segment_name
 ,dd.renewal_bill_gcr_usd_amt
 ,dd.renewal_bill_product_month_qty

,dd.renewal_qty
 ,dd.expiry_qty
 ,coalesce(fixed_gcr_amt,0) as fixed_gcr_amt
 ,coalesce(fixed_renewal_month_qty,0) as fixed_renewal_month_qty
 ,coalesce(fixed_renewal_qty,0) as fixed_renewal_qty
 ,coalesce(fixed_expiries,0) as fixed_expiries

-- ,coalesce(fixed_cash_renewal_gcr_amt,0) as fixed_cash_renewal_gcr_amt
 from  cte_full_results_daily dd 
 left join fixed_period_mix_renewals t
 on dd.analysis_type=t.analysis_type
and dd.region_2_name=t.region_2_name
and dd.product_family_name=t.product_family_name
AND dd.product_pnl_category_name=t.product_pnl_category_name
AND dd.product_pnl_line_name=t.product_pnl_line_name
AND dd.product_pnl_version_name=t.product_pnl_version_name
--AND dd.product_pnl_subline_name=t.product_pnl_subline_name
AND dd.payable_bill_line_flag=t.payable_bill_line_flag
AND dd.domain_bulk_pricing_flag=t.domain_bulk_pricing_flag
and dd.product_period_name=t.product_period_name
and dd.product_period_qty=t.product_period_qty
and dd.first_expiry_sequence_flag=t.first_expiry_sequence_flag
and dd.customer_type_name=t.customer_type_name
and dd.pnl_pillar_name=t.pnl_pillar_name
and dd.fin_pnl_category=t.fin_pnl_category
and dd.fin_pnl_line=t.fin_pnl_line
and dd.fin_pnl_subline=t.fin_pnl_subline
and dd.fin_investor_relation_class_name=t.fin_investor_relation_class_name
and dd.fin_investor_relation_subclass_name=t.fin_investor_relation_subclass_name
and dd.fin_investor_relation_segment_name=t.fin_investor_relation_segment_name
  )
--- daily 


select
     
     dd.as_of_date
   ,dd.relative_date
,dd.relative_Date_period_name
 ,dd.analysis_type
,dd.region_2_name
,dd.product_family_name
,dd.product_pnl_category_name
,dd.product_pnl_line_name
,dd.product_pnl_version_name
--,dd.product_pnl_subline_name
,dd.payable_bill_line_flag
,dd.domain_bulk_pricing_flag
,dd.product_period_name
,dd.product_period_qty
,dd.first_expiry_sequence_flag
,dd.customer_Type_name
,dd.pnl_pillar_name
,dd.fin_pnl_category,
dd.fin_pnl_line,
dd.fin_pnl_subline,
dd.fin_investor_relation_class_name

,dd.fin_investor_relation_subclass_name

,dd.fin_investor_relation_segment_name
, dd.renewal_bill_gcr_usd_amt
, dd.renewal_bill_product_month_qty
 , dd.renewal_qty
,dd.expiry_qty

 ,coalesce(fixed_gcr_amt,0) as fixed_gcr_amt
 ,coalesce(fixed_renewal_month_qty,0) as fixed_renewal_month_qty
 ,coalesce(fixed_renewal_qty,0) as fixed_renewal_qty
 ,coalesce(fixed_expiries,0) as fixed_expiries

 ,'Daily' as date_granularity
  from cte_final_Results_daily dd 
  where  coalesce(renewal_bill_gcr_usd_amt,0)<>0
  or coalesce(renewal_bill_product_month_qty,0)<>0
  or coalesce(renewal_qty,0)<>0
  or coalesce(expiry_qty,0)<>0
  or coalesce(fixed_gcr_amt,0)<>0
 or coalesce(fixed_renewal_month_qty,0)<>0
  or coalesce(fixed_renewal_qty,0)<>0
  or coalesce(fixed_expiries,0)<>0;








-----weekly 


Drop table if exists actual_weekly_period_renewals ;

create temp table actual_weekly_period_renewals 
distkey(product_pnl_version_name)
sortkey(as_of_date)
 as 
select      
date(DATEADD(d, -DATEPART(dow, bill_mst_Date), bill_mst_Date))  as as_of_date,
relative_week,
relative_week_period_name,
analysis_type,
customer_type_name,
pnl_pillar_name,
region_2_name,
product_family_name,
product_pnl_category_name,
product_pnl_line_name,
product_pnl_version_name,
-- product_pnl_subline_name,
payable_bill_line_flag,
domain_bulk_pricing_flag,
first_Expiry_sequence_flag,
product_period_name,
product_period_qty,
fin_pnl_category,
fin_pnl_line,
fin_pnl_subline,
fin_investor_relation_class_name

,fin_investor_relation_subclass_name

,fin_investor_relation_segment_name
,sum(renewal_bill_gcr_usd_amt) as renewal_bill_gcr_usd_amt,
sum(renewal_bill_product_month_qty) as renewal_bill_product_month_qty,
sum (renewal_qty) as renewal_qty,
sum(expiry_qty) AS expiry_qty
    
    from dev.ba_ecommerce.renewals_360_agg  a
    cross join renewal_max_dates b 
    where relative_week>min_relative_renewal_week
    and  relative_week_period_name in ('Current Year', 'Prior Year (1)') 
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22;
    
    
 

    
    


DROP TABLE IF EXISTS fixed_mix_weekly_actual_renewals;


create temporary table fixed_mix_weekly_actual_renewals as 

with cte_distinct_dates_weekly as 

(
select distinct 
as_of_date
,relative_week
,relative_week_period_name
from actual_weekly_period_renewals

)
, cte_Distinct_dims_weekly as 

(select distinct 
analysis_type,
region_2_name,
product_family_name,
product_pnl_category_name,
product_pnl_line_name,
product_pnl_version_name,
--product_pnl_subline_name,
payable_bill_line_flag,
domain_bulk_pricing_flag,
product_period_name,
product_period_qty,
first_expiry_sequence_flag,
customer_Type_name,
pnl_pillar_name,
fin_pnl_category,
fin_pnl_line,
fin_pnl_subline,
fin_investor_relation_class_name

,fin_investor_relation_subclass_name

,fin_investor_relation_segment_name
from 
actual_weekly_period_renewals
union
select distinct 
analysis_type,
region_2_name,
product_family_name,
product_pnl_category_name,
product_pnl_line_name,
product_pnl_version_name,
--product_pnl_subline_name,
payable_bill_line_flag,
domain_bulk_pricing_flag,
product_period_name,
product_period_qty,
first_expiry_sequence_flag,
customer_Type_name,
pnl_pillar_name,
fin_pnl_category,
fin_pnl_line,
fin_pnl_subline,
fin_investor_relation_class_name

,fin_investor_relation_subclass_name

,fin_investor_relation_segment_name
from fixed_period_mix_renewals
)


  , cte_cross_weekly AS
    (
        SELECT *
        FROM
            cte_distinct_dates_weekly
            CROSS JOIN
                cte_distinct_dims_weekly
    )
  , cte_full_results_weekly AS
   (
   
   select 
   dd.as_of_date
   ,dd.relative_week
,dd.relative_week_period_name
 ,dd.analysis_type
,dd.region_2_name
,dd.product_family_name
,dd.product_pnl_category_name
,dd.product_pnl_line_name
,dd.product_pnl_version_name
--product_pnl_subline_name,
,dd.payable_bill_line_flag
,dd.domain_bulk_pricing_flag
,dd.product_period_name
,dd.product_period_qty
,dd.first_expiry_sequence_flag
,dd.customer_Type_name
,dd.pnl_pillar_name
,dd.fin_pnl_category,
dd.fin_pnl_line,
dd.fin_pnl_subline,
dd.fin_investor_relation_class_name

,dd.fin_investor_relation_subclass_name

,dd.fin_investor_relation_segment_name

,coalesce(renewal_bill_gcr_usd_amt,0) as renewal_bill_gcr_usd_amt
, coalesce(renewal_bill_product_month_qty,0) as renewal_bill_product_month_qty
 , coalesce (renewal_qty,0) as renewal_qty
,coalesce(expiry_qty,0) AS expiry_qty
    

   
  FROM
            cte_cross_weekly dd
            LEFT OUTER JOIN actual_weekly_period_renewals t
            
          on dd.as_of_Date=t.as_of_Date
    and dd.analysis_type=t.analysis_type
AND dd.region_2_name=t.region_2_name
and dd.product_family_name=t.product_family_name
AND dd.product_pnl_category_name=t.product_pnl_category_name
AND dd.product_pnl_line_name=t.product_pnl_line_name
AND dd.product_pnl_version_name=t.product_pnl_version_name
--AND dd.product_pnl_subline_name=t.product_pnl_subline_name
AND dd.payable_bill_line_flag=t.payable_bill_line_flag
AND dd.domain_bulk_pricing_flag=t.domain_bulk_pricing_flag
and dd.product_period_name=t.product_period_name
and dd.product_period_qty=t.product_period_qty
and dd.first_expiry_sequence_flag=t.first_expiry_sequence_flag
and dd.customer_type_name=t.customer_type_name
and dd.pnl_pillar_name=t.pnl_pillar_name
and dd.fin_pnl_category=t.fin_pnl_category
and dd.fin_pnl_line=t.fin_pnl_line
and dd.fin_pnl_subline=t.fin_pnl_subline
and dd.fin_investor_relation_class_name=t.fin_investor_relation_class_name
and dd.fin_investor_relation_subclass_name=t.fin_investor_relation_subclass_name
and dd.fin_investor_relation_segment_name=t.fin_investor_relation_segment_name
where t.as_of_Date is null
   union all
   
select 
   dd.as_of_date
   ,dd.relative_week
,dd.relative_week_period_name
, dd.analysis_type
,dd.region_2_name
,dd.product_family_name
,dd.product_pnl_category_name
,dd.product_pnl_line_name
,dd.product_pnl_version_name
--,dd.product_pnl_subline_name
,dd.payable_bill_line_flag
,dd.domain_bulk_pricing_flag
,dd.product_period_name
,dd.product_period_qty
,dd.first_expiry_sequence_flag
,dd.customer_Type_name
,dd.pnl_pillar_name
,dd.fin_pnl_category
,dd.fin_pnl_line
, dd.fin_pnl_subline
, dd.fin_investor_relation_class_name
, dd.fin_investor_relation_subclass_name
, dd.fin_investor_relation_segment_name

,coalesce(renewal_bill_gcr_usd_amt,0) as renewal_bill_gcr_usd_amt
, coalesce(renewal_bill_product_month_qty,0) as renewal_bill_product_month_qty
 , coalesce (renewal_qty,0) as renewal_qty
,coalesce(expiry_qty,0) AS expiry_qty
from 
  actual_weekly_period_renewals dd
  )
  , cte_final_Results_weekly as 
  
  (select
  
     dd.as_of_date
    ,dd.relative_week
   
,dd.relative_week_period_name
 ,dd.analysis_type
,dd.region_2_name
,dd.product_family_name
,dd.product_pnl_category_name
,dd.product_pnl_line_name
,dd.product_pnl_version_name
--,dd.product_pnl_subline_name
,dd.payable_bill_line_flag
,dd.domain_bulk_pricing_flag
,dd.product_period_name
,dd.product_period_qty
,dd.first_expiry_sequence_flag
,dd.customer_Type_name
,dd.pnl_pillar_name
,dd.fin_pnl_category
,dd.fin_pnl_line,
dd.fin_pnl_subline,
dd.fin_investor_relation_class_name

,dd.fin_investor_relation_subclass_name

,dd.fin_investor_relation_segment_name
 ,dd.renewal_bill_gcr_usd_amt
 ,dd.renewal_bill_product_month_qty

,dd.renewal_qty
 ,dd.expiry_qty
 ,coalesce(fixed_gcr_amt,0) as fixed_gcr_amt
 ,coalesce(fixed_renewal_month_qty,0) as fixed_renewal_month_qty
 ,coalesce(fixed_renewal_qty,0) as fixed_renewal_qty
 ,coalesce(fixed_expiries,0) as fixed_expiries

-- ,coalesce(fixed_cash_renewal_gcr_amt,0) as fixed_cash_renewal_gcr_amt
 from  cte_full_results_weekly dd 
 left join fixed_period_mix_renewals t
 on dd.analysis_type=t.analysis_type
and dd.region_2_name=t.region_2_name
and dd.product_family_name=t.product_family_name
AND dd.product_pnl_category_name=t.product_pnl_category_name
AND dd.product_pnl_line_name=t.product_pnl_line_name
AND dd.product_pnl_version_name=t.product_pnl_version_name
--AND dd.product_pnl_subline_name=t.product_pnl_subline_name
AND dd.payable_bill_line_flag=t.payable_bill_line_flag
AND dd.domain_bulk_pricing_flag=t.domain_bulk_pricing_flag
and dd.product_period_name=t.product_period_name
and dd.product_period_qty=t.product_period_qty
and dd.first_expiry_sequence_flag=t.first_expiry_sequence_flag
and dd.customer_type_name=t.customer_type_name
and dd.pnl_pillar_name=t.pnl_pillar_name
and dd.fin_pnl_category=t.fin_pnl_category
and dd.fin_pnl_line=t.fin_pnl_line
and dd.fin_pnl_subline=t.fin_pnl_subline
and dd.fin_investor_relation_class_name=t.fin_investor_relation_class_name
and dd.fin_investor_relation_subclass_name=t.fin_investor_relation_subclass_name
and dd.fin_investor_relation_segment_name=t.fin_investor_relation_segment_name   
  )
--- weekly


select
     
     dd.as_of_date
   ,dd.relative_week
,dd.relative_week_period_name
 ,dd.analysis_type
,dd.region_2_name
,dd.product_family_name
,dd.product_pnl_category_name
,dd.product_pnl_line_name
,dd.product_pnl_version_name
--,dd.product_pnl_subline_name
,dd.payable_bill_line_flag
,dd.domain_bulk_pricing_flag
,dd.product_period_name
,dd.product_period_qty
,dd.first_expiry_sequence_flag
,dd.customer_Type_name
,dd.pnl_pillar_name
,dd.fin_pnl_category
,dd.fin_pnl_line,
dd.fin_pnl_subline,
dd.fin_investor_relation_class_name

,dd.fin_investor_relation_subclass_name

,dd.fin_investor_relation_segment_name
, dd.renewal_bill_gcr_usd_amt
, dd.renewal_bill_product_month_qty
 , dd.renewal_qty
,dd.expiry_qty

 ,coalesce(fixed_gcr_amt,0) as fixed_gcr_amt
 ,coalesce(fixed_renewal_month_qty,0) as fixed_renewal_month_qty
 ,coalesce(fixed_renewal_qty,0) as fixed_renewal_qty
 ,coalesce(fixed_expiries,0) as fixed_expiries

 ,'Weekly' as date_granularity
  from cte_final_Results_weekly dd 
  where  coalesce(renewal_bill_gcr_usd_amt,0)<>0
  or coalesce(renewal_bill_product_month_qty,0)<>0
  or coalesce(renewal_qty,0)<>0
  or coalesce(expiry_qty,0)<>0
  or coalesce(fixed_gcr_amt,0)<>0
 or coalesce(fixed_renewal_month_qty,0)<>0
  or coalesce(fixed_renewal_qty,0)<>0
  or coalesce(fixed_expiries,0)<>0;







--monthly 







Drop table if exists actual_monthly_period_renewals ;

create temp table actual_monthly_period_renewals 
distkey(product_pnl_version_name)
sortkey(as_of_date)
 as 
select      
  date(date_trunc('month',bill_mst_date)) as as_of_Date,
relative_month,
relative_month_period_name,
analysis_type,
customer_type_name,
pnl_pillar_name,
region_2_name,
product_family_name,
product_pnl_category_name,
product_pnl_line_name,
product_pnl_version_name,
-- product_pnl_subline_name,
payable_bill_line_flag,
domain_bulk_pricing_flag,
first_Expiry_sequence_flag,
product_period_name,
product_period_qty
,fin_pnl_category
,fin_pnl_line,
fin_pnl_subline,
fin_investor_relation_class_name

,fin_investor_relation_subclass_name

,fin_investor_relation_segment_name
,sum(renewal_bill_gcr_usd_amt) as renewal_bill_gcr_usd_amt,
sum(renewal_bill_product_month_qty) as renewal_bill_product_month_qty,
sum (renewal_qty) as renewal_qty,
sum(expiry_qty) AS expiry_qty
    
    from dev.ba_ecommerce.renewals_360_agg  a

    where 
 relative_month_period_name in ('Current Year', 'Prior Year (1)') 
    group by  1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22;
    
    
 

    
    


DROP TABLE IF EXISTS fixed_mix_monthly_actual_renewals;


create temporary table fixed_mix_monthly_actual_renewals as 

with cte_distinct_dates_monthly as 

(
select distinct 
as_of_date
,relative_month
,relative_month_period_name
from actual_monthly_period_renewals

)
, cte_Distinct_dims_monthly as 

(select distinct 
analysis_type,
region_2_name,
product_family_name,
product_pnl_category_name,
product_pnl_line_name,
product_pnl_version_name,
--product_pnl_subline_name,
payable_bill_line_flag,
domain_bulk_pricing_flag,
product_period_name,
product_period_qty,
first_expiry_sequence_flag,
customer_Type_name,
pnl_pillar_name,
fin_pnl_category,
fin_pnl_line,
fin_pnl_subline,
fin_investor_relation_class_name

,fin_investor_relation_subclass_name

,fin_investor_relation_segment_name
from 
actual_monthly_period_renewals
union
select distinct 
analysis_type,
region_2_name,
product_family_name,
product_pnl_category_name,
product_pnl_line_name,
product_pnl_version_name,
--product_pnl_subline_name,
payable_bill_line_flag,
domain_bulk_pricing_flag,
product_period_name,
product_period_qty,
first_expiry_sequence_flag,
customer_Type_name,
pnl_pillar_name,
fin_pnl_category,
fin_pnl_line,
fin_pnl_subline,
fin_investor_relation_class_name

,fin_investor_relation_subclass_name

,fin_investor_relation_segment_name
from fixed_period_mix_renewals
)


  , cte_cross_monthly AS
    (
        SELECT *
        FROM
            cte_distinct_dates_monthly
            CROSS JOIN
                cte_distinct_dims_monthly
    )
  , cte_full_results_monthly AS
   (
   
   select 
   dd.as_of_date
   ,dd.relative_month
,dd.relative_month_period_name
 ,dd.analysis_type
,dd.region_2_name
,dd.product_family_name
,dd.product_pnl_category_name
,dd.product_pnl_line_name
,dd.product_pnl_version_name
--product_pnl_subline_name,
,dd.payable_bill_line_flag
,dd.domain_bulk_pricing_flag
,dd.product_period_name
,dd.product_period_qty
,dd.first_expiry_sequence_flag
,dd.customer_Type_name
,dd.pnl_pillar_name
,dd.fin_pnl_category
,dd.fin_pnl_line
,dd.fin_pnl_subline,
dd.fin_investor_relation_class_name

,dd.fin_investor_relation_subclass_name

,dd.fin_investor_relation_segment_name
,coalesce(renewal_bill_gcr_usd_amt,0) as renewal_bill_gcr_usd_amt
, coalesce(renewal_bill_product_month_qty,0) as renewal_bill_product_month_qty
 , coalesce (renewal_qty,0) as renewal_qty
,coalesce(expiry_qty,0) AS expiry_qty
    

   
  FROM
            cte_cross_monthly dd
            LEFT OUTER JOIN actual_monthly_period_renewals t
            
          on dd.as_of_Date=t.as_of_Date
    and dd.analysis_type=t.analysis_type
AND dd.region_2_name=t.region_2_name
and dd.product_family_name=t.product_family_name
AND dd.product_pnl_category_name=t.product_pnl_category_name
AND dd.product_pnl_line_name=t.product_pnl_line_name
AND dd.product_pnl_version_name=t.product_pnl_version_name
--AND dd.product_pnl_subline_name=t.product_pnl_subline_name
AND dd.payable_bill_line_flag=t.payable_bill_line_flag
AND dd.domain_bulk_pricing_flag=t.domain_bulk_pricing_flag
and dd.product_period_name=t.product_period_name
and dd.product_period_qty=t.product_period_qty
and dd.first_expiry_sequence_flag=t.first_expiry_sequence_flag
and dd.customer_type_name=t.customer_type_name
and dd.pnl_pillar_name=t.pnl_pillar_name
and dd.fin_pnl_category=t.fin_pnl_category
and dd.fin_pnl_line=t.fin_pnl_line
and dd.fin_pnl_subline=t.fin_pnl_subline
and dd.fin_investor_relation_class_name=t.fin_investor_relation_class_name
and dd.fin_investor_relation_subclass_name=t.fin_investor_relation_subclass_name
and dd.fin_investor_relation_segment_name=t.fin_investor_relation_segment_name     
where t.as_of_Date is null
   union all
   
select 
   dd.as_of_date
   ,dd.relative_month
,dd.relative_month_period_name
, dd.analysis_type
,dd.region_2_name
,dd.product_family_name
,dd.product_pnl_category_name
,dd.product_pnl_line_name
,dd.product_pnl_version_name
--,dd.product_pnl_subline_name
,dd.payable_bill_line_flag
,dd.domain_bulk_pricing_flag
,dd.product_period_name
,dd.product_period_qty
,dd.first_expiry_sequence_flag
,dd.customer_Type_name
,dd.pnl_pillar_name
,dd.fin_pnl_category
 ,dd.fin_pnl_line
,dd.fin_pnl_subline
, dd.fin_investor_relation_class_name
 ,dd.fin_investor_relation_subclass_name
, dd.fin_investor_relation_segment_name 

,coalesce(renewal_bill_gcr_usd_amt,0) as renewal_bill_gcr_usd_amt
, coalesce(renewal_bill_product_month_qty,0) as renewal_bill_product_month_qty
 , coalesce (renewal_qty,0) as renewal_qty
,coalesce(expiry_qty,0) AS expiry_qty
from 
  actual_monthly_period_renewals dd
  )
  , cte_final_Results_monthly as 
  
  (select
  
     dd.as_of_date
    ,dd.relative_month
   
,dd.relative_month_period_name
 ,dd.analysis_type
,dd.region_2_name
,dd.product_family_name
,dd.product_pnl_category_name
,dd.product_pnl_line_name
,dd.product_pnl_version_name
--,dd.product_pnl_subline_name
,dd.payable_bill_line_flag
,dd.domain_bulk_pricing_flag
,dd.product_period_name
,dd.product_period_qty
,dd.first_expiry_sequence_flag
,dd.customer_Type_name
,dd.pnl_pillar_name
,dd.fin_pnl_category
 ,dd.fin_pnl_line
,dd.fin_pnl_subline
, dd.fin_investor_relation_class_name
 ,dd.fin_investor_relation_subclass_name
, dd.fin_investor_relation_segment_name 
 ,dd.renewal_bill_gcr_usd_amt
 ,dd.renewal_bill_product_month_qty

,dd.renewal_qty
 ,dd.expiry_qty
 ,coalesce(fixed_gcr_amt,0) as fixed_gcr_amt
 ,coalesce(fixed_renewal_month_qty,0) as fixed_renewal_month_qty
 ,coalesce(fixed_renewal_qty,0) as fixed_renewal_qty
 ,coalesce(fixed_expiries,0) as fixed_expiries

-- ,coalesce(fixed_cash_renewal_gcr_amt,0) as fixed_cash_renewal_gcr_amt
 from  cte_full_results_monthly dd 
 left join fixed_period_mix_renewals t
 on dd.analysis_type=t.analysis_type
and dd.region_2_name=t.region_2_name
and dd.product_family_name=t.product_family_name
AND dd.product_pnl_category_name=t.product_pnl_category_name
AND dd.product_pnl_line_name=t.product_pnl_line_name
AND dd.product_pnl_version_name=t.product_pnl_version_name
--AND dd.product_pnl_subline_name=t.product_pnl_subline_name
AND dd.payable_bill_line_flag=t.payable_bill_line_flag
AND dd.domain_bulk_pricing_flag=t.domain_bulk_pricing_flag
and dd.product_period_name=t.product_period_name
and dd.product_period_qty=t.product_period_qty
and dd.first_expiry_sequence_flag=t.first_expiry_sequence_flag
and dd.customer_type_name=t.customer_type_name
and dd.pnl_pillar_name=t.pnl_pillar_name
and dd.fin_pnl_category=t.fin_pnl_category
and dd.fin_pnl_line=t.fin_pnl_line
and dd.fin_pnl_subline=t.fin_pnl_subline
and dd.fin_investor_relation_class_name=t.fin_investor_relation_class_name
and dd.fin_investor_relation_subclass_name=t.fin_investor_relation_subclass_name
and dd.fin_investor_relation_segment_name=t.fin_investor_relation_segment_name     
  )
--- monthly


select
     
     dd.as_of_date
   ,dd.relative_month
,dd.relative_month_period_name
 ,dd.analysis_type
,dd.region_2_name
,dd.product_family_name
,dd.product_pnl_category_name
,dd.product_pnl_line_name
,dd.product_pnl_version_name
--,dd.product_pnl_subline_name
,dd.payable_bill_line_flag
,dd.domain_bulk_pricing_flag
,dd.product_period_name
,dd.product_period_qty
,dd.first_expiry_sequence_flag
,dd.customer_Type_name
,dd.pnl_pillar_name
,dd.fin_pnl_category
 ,dd.fin_pnl_line
,dd.fin_pnl_subline
, dd.fin_investor_relation_class_name
 ,dd.fin_investor_relation_subclass_name
, dd.fin_investor_relation_segment_name  
, dd.renewal_bill_gcr_usd_amt
, dd.renewal_bill_product_month_qty
 , dd.renewal_qty
,dd.expiry_qty

 ,coalesce(fixed_gcr_amt,0) as fixed_gcr_amt
 ,coalesce(fixed_renewal_month_qty,0) as fixed_renewal_month_qty
 ,coalesce(fixed_renewal_qty,0) as fixed_renewal_qty
 ,coalesce(fixed_expiries,0) as fixed_expiries

 ,'Monthly' as date_granularity
  from cte_final_Results_monthly dd 
  where  coalesce(renewal_bill_gcr_usd_amt,0)<>0
  or coalesce(renewal_bill_product_month_qty,0)<>0
  or coalesce(renewal_qty,0)<>0
  or coalesce(expiry_qty,0)<>0
  or coalesce(fixed_gcr_amt,0)<>0
 or coalesce(fixed_renewal_month_qty,0)<>0
  or coalesce(fixed_renewal_qty,0)<>0
  or coalesce(fixed_expiries,0)<>0;
  
  


   
truncate table ba_ecommerce.renewal_fixed_rate_mix_adjusted_QS;



insert into ba_ecommerce.renewal_fixed_rate_mix_adjusted_QS
Select *, 'Dec24-Feb25'  as Fixed_mix_period  from fixed_mix_daily_actual_renewals;

insert into ba_ecommerce.renewal_fixed_rate_mix_adjusted_QS
Select *,'Dec24-Feb25'  as Fixed_mix_period from fixed_mix_weekly_actual_renewals;


insert into ba_ecommerce.renewal_fixed_rate_mix_adjusted_QS 
Select *, 'Dec24-Feb25'  as Fixed_mix_period from fixed_mix_monthly_actual_renewals;






INSERT INTO dev.ba_ecommerce.renewal_job_alerts 



SELECT
    'Renewals Fixed Mix Adjusted' AS dataset_name,
        MAX(as_of_Date) AS max_date,
           (CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', GETDATE())::date - 1)   as data_expected_date,
    GETDATE() AS run_ts,

 
    CASE
        WHEN COUNT(*) = 0 THEN 'FAILED'
        WHEN MAX(as_of_Date) <   (CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', GETDATE())::date - 1)  THEN 'FAILED'
        ELSE 'SUCCESS'
    END AS status,
    
  COUNT(*) AS row_count,
    CASE
        WHEN COUNT(*) = 0 THEN 'No rows found in dataset'
        WHEN MAX(as_of_Date) <   (CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', GETDATE())::date - 1)  THEN 'Max date is stale'
        ELSE NULL END AS error_message
        
    from  ba_ecommerce.renewal_fixed_rate_mix_adjusted_QS 
    
    
    
    
    
 ;









 




