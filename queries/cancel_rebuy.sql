

--JIRA:  HAT-3923
--https://godaddy-corp.atlassian.net/browse/HAT-3923
Drop table if exists ba_Ecommerce.cancel_rebuy;
create table ba_ecommerce.cancel_rebuy as 


---expirations that got cancelled
with cte as (select distinct resource_id,  prior_bill_shopper_id, product_family_name, 
prior_bill_product_pnl_group_name,prior_bill_product_pnl_line_name, prior_bill_product_pnl_Category_name,
prior_bill_product_pnl_subline_name,prior_bill_product_pnl_version_name,
entitlement_cancel_mst_date
from dev.dna_approved.renewal_360
where entitlement_cancel_mst_date is not null 
and bill_exclude_reason_desc is null
and  lower(prior_bill_product_pnl_group_name) <> 'domains'
and prior_bill_primary_product_flag=true
  --  and coa_resource_id is null 
and entitlement_cancel_mst_date between '2024-01-01' and current_Date-60 )
--and (LOWER(subscription_cancel_by_name ) not LIKE '%migration%'
--    OR LOWER(subscription_cancel_by_name) not LIKE '%migr%'
 --   OR LOWER(subscription_cancel_by_name) not LIKE '%transferaway%') )


,
---new orders
cte_2 as (
select distinct resource_id, prior_bill_modified_mst_date ,  prior_bill_shopper_id , product_family_name, prior_bill_product_pnl_group_name,prior_bill_product_pnl_Category_name,prior_bill_product_pnl_line_name, prior_bill_product_pnl_subline_name,prior_bill_product_pnl_version_name
from dev.dna_approved.renewal_360
where prior_bill_modified_mst_date between '2024-01-01' and current_date -1
and bill_exclude_reason_desc is null 
and prior_bill_product_pnl_new_renewal_name='New Purchase' and prior_bill_sequence_number=1
and prior_bill_primary_product_flag=true
and  lower(prior_bill_product_pnl_group_name) <> 'domains'
)

select
  cte.resource_id,
  cte.product_family_name,
  cte.prior_bill_shopper_id,
  cte.entitlement_cancel_mst_date,
  max(case
        when cte_2.prior_Bill_shopper_id is not null
       
          then 1
        else 0
      end) as cancel_rebuy_flag,
  max(case
        when cte_2.prior_Bill_shopper_id is not null
         and cte.prior_bill_product_pnl_group_name = cte_2.prior_bill_product_pnl_group_name
          then 1
        else 0
      end) as cancel_rebuy_product_pnl_group_flag,
  max(case
        when cte_2.prior_Bill_shopper_id is not null
         and cte.prior_bill_product_pnl_subline_name = cte_2.prior_bill_product_pnl_subline_name
          then 1
        else 0
      end) as cancel_rebuy_product_pnl_category_flag,
  max(case
        when cte_2.prior_Bill_shopper_id is not null
         and cte.prior_bill_product_pnl_line_name = cte_2.prior_bill_product_pnl_line_name
          then 1
        else 0
      end) as cancel_rebuy_product_pnl_line_flag,
  max(case
        when cte_2.prior_Bill_shopper_id is not null
         and cte.prior_bill_product_pnl_version_name = cte_2.prior_bill_product_pnl_version_name
          then 1
        else 0
      end) as cancel_rebuy_product_pnl_version_flag
from cte
left join cte_2
  on cte.prior_bill_shopper_id = cte_2.prior_bill_shopper_id
 and cte.product_family_name = cte_2.product_family_name
 and cte_2.prior_bill_modified_mst_date between dateadd(day, 30, cte.entitlement_cancel_mst_date)
   and dateadd(day, 60, cte.entitlement_cancel_mst_date)
group by
  cte.resource_id,
  cte.product_family_name,
  cte.prior_bill_shopper_id,
  cte.entitlement_cancel_mst_date
  
  
;
  
