/********************************************************************************************************

Version History:

Date              Author             Description
2023-06-05        Ray M           MERPAY-781
*********************************************************************************************************/

----------------------------------------------------------------------------------------------------------
--MMI DATA
-- REMOVING DUPLICATION FROM THE MMI DATA

create or replace table wpay_analytics.mmi_area_industry as
(
select --mt.*
  mt.file_name,
  mt.pdh_load_time,
  mt.payload_id,
  mt.area,
  mt.area_level,
  mt.industry_name,
  mt.industry_code,
  left(mt.period,7) as period,
  mt.spend_index,
  mt.txn_index,
  mt.spend_density_index,
  mt.spend_growth_yoy,
  mt.spend_growth_mom,
  mt.txn_growth_yoy,
  mt.txn_growth_mom,
  mt.spend_density_growth_yoy,
  mt.spend_density_growth_mom,
  mt.avg_ticket,
  mt.avg_visits_per_cust,
  mt.avg_spend_per_cust,
  mt.ecom_ind_rank,
  mt.ecom_txns_share,
  mt.ecom_ind_rank_share
from 
  pdh_rd_external.mmi_area_industry mt
inner join
  (
  select
    area,
    industry_name,
    industry_code,
    left(period,7) as period,
    --count(area),
    max(file_date) as max_date,
    --min(file_date)
  from 
    pdh_rd_external.mmi_area_industry
  group by 1,2,3,4
  ) t
on mt.area = t.area
  and mt.industry_name = t.industry_name
  and mt.industry_code = t.industry_code
  and left(mt.period,7) = left(t.period,7)
  and mt.file_date = t.max_date

);

----------------------------------------------------------------------------------------------------------------
-- Assigning previous month/year MMI AVG Ticket

create or replace table wpay_analytics.mmi_area_industry_edit as
(
  select 
    mmi_area.*,
    prev_y.prev_year as Year_period,
    prev_y.prev_month as Month_period,
    prev_m.prev_avg_ticket as b_avg_ticket_LM,
    prev_y.prev_avg_ticket as b_avg_ticket_LY,
    case when cast(prev_m.prev_avg_ticket as int) <> 0
    then
    ((mmi_area.avg_ticket - prev_m.prev_avg_ticket)/prev_m.prev_avg_ticket)  
    else 0 end 
    as b_avg_ticket_mom,
    case when cast(prev_y.prev_avg_ticket as int) <> 0
    then
    ((mmi_area.avg_ticket - prev_y.prev_avg_ticket)/prev_y.prev_avg_ticket)  
    else 0 end 
    as b_avg_ticket_yoy
from wpay_analytics.mmi_area_industry mmi_area
left join
(
select 
  curr.area,
  curr.industry_code,
  left(curr.period,7) as period,
  left(prev.Period,4) as prev_year,
  right(left(prev.Period,7),2) as prev_month,
  prev.avg_ticket as prev_avg_ticket,
from wpay_analytics.mmi_area_industry curr
left join 
  wpay_analytics.mmi_area_industry prev
on curr.area = prev.area
    and curr.industry_code = prev.industry_code
    and right(left(curr.Period,7),2) = right(left(prev.Period,7),2)
    and (cast(left(curr.Period,4) as int)-1) = cast(left(prev.Period,4) as int)
order by curr.area,curr.industry_code, curr.period
) prev_y
on
  mmi_area.area = prev_y.area
  and mmi_area.industry_code = prev_y.industry_code
  and left(mmi_area.Period,4) = left(prev_y.Period,4)
  and right(left(mmi_area.Period,7),2) = right(left(prev_y.Period,7),2)
left join
(
select 
  curr.area,
  curr.industry_code,
  left(curr.period,7) as period,
  prev.avg_ticket as prev_avg_ticket,
from wpay_analytics.mmi_area_industry curr
left join 
  wpay_analytics.mmi_area_industry prev
on curr.area = prev.area
 and curr.industry_code = prev.industry_code
    and left(cast(date_add(cast(concat(left(curr.Period,7),'-01') as date), interval -1 month) as string),4) = cast(left(prev.Period,4) as string)
    and substring(cast(date_add(cast(concat(left(curr.Period,7),'-01') as date), interval -1 month) as string),6,2)  = right(left(prev.Period,7),2)
order by curr.area,curr.industry_code, curr.period
) prev_m
on
  mmi_area.area = prev_m.area
  and mmi_area.industry_code = prev_m.industry_code
  and left(mmi_area.Period,4) = left(prev_m.Period,4)
  and right(left(mmi_area.Period,7),2) = right(left(prev_m.Period,7),2)
--order by store_trans.Store, store_trans.Year,store_trans.Month
)
;

----------------------------------------------------------------------------------------------------
----- OVERALL BENCHMARKS
-- LGA/STATE

create or replace table pdh_staging_ds.temp_location as (
-------------------------------------------------------------------------------------------------------------------
-- Store base table + mastercard linking and benchmark

with
final_lga as 
(select
  *
from
(
select 
  distinct stores.store_id,
  stores.country,
  stores.state,
  stores.division,
  stores.brand,
  stores.industry,
  stores.industry_name,
  --distinct stores.store_id,
  stores.site_name,
  stores.suburb,
  stores.postcode,
  stores.caid,
  --stores.is_closed,
  stores.company,
  stores.invoice_group,
  stores.merchant_group,
  stores.merchant_portal_mg,
  stores.merchant_portal_cg,
  --stores.is_online,
  stores.LGA as LGA,
  stores.LGA_id as LGA_id
from
  wpay_analytics.ref_store_ind stores
left join
(
  select *
  from
  (select 
  distinct mmi_postcode.Merchant_Post_Code,
  mmi_postcode.City_Name,
  key.level3,
  upper(key.level2) as level2,
  key.level2_id as level2_id
from
  wpay_analytics.ref_merchant_mmi_postcode mmi_postcode
left join
  wpay_analytics.mmi_aus_mapping_key_edit key
on 
  (cast(mmi_postcode.Level_3_Id as string) = cast(key.level3_id as string))
  and (mmi_postcode.Province_Code = key.State)
  ) 
  --where level2 is not null
 ) mmi_location
on 
  (cast(stores.postcode as string) = cast(mmi_location.Merchant_Post_Code as string))
  --and (replace(upper(mmi_location.City_Name),' ','') = replace(upper(stores.suburb),' ',''))
) store_lga
left join
  (
  select 
    area,
    period,
    --industry_name,
    industry_code,
    left(period,4) as Year,
    right(period,2) as Month,
    CASE 
      WHEN spend_growth_yoy = 99999
        THEN NULL
      ELSE spend_growth_yoy
      END AS spend_growth_yoy,
    CASE 
      WHEN spend_growth_mom = 99999
        THEN NULL
      ELSE spend_growth_mom
      END AS spend_growth_mom,
    CASE
      WHEN txn_growth_yoy = 99999
        THEN NULL
      ELSE txn_growth_yoy
      END AS txn_growth_yoy,
    CASE
      WHEN txn_growth_mom = 99999
        THEN NULL
      ELSE txn_growth_mom
      END AS txn_growth_mom,
    CASE
      WHEN spend_density_growth_yoy = 99999
        THEN NULL
      ELSE spend_density_growth_yoy
      END AS spend_density_growth_yoy,
    CASE
      WHEN spend_density_growth_mom = 99999
        THEN NULL
      ELSE spend_density_growth_mom
      END AS spend_density_growth_mom,
    CASE
      WHEN avg_ticket = 99999
        THEN NULL
      ELSE avg_ticket
      END AS avg_ticket,
    b_avg_ticket_LM,
    b_avg_ticket_LY,
    b_avg_ticket_mom,
    b_avg_ticket_yoy,
    CASE
      WHEN avg_visits_per_cust = 99999
        THEN NULL
      ELSE avg_visits_per_cust
      END AS avg_visits_per_cust,
    CASE
      WHEN avg_spend_per_cust = 99999
        THEN NULL
      ELSE avg_spend_per_cust
      END AS avg_spend_per_cust,
    CASE
      WHEN ecom_ind_rank = 99999
        THEN NULL
      ELSE ecom_ind_rank
      END AS ecom_ind_rank,
    CASE
      WHEN ecom_txns_share = 99999
        THEN NULL
      ELSE ecom_txns_share
      END AS ecom_txns_share,
    CASE
      WHEN ecom_ind_rank_share = 99999
        THEN NULL
      ELSE ecom_ind_rank_share
      END AS ecom_ind_rank_share
  from
    wpay_analytics.mmi_area_industry_edit
  where
    area_level = 'local_govt_area'
  ) ind_lga
on 
  ((cast(store_lga.LGA_id as string) = cast(ind_lga.area as string))
  
  and cast(store_lga.industry as string) = cast(ind_lga.industry_code as string)))

----------------------------------------
-- wpay store transactions
,
 store_trans as 
(select 
  amount.Year as Year,
  amount.Month as Month,
  amount.State as State,
  --amount.Scheme as Scheme,
  --amount.Online as Online,
  --amount.Card_Type as Card_Type,
  amount.Store as Store,
  amount.Brand as Brand,
  amount.Suburb as Suburb,
  amount.trans_amount as trans_amount,
  counts.trans_count as trans_count,
from
(
select
  left(cast(trans.transaction_date as string),4) as Year,
  substring(cast(trans.transaction_date as string),6,2) as Month,
 -- right(cast(trans.transaction_date as string),2) as Day,
  trans.state as State,
  --trans.scheme as Scheme,
  --trans.is_online as Online,
  --trans.card_type as Card_Type,
  trans.store_id as Store,
  stores.brand as Brand,
  stores.suburb as Suburb,
  --sum(trans.transaction_count) as trans_count,
  sum(trans.settlement_amount_inc_gst) as trans_amount,
from
  pdh_sf_mep_vw.wpay_all_merchants_daily_fees_summary_hist trans
left join 
  wpay_analytics.ref_store_ind stores
on
  trans.store_id = stores.store_id
where
  (trans.approval_flag = 'A' ) 
  and ((trans.tran_type_description = 'REFUND')
  or (trans.tran_type_description = 'REFUND REVERSAL')
  or (trans.tran_type_description = 'PURCHASE')
  or (trans.tran_type_description = 'PURCHASE REVERSAL')
  or (trans.tran_type_description = 'PURCHASE WITH CASHOUT')
  or (trans.tran_type_description = 'PURCHASE WITH CASHOUT REVERSAL')
  or (trans.tran_type_description = 'REDEMPTION')
  or (trans.tran_type_description = 'REDEMPTION REVERSAL')
  )
group by
  1,2,3,4,5,6
) amount
left join
  (
    select
  left(cast(trans.transaction_date as string),4) as Year,
  substring(cast(trans.transaction_date as string),6,2) as Month,
  --right(cast(trans.transaction_date as string),2) as Day,
  trans.state as State,
  --trans.scheme as Scheme,
  --trans.is_online as Online,
  --trans.card_type as Card_Type,
  trans.store_id as Store,
  stores.brand as Brand,
  stores.suburb as Suburb,
  --sum(trans.transaction_count) as trans_count,
  (sum(case when trans.tran_type_description in 
  ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL') then transaction_count else 0 end)
  -
  sum(case when trans.tran_type_description in 
  ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL') then transaction_count else 0 end)
  ) as trans_count
  --sum(trans.settlement_amount_inc_gst) as trans_amount,
from
  pdh_sf_mep_vw.wpay_all_merchants_daily_fees_summary_hist trans
left join 
  wpay_analytics.ref_store_ind stores
on
  trans.store_id = stores.store_id
where
  (trans.approval_flag = 'A' ) 
  -- and ((trans.tran_type_description = 'PURCHASE')
  -- or (trans.tran_type_description = 'PURCHASE REVERSAL')
  -- or (trans.tran_type_description = 'PURCHASE WITH CASHOUT')
  -- or (trans.tran_type_description = 'PURCHASE WITH CASHOUT REVERSAL')
  -- or (trans.tran_type_description = 'REDEMPTION')
  -- or (trans.tran_type_description = 'REDEMPTION REVERSAL')
  --)
  group by
  1,2,3,4,5,6) counts
on (amount.Year = counts.Year)
   and (amount.Month = counts.Month)
   and (amount.State = counts.State)
   --and (amount.Scheme = counts.Scheme)
   --and (amount.Online = counts.Online)
   --and (amount.Card_Type = counts.Card_Type)
   and (amount.Store = counts.Store)
   and (amount.Brand = counts.Brand)
   and (amount.Suburb = counts.Suburb)
order by Store,year,month)
-------------------------------------------------------------------------------------------------------------

-- Joined on same table to get previous month/year

, store_internal as
(
  select 
    store_trans.*,
    case when cast(prev_m.prev_Amount as int) <> 0 then
    ((store_trans.trans_amount - prev_m.prev_Amount)/prev_m.prev_Amount) 
    else 0 end 
    as trans_amount_mom,
    case when cast(prev_y.prev_Amount as int) <> 0 then
    ((store_trans.trans_amount - prev_y.prev_Amount)/prev_y.prev_Amount) 
    else 0 end 
    as trans_amount_yoy,
    case when cast(prev_m.prev_Count as int) <> 0 then
    ((store_trans.trans_count - prev_m.prev_Count)/prev_m.prev_Count) 
    else 0 end
    as trans_count_mom,
    case when cast(prev_y.prev_Count as int) <> 0 then
    ((store_trans.trans_count - prev_y.prev_Count)/prev_y.prev_Count) 
    else 0 end
    as trans_count_yoy,
    prev_m.prev_Amount as trans_amount_LM,
    prev_y.prev_Amount as trans_amount_LY,
    prev_m.prev_Count as trans_count_LM,
    prev_y.prev_Count as trans_count_LY,
    case when cast(store_trans.trans_count as int) <> 0 then
    (store_trans.trans_amount/store_trans.trans_count) 
    else 0 end 
    as trans_value,
    case when cast(prev_m.prev_Count as int) <> 0
    then
    (prev_m.prev_Amount/prev_m.prev_Count)  
    else 0 end 
    as trans_value_LM,
    case when cast(prev_y.prev_Count as int) <> 0
    then
    (prev_y.prev_Amount/prev_y.prev_Count)  
    else 0 end 
    as trans_value_LY
from store_trans
left join
(
select 
  curr.*,
  prev.Year as prev_year,
  prev.Month as prev_month,
  prev.trans_amount as prev_Amount,
  prev.trans_count as prev_Count
from store_trans curr
left join 
  store_trans prev
on curr.Store = prev.Store
    and curr.Month = prev.Month
    and (cast(curr.Year as int)-1) = cast(prev.Year as int)
order by curr.Store,curr.Year,curr.Month
) prev_y
on
  store_trans.Store = prev_y.Store
  and store_trans.Year = prev_y.Year
  and store_trans.Month = prev_y.Month
left join
(
select 
  curr.*,
  prev.trans_amount as prev_Amount,
  prev.trans_count as prev_Count
from store_trans curr
left join 
  store_trans prev
on curr.Store = prev.Store
    and left(cast(date_add(cast(concat(concat(concat(concat(curr.Year,'-'),curr.Month),'-'),'01') as date), interval -1 month) as string),4) = cast(prev.Year as string)
    and substring(cast(date_add(cast(concat(concat(concat(concat(curr.Year,'-'),curr.Month),'-'),'01') as date), interval -1 month) as string),6,2)  = prev.Month
order by curr.Store,curr.Year,curr.Month
) prev_m
on
  store_trans.Store = prev_m.Store
  and store_trans.Year = prev_m.Year
  and store_trans.Month = prev_m.Month
order by store_trans.Store, store_trans.Year,store_trans.Month
)

---------------------------------------------------------------------------------------------------------------
,
wpay_store_trans as
  (select 
    distinct stores.store_id,
    stores.country,
    stores.state,
    stores.division,
    stores.brand,
    stores.company,
    stores.invoice_group,
    stores.merchant_group,
    stores.merchant_portal_mg,
    stores.merchant_portal_cg,
    stores.industry,
    stores.industry_name,
    --distinct stores.store_id,
    stores.site_name,
    stores.suburb,
    stores.postcode,
    stores.LGA,
    stores.LGA_id,
    stores.caid,
    --stores.is_closed,
    --stores.is_online,
    store_internal.Year as Year,
    store_internal.Month as Month,
    store_internal.trans_amount as trans_amount,
    store_internal.trans_count as trans_count,
    store_internal.trans_amount_mom as trans_amount_mom,
    store_internal.trans_amount_yoy as trans_amount_yoy,
    store_internal.trans_count_mom as trans_count_mom,
    store_internal.trans_count_yoy as trans_count_yoy,
    store_internal.trans_amount_LM as trans_amount_LM,
    store_internal.trans_amount_LY as trans_amount_LY,
    store_internal.trans_count_LM as trans_count_LM,
    store_internal.trans_count_LY as trans_count_LY,
    store_internal.trans_value as trans_value,
    store_internal.trans_value_LM as trans_value_LM,
    store_internal.trans_value_LY as trans_value_LY
  from
    wpay_analytics.ref_store_ind stores
  inner join
    store_internal
  on stores.store_id = store_internal.Store
  --group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33
  order by store_id
  )

-------------------------------------------
--- adding only LGA to wpay_trans

, wpay_LGA as
 (select 
    wpay_store_trans.*,
    --final_lga.LGA,
    --final_lga.LGA_id,
    final_lga.area,
    --final_lga.industry_name,
    final_lga.spend_growth_yoy,
    final_lga.spend_growth_mom,
    final_lga.txn_growth_yoy,
    final_lga.txn_growth_mom,
    final_lga.spend_density_growth_yoy,
    final_lga.spend_density_growth_mom,
    final_lga.avg_ticket,
    final_lga.b_avg_ticket_LM,
    final_lga.b_avg_ticket_LY,
    final_lga.b_avg_ticket_mom,
    final_lga.b_avg_ticket_yoy,
    final_lga.avg_visits_per_cust,
    final_lga.avg_spend_per_cust,
    final_lga.ecom_ind_rank,
    final_lga.ecom_txns_share,
    final_lga.ecom_ind_rank_share,
    --final_lga.Benchmark
  from 
    wpay_store_trans
  left join 
    final_lga
  on (wpay_store_trans.store_id = final_lga.store_id)
    and (wpay_store_trans.Year = final_lga.Year)
    --and (wpay_store_trans.Month = final_lga.Month)
    and (wpay_store_trans.Month = final_lga.Month)
  --where wpay_store_trans.Year ='2022'
  --    and wpay_store_trans.Month ='07'
  --order by final_lga.Benchmark
 )

------------------------------------------------------------
---- adding benchmark 
,

MaxMMIDate as (
select max(Period) as max_date
from  wpay_analytics.mmi_area_industry_edit
),

lga_bench as 
(
  select *,
  case when 
    (coalesce(spend_growth_yoy,spend_growth_mom,txn_growth_yoy,txn_growth_mom) is null 
    and concat(concat(Year,'-'),Month) <= (select max_date from MaxMMIDate))
    then 'State'
    else 'LGA'
    end as Benchmark
    from wpay_LGA
)


--------------------------------------------
--- going over LGA and filling nulls with State
, lga_benchmark as
(
  select
    'Store' as Aggregation_Level,
    lga_bench.store_id,
    'All' as store_status,
    lga_bench.country,
    lga_bench.state,
    lga_bench.division,
    lga_bench.brand,
    lga_bench.industry,
    lga_bench.site_name,
    lga_bench.suburb,
    cast(lga_bench.postcode as string) as postcode,
    cast(lga_bench.caid as string)  as caid,
    --cast(lga_bench.is_closed as string) as is_closed,
    lga_bench.company,
    lga_bench.invoice_group,
    lga_bench.merchant_group,
    lga_bench.merchant_portal_mg,
    lga_bench.merchant_portal_cg,
    'All' as is_online,
    'All' as scheme,
    'All' as card_type,
    lga_bench.LGA,
    cast(lga_bench.LGA_id as string) as LGA_id,
    lga_bench.area,
    lga_bench.industry_name,
    --final_lga.industry_code,
    --wpay_LGA.Period,
    concat(lga_bench.Year,concat('-',lga_bench.Month)) as Period,
    lga_bench.Year,
    lga_bench.Month,
    lga_bench.trans_amount as w_trans_amount,
    lga_bench.trans_count as w_trans_count,
    lga_bench.trans_value as w_trans_value,
    lga_bench.trans_amount_LM as w_trans_amount_LM,
    lga_bench.trans_count_LM as w_trans_count_LM,
    lga_bench.trans_value_LM as w_trans_value_LM,
    lga_bench.trans_amount_LY as w_trans_amount_LY,
    lga_bench.trans_count_LY as w_trans_count_LY,
    lga_bench.trans_value_LY as w_trans_value_LY,
    lga_bench.trans_amount_mom as w_trans_amount_mom,
    lga_bench.trans_amount_yoy as w_trans_amount_yoy,
    lga_bench.trans_count_mom as w_trans_count_mom,
    lga_bench.trans_count_yoy as w_trans_count_yoy,
    case when cast(lga_bench.trans_value_LM as int) <> 0
    then ((lga_bench.trans_value - lga_bench.trans_value_LM)/lga_bench.trans_value_LM)  
    else 0 end 
    as w_trans_value_mom,
    case when cast(lga_bench.trans_value_LY as int) <> 0
    then ((lga_bench.trans_value - lga_bench.trans_value_LY)/lga_bench.trans_value_LY)  
    else 0 end 
    as w_trans_value_yoy,
    case when cust_info.unique_customer_count <> 0 then
    lga_bench.trans_count/cust_info.unique_customer_count 
    else 0 end as avg_cust_visits,
    case when cust_info.unique_customer_count <> 0 then
    lga_bench.trans_amount/cust_info.unique_customer_count 
    else 0 end as avg_cust_spend,
    cust_info.unique_card_count as unique_cust,
    cust_info.unique_customer_count as unique_card,
    CASE 
      WHEN lga_bench.Benchmark = 'State'
        THEN ind_key.spend_growth_yoy
      ELSE lga_bench.spend_growth_yoy
      END AS b_spend_growth_yoy,
    CASE 
      WHEN lga_bench.Benchmark = 'State'
        THEN ind_key.spend_growth_mom
      ELSE lga_bench.spend_growth_mom
      END AS b_spend_growth_mom,
    CASE
      WHEN lga_bench.Benchmark = 'State'
        THEN ind_key.txn_growth_yoy
      ELSE lga_bench.txn_growth_yoy
      END AS b_txn_growth_yoy,
    CASE
      WHEN lga_bench.Benchmark = 'State'
        THEN ind_key.txn_growth_mom
      ELSE lga_bench.txn_growth_mom
      END AS b_txn_growth_mom,
    CASE
      WHEN lga_bench.Benchmark = 'State'
        THEN ind_key.spend_density_growth_yoy
      ELSE lga_bench.spend_density_growth_yoy
      END AS b_spend_density_growth_yoy,
    CASE
      WHEN lga_bench.Benchmark = 'State'
        THEN ind_key.spend_density_growth_mom
      ELSE lga_bench.spend_density_growth_mom
      END AS b_spend_density_growth_mom,
    CASE
      WHEN lga_bench.Benchmark = 'State'
        THEN ind_key.avg_ticket
      ELSE lga_bench.avg_ticket
      END AS b_avg_ticket,
    CASE
      WHEN lga_bench.Benchmark = 'State'
        THEN ind_key.b_avg_ticket_LM
      ELSE lga_bench.b_avg_ticket_LM
      END AS b_avg_ticket_LM,
    CASE
      WHEN lga_bench.Benchmark = 'State'
        THEN ind_key.b_avg_ticket_LY
      ELSE lga_bench.b_avg_ticket_LY
      END AS b_avg_ticket_LY,
    CASE
      WHEN lga_bench.Benchmark = 'State'
        THEN ind_key.b_avg_ticket_mom
      ELSE lga_bench.b_avg_ticket_mom
      END AS b_avg_ticket_mom,
    CASE
      WHEN lga_bench.Benchmark = 'State'
        THEN ind_key.b_avg_ticket_yoy
      ELSE lga_bench.b_avg_ticket_yoy
      END AS b_avg_ticket_yoy,
    CASE
      WHEN lga_bench.Benchmark = 'State'
        THEN ind_key.avg_visits_per_cust
      ELSE lga_bench.avg_visits_per_cust
      END AS b_avg_visits_per_cust,
    CASE
      WHEN lga_bench.Benchmark = 'State'
        THEN ind_key.avg_spend_per_cust
      ELSE lga_bench.avg_spend_per_cust
      END AS b_avg_spend_per_cust,
    CASE
      WHEN lga_bench.Benchmark = 'State'
        THEN ind_key.ecom_ind_rank
      ELSE lga_bench.ecom_ind_rank
      END AS b_ecom_ind_rank,
    CASE
      WHEN lga_bench.Benchmark = 'State'
        THEN ind_key.ecom_txns_share
      ELSE lga_bench.ecom_txns_share
      END AS b_ecom_txns_share,
    CASE
      WHEN lga_bench.Benchmark = 'State'
        THEN ind_key.ecom_ind_rank_share
      ELSE lga_bench.ecom_ind_rank_share
      END AS b_ecom_ind_rank_share,
    --lga_bench.Benchmark,
    lga_bench.industry_name as Benchmark,
    lga_bench.Benchmark as Benchmark_Location,
    'External' as Benchmark_lvl
  from 
    lga_bench
  left join
    ( select distinct ind.*, 
    key.State as state,
    left(ind.period,4) as Year,
    right(ind.period,2) as Month
    from
    wpay_analytics.mmi_area_industry_edit ind
    left join 
      wpay_analytics.mmi_aus_mapping_key_edit key
      on cast(ind.area as string) = cast(key.level1_id as string)
    where area_level = 'state') ind_key
  on cast(lga_bench.state as string) = cast(ind_key.state as string)
     and cast(lga_bench.industry as string) = cast(ind_key.industry_code as string) 
     --and (final_lga.period = ind_key.period ) )
     and cast(trim(lga_bench.Year) as string) = cast(trim(ind_key.Year) as string) 
     --and cast(trim(lga_bench.Month) as string) = cast(trim(ind_key.Month) as string) 
     and (cast(lga_bench.Month as int) = cast(ind_key.Month as int))
  left join
    (
  select 
      --m.company,
      --m.invoice_group,    
      --m.merchant_group,
      --m.division, 
      --m.brand,    
      m.store_id,   
      --m.site_name,    
     -- m.suburb,   
     -- m.state,    
      --m.postcode,
     -- m.country,
      --m.is_online,
      --fee.calc_scheme as scheme,
      --fee.calc_card_type as card_type,
      left(cast(fee.switch_tran_date as string),7) Transaction_Date,
      -- sum(case when fee.calc_tran_type_description in
      -- ('REFUND','REFUND REVERSAL','PURCHASE','PURCHASE REVERSAL','PURCHASE WITH CASHOUT','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION','REDEMPTION REVERSAL') then fee.transaction_total else 0 end)/100 as trans_amount,
      -- (sum(case when fee.calc_tran_type_description in
      -- ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL') then 1 else 0 end)
      -- -
      -- sum(case when fee.calc_tran_type_description in 
      -- ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL') then 1 else 0 end)
      -- ) as trans_count,
      count(distinct fee.card_id) unique_card_count,
      count(distinct fee.card_id) unique_customer_count
      -- (count(1) / count(distinct fee.card_id)) frequency_of_visits,
      -- (  sum(case when fee.calc_tran_type_description in
      -- ('REFUND','REFUND REVERSAL','PURCHASE','PURCHASE REVERSAL','PURCHASE WITH CASHOUT','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION','REDEMPTION REVERSAL') then fee.transaction_total else 0 end) 
      -- / (100*count(distinct fee.card_id))) spend_per_card_holder 
      from wpay_analytics.wpay_billing_engine_tran_dtl fee  
      inner join
      `pdh_ref_ds.ref_store_details` m 
      on LEFT(FEE.net_term_id, 5) = M.store_id
      --inner join
      --wpay_analytics.wpay_card_identity_model cim
      --on trim(cim.scdr_token) = trim(fee.scdr_token)
      where fee.calc_approval_flag = 'A'
	  and left(cast(fee.tstamp_trans as string),7) > '2022-07'
and fee.calc_tran_type_description in ('REFUND','REFUND REVERSAL','PURCHASE','PURCHASE REVERSAL','PURCHASE WITH CASHOUT','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION','REDEMPTION REVERSAL')
      group by 1,2
      ) cust_info
  on cust_info.store_id = lga_bench.store_id
     and concat(lga_bench.Year,concat('-',lga_bench.Month)) = cust_info.Transaction_Date
  order by lga_bench.Year, lga_bench.Month, lga_bench.store_id
)
-- end
--------------------------------------------------------------------------------------------------------------

--------------------------------------------------------------------------------------------------------------
-- STATE/AUS
--------------------------------------------------------------------------------------------------------------

-- Store base table + mastercard linking and benchmark

,
 final_state as 
(select
  *
from
(
select 
  stores.country,
  stores.state,
  stores.division,
  stores.brand,
  stores.company,
  stores.industry,
  stores.industry_name,
  --distinct stores.store_id,
  --stores.site_name,
  --stores.suburb,
  --stores.postcode,
  --stores.caid,
  --stores.is_closed,
  --stores.company,
  stores.invoice_group,
  stores.merchant_group,
  stores.merchant_portal_mg,
  stores.merchant_portal_cg
  --stores.is_online,
from
  wpay_analytics.ref_store_ind stores
group by 1,2,3,4,5,6,7,8,9,10,11) store_info
left join
  (
  select 
    area,
    period,
   -- industry_name,
    industry_code,
    left(period,4) as Year,
    right(period,2) as Month,
    key.State as state_mmi,
    CASE 
      WHEN spend_growth_yoy = 99999
        THEN NULL
      ELSE spend_growth_yoy
      END AS spend_growth_yoy,
    CASE 
      WHEN spend_growth_mom = 99999
        THEN NULL
      ELSE spend_growth_mom
      END AS spend_growth_mom,
    CASE
      WHEN txn_growth_yoy = 99999
        THEN NULL
      ELSE txn_growth_yoy
      END AS txn_growth_yoy,
    CASE
      WHEN txn_growth_mom = 99999
        THEN NULL
      ELSE txn_growth_mom
      END AS txn_growth_mom,
    CASE
      WHEN spend_density_growth_yoy = 99999
        THEN NULL
      ELSE spend_density_growth_yoy
      END AS spend_density_growth_yoy,
    CASE
      WHEN spend_density_growth_mom = 99999
        THEN NULL
      ELSE spend_density_growth_mom
      END AS spend_density_growth_mom,
    CASE
      WHEN avg_ticket = 99999
        THEN NULL
      ELSE avg_ticket
      END AS avg_ticket,
    b_avg_ticket_LM,
    b_avg_ticket_LY,
    b_avg_ticket_mom,
    b_avg_ticket_yoy,
    CASE
      WHEN avg_visits_per_cust = 99999
        THEN NULL
      ELSE avg_visits_per_cust
      END AS avg_visits_per_cust,
    CASE
      WHEN avg_spend_per_cust = 99999
        THEN NULL
      ELSE avg_spend_per_cust
      END AS avg_spend_per_cust,
    CASE
      WHEN ecom_ind_rank = 99999
        THEN NULL
      ELSE ecom_ind_rank
      END AS ecom_ind_rank,
    CASE
      WHEN ecom_txns_share = 99999
        THEN NULL
      ELSE ecom_txns_share
      END AS ecom_txns_share,
    CASE
      WHEN ecom_ind_rank_share = 99999
        THEN NULL
      ELSE ecom_ind_rank_share
      END AS ecom_ind_rank_share
  from
    wpay_analytics.mmi_area_industry_edit
  left join
    wpay_analytics.mmi_aus_mapping_key_edit key
    on cast(key.level1_id as string) = cast(area as string)
  where
    area_level = 'state'
  group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22
  ) ind_lga
on 
  cast(store_info.state as string) = cast(ind_lga.state_mmi as string)
  and cast(store_info.industry as string) = cast(ind_lga.industry_code as string)
)

----------------------------------------
-- wpay store transactions
,
 store_trans_state as 
(select 
  amount.Year as Year,
  amount.Month as Month,
  amount.Country as Country,
  amount.State as State,
  --amount.Scheme as Scheme,
  --amount.Online as Online,
  --amount.Card_Type as Card_Type,
  --amount.Store as Store,
  amount.industry as Industry,
  amount.industry_name as industry_name,
  amount.Brand as Brand,
  amount.division as division,
  amount.company as company,
  amount.invoice_group as invoice_group,
  amount.merchant_group as merchant_group,
  amount.merchant_portal_mg as merchant_portal_mg,
  amount.merchant_portal_cg as merchant_portal_cg,
  --amount.Suburb as Suburb,
  amount.trans_amount as trans_amount,
  counts.trans_count as trans_count,
from
(
select
  left(cast(trans.transaction_date as string),4) as Year,
  substring(cast(trans.transaction_date as string),6,2) as Month,
 -- right(cast(trans.transaction_date as string),2) as Day,
  stores.country as Country,
  trans.state as State,
  --trans.scheme as Scheme,
  --trans.is_online as Online,
  --trans.card_type as Card_Type,
  --trans.store_id as Store,
  stores.industry as Industry,
  stores.industry_name as industry_name,
  stores.brand as Brand,
  stores.division as division,
  stores.company as company,
  stores.invoice_group as invoice_group,
  stores.merchant_group as merchant_group,
  stores.merchant_portal_mg as merchant_portal_mg,
  stores.merchant_portal_cg as merchant_portal_cg,
  --stores.suburb as Suburb,
  --sum(trans.transaction_count) as trans_count,
  sum(trans.settlement_amount_inc_gst) as trans_amount,
from
  pdh_sf_mep_vw.wpay_all_merchants_daily_fees_summary_hist trans
left join 
  wpay_analytics.ref_store_ind stores
on
  trans.store_id = stores.store_id
where
  (trans.approval_flag = 'A' ) 
  and ((trans.tran_type_description = 'REFUND')
  or (trans.tran_type_description = 'REFUND REVERSAL')
  or (trans.tran_type_description = 'PURCHASE')
  or (trans.tran_type_description = 'PURCHASE REVERSAL')
  or (trans.tran_type_description = 'PURCHASE WITH CASHOUT')
  or (trans.tran_type_description = 'PURCHASE WITH CASHOUT REVERSAL')
  or (trans.tran_type_description = 'REDEMPTION')
  or (trans.tran_type_description = 'REDEMPTION REVERSAL')
  )
group by
  1,2,3,4,5,6,7,8,9,10,11,12,13
) amount
left join
  (
    select
  left(cast(trans.transaction_date as string),4) as Year,
  substring(cast(trans.transaction_date as string),6,2) as Month,
  --right(cast(trans.transaction_date as string),2) as Day,
  stores.country as Country,
  trans.state as State,
  --trans.scheme as Scheme,
  --trans.is_online as Online,
  --trans.card_type as Card_Type,
  --trans.store_id as Store,
  stores.industry as Industry,
  stores.brand as Brand,
  stores.division as division,
  stores.company as company,
  stores.invoice_group as invoice_group,
  stores.merchant_group as merchant_group,
  stores.merchant_portal_mg as merchant_portal_mg,
  stores.merchant_portal_cg as merchant_portal_cg,
  --stores.suburb as Suburb,
  --sum(trans.transaction_count) as trans_count,
  (sum(case when trans.tran_type_description in 
  ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL') then transaction_count else 0 end)
  -
  sum(case when trans.tran_type_description in 
  ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL') then transaction_count else 0 end)
  ) as trans_count
  --sum(trans.settlement_amount_inc_gst) as trans_amount,
from
  pdh_sf_mep_vw.wpay_all_merchants_daily_fees_summary_hist trans
left join 
  wpay_analytics.ref_store_ind  stores
on
  trans.store_id = stores.store_id
where
  (trans.approval_flag = 'A' ) 
  -- and ((trans.tran_type_description = 'PURCHASE')
  -- or (trans.tran_type_description = 'PURCHASE REVERSAL')
  -- or (trans.tran_type_description = 'PURCHASE WITH CASHOUT')
  -- or (trans.tran_type_description = 'PURCHASE WITH CASHOUT REVERSAL')
  -- or (trans.tran_type_description = 'REDEMPTION')
  -- or (trans.tran_type_description = 'REDEMPTION REVERSAL')
  -- )
  group by
  1,2,3,4,5,6,7,8,9,10,11,12) counts
on (amount.Year = counts.Year)
   and (amount.Month = counts.Month)
   and (amount.State = counts.State)
   --and (amount.Scheme = counts.Scheme)
   --and (amount.Online = counts.Online)
   --and (amount.Card_Type = counts.Card_Type)
   --and (amount.Store = counts.Store)
   and (amount.Brand = counts.Brand)
   and (amount.division = counts.division)
   and (amount.company = counts.company)
   and (amount.invoice_group = counts.invoice_group)
   and (amount.merchant_group = counts.merchant_group)
   and (amount.merchant_portal_mg = counts.merchant_portal_mg)
   and (amount.merchant_portal_cg = counts.merchant_portal_cg)
   --and (amount.Suburb = counts.Suburb))
)
---------------------------------------------------------------------------------------------------------

-- Joined on same table to get previous month/year

, state_internal as
(
  select 
    store_trans_state.*,
    case when cast(prev_m.prev_Amount as int) <> 0 then
    ((store_trans_state.trans_amount - prev_m.prev_Amount)/prev_m.prev_Amount) 
    else 0 end
    as trans_amount_mom,
    case when cast(prev_y.prev_Amount as int) <> 0 then
    ((store_trans_state.trans_amount - prev_y.prev_Amount)/prev_y.prev_Amount) 
    else 0 end
    as trans_amount_yoy,
    case when cast(prev_m.prev_Count as int) <> 0 then
    ((store_trans_state.trans_count - prev_m.prev_Count)/prev_m.prev_Count) 
    else 0 end
    as trans_count_mom,
    case when cast(prev_y.prev_Count as int) <> 0 then
    ((store_trans_state.trans_count - prev_y.prev_Count)/prev_y.prev_Count) 
    else 0 end
    as trans_count_yoy,
    prev_m.prev_Amount as trans_amount_LM,
    prev_m.prev_Count as trans_count_LM,
    prev_y.prev_Amount as trans_amount_LY,
    prev_y.prev_Count as trans_count_LY,
    case when cast(store_trans_state.trans_count as int) <> 0 then
    (store_trans_state.trans_amount/store_trans_state.trans_count) 
    else 0 end 
    as trans_value,
    case when cast(prev_m.prev_Count as int) <> 0
    then
    (prev_m.prev_Amount/prev_m.prev_Count)  
    else 0 end 
    as trans_value_LM,
    case when cast(prev_y.prev_Count as int) <> 0
    then
    (prev_y.prev_Amount/prev_y.prev_Count)  
    else 0 end 
    as trans_value_LY
from store_trans_state
left join
(
select 
  curr.*,
  prev.Year as prev_year,
  prev.Month as prev_month,
  prev.trans_amount as prev_Amount,
  prev.trans_count as prev_Count
from store_trans_state curr
left join 
  store_trans_state prev
on  (curr.State = prev.State)
   and (curr.Brand = prev.Brand)
   and (curr.division = prev.division)
   and (curr.company = prev.company)
   and (curr.invoice_group = prev.invoice_group)
   and (curr.merchant_group = prev.merchant_group)
   and (curr.merchant_portal_mg = prev.merchant_portal_mg)
   and (curr.merchant_portal_cg = prev.merchant_portal_cg)
    and curr.Month = prev.Month
    and (cast(curr.Year as int)-1) = cast(prev.Year as int)
order by curr.State,curr.Year,curr.Month
) prev_y
on
  (store_trans_state.State = prev_y.State)
   and (store_trans_state.Brand = prev_y.Brand)
   and (store_trans_state.division = prev_y.division)
   and (store_trans_state.company = prev_y.company)
   and (store_trans_state.invoice_group = prev_y.invoice_group)
   and (store_trans_state.merchant_group = prev_y.merchant_group)
   and (store_trans_state.merchant_portal_mg = prev_y.merchant_portal_mg)
   and (store_trans_state.merchant_portal_cg = prev_y.merchant_portal_cg)
  and store_trans_state.Year = prev_y.Year
  and store_trans_state.Month = prev_y.Month
left join
(
select 
  curr.*,
  prev.trans_amount as prev_Amount,
  prev.trans_count as prev_Count
from store_trans_state curr
left join 
  store_trans_state prev
on (curr.State = prev.State)
   and (curr.Brand = prev.Brand)
   and (curr.division = prev.division)
   and (curr.company = prev.company)
   and (curr.invoice_group = prev.invoice_group)
   and (curr.merchant_group = prev.merchant_group)
   and (curr.merchant_portal_mg = prev.merchant_portal_mg)
   and (curr.merchant_portal_cg = prev.merchant_portal_cg)
    and left(cast(date_add(cast(concat(concat(concat(concat(curr.Year,'-'),curr.Month),'-'),'01') as date), interval -1 month) as string),4) = cast(prev.Year as string)
    and substring(cast(date_add(cast(concat(concat(concat(concat(curr.Year,'-'),curr.Month),'-'),'01') as date), interval -1 month) as string),6,2)  = prev.Month
order by curr.State,curr.Year,curr.Month
) prev_m
on
  (store_trans_state.State = prev_m.State)
   and (store_trans_state.Brand = prev_m.Brand)
   and (store_trans_state.division = prev_m.division)
   and (store_trans_state.company = prev_m.company)
   and (store_trans_state.invoice_group = prev_m.invoice_group)
   and (store_trans_state.merchant_group = prev_m.merchant_group)
   and (store_trans_state.merchant_portal_mg = prev_m.merchant_portal_mg)
   and (store_trans_state.merchant_portal_cg = prev_m.merchant_portal_cg)
  and store_trans_state.Year = prev_m.Year
  and store_trans_state.Month = prev_m.Month
order by store_trans_state.State, store_trans_state.Year,store_trans_state.Month
)

---------------------------------------------------------------------------------------------------------

--- adding only LGA to wpay_trans

, wpay_State as
 (select 
    state_internal.*,
    final_state.area,
    --final_state.industry_name,
    final_state.spend_growth_yoy,
    final_state.spend_growth_mom,
    final_state.txn_growth_yoy,
    final_state.txn_growth_mom,
    final_state.spend_density_growth_yoy,
    final_state.spend_density_growth_mom,
    final_state.avg_ticket,
    final_state.b_avg_ticket_LM,
    final_state.b_avg_ticket_LY,
    final_state.b_avg_ticket_mom,
    final_state.b_avg_ticket_yoy,
    final_state.avg_visits_per_cust,
    final_state.avg_spend_per_cust,
    final_state.ecom_ind_rank,
    final_state.ecom_txns_share,
    final_state.ecom_ind_rank_share,
    --final_lga.Benchmark
  from 
    state_internal
  left join 
    final_state
  on (state_internal.state = final_state.state)
    and (state_internal.Brand = final_state.Brand)
    and (state_internal.division = final_state.division)
    and (state_internal.company = final_state.company)
    and (state_internal.invoice_group = final_state.invoice_group)
    and (state_internal.merchant_group = final_state.merchant_group)
    and (state_internal.merchant_portal_mg = final_state.merchant_portal_mg)
    and (state_internal.merchant_portal_cg = final_state.merchant_portal_cg)
    and (state_internal.Year = final_state.Year)
    --and (state_internal.Month = final_state.Month)
    and (cast(state_internal.Month as int) = cast(final_state.Month as int))
  --where state_internal.Year ='2022'
  --    and state_internal.Month ='07'
  --order by final_lga.Benchmark
 )

------------------------------------------------------------
---- adding benchmark 
,
state_bench as 
(
  select *,
  case when 
    (coalesce(spend_growth_yoy,spend_growth_mom,txn_growth_yoy,txn_growth_mom) is null 
    and concat(concat(Year,'-'),Month) <= (select max_date from MaxMMIDate))
    then 'Country'
    else 'State'
    end as Benchmark
    from wpay_State
)


--------------------------------------------
--- going over LGA and filling nulls with State
, state_benchmark as
(
  select
    'State' as Aggregation_Level,
    'All' as store_id,
    'All' as store_status,
    state_bench.country,
    state_bench.state,
    state_bench.division,
    state_bench.brand,
    state_bench.industry,
    'All' as site_name,
    'All' as suburb,
    'All' as postcode,
    'All' as caid,
    --'All' as is_closed,
    state_bench.company,
    state_bench.invoice_group,
    state_bench.merchant_group,
    state_bench.merchant_portal_mg,
    state_bench.merchant_portal_cg,
    'All' as is_online,
    'All' as scheme,
    'All' as card_type,
    'All' as LGA,
    'All' as LGA_id,
    state_bench.area,
    state_bench.industry_name,
    --final_lga.industry_code,
    --wpay_LGA.Period,
    concat(state_bench.Year,concat('-',state_bench.Month)) as Period,
    state_bench.Year,
    state_bench.Month,
    state_bench.trans_amount as w_trans_amount,
    state_bench.trans_count as w_trans_count,
    state_bench.trans_value as w_trans_value,
    state_bench.trans_amount_LM as w_trans_amount_LM,
    state_bench.trans_count_LM as w_trans_count_LM,
    state_bench.trans_value_LM as w_trans_value_LM,
    state_bench.trans_amount_LY as w_trans_amount_LY,
    state_bench.trans_count_LY as w_trans_count_LY,
    state_bench.trans_value_LY as w_trans_value_LY,
    state_bench.trans_amount_mom as w_trans_amount_mom,
    state_bench.trans_amount_yoy as w_trans_amount_yoy,
    state_bench.trans_count_mom as w_trans_count_mom,
    state_bench.trans_count_yoy as w_trans_count_yoy,
    case when cast(state_bench.trans_value_LM as int) <> 0
    then ((state_bench.trans_value - state_bench.trans_value_LM)/state_bench.trans_value_LM)  
    else 0 end 
    as w_trans_value_mom,
    case when cast(state_bench.trans_value_LY as int) <> 0
    then ((state_bench.trans_value - state_bench.trans_value_LY)/state_bench.trans_value_LY)  
    else 0 end 
    as w_trans_value_yoy,
    case when cust_info.unique_customer_count <> 0 then
    state_bench.trans_count/cust_info.unique_customer_count 
    else 0 end as avg_cust_visits,
    case when cust_info.unique_customer_count <> 0 then
    state_bench.trans_amount/cust_info.unique_customer_count 
    else 0 end as avg_cust_spend,
    cust_info.unique_card_count as unique_cust,
    cust_info.unique_customer_count as unique_card,
    CASE 
      WHEN state_bench.Benchmark = 'Country'
        THEN ind_key.spend_growth_yoy
      ELSE state_bench.spend_growth_yoy
      END AS b_spend_growth_yoy,
    CASE 
      WHEN state_bench.Benchmark = 'Country'
        THEN ind_key.spend_growth_mom
      ELSE state_bench.spend_growth_mom
      END AS b_spend_growth_mom,
    CASE
      WHEN state_bench.Benchmark = 'Country'
        THEN ind_key.txn_growth_yoy
      ELSE state_bench.txn_growth_yoy
      END AS b_txn_growth_yoy,
    CASE
      WHEN state_bench.Benchmark = 'Country'
        THEN ind_key.txn_growth_mom
      ELSE state_bench.txn_growth_mom
      END AS b_txn_growth_mom,
    CASE
      WHEN state_bench.Benchmark = 'Country'
        THEN ind_key.spend_density_growth_yoy
      ELSE state_bench.spend_density_growth_yoy
      END AS b_spend_density_growth_yoy,
    CASE
      WHEN state_bench.Benchmark = 'Country'
        THEN ind_key.spend_density_growth_mom
      ELSE state_bench.spend_density_growth_mom
      END AS b_spend_density_growth_mom,
    CASE
      WHEN state_bench.Benchmark = 'Country'
        THEN ind_key.avg_ticket
      ELSE state_bench.avg_ticket
      END AS b_avg_ticket,
      CASE
      WHEN state_bench.Benchmark = 'Country'
        THEN ind_key.b_avg_ticket_LM
      ELSE state_bench.b_avg_ticket_LM
      END AS b_avg_ticket_LM,
      CASE
      WHEN state_bench.Benchmark = 'Country'
        THEN ind_key.b_avg_ticket_LY
      ELSE state_bench.b_avg_ticket_LY
      END AS b_avg_ticket_LY,
      CASE
      WHEN state_bench.Benchmark = 'Country'
        THEN ind_key.b_avg_ticket_mom
      ELSE state_bench.b_avg_ticket_mom
      END AS b_avg_ticket_mom,
      CASE
      WHEN state_bench.Benchmark = 'Country'
        THEN ind_key.b_avg_ticket_yoy
      ELSE state_bench.b_avg_ticket_yoy
      END AS b_avg_ticket_yoy,
    CASE
      WHEN state_bench.Benchmark = 'Country'
        THEN ind_key.avg_visits_per_cust
      ELSE state_bench.avg_visits_per_cust
      END AS b_avg_visits_per_cust,
    CASE
      WHEN state_bench.Benchmark = 'Country'
        THEN ind_key.avg_spend_per_cust
      ELSE state_bench.avg_spend_per_cust
      END AS b_avg_spend_per_cust,
    CASE
      WHEN state_bench.Benchmark = 'Country'
        THEN ind_key.ecom_ind_rank
      ELSE state_bench.ecom_ind_rank
      END AS b_ecom_ind_rank,
    CASE
      WHEN state_bench.Benchmark = 'Country'
        THEN ind_key.ecom_txns_share
      ELSE state_bench.ecom_txns_share
      END AS b_ecom_txns_share,
    CASE
      WHEN state_bench.Benchmark = 'Country'
        THEN ind_key.ecom_ind_rank_share
      ELSE state_bench.ecom_ind_rank_share
      END AS b_ecom_ind_rank_share,
    --state_bench.Benchmark,
    state_bench.industry_name as Benchmark,
    state_bench.Benchmark as Benchmark_Location,
    'External' as Benchmark_lvl
  from 
    state_bench
  left join
    ( select distinct ind.*, 
    left(ind.period,4) as Year,
    right(ind.period,2) as Month
    from
    wpay_analytics.mmi_area_industry_edit ind
    where area_level = 'Country') ind_key
  on cast(state_bench.industry as string) = cast(ind_key.industry_code as string) 
     --and (final_lga.period = ind_key.period ) )
     and cast(trim(state_bench.Year) as string) = cast(trim(ind_key.Year) as string) 
     --and cast(trim(state_bench.Month) as string) = cast(trim(ind_key.Month) as string)
     and (cast(state_bench.Month as int) = cast(ind_key.Month as int)) 
  left join
    (
  select 
      m.company,
      m.invoice_group,    
      m.merchant_group,
      m.division, 
      m.brand, 
      m.merchant_portal_mg,
      m.merchant_portal_cg,   
      --m.store_id,   
      --m.site_name,    
     -- m.suburb,   
      m.state,    
      --m.postcode,
     -- m.country,
      --m.is_online,
      --fee.calc_scheme as scheme,
      --fee.calc_card_type as card_type,
      left(cast(fee.switch_tran_date as string),7) Transaction_Date,
      -- sum(case when fee.calc_tran_type_description in
      -- ('REFUND','REFUND REVERSAL','PURCHASE','PURCHASE REVERSAL','PURCHASE WITH CASHOUT','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION','REDEMPTION REVERSAL') then fee.transaction_total else 0 end)/100 as trans_amount,
      -- (sum(case when fee.calc_tran_type_description in
      -- ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL') then 1 else 0 end)
      -- -
      -- sum(case when fee.calc_tran_type_description in 
      -- ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL') then 1 else 0 end)
      -- ) as trans_count,
      count(distinct fee.card_id) unique_card_count,
      count(distinct fee.card_id) unique_customer_count
      -- (count(1) / count(distinct fee.card_id)) frequency_of_visits,
      -- (  sum(case when fee.calc_tran_type_description in
      -- ('REFUND','REFUND REVERSAL','PURCHASE','PURCHASE REVERSAL','PURCHASE WITH CASHOUT','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION','REDEMPTION REVERSAL') then fee.transaction_total else 0 end) 
      -- / (100*count(distinct fee.card_id))) spend_per_card_holder 
      from wpay_analytics.wpay_billing_engine_tran_dtl fee  
      inner join
      `pdh_ref_ds.ref_store_details` m 
      on LEFT(FEE.net_term_id, 5) = M.store_id
      --inner join
      --wpay_analytics.wpay_card_identity_model cim
      --on trim(cim.scdr_token) = trim(fee.scdr_token)
      where fee.calc_approval_flag = 'A'
	  and left(cast(fee.tstamp_trans as string),7) > '2022-07'
and fee.calc_tran_type_description in ('REFUND','REFUND REVERSAL','PURCHASE','PURCHASE REVERSAL','PURCHASE WITH CASHOUT','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION','REDEMPTION REVERSAL')
      group by 1,2,3,4,5,6,7,8,9
      ) cust_info
  on cust_info.company = state_bench.company
    and cust_info.merchant_group = state_bench.merchant_group
    and cust_info.division = state_bench.division
    and cust_info.state = state_bench.state
    and cust_info.invoice_group = state_bench.invoice_group
    and cust_info.brand = state_bench.brand
    and cust_info.merchant_portal_mg = state_bench.merchant_portal_mg
    and cust_info.merchant_portal_cg = state_bench.merchant_portal_cg
    and concat(state_bench.Year,concat('-',state_bench.Month)) = cust_info.Transaction_Date
  order by state_bench.Benchmark
)
-- end

--------------------------------------------------------------------------------------------------------------
-- END
--------------------------------------------------------------------------------------------------------------

--------------------------------------------------------------------------------------------------------------
-- Country
--------------------------------------------------------------------------------------------------------------
-- Store base table + mastercard linking and benchmark

,
 final_country as 
(select
  *
from
(
select 
  stores.country,
  --stores.state,
  stores.division,
  stores.brand,
  stores.company,
  stores.industry,
  stores.industry_name,
  --distinct stores.store_id,
  --stores.site_name,
  --stores.suburb,
  --stores.postcode,
  --stores.caid,
  --stores.is_closed,
  --stores.company,
  stores.invoice_group,
  stores.merchant_group,
  stores.merchant_portal_mg,
  stores.merchant_portal_cg
  --stores.is_online,
from
  wpay_analytics.ref_store_ind stores
group by 1,2,3,4,5,6,7,8,9,10) store_info
left join
  (
  select 
    area,
    period,
   -- industry_name,
    industry_code,
    left(period,4) as Year,
    right(period,2) as Month,
    CASE 
      WHEN spend_growth_yoy = 99999
        THEN NULL
      ELSE spend_growth_yoy
      END AS spend_growth_yoy,
    CASE 
      WHEN spend_growth_mom = 99999
        THEN NULL
      ELSE spend_growth_mom
      END AS spend_growth_mom,
    CASE
      WHEN txn_growth_yoy = 99999
        THEN NULL
      ELSE txn_growth_yoy
      END AS txn_growth_yoy,
    CASE
      WHEN txn_growth_mom = 99999
        THEN NULL
      ELSE txn_growth_mom
      END AS txn_growth_mom,
    CASE
      WHEN spend_density_growth_yoy = 99999
        THEN NULL
      ELSE spend_density_growth_yoy
      END AS spend_density_growth_yoy,
    CASE
      WHEN spend_density_growth_mom = 99999
        THEN NULL
      ELSE spend_density_growth_mom
      END AS spend_density_growth_mom,
    CASE
      WHEN avg_ticket = 99999
        THEN NULL
      ELSE avg_ticket
      END AS avg_ticket,
    b_avg_ticket_LM,
    b_avg_ticket_LY,
    b_avg_ticket_mom,
    b_avg_ticket_yoy,
    CASE
      WHEN avg_visits_per_cust = 99999
        THEN NULL
      ELSE avg_visits_per_cust
      END AS avg_visits_per_cust,
    CASE
      WHEN avg_spend_per_cust = 99999
        THEN NULL
      ELSE avg_spend_per_cust
      END AS avg_spend_per_cust,
    CASE
      WHEN ecom_ind_rank = 99999
        THEN NULL
      ELSE ecom_ind_rank
      END AS ecom_ind_rank,
    CASE
      WHEN ecom_txns_share = 99999
        THEN NULL
      ELSE ecom_txns_share
      END AS ecom_txns_share,
    CASE
      WHEN ecom_ind_rank_share = 99999
        THEN NULL
      ELSE ecom_ind_rank_share
      END AS ecom_ind_rank_share
  from
    wpay_analytics.mmi_area_industry_edit
  where
    area_level = 'country'
  group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21
  ) ind_lga
on 
 cast(store_info.industry as string) = cast(ind_lga.industry_code as string)
)

----------------------------------------
-- wpay store transactions
,
 store_trans_country as 
(select 
  amount.Year as Year,
  amount.Month as Month,
  amount.Country as Country,
  --amount.State as State,
  --amount.Scheme as Scheme,
  --amount.Online as Online,
  --amount.Card_Type as Card_Type,
  --amount.Store as Store,
  amount.industry as Industry,
  amount.industry_name as industry_name,
  amount.Brand as Brand,
  amount.division as division,
  amount.company as company,
  amount.invoice_group as invoice_group,
  amount.merchant_group as merchant_group,
  amount.merchant_portal_mg as merchant_portal_mg,
  amount.merchant_portal_cg as merchant_portal_cg,
  --amount.Suburb as Suburb,
  amount.trans_amount as trans_amount,
  counts.trans_count as trans_count,
from
(
select
  left(cast(trans.transaction_date as string),4) as Year,
  substring(cast(trans.transaction_date as string),6,2) as Month,
 -- right(cast(trans.transaction_date as string),2) as Day,
  stores.country as Country,
  --trans.state as State,
  --trans.scheme as Scheme,
  --trans.is_online as Online,
  --trans.card_type as Card_Type,
  --trans.store_id as Store,
  stores.industry as Industry,
  stores.industry_name as industry_name,
  stores.brand as Brand,
  stores.division as division,
  stores.company as company,
  stores.invoice_group as invoice_group,
  stores.merchant_group as merchant_group,
  stores.merchant_portal_mg as merchant_portal_mg,
  stores.merchant_portal_cg as merchant_portal_cg,
  --stores.suburb as Suburb,
  --sum(trans.transaction_count) as trans_count,
  sum(trans.settlement_amount_inc_gst) as trans_amount,
from
  pdh_sf_mep_vw.wpay_all_merchants_daily_fees_summary_hist trans
left join 
  wpay_analytics.ref_store_ind stores
on
  trans.store_id = stores.store_id
where
  (trans.approval_flag = 'A' ) 
  and ((trans.tran_type_description = 'REFUND')
  or (trans.tran_type_description = 'REFUND REVERSAL')
  or (trans.tran_type_description = 'PURCHASE')
  or (trans.tran_type_description = 'PURCHASE REVERSAL')
  or (trans.tran_type_description = 'PURCHASE WITH CASHOUT')
  or (trans.tran_type_description = 'PURCHASE WITH CASHOUT REVERSAL')
  or (trans.tran_type_description = 'REDEMPTION')
  or (trans.tran_type_description = 'REDEMPTION REVERSAL')
  )
group by
  1,2,3,4,5,6,7,8,9,10,11,12
) amount
left join
  (
    select
  left(cast(trans.transaction_date as string),4) as Year,
  substring(cast(trans.transaction_date as string),6,2) as Month,
  --right(cast(trans.transaction_date as string),2) as Day,
  stores.country as Country,
  --trans.state as State,
  --trans.scheme as Scheme,
  --trans.is_online as Online,
  --trans.card_type as Card_Type,
  --trans.store_id as Store,
  stores.industry as Industry,
  stores.brand as Brand,
  stores.division as division,
  stores.company as company,
  stores.invoice_group as invoice_group,
  stores.merchant_group as merchant_group,
  stores.merchant_portal_mg as merchant_portal_mg,
  stores.merchant_portal_cg as merchant_portal_cg,
  --stores.suburb as Suburb,
  --sum(trans.transaction_count) as trans_count,
  (sum(case when trans.tran_type_description in 
  ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL') then transaction_count else 0 end)
  -
  sum(case when trans.tran_type_description in 
  ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL') then transaction_count else 0 end)
  ) as trans_count
  --sum(trans.settlement_amount_inc_gst) as trans_amount,
from
  pdh_sf_mep_vw.wpay_all_merchants_daily_fees_summary_hist trans
left join 
  wpay_analytics.ref_store_ind  stores
on
  trans.store_id = stores.store_id
where
  (trans.approval_flag = 'A' ) 
  -- and ((trans.tran_type_description = 'PURCHASE')
  -- or (trans.tran_type_description = 'PURCHASE REVERSAL')
  -- or (trans.tran_type_description = 'PURCHASE WITH CASHOUT')
  -- or (trans.tran_type_description = 'PURCHASE WITH CASHOUT REVERSAL')
  -- or (trans.tran_type_description = 'REDEMPTION')
  -- or (trans.tran_type_description = 'REDEMPTION REVERSAL')
  -- )
  group by
  1,2,3,4,5,6,7,8,9,10,11) counts
on (amount.Year = counts.Year)
   and (amount.Month = counts.Month)
   --and (amount.State = counts.State)
   --and (amount.Scheme = counts.Scheme)
   --and (amount.Online = counts.Online)
   --and (amount.Card_Type = counts.Card_Type)
   --and (amount.Store = counts.Store)
   and (amount.Brand = counts.Brand)
   and (amount.division = counts.division)
   and (amount.company = counts.company)
   and (amount.invoice_group = counts.invoice_group)
   and (amount.merchant_group = counts.merchant_group)
   and (amount.merchant_portal_mg = counts.merchant_portal_mg)
   and (amount.merchant_portal_cg = counts.merchant_portal_cg)
   --and (amount.Suburb = counts.Suburb))
)
------------------------------------------------------------------------------------------------------------

, country_internal as
(
  select 
    store_trans_country.*,
    case when prev_m.prev_Amount <> 0 then
    ((store_trans_country.trans_amount - prev_m.prev_Amount)/prev_m.prev_Amount) 
    else 0 end as trans_amount_mom,
    case when prev_y.prev_Amount <> 0 then
    ((store_trans_country.trans_amount - prev_y.prev_Amount)/prev_y.prev_Amount) 
    else 0 end as trans_amount_yoy,
    case when cast(prev_m.prev_Count as int) <> 0 then
    ((store_trans_country.trans_count - prev_m.prev_Count)/prev_m.prev_Count) 
    else 0 end 
    as trans_count_mom,
    case when cast(prev_y.prev_Count as int) <> 0 then
    ((store_trans_country.trans_count - prev_y.prev_Count)/prev_y.prev_Count) 
    else 0 end 
    as trans_count_yoy,
    prev_m.prev_Amount as trans_amount_LM,
    prev_m.prev_Count as trans_count_LM,
    prev_y.prev_Amount as trans_amount_LY,
    prev_y.prev_Count as trans_count_LY,
    case when cast(store_trans_country.trans_count as int) <> 0 then
    (store_trans_country.trans_amount/store_trans_country.trans_count) 
    else 0 end 
    as trans_value,
    case when cast(prev_m.prev_Count as int) <> 0
    then
    (prev_m.prev_Amount/prev_m.prev_Count)  
    else 0 end 
    as trans_value_LM,
    case when cast(prev_y.prev_Count as int) <> 0
    then
    (prev_y.prev_Amount/prev_y.prev_Count)  
    else 0 end 
    as trans_value_LY
from store_trans_country
left join
(
select 
  curr.*,
  prev.Year as prev_year,
  prev.Month as prev_month,
  prev.trans_amount as prev_Amount,
  prev.trans_count as prev_Count
from store_trans_country curr
left join 
  store_trans_country prev
on  
   (curr.Brand = prev.Brand)
   and (curr.division = prev.division)
   and (curr.company = prev.company)
   and (curr.invoice_group = prev.invoice_group)
   and (curr.merchant_group = prev.merchant_group)
   and (curr.merchant_portal_mg = prev.merchant_portal_mg)
   and (curr.merchant_portal_cg = prev.merchant_portal_cg)
    and curr.Month = prev.Month
    and (cast(curr.Year as int)-1) = cast(prev.Year as int)
order by curr.Year,curr.Month
) prev_y
on
    (store_trans_country.Brand = prev_y.Brand)
   and (store_trans_country.division = prev_y.division)
   and (store_trans_country.company = prev_y.company)
   and (store_trans_country.invoice_group = prev_y.invoice_group)
   and (store_trans_country.merchant_group = prev_y.merchant_group)
   and (store_trans_country.merchant_portal_mg = prev_y.merchant_portal_mg)
   and (store_trans_country.merchant_portal_cg = prev_y.merchant_portal_cg)
  and store_trans_country.Year = prev_y.Year
  and store_trans_country.Month = prev_y.Month
left join
(
select 
  curr.*,
  prev.trans_amount as prev_Amount,
  prev.trans_count as prev_Count
from store_trans_country curr
left join 
  store_trans_country prev
on 
    (curr.Brand = prev.Brand)
   and (curr.division = prev.division)
   and (curr.company = prev.company)
   and (curr.invoice_group = prev.invoice_group)
   and (curr.merchant_group = prev.merchant_group)
   and (curr.merchant_portal_mg = prev.merchant_portal_mg)
   and (curr.merchant_portal_cg = prev.merchant_portal_cg)
    and left(cast(date_add(cast(concat(concat(concat(concat(curr.Year,'-'),curr.Month),'-'),'01') as date), interval -1 month) as string),4) = cast(prev.Year as string)
    and substring(cast(date_add(cast(concat(concat(concat(concat(curr.Year,'-'),curr.Month),'-'),'01') as date), interval -1 month) as string),6,2)  = prev.Month
order by curr.Year,curr.Month
) prev_m
on
    (store_trans_country.Brand = prev_m.Brand)
   and (store_trans_country.division = prev_m.division)
   and (store_trans_country.company = prev_m.company)
   and (store_trans_country.invoice_group = prev_m.invoice_group)
   and (store_trans_country.merchant_group = prev_m.merchant_group)
   and (store_trans_country.merchant_portal_mg = prev_m.merchant_portal_mg)
   and (store_trans_country.merchant_portal_cg = prev_m.merchant_portal_cg)
  and store_trans_country.Year = prev_m.Year
  and store_trans_country.Month = prev_m.Month
order by store_trans_country.Year,store_trans_country.Month
)

-----------------------------------------------------------------------------------------------------------

--- adding only LGA to wpay_trans

, wpay_country as
 (select 
    country_internal.*,
    final_country.area,
    --final_country.industry_name,
    final_country.spend_growth_yoy,
    final_country.spend_growth_mom,
    final_country.txn_growth_yoy,
    final_country.txn_growth_mom,
    final_country.spend_density_growth_yoy,
    final_country.spend_density_growth_mom,
    final_country.avg_ticket,
    final_country.b_avg_ticket_LM,
    final_country.b_avg_ticket_LY,
    final_country.b_avg_ticket_mom,
    final_country.b_avg_ticket_yoy,
    final_country.avg_visits_per_cust,
    final_country.avg_spend_per_cust,
    final_country.ecom_ind_rank,
    final_country.ecom_txns_share,
    final_country.ecom_ind_rank_share,
    --final_lga.Benchmark
  from 
    country_internal
  left join 
    final_country
  on 
    (country_internal.Brand = final_country.Brand)
    and (country_internal.division = final_country.division)
    and (country_internal.company = final_country.company)
    and (country_internal.invoice_group = final_country.invoice_group)
    and (country_internal.merchant_group = final_country.merchant_group)
    and (country_internal.merchant_portal_mg = final_country.merchant_portal_mg)
    and (country_internal.merchant_portal_cg = final_country.merchant_portal_cg)
    and (country_internal.Year = final_country.Year)
    --and (country_internal.Month = final_country.Month)
    and (cast(country_internal.Month as int) = cast(final_country.Month as int))
  --where country_internal.Year ='2022'
  --    and country_internal.Month ='07'
  --order by final_lga.Benchmark
 )

------------------------------------------------------------
---- adding benchmark 
,
country_bench as 
(
  select *,
  'Country' as Benchmark
  from wpay_country
)


--------------------------------------------
--- going over LGA and filling nulls with State
, country_benchmark as
(
  select
    'Country' as Aggregation_Level,
    'All' as store_id,
    'All' as store_status,
    country_bench.country,
    'All' as state,
    country_bench.division,
    country_bench.brand,
    country_bench.industry,
    'All' as site_name,
    'All' as suburb,
    'All' as postcode,
    'All' as caid,
    --'All' as is_closed,
    country_bench.company,
    country_bench.invoice_group,
    country_bench.merchant_group,
    country_bench.merchant_portal_mg,
    country_bench.merchant_portal_cg,
    'All' as is_online,
    'All' as scheme,
    'All' as card_type,
    'All' as LGA,
    'All' as LGA_id,
    country_bench.area,
    country_bench.industry_name,
    --final_lga.industry_code,
    --wpay_LGA.Period,
    concat(country_bench.Year,concat('-',country_bench.Month)) as Period,
    country_bench.Year,
    country_bench.Month,
    country_bench.trans_amount as w_trans_amount,
    country_bench.trans_count as w_trans_count,
    country_bench.trans_value as w_trans_value,
    country_bench.trans_amount_LM as w_trans_amount_LM,
    country_bench.trans_count_LM as w_trans_count_LM,
    country_bench.trans_value_LM as w_trans_value_LM,
    country_bench.trans_amount_LY as w_trans_amount_LY,
    country_bench.trans_count_LY as w_trans_count_LY,
    country_bench.trans_value_LY as w_trans_value_LY,
    country_bench.trans_amount_mom as w_trans_amount_mom,
    country_bench.trans_amount_yoy as w_trans_amount_yoy,
    country_bench.trans_count_mom as w_trans_count_mom,
    country_bench.trans_count_yoy as w_trans_count_yoy,
    case when cast(country_bench.trans_value_LM as int) <> 0
    then ((country_bench.trans_value - country_bench.trans_value_LM)/country_bench.trans_value_LM)  
    else 0 end 
    as w_trans_value_mom,
    case when cast(country_bench.trans_value_LY as int) <> 0
    then ((country_bench.trans_value - country_bench.trans_value_LY)/country_bench.trans_value_LY)  
    else 0 end 
    as w_trans_value_yoy,
    case when cust_info.unique_customer_count <> 0 then
    country_bench.trans_count/cust_info.unique_customer_count 
    else 0 end as avg_cust_visits,
    case when cust_info.unique_customer_count <> 0 then
    country_bench.trans_amount/cust_info.unique_customer_count 
    else 0 end as avg_cust_spend,
    cust_info.unique_card_count as unique_cust,
    cust_info.unique_customer_count as unique_card,
    country_bench.spend_growth_yoy as b_spend_growth_yoy,
    country_bench.spend_growth_mom as b_spend_growth_mom,
    country_bench.txn_growth_yoy as b_txn_growth_yoy,
    country_bench.txn_growth_mom as b_txn_growth_mom,
    country_bench.spend_density_growth_yoy as b_spend_density_growth_yoy,
    country_bench.spend_density_growth_mom as b_spend_density_growth_mom,
    country_bench.avg_ticket as b_avg_ticket,
    country_bench.b_avg_ticket_LM as b_avg_ticket_LM,
    country_bench.b_avg_ticket_LY as b_avg_ticket_LY,
    country_bench.b_avg_ticket_mom as b_avg_ticket_mom,
    country_bench.b_avg_ticket_yoy as b_avg_ticket_yoy,
    country_bench.avg_visits_per_cust as b_avg_visits_per_cust,
    country_bench.avg_spend_per_cust as b_avg_spend_per_cust,
    country_bench.ecom_ind_rank as b_ecom_ind_rank,
    country_bench.ecom_txns_share as b_ecom_txns_share,
    country_bench.ecom_ind_rank_share as b_ecom_ind_rank_share,
    --country_bench.Benchmark,
    country_bench.industry_name as Benchmark,
    country_bench.Benchmark as Benchmark_Location,
    'External' as Benchmark_lvl
  from 
    country_bench
  left join
    (
  select 
      m.company,
      m.invoice_group,    
      m.merchant_group,
      m.division, 
      m.brand, 
      m.merchant_portal_mg,
      m.merchant_portal_cg,   
      --m.store_id,   
      --m.site_name,    
     -- m.suburb,   
      --m.state,    
      --m.postcode,
      m.country,
      --m.is_online,
      --fee.calc_scheme as scheme,
      --fee.calc_card_type as card_type,
      left(cast(fee.switch_tran_date as string),7) Transaction_Date,
      -- sum(case when fee.calc_tran_type_description in
      -- ('REFUND','REFUND REVERSAL','PURCHASE','PURCHASE REVERSAL','PURCHASE WITH CASHOUT','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION','REDEMPTION REVERSAL') then fee.transaction_total else 0 end)/100 as trans_amount,
      -- (sum(case when fee.calc_tran_type_description in
      -- ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL') then 1 else 0 end)
      -- -
      -- sum(case when fee.calc_tran_type_description in 
      -- ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL') then 1 else 0 end)
      -- ) as trans_count,
      count(distinct fee.card_id) unique_card_count,
      count(distinct fee.card_id) unique_customer_count
      -- (count(1) / count(distinct fee.card_id)) frequency_of_visits,
      -- (  sum(case when fee.calc_tran_type_description in
      -- ('REFUND','REFUND REVERSAL','PURCHASE','PURCHASE REVERSAL','PURCHASE WITH CASHOUT','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION','REDEMPTION REVERSAL') then fee.transaction_total else 0 end) 
      -- / (100*count(distinct fee.card_id))) spend_per_card_holder 
      from wpay_analytics.wpay_billing_engine_tran_dtl fee  
      inner join
      `pdh_ref_ds.ref_store_details` m 
      on LEFT(FEE.net_term_id, 5) = M.store_id
      --inner join
      --wpay_analytics.wpay_card_identity_model cim
      --on trim(cim.scdr_token) = trim(fee.scdr_token)
      where fee.calc_approval_flag = 'A'
	  and left(cast(fee.tstamp_trans as string),7) > '2022-07'
and fee.calc_tran_type_description in ('REFUND','REFUND REVERSAL','PURCHASE','PURCHASE REVERSAL','PURCHASE WITH CASHOUT','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION','REDEMPTION REVERSAL')
      group by 1,2,3,4,5,6,7,8,9
      ) cust_info
  on cust_info.company = country_bench.company
    and cust_info.merchant_group = country_bench.merchant_group
    and cust_info.division = country_bench.division
    and cust_info.country = country_bench.country
    and cust_info.invoice_group = country_bench.invoice_group
    and cust_info.brand = country_bench.brand
    and cust_info.merchant_portal_mg = country_bench.merchant_portal_mg
    and cust_info.merchant_portal_cg = country_bench.merchant_portal_cg
    and concat(country_bench.Year,concat('-',country_bench.Month)) = cust_info.Transaction_Date
)
-- end

select 
  *
  from lga_benchmark
union all
select 
  *
  from state_benchmark
union all
select 
  *
  from country_benchmark

);

--------------------------------------------------------------------------------------------------------------
-- END
--------------------------------------------------------------------------------------------------------------

-- FILTERS
-- ONLINE/OFFLINE + SCHEME + CARD TYPE

create or replace table pdh_staging_ds.temp_filters_combo as (
-------------------------------------------------------------------------------------------------------------------
-- LGA/STATE
-------------------------------------------------------------------------------------------------------------------
-- Store base table + mastercard linking and benchmark

with
final_lga_filt as 
(select
  *
from
(
select 
  distinct stores.store_id,
  stores.country,
  stores.state,
  stores.division,
  stores.brand,
  stores.industry,
  stores.industry_name,
  --distinct stores.store_id,
  stores.site_name,
  stores.suburb,
  stores.postcode,
  stores.caid,
  --stores.is_closed,
  stores.company,
  stores.invoice_group,
  stores.merchant_group,
  stores.merchant_portal_mg,
  stores.merchant_portal_cg,
  --stores.is_online,
  stores.LGA as LGA,
  stores.LGA_id as LGA_id
from
  wpay_analytics.ref_store_ind stores
left join
(
  select *
  from
  (select 
  distinct mmi_postcode.Merchant_Post_Code,
  mmi_postcode.City_Name,
  key.level3,
  upper(key.level2) as level2,
  key.level2_id as level2_id
from
  wpay_analytics.ref_merchant_mmi_postcode mmi_postcode
left join
  wpay_analytics.mmi_aus_mapping_key_edit key
on 
  (cast(mmi_postcode.Level_3_Id as string) = cast(key.level3_id as string))
  and (mmi_postcode.Province_Code = key.State)
  ) 
  --where level2 is not null
 ) mmi_location
on 
  (cast(stores.postcode as string) = cast(mmi_location.Merchant_Post_Code as string))
  --and (replace(upper(mmi_location.City_Name),' ','') = replace(upper(stores.suburb),' ',''))
) store_lga
left join
  (
  select 
    area,
    period,
    --industry_name,
    industry_code,
    left(period,4) as Year,
    right(period,2) as Month,
    CASE 
      WHEN spend_growth_yoy = 99999
        THEN NULL
      ELSE spend_growth_yoy
      END AS spend_growth_yoy,
    CASE 
      WHEN spend_growth_mom = 99999
        THEN NULL
      ELSE spend_growth_mom
      END AS spend_growth_mom,
    CASE
      WHEN txn_growth_yoy = 99999
        THEN NULL
      ELSE txn_growth_yoy
      END AS txn_growth_yoy,
    CASE
      WHEN txn_growth_mom = 99999
        THEN NULL
      ELSE txn_growth_mom
      END AS txn_growth_mom,
    CASE
      WHEN spend_density_growth_yoy = 99999
        THEN NULL
      ELSE spend_density_growth_yoy
      END AS spend_density_growth_yoy,
    CASE
      WHEN spend_density_growth_mom = 99999
        THEN NULL
      ELSE spend_density_growth_mom
      END AS spend_density_growth_mom,
    CASE
      WHEN avg_ticket = 99999
        THEN NULL
      ELSE avg_ticket
      END AS avg_ticket,
    b_avg_ticket_LM,
    b_avg_ticket_LY,
    b_avg_ticket_mom,
    b_avg_ticket_yoy,
    CASE
      WHEN avg_visits_per_cust = 99999
        THEN NULL
      ELSE avg_visits_per_cust
      END AS avg_visits_per_cust,
    CASE
      WHEN avg_spend_per_cust = 99999
        THEN NULL
      ELSE avg_spend_per_cust
      END AS avg_spend_per_cust,
    CASE
      WHEN ecom_ind_rank = 99999
        THEN NULL
      ELSE ecom_ind_rank
      END AS ecom_ind_rank,
    CASE
      WHEN ecom_txns_share = 99999
        THEN NULL
      ELSE ecom_txns_share
      END AS ecom_txns_share,
    CASE
      WHEN ecom_ind_rank_share = 99999
        THEN NULL
      ELSE ecom_ind_rank_share
      END AS ecom_ind_rank_share
  from
    wpay_analytics.mmi_area_industry_edit
  where
    area_level = 'local_govt_area'
  ) ind_lga
on 
  ((cast(store_lga.LGA_id as string) = cast(ind_lga.area as string))
  and cast(store_lga.industry as string) = cast(ind_lga.industry_code as string)))

----------------------------------------
-- wpay store transactions
,
  store_trans_filt as 
(select 
  amount.Year as Year,
  amount.Month as Month,
  amount.State as State,
  amount.Scheme as Scheme,
  amount.is_online as is_online,
  amount.scheme_card_type as scheme_card_type,
  amount.Store as Store,
  amount.Brand as Brand,
  amount.Suburb as Suburb,
  amount.trans_amount as trans_amount,
  counts.trans_count as trans_count,
from
(
select
  left(cast(trans.transaction_date as string),4) as Year,
  substring(cast(trans.transaction_date as string),6,2) as Month,
 -- right(cast(trans.transaction_date as string),2) as Day,
  trans.state as State,
  trans.scheme as Scheme,
  trans.is_online as is_online,
  trans.scheme_card_type as scheme_card_type,
  trans.store_id as Store,
  stores.brand as Brand,
  stores.suburb as Suburb,
  --sum(trans.transaction_count) as trans_count,
  sum(trans.settlement_amount_inc_gst) as trans_amount,
from
  pdh_sf_mep_vw.wpay_all_merchants_daily_fees_summary_hist trans
left join 
  wpay_analytics.ref_store_ind stores
on
  trans.store_id = stores.store_id
where
  (trans.approval_flag = 'A' ) 
  and ((trans.tran_type_description = 'REFUND')
  or (trans.tran_type_description = 'REFUND REVERSAL')
  or (trans.tran_type_description = 'PURCHASE')
  or (trans.tran_type_description = 'PURCHASE REVERSAL')
  or (trans.tran_type_description = 'PURCHASE WITH CASHOUT')
  or (trans.tran_type_description = 'PURCHASE WITH CASHOUT REVERSAL')
  or (trans.tran_type_description = 'REDEMPTION')
  or (trans.tran_type_description = 'REDEMPTION REVERSAL')
  )
group by
  1,2,3,4,5,6,7,8,9
) amount
left join
  (
    select
  left(cast(trans.transaction_date as string),4) as Year,
  substring(cast(trans.transaction_date as string),6,2) as Month,
  --right(cast(trans.transaction_date as string),2) as Day,
  trans.state as State,
  trans.scheme as Scheme,
  trans.is_online as is_online,
  trans.scheme_card_type as scheme_card_type,
  trans.store_id as Store,
  stores.brand as Brand,
  stores.suburb as Suburb,
  --sum(trans.transaction_count) as trans_count,
  (sum(case when trans.tran_type_description in 
  ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL') then transaction_count else 0 end)
  -
  sum(case when trans.tran_type_description in 
  ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL') then transaction_count else 0 end)
  ) as trans_count
  --sum(trans.settlement_amount_inc_gst) as trans_amount,
from
  pdh_sf_mep_vw.wpay_all_merchants_daily_fees_summary_hist trans
left join 
  wpay_analytics.ref_store_ind stores
on
  trans.store_id = stores.store_id
where
  (trans.approval_flag = 'A' ) 
  -- and ((trans.tran_type_description = 'PURCHASE')
  -- or (trans.tran_type_description = 'PURCHASE REVERSAL')
  -- or (trans.tran_type_description = 'PURCHASE WITH CASHOUT')
  -- or (trans.tran_type_description = 'PURCHASE WITH CASHOUT REVERSAL')
  -- or (trans.tran_type_description = 'REDEMPTION')
  -- or (trans.tran_type_description = 'REDEMPTION REVERSAL')
  -- )
  group by
  1,2,3,4,5,6,7,8,9) counts
on (amount.Year = counts.Year)
   and (amount.Month = counts.Month)
   and (amount.State = counts.State)
   and (amount.Scheme = counts.Scheme)
   and (amount.is_online = counts.is_online)
   and (amount.scheme_card_type = counts.scheme_card_type)
   and (amount.Store = counts.Store)
   and (amount.Brand = counts.Brand)
   and (amount.Suburb = counts.Suburb)
order by Store,year,month)

-------------------------------------------------------------------------------------------------------------

-- Joined on same table to get previous month/year

, store_internal_filt as
(
  select 
    store_trans_filt.*,
    case when cast(prev_m.prev_Amount as int) <> 0
    then
    ((store_trans_filt.trans_amount - prev_m.prev_Amount)/prev_m.prev_Amount)  
    else 0 end 
    as trans_amount_mom,
    case when cast(prev_y.prev_Amount as int) <> 0
    then
    ((store_trans_filt.trans_amount - prev_y.prev_Amount)/prev_y.prev_Amount)  
    else 0 end
    as trans_amount_yoy,
    case when cast(prev_m.prev_Count as int) <> 0
    then
    ((store_trans_filt.trans_count - prev_m.prev_Count)/prev_m.prev_Count)  
    else 0 end
    as trans_count_mom,
    case when cast(prev_y.prev_Count as int) <> 0
    then
    ((store_trans_filt.trans_count - prev_y.prev_Count)/prev_y.prev_Count)  
    else 0 end
    as trans_count_yoy,
    prev_m.prev_Amount as trans_amount_LM,
    prev_m.prev_Count as trans_count_LM,
    prev_y.prev_Amount as trans_amount_LY,
    prev_y.prev_Count as trans_count_LY,
    case when cast(store_trans_filt.trans_count as int) <> 0 then
    (store_trans_filt.trans_amount/store_trans_filt.trans_count) 
    else 0 end 
    as trans_value,
    case when cast(prev_m.prev_Count as int) <> 0
    then
    (prev_m.prev_Amount/prev_m.prev_Count)  
    else 0 end 
    as trans_value_LM,
    case when cast(prev_y.prev_Count as int) <> 0
    then
    (prev_y.prev_Amount/prev_y.prev_Count)  
    else 0 end 
    as trans_value_LY
from store_trans_filt
left join
(
select 
  curr.*,
  prev.Year as prev_year,
  prev.Month as prev_month,
  prev.trans_amount as prev_Amount,
  prev.trans_count as prev_Count
from store_trans_filt curr
left join 
  store_trans_filt prev
on curr.Store = prev.Store
    and curr.Month = prev.Month
    and (cast(curr.Year as int)-1) = cast(prev.Year as int)
    and curr.is_online = prev.is_online
    and curr.scheme_card_type = prev.scheme_card_type
    and curr.scheme = prev.scheme
order by curr.Store,curr.Year,curr.Month
) prev_y
on
  store_trans_filt.Store = prev_y.Store
  and store_trans_filt.Year = prev_y.Year
  and store_trans_filt.Month = prev_y.Month
  and store_trans_filt.is_online = prev_y.is_online
  and store_trans_filt.scheme = prev_y.scheme
  and store_trans_filt.scheme_card_type = prev_y.scheme_card_type
left join
(
select 
  curr.*,
  prev.trans_amount as prev_Amount,
  prev.trans_count as prev_Count
from store_trans_filt curr
left join 
  store_trans_filt prev
on curr.Store = prev.Store
    and left(cast(date_add(cast(concat(concat(concat(concat(curr.Year,'-'),curr.Month),'-'),'01') as date), interval -1 month) as string),4) = cast(prev.Year as string)
    and substring(cast(date_add(cast(concat(concat(concat(concat(curr.Year,'-'),curr.Month),'-'),'01') as date), interval -1 month) as string),6,2)  = prev.Month
    and curr.is_online = prev.is_online
    and curr.scheme = prev.scheme
    and curr.scheme_card_type = prev.scheme_card_type
order by curr.Store,curr.Year,curr.Month
) prev_m
on
  store_trans_filt.Store = prev_m.Store
  and store_trans_filt.Year = prev_m.Year
  and store_trans_filt.Month = prev_m.Month
  and store_trans_filt.is_online = prev_m.is_online
  and store_trans_filt.scheme = prev_m.scheme
  and store_trans_filt.scheme_card_type = prev_m.scheme_card_type
order by store_trans_filt.Store, store_trans_filt.Year,store_trans_filt.Month
)

---------------------------------------------------------------------------------------------------------------
,
wpay_store_trans_filt as
  (select 
    distinct stores.store_id,
    stores.country,
    stores.state,
    stores.division,
    stores.brand,
    stores.company,
    stores.invoice_group,
    stores.merchant_group,
    stores.merchant_portal_mg,
    stores.merchant_portal_cg,
    stores.industry,
    stores.industry_name as industry_name,
    --distinct stores.store_id,
    stores.site_name,
    stores.suburb,
    stores.postcode,
    stores.LGA,
    stores.LGA_id,
    stores.caid,
    --stores.is_closed,
    store_internal_filt.is_online,
    store_internal_filt.scheme,
    store_internal_filt.scheme_card_type,
    store_internal_filt.Year as Year,
    store_internal_filt.Month as Month,
    store_internal_filt.trans_amount as trans_amount,
    store_internal_filt.trans_count as trans_count,
    store_internal_filt.trans_amount_mom as trans_amount_mom,
    store_internal_filt.trans_amount_yoy as trans_amount_yoy,
    store_internal_filt.trans_count_mom as trans_count_mom,
    store_internal_filt.trans_count_yoy as trans_count_yoy,
    store_internal_filt.trans_amount_LM as trans_amount_LM,
    store_internal_filt.trans_count_LM as trans_count_LM,
    store_internal_filt.trans_amount_LY as trans_amount_LY,
    store_internal_filt.trans_count_LY as trans_count_LY,
    store_internal_filt.trans_value as trans_value,
    store_internal_filt.trans_value_LM as trans_value_LM,
    store_internal_filt.trans_value_LY as trans_value_LY
  from
    wpay_analytics.ref_store_ind stores
  inner join
    store_internal_filt
  on stores.store_id = store_internal_filt.Store
  --group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36
  order by store_id
  )

-------------------------------------------
--- adding only LGA to wpay_trans

, wpay_LGA_filt as
 (select 
    wpay_store_trans_filt.*,
    --final_lga_filt.LGA,
    --final_lga_filt.LGA_id,
    final_lga_filt.area,
    --final_lga_filt.industry_name,
    final_lga_filt.spend_growth_yoy,
    final_lga_filt.spend_growth_mom,
    final_lga_filt.txn_growth_yoy,
    final_lga_filt.txn_growth_mom,
    final_lga_filt.spend_density_growth_yoy,
    final_lga_filt.spend_density_growth_mom,
    final_lga_filt.avg_ticket,
    final_lga_filt.b_avg_ticket_LM,
    final_lga_filt.b_avg_ticket_LY,
    final_lga_filt.b_avg_ticket_mom,
    final_lga_filt.b_avg_ticket_yoy,
    final_lga_filt.avg_visits_per_cust,
    final_lga_filt.avg_spend_per_cust,
    final_lga_filt.ecom_ind_rank,
    final_lga_filt.ecom_txns_share,
    final_lga_filt.ecom_ind_rank_share,
    --final_lga.Benchmark
  from 
    wpay_store_trans_filt
  left join 
    final_lga_filt
  on (wpay_store_trans_filt.store_id = final_lga_filt.store_id)
    and (wpay_store_trans_filt.Year = final_lga_filt.Year)
    --and (wpay_store_trans.Month = final_lga.Month)
    and (cast(wpay_store_trans_filt.Month as int) = cast(final_lga_filt.Month as int))
  --where wpay_store_trans_filt.Year ='2022'
  --    and wpay_store_trans_filt.Month ='07'
  --order by final_lga.Benchmark
 )

------------------------------------------------------------
---- adding benchmark 
,

MaxMMIDate as (
select max(Period) as max_date
from  wpay_analytics.mmi_area_industry_edit
),

lga_bench_filt as 
(
  select *,
  case when 
    (coalesce(spend_growth_yoy,spend_growth_mom,txn_growth_yoy,txn_growth_mom) is null 
    and concat(concat(Year,'-'),Month) <= (select max_date from MaxMMIDate))
    then 'State'
    else 'LGA'
    end as Benchmark
    from wpay_LGA_filt
)


--------------------------------------------
--- going over LGA and filling nulls with State
, lga_benchmark_filt as
(
  select
    'Store' as Aggregation_Level,
    lga_bench_filt.store_id,
    'All' as store_status,
    lga_bench_filt.country,
    lga_bench_filt.state,
    lga_bench_filt.division,
    lga_bench_filt.brand,
    lga_bench_filt.industry,
    lga_bench_filt.site_name,
    lga_bench_filt.suburb,
    cast(lga_bench_filt.postcode as string) as postcode,
    cast(lga_bench_filt.caid as string)  as caid,
    --cast(lga_bench_filt.is_closed as string) as is_closed,
    lga_bench_filt.company,
    lga_bench_filt.invoice_group,
    lga_bench_filt.merchant_group,
    lga_bench_filt.merchant_portal_mg,
    lga_bench_filt.merchant_portal_cg,
    cast(lga_bench_filt.is_online as string) as is_online,
    cast(lga_bench_filt.scheme as string) as scheme,
    cast(lga_bench_filt.scheme_card_type as string) as card_type,
    lga_bench_filt.LGA,
    cast(lga_bench_filt.LGA_id as string) as LGA_id,
    lga_bench_filt.area,
    lga_bench_filt.industry_name,
    --final_lga.industry_code,
    --wpay_LGA.Period,
    concat(lga_bench_filt.Year,concat('-',lga_bench_filt.Month)) as Period,
    lga_bench_filt.Year,
    lga_bench_filt.Month,
    lga_bench_filt.trans_amount as w_trans_amount,
    lga_bench_filt.trans_count as w_trans_count,
    lga_bench_filt.trans_value as w_trans_value,
    lga_bench_filt.trans_amount_LM as w_trans_amount_LM,
    lga_bench_filt.trans_count_LM as w_trans_count_LM,
    lga_bench_filt.trans_value_LM as w_trans_value_LM,
    lga_bench_filt.trans_amount_LY as w_trans_amount_LY,
    lga_bench_filt.trans_count_LY as w_trans_count_LY,
    lga_bench_filt.trans_value_LY as w_trans_value_LY,
    lga_bench_filt.trans_amount_mom as w_trans_amount_mom,
    lga_bench_filt.trans_amount_yoy as w_trans_amount_yoy,
    lga_bench_filt.trans_count_mom as w_trans_count_mom,
    lga_bench_filt.trans_count_yoy as w_trans_count_yoy,
    case when cast(lga_bench_filt.trans_value_LM as int) <> 0
    then ((lga_bench_filt.trans_value - lga_bench_filt.trans_value_LM)/lga_bench_filt.trans_value_LM)  
    else 0 end 
    as w_trans_value_mom,
    case when cast(lga_bench_filt.trans_value_LY as int) <> 0
    then ((lga_bench_filt.trans_value - lga_bench_filt.trans_value_LY)/lga_bench_filt.trans_value_LY)  
    else 0 end 
    as w_trans_value_yoy,
    case when cust_info.unique_customer_count <> 0 then
    lga_bench_filt.trans_count/cust_info.unique_customer_count 
    else 0 end as avg_cust_visits,
    case when cust_info.unique_customer_count <> 0 then
    lga_bench_filt.trans_amount/cust_info.unique_customer_count
    else 0 end as avg_cust_spend,
    cust_info.unique_card_count as unique_cust,
    cust_info.unique_customer_count as unique_card,
    CASE 
      WHEN lga_bench_filt.Benchmark = 'State'
        THEN ind_key.spend_growth_yoy
      ELSE lga_bench_filt.spend_growth_yoy
      END AS b_spend_growth_yoy,
    CASE 
      WHEN lga_bench_filt.Benchmark = 'State'
        THEN ind_key.spend_growth_mom
      ELSE lga_bench_filt.spend_growth_mom
      END AS b_spend_growth_mom,
    CASE
      WHEN lga_bench_filt.Benchmark = 'State'
        THEN ind_key.txn_growth_yoy
      ELSE lga_bench_filt.txn_growth_yoy
      END AS b_txn_growth_yoy,
    CASE
      WHEN lga_bench_filt.Benchmark = 'State'
        THEN ind_key.txn_growth_mom
      ELSE lga_bench_filt.txn_growth_mom
      END AS b_txn_growth_mom,
    CASE
      WHEN lga_bench_filt.Benchmark = 'State'
        THEN ind_key.spend_density_growth_yoy
      ELSE lga_bench_filt.spend_density_growth_yoy
      END AS b_spend_density_growth_yoy,
    CASE
      WHEN lga_bench_filt.Benchmark = 'State'
        THEN ind_key.spend_density_growth_mom
      ELSE lga_bench_filt.spend_density_growth_mom
      END AS b_spend_density_growth_mom,
    CASE
      WHEN lga_bench_filt.Benchmark = 'State'
        THEN ind_key.avg_ticket
      ELSE lga_bench_filt.avg_ticket
      END AS b_avg_ticket,
    CASE
      WHEN lga_bench_filt.Benchmark = 'State'
        THEN ind_key.b_avg_ticket_LM
      ELSE lga_bench_filt.b_avg_ticket_LM
      END AS b_avg_ticket_LM,
    CASE
      WHEN lga_bench_filt.Benchmark = 'State'
        THEN ind_key.b_avg_ticket_LY
      ELSE lga_bench_filt.b_avg_ticket_LY
      END AS b_avg_ticket_LY,
    CASE
      WHEN lga_bench_filt.Benchmark = 'State'
        THEN ind_key.b_avg_ticket_mom
      ELSE lga_bench_filt.b_avg_ticket_mom
      END AS b_avg_ticket_mom,
    CASE
      WHEN lga_bench_filt.Benchmark = 'State'
        THEN ind_key.b_avg_ticket_yoy
      ELSE lga_bench_filt.b_avg_ticket_yoy
      END AS b_avg_ticket_yoy,
    CASE
      WHEN lga_bench_filt.Benchmark = 'State'
        THEN ind_key.avg_visits_per_cust
      ELSE lga_bench_filt.avg_visits_per_cust
      END AS b_avg_visits_per_cust,
    CASE
      WHEN lga_bench_filt.Benchmark = 'State'
        THEN ind_key.avg_spend_per_cust
      ELSE lga_bench_filt.avg_spend_per_cust
      END AS b_avg_spend_per_cust,
    CASE
      WHEN lga_bench_filt.Benchmark = 'State'
        THEN ind_key.ecom_ind_rank
      ELSE lga_bench_filt.ecom_ind_rank
      END AS b_ecom_ind_rank,
    CASE
      WHEN lga_bench_filt.Benchmark = 'State'
        THEN ind_key.ecom_txns_share
      ELSE lga_bench_filt.ecom_txns_share
      END AS b_ecom_txns_share,
    CASE
      WHEN lga_bench_filt.Benchmark = 'State'
        THEN ind_key.ecom_ind_rank_share
      ELSE lga_bench_filt.ecom_ind_rank_share
      END AS b_ecom_ind_rank_share,
    --lga_bench_filt.Benchmark,
    lga_bench_filt.industry_name as Benchmark,
    lga_bench_filt.Benchmark as Benchmark_Location,
    'External' as Benchmark_lvl
  from 
    lga_bench_filt
  left join
    ( select distinct ind.*, 
    key.State as state,
    left(ind.period,4) as Year,
    right(ind.period,2) as Month
    from
    wpay_analytics.mmi_area_industry_edit ind
    left join 
      wpay_analytics.mmi_aus_mapping_key_edit key
      on cast(ind.area as string) = cast(key.level1_id as string)
    where area_level = 'state') ind_key
  on cast(lga_bench_filt.state as string) = cast(ind_key.state as string)
     and cast(lga_bench_filt.industry as string) = cast(ind_key.industry_code as string) 
     --and (final_lga.period = ind_key.period ) )
     and cast(trim(lga_bench_filt.Year) as string) = cast(trim(ind_key.Year) as string) 
     --and cast(trim(lga_bench.Month) as string) = cast(trim(ind_key.Month) as string) 
     and (cast(lga_bench_filt.Month as int) = cast(ind_key.Month as int))
  left join
    (
  select 
      --m.company,
      --m.invoice_group,    
      --m.merchant_group,
      --m.division, 
      --m.brand,    
      m.store_id,   
      --m.site_name,    
     -- m.suburb,   
     -- m.state,    
      --m.postcode,
     -- m.country,
      m.is_online,
      fee.calc_scheme as scheme,
      fee.calc_card_type as card_type,
      left(cast(fee.switch_tran_date as string),7) Transaction_Date,
      -- sum(case when fee.calc_tran_type_description in
      -- ('REFUND','REFUND REVERSAL','PURCHASE','PURCHASE REVERSAL','PURCHASE WITH CASHOUT','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION','REDEMPTION REVERSAL') then fee.transaction_total else 0 end)/100 as trans_amount,
      -- (sum(case when fee.calc_tran_type_description in
      -- ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL') then 1 else 0 end)
      -- -
      -- sum(case when fee.calc_tran_type_description in 
      -- ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL') then 1 else 0 end)
      -- ) as trans_count,
      count(distinct fee.card_id) unique_card_count,
      count(distinct fee.card_id) unique_customer_count
      -- (count(1) / count(distinct fee.card_id)) frequency_of_visits,
      -- (  sum(case when fee.calc_tran_type_description in
      -- ('REFUND','REFUND REVERSAL','PURCHASE','PURCHASE REVERSAL','PURCHASE WITH CASHOUT','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION','REDEMPTION REVERSAL') then fee.transaction_total else 0 end) 
      -- / (100*count(distinct fee.card_id))) spend_per_card_holder 
      from wpay_analytics.wpay_billing_engine_tran_dtl fee  
      inner join
      `pdh_ref_ds.ref_store_details` m 
      on LEFT(FEE.net_term_id, 5) = M.store_id
      --inner join
      --wpay_analytics.wpay_card_identity_model cim
      --on trim(cim.scdr_token) = trim(fee.scdr_token)
      where fee.calc_approval_flag = 'A'
	  and left(cast(fee.tstamp_trans as string),7) > '2022-07'
and fee.calc_tran_type_description in ('REFUND','REFUND REVERSAL','PURCHASE','PURCHASE REVERSAL','PURCHASE WITH CASHOUT','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION','REDEMPTION REVERSAL')
      group by 1,2,3,4,5
      ) cust_info
  on cust_info.store_id = lga_bench_filt.store_id
     and concat(lga_bench_filt.Year,concat('-',lga_bench_filt.Month)) = cust_info.Transaction_Date
     and cust_info.is_online = lga_bench_filt.is_online
     and cust_info.scheme = lga_bench_filt.scheme
     and cust_info.card_type = lga_bench_filt.scheme_card_type
  order by lga_bench_filt.Year, lga_bench_filt.Month, lga_bench_filt.store_id
)
-- end

--------------------------------------------------------------------------------------------------------------

--------------------------------------------------------------------------------------------------------------
-- STATE/AUS
--------------------------------------------------------------------------------------------------------------

-- Store base table + mastercard linking and benchmark

,
 final_state_filt as 
(select
  *
from
(
select 
  stores.country,
  stores.state,
  stores.division,
  stores.brand,
  stores.company,
  stores.industry,
  stores.industry_name,
  --distinct stores.store_id,
  --stores.site_name,
  --stores.suburb,
  --stores.postcode,
  --stores.caid,
  --stores.is_closed,
  --stores.company,
  stores.invoice_group,
  stores.merchant_group,
  stores.merchant_portal_mg,
  stores.merchant_portal_cg
  --stores.is_online,
from
  wpay_analytics.ref_store_ind stores
group by 1,2,3,4,5,6,7,8,9,10,11) store_info
left join
  (
  select 
    area,
    period,
    --industry_name,
    industry_code,
    left(period,4) as Year,
    right(period,2) as Month,
    key.State as state_mmi,
    CASE 
      WHEN spend_growth_yoy = 99999
        THEN NULL
      ELSE spend_growth_yoy
      END AS spend_growth_yoy,
    CASE 
      WHEN spend_growth_mom = 99999
        THEN NULL
      ELSE spend_growth_mom
      END AS spend_growth_mom,
    CASE
      WHEN txn_growth_yoy = 99999
        THEN NULL
      ELSE txn_growth_yoy
      END AS txn_growth_yoy,
    CASE
      WHEN txn_growth_mom = 99999
        THEN NULL
      ELSE txn_growth_mom
      END AS txn_growth_mom,
    CASE
      WHEN spend_density_growth_yoy = 99999
        THEN NULL
      ELSE spend_density_growth_yoy
      END AS spend_density_growth_yoy,
    CASE
      WHEN spend_density_growth_mom = 99999
        THEN NULL
      ELSE spend_density_growth_mom
      END AS spend_density_growth_mom,
    CASE
      WHEN avg_ticket = 99999
        THEN NULL
      ELSE avg_ticket
      END AS avg_ticket,
    b_avg_ticket_LM,
    b_avg_ticket_LY,
    b_avg_ticket_mom,
    b_avg_ticket_yoy,
    CASE
      WHEN avg_visits_per_cust = 99999
        THEN NULL
      ELSE avg_visits_per_cust
      END AS avg_visits_per_cust,
    CASE
      WHEN avg_spend_per_cust = 99999
        THEN NULL
      ELSE avg_spend_per_cust
      END AS avg_spend_per_cust,
    CASE
      WHEN ecom_ind_rank = 99999
        THEN NULL
      ELSE ecom_ind_rank
      END AS ecom_ind_rank,
    CASE
      WHEN ecom_txns_share = 99999
        THEN NULL
      ELSE ecom_txns_share
      END AS ecom_txns_share,
    CASE
      WHEN ecom_ind_rank_share = 99999
        THEN NULL
      ELSE ecom_ind_rank_share
      END AS ecom_ind_rank_share
  from
    wpay_analytics.mmi_area_industry_edit
  left join
    wpay_analytics.mmi_aus_mapping_key_edit key
    on cast(key.level1_id as string) = cast(area as string)
  where
    area_level = 'state'
  group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22
  ) ind_lga
on 
  cast(store_info.state as string) = cast(ind_lga.state_mmi as string)
  and cast(store_info.industry as string) = cast(ind_lga.industry_code as string)
)

----------------------------------------
-- wpay store transactions
,
 store_trans_state_filt as 
(select 
  amount.Year as Year,
  amount.Month as Month,
  amount.Country as Country,
  amount.State as State,
  --amount.Scheme as Scheme,
  amount.is_online as is_online,
  amount.scheme as scheme,
  amount.scheme_card_type as scheme_card_type,
  --amount.Store as Store,
  amount.industry as Industry,
  amount.industry_name as industry_name,
  amount.Brand as Brand,
  amount.division as division,
  amount.company as company,
  amount.invoice_group as invoice_group,
  amount.merchant_group as merchant_group,
  amount.merchant_portal_mg as merchant_portal_mg,
  amount.merchant_portal_cg as merchant_portal_cg,
  --amount.Suburb as Suburb,
  amount.trans_amount as trans_amount,
  counts.trans_count as trans_count,
from
(
select
  left(cast(trans.transaction_date as string),4) as Year,
  substring(cast(trans.transaction_date as string),6,2) as Month,
 -- right(cast(trans.transaction_date as string),2) as Day,
  stores.country as Country,
  trans.state as State,
  --trans.scheme as Scheme,
  trans.is_online as is_online,
  trans.scheme as scheme,
  trans.scheme_card_type as scheme_card_type,
  --trans.store_id as Store,
  stores.industry as Industry,
  stores.industry_name as industry_name,
  stores.brand as Brand,
  stores.division as division,
  stores.company as company,
  stores.invoice_group as invoice_group,
  stores.merchant_group as merchant_group,
  stores.merchant_portal_mg as merchant_portal_mg,
  stores.merchant_portal_cg as merchant_portal_cg,
  --stores.suburb as Suburb,
  --sum(trans.transaction_count) as trans_count,
  sum(trans.settlement_amount_inc_gst) as trans_amount,
from
  pdh_sf_mep_vw.wpay_all_merchants_daily_fees_summary_hist trans
left join 
  wpay_analytics.ref_store_ind stores
on
  trans.store_id = stores.store_id
where
  (trans.approval_flag = 'A' ) 
  and ((trans.tran_type_description = 'REFUND')
  or (trans.tran_type_description = 'REFUND REVERSAL')
  or (trans.tran_type_description = 'PURCHASE')
  or (trans.tran_type_description = 'PURCHASE REVERSAL')
  or (trans.tran_type_description = 'PURCHASE WITH CASHOUT')
  or (trans.tran_type_description = 'PURCHASE WITH CASHOUT REVERSAL')
  or (trans.tran_type_description = 'REDEMPTION')
  or (trans.tran_type_description = 'REDEMPTION REVERSAL')
  )
group by
  1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16
) amount
left join
  (
    select
  left(cast(trans.transaction_date as string),4) as Year,
  substring(cast(trans.transaction_date as string),6,2) as Month,
  --right(cast(trans.transaction_date as string),2) as Day,
  stores.country as Country,
  trans.state as State,
  --trans.scheme as Scheme,
  trans.is_online as is_online,
  trans.scheme as scheme,
  trans.scheme_card_type as scheme_card_type,
  --trans.store_id as Store,
  stores.industry as Industry,
  stores.brand as Brand,
  stores.division as division,
  stores.company as company,
  stores.invoice_group as invoice_group,
  stores.merchant_group as merchant_group,
  stores.merchant_portal_mg as merchant_portal_mg,
  stores.merchant_portal_cg as merchant_portal_cg,
  --stores.suburb as Suburb,
  --sum(trans.transaction_count) as trans_count,
  (sum(case when trans.tran_type_description in 
  ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL') then transaction_count else 0 end)
  -
  sum(case when trans.tran_type_description in 
  ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL') then transaction_count else 0 end)
  ) as trans_count
  --sum(trans.settlement_amount_inc_gst) as trans_amount,
from
  pdh_sf_mep_vw.wpay_all_merchants_daily_fees_summary_hist trans
left join 
  wpay_analytics.ref_store_ind  stores
on
  trans.store_id = stores.store_id
where
  (trans.approval_flag = 'A' ) 
  -- and ((trans.tran_type_description = 'PURCHASE')
  -- or (trans.tran_type_description = 'PURCHASE REVERSAL')
  -- or (trans.tran_type_description = 'PURCHASE WITH CASHOUT')
  -- or (trans.tran_type_description = 'PURCHASE WITH CASHOUT REVERSAL')
  -- or (trans.tran_type_description = 'REDEMPTION')
  -- or (trans.tran_type_description = 'REDEMPTION REVERSAL')
  -- )
  group by
  1,2,3,4,5,6,7,8,9,10,11,12,13,14,15) counts
on (amount.Year = counts.Year)
   and (amount.Month = counts.Month)
   and (amount.State = counts.State)
   --and (amount.Scheme = counts.Scheme)
   --and (amount.Online = counts.Online)
   --and (amount.Card_Type = counts.Card_Type)
   --and (amount.Store = counts.Store)
   and (amount.Brand = counts.Brand)
   and (amount.division = counts.division)
   and (amount.company = counts.company)
   and (amount.invoice_group = counts.invoice_group)
   and (amount.merchant_group = counts.merchant_group)
   and (amount.merchant_portal_mg = counts.merchant_portal_mg)
   and (amount.merchant_portal_cg = counts.merchant_portal_cg)
   and (amount.is_online = counts.is_online)
   and (amount.scheme = counts.scheme)
   and (amount.scheme_card_type = counts.scheme_card_type)
   --and (amount.Suburb = counts.Suburb))
)
---------------------------------------------------------------------------------------------------------

-- Joined on same table to get previous month/year

, state_internal_filt as
(
  select 
    store_trans_state_filt.*,
    case when cast(prev_m.prev_Amount as int) <> 0
    then
    ((store_trans_state_filt.trans_amount - prev_m.prev_Amount)/prev_m.prev_Amount) 
    else 0 end
    as trans_amount_mom,
    case when cast(prev_y.prev_Amount as int) <> 0
    then
    ((store_trans_state_filt.trans_amount - prev_y.prev_Amount)/prev_y.prev_Amount) 
    else 0 end
     as trans_amount_yoy,
    case when cast(prev_m.prev_Count as int) <> 0
    then
    ((store_trans_state_filt.trans_count - prev_m.prev_Count)/prev_m.prev_Count) 
    else 0 end
     as trans_count_mom,
    case when cast(prev_y.prev_Count as int) <> 0
    then
    ((store_trans_state_filt.trans_count - prev_y.prev_Count)/prev_y.prev_Count) 
    else 0 end
     as trans_count_yoy,
    prev_m.prev_Amount as trans_amount_LM,
    prev_m.prev_Count as trans_count_LM,
    prev_y.prev_Amount as trans_amount_LY,
    prev_y.prev_Count as trans_count_LY,
    case when cast(store_trans_state_filt.trans_count as int) <> 0 then
    (store_trans_state_filt.trans_amount/store_trans_state_filt.trans_count) 
    else 0 end 
    as trans_value,
    case when cast(prev_m.prev_Count as int) <> 0
    then
    (prev_m.prev_Amount/prev_m.prev_Count)  
    else 0 end 
    as trans_value_LM,
    case when cast(prev_y.prev_Count as int) <> 0
    then
    (prev_y.prev_Amount/prev_y.prev_Count)  
    else 0 end 
    as trans_value_LY
from store_trans_state_filt
left join
(
select 
  curr.*,
  prev.Year as prev_year,
  prev.Month as prev_month,
  prev.trans_amount as prev_Amount,
  prev.trans_count as prev_Count
from store_trans_state_filt curr
left join 
  store_trans_state_filt prev
on  (curr.State = prev.State)
   and (curr.Brand = prev.Brand)
   and (curr.division = prev.division)
   and (curr.company = prev.company)
   and (curr.invoice_group = prev.invoice_group)
   and (curr.merchant_group = prev.merchant_group)
   and (curr.merchant_portal_mg = prev.merchant_portal_mg)
   and (curr.merchant_portal_cg = prev.merchant_portal_cg)
  and curr.Month = prev.Month
  and (cast(curr.Year as int)-1) = cast(prev.Year as int)
  and (curr.is_online = prev.is_online)
  and (curr.scheme = prev.scheme)
  and (curr.scheme_card_type = prev.scheme_card_type)
order by curr.State,curr.Year,curr.Month
) prev_y
on
  (store_trans_state_filt.State = prev_y.State)
   and (store_trans_state_filt.Brand = prev_y.Brand)
   and (store_trans_state_filt.division = prev_y.division)
   and (store_trans_state_filt.company = prev_y.company)
   and (store_trans_state_filt.invoice_group = prev_y.invoice_group)
   and (store_trans_state_filt.merchant_group = prev_y.merchant_group)
   and (store_trans_state_filt.merchant_portal_mg = prev_y.merchant_portal_mg)
   and (store_trans_state_filt.merchant_portal_cg = prev_y.merchant_portal_cg)
  and store_trans_state_filt.Year = prev_y.Year
  and store_trans_state_filt.Month = prev_y.Month
  and (store_trans_state_filt.is_online = prev_y.is_online)
  and (store_trans_state_filt.scheme = prev_y.scheme)
  and (store_trans_state_filt.scheme_card_type = prev_y.scheme_card_type)
left join
(
select 
  curr.*,
  prev.trans_amount as prev_Amount,
  prev.trans_count as prev_Count
from store_trans_state_filt curr
left join 
  store_trans_state_filt prev
on (curr.State = prev.State)
   and (curr.Brand = prev.Brand)
   and (curr.division = prev.division)
   and (curr.company = prev.company)
   and (curr.invoice_group = prev.invoice_group)
   and (curr.merchant_group = prev.merchant_group)
   and (curr.merchant_portal_mg = prev.merchant_portal_mg)
   and (curr.merchant_portal_cg = prev.merchant_portal_cg)
    and left(cast(date_add(cast(concat(concat(concat(concat(curr.Year,'-'),curr.Month),'-'),'01') as date), interval -1 month) as string),4) = cast(prev.Year as string)
    and substring(cast(date_add(cast(concat(concat(concat(concat(curr.Year,'-'),curr.Month),'-'),'01') as date), interval -1 month) as string),6,2)  = prev.Month
  and (curr.is_online = prev.is_online)
  and (curr.scheme = prev.scheme)
  and (curr.scheme_card_type = prev.scheme_card_type)
order by curr.State,curr.Year,curr.Month
) prev_m
on
  (store_trans_state_filt.State = prev_m.State)
   and (store_trans_state_filt.Brand = prev_m.Brand)
   and (store_trans_state_filt.division = prev_m.division)
   and (store_trans_state_filt.company = prev_m.company)
   and (store_trans_state_filt.invoice_group = prev_m.invoice_group)
   and (store_trans_state_filt.merchant_group = prev_m.merchant_group)
   and (store_trans_state_filt.merchant_portal_mg = prev_m.merchant_portal_mg)
   and (store_trans_state_filt.merchant_portal_cg = prev_m.merchant_portal_cg)
  and store_trans_state_filt.Year = prev_m.Year
  and store_trans_state_filt.Month = prev_m.Month
  and (store_trans_state_filt.is_online = prev_m.is_online)
  and (store_trans_state_filt.scheme = prev_m.scheme)
  and (store_trans_state_filt.scheme_card_type = prev_m.scheme_card_type)
order by store_trans_state_filt.State, store_trans_state_filt.Year,store_trans_state_filt.Month
)

---------------------------------------------------------------------------------------------------------

--- adding only LGA to wpay_trans

, wpay_State_filt as
 (select 
    state_internal_filt.*,
    final_state_filt.area,
    --final_state_filt.industry_name,
    final_state_filt.spend_growth_yoy,
    final_state_filt.spend_growth_mom,
    final_state_filt.txn_growth_yoy,
    final_state_filt.txn_growth_mom,
    final_state_filt.spend_density_growth_yoy,
    final_state_filt.spend_density_growth_mom,
    final_state_filt.avg_ticket,
    final_state_filt.b_avg_ticket_LM,
    final_state_filt.b_avg_ticket_LY,
    final_state_filt.b_avg_ticket_mom,
    final_state_filt.b_avg_ticket_yoy,
    final_state_filt.avg_visits_per_cust,
    final_state_filt.avg_spend_per_cust,
    final_state_filt.ecom_ind_rank,
    final_state_filt.ecom_txns_share,
    final_state_filt.ecom_ind_rank_share,
    --final_lga.Benchmark
  from 
    state_internal_filt
  left join 
    final_state_filt
  on (state_internal_filt.state = final_state_filt.state)
    and (state_internal_filt.Brand = final_state_filt.Brand)
    and (state_internal_filt.division = final_state_filt.division)
    and (state_internal_filt.company = final_state_filt.company)
    and (state_internal_filt.invoice_group = final_state_filt.invoice_group)
    and (state_internal_filt.merchant_group = final_state_filt.merchant_group)
    and (state_internal_filt.merchant_portal_mg = final_state_filt.merchant_portal_mg)
    and (state_internal_filt.merchant_portal_cg = final_state_filt.merchant_portal_cg)
    and (state_internal_filt.Year = final_state_filt.Year)
    --and (state_internal.Month = final_state.Month)
    and (cast(state_internal_filt.Month as int) = cast(final_state_filt.Month as int))
  --where state_internal_filt.Year ='2022'
  --    and state_internal_filt.Month ='07'
  --order by final_lga.Benchmark
 )

------------------------------------------------------------
---- adding benchmark 
,
state_bench_filt as 
(
  select *,
  case when 
    (coalesce(spend_growth_yoy,spend_growth_mom,txn_growth_yoy,txn_growth_mom) is null 
    and concat(concat(Year,'-'),Month) <= (select max_date from MaxMMIDate))
    then 'Country'
    else 'State'
    end as Benchmark
    from wpay_State_filt
)


--------------------------------------------
--- going over LGA and filling nulls with State
, state_benchmark_filt as
(
  select
    'State' as Aggregation_Level,
    'All' as store_id,
    'All' as store_status,
    state_bench_filt.country,
    state_bench_filt.state,
    state_bench_filt.division,
    state_bench_filt.brand,
    state_bench_filt.industry,
    'All' as site_name,
    'All' as suburb,
    'All' as postcode,
    'All' as caid,
   -- 'All' as is_closed,
    state_bench_filt.company,
    state_bench_filt.invoice_group,
    state_bench_filt.merchant_group,
    state_bench_filt.merchant_portal_mg,
    state_bench_filt.merchant_portal_cg,
    cast(state_bench_filt.is_online as string) as is_online,
    cast(state_bench_filt.scheme as string) as scheme,
    cast(state_bench_filt.scheme_card_type as string) as card_type,
    'All' as LGA,
    'All' as LGA_id,
    state_bench_filt.area,
    state_bench_filt.industry_name,
    --final_lga.industry_code,
    --wpay_LGA.Period,
    concat(state_bench_filt.Year,concat('-',state_bench_filt.Month)) as Period,
    state_bench_filt.Year,
    state_bench_filt.Month,
    state_bench_filt.trans_amount as w_trans_amount,
    state_bench_filt.trans_count as w_trans_count,
    state_bench_filt.trans_value as w_trans_value,
    state_bench_filt.trans_amount_LM as w_trans_amount_LM,
    state_bench_filt.trans_count_LM as w_trans_count_LM,
    state_bench_filt.trans_value_LM as w_trans_value_LM,
    state_bench_filt.trans_amount_LY as w_trans_amount_LY,
    state_bench_filt.trans_count_LY as w_trans_count_LY,
    state_bench_filt.trans_value_LY as w_trans_value_LY,
    state_bench_filt.trans_amount_mom as w_trans_amount_mom,
    state_bench_filt.trans_amount_yoy as w_trans_amount_yoy,
    state_bench_filt.trans_count_mom as w_trans_count_mom,
    state_bench_filt.trans_count_yoy as w_trans_count_yoy,
    case when cast(state_bench_filt.trans_value_LM as int) <> 0
    then ((state_bench_filt.trans_value - state_bench_filt.trans_value_LM)/state_bench_filt.trans_value_LM)  
    else 0 end 
    as w_trans_value_mom,
    case when cast(state_bench_filt.trans_value_LY as int) <> 0
    then ((state_bench_filt.trans_value - state_bench_filt.trans_value_LY)/state_bench_filt.trans_value_LY)  
    else 0 end 
    as w_trans_value_yoy,
    case when cust_info.unique_customer_count <> 0 then
    state_bench_filt.trans_count/cust_info.unique_customer_count 
    else 0 end as avg_cust_visits,
    case when cust_info.unique_customer_count <> 0 then
    state_bench_filt.trans_amount/cust_info.unique_customer_count 
    else 0 end as avg_cust_spend,
    cust_info.unique_card_count as unique_cust,
    cust_info.unique_customer_count as unique_card,
    CASE 
      WHEN state_bench_filt.Benchmark = 'Country'
        THEN ind_key.spend_growth_yoy
      ELSE state_bench_filt.spend_growth_yoy
      END AS b_spend_growth_yoy,
    CASE 
      WHEN state_bench_filt.Benchmark = 'Country'
        THEN ind_key.spend_growth_mom
      ELSE state_bench_filt.spend_growth_mom
      END AS b_spend_growth_mom,
    CASE
      WHEN state_bench_filt.Benchmark = 'Country'
        THEN ind_key.txn_growth_yoy
      ELSE state_bench_filt.txn_growth_yoy
      END AS b_txn_growth_yoy,
    CASE
      WHEN state_bench_filt.Benchmark = 'Country'
        THEN ind_key.txn_growth_mom
      ELSE state_bench_filt.txn_growth_mom
      END AS b_txn_growth_mom,
    CASE
      WHEN state_bench_filt.Benchmark = 'Country'
        THEN ind_key.spend_density_growth_yoy
      ELSE state_bench_filt.spend_density_growth_yoy
      END AS b_spend_density_growth_yoy,
    CASE
      WHEN state_bench_filt.Benchmark = 'Country'
        THEN ind_key.spend_density_growth_mom
      ELSE state_bench_filt.spend_density_growth_mom
      END AS b_spend_density_growth_mom,
    CASE
      WHEN state_bench_filt.Benchmark = 'Country'
        THEN ind_key.avg_ticket
      ELSE state_bench_filt.avg_ticket
      END AS b_avg_ticket,
    CASE
      WHEN state_bench_filt.Benchmark = 'Country'
        THEN ind_key.b_avg_ticket_LM
      ELSE state_bench_filt.b_avg_ticket_LM
      END AS b_avg_ticket_LM,
    CASE
      WHEN state_bench_filt.Benchmark = 'Country'
        THEN ind_key.b_avg_ticket_LY
      ELSE state_bench_filt.b_avg_ticket_LY
      END AS b_avg_ticket_LY,
    CASE
      WHEN state_bench_filt.Benchmark = 'Country'
        THEN ind_key.b_avg_ticket_mom
      ELSE state_bench_filt.b_avg_ticket_mom
      END AS b_avg_ticket_mom,
    CASE
      WHEN state_bench_filt.Benchmark = 'Country'
        THEN ind_key.b_avg_ticket_yoy
      ELSE state_bench_filt.b_avg_ticket_yoy
      END AS b_avg_ticket_yoy,
    CASE
      WHEN state_bench_filt.Benchmark = 'Country'
        THEN ind_key.avg_visits_per_cust
      ELSE state_bench_filt.avg_visits_per_cust
      END AS b_avg_visits_per_cust,
    CASE
      WHEN state_bench_filt.Benchmark = 'Country'
        THEN ind_key.avg_spend_per_cust
      ELSE state_bench_filt.avg_spend_per_cust
      END AS b_avg_spend_per_cust,
    CASE
      WHEN state_bench_filt.Benchmark = 'Country'
        THEN ind_key.ecom_ind_rank
      ELSE state_bench_filt.ecom_ind_rank
      END AS b_ecom_ind_rank,
    CASE
      WHEN state_bench_filt.Benchmark = 'Country'
        THEN ind_key.ecom_txns_share
      ELSE state_bench_filt.ecom_txns_share
      END AS b_ecom_txns_share,
    CASE
      WHEN state_bench_filt.Benchmark = 'Country'
        THEN ind_key.ecom_ind_rank_share
      ELSE state_bench_filt.ecom_ind_rank_share
      END AS b_ecom_ind_rank_share,
    --state_bench_filt.Benchmark,
    state_bench_filt.industry_name as Benchmark,
    state_bench_filt.Benchmark as Benchmark_Location,
    'External' as Benchmark_lvl
  from 
    state_bench_filt
  left join
    ( select distinct ind.*, 
    left(ind.period,4) as Year,
    right(ind.period,2) as Month
    from
    wpay_analytics.mmi_area_industry_edit ind
    where area_level = 'Country') ind_key
  on cast(state_bench_filt.industry as string) = cast(ind_key.industry_code as string) 
     --and (final_lga.period = ind_key.period ) )
     and cast(trim(state_bench_filt.Year) as string) = cast(trim(ind_key.Year) as string) 
     --and cast(trim(state_bench.Month) as string) = cast(trim(ind_key.Month) as string)
     and (cast(state_bench_filt.Month as int) = cast(ind_key.Month as int)) 
  left join
    (
  select 
      m.company,
      m.invoice_group,    
      m.merchant_group,
      m.division, 
      m.brand, 
      m.merchant_portal_mg,
      m.merchant_portal_cg,   
      --m.store_id,   
      --m.site_name,    
     -- m.suburb,   
      m.state,    
      --m.postcode,
     -- m.country,
      m.is_online,
      fee.calc_scheme as scheme,
      fee.calc_card_type as card_type,
      left(cast(fee.switch_tran_date as string),7) Transaction_Date,
      -- sum(case when fee.calc_tran_type_description in
      -- ('REFUND','REFUND REVERSAL','PURCHASE','PURCHASE REVERSAL','PURCHASE WITH CASHOUT','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION','REDEMPTION REVERSAL') then fee.transaction_total else 0 end)/100 as trans_amount,
      -- (sum(case when fee.calc_tran_type_description in
      -- ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL') then 1 else 0 end)
      -- -
      -- sum(case when fee.calc_tran_type_description in 
      -- ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL') then 1 else 0 end)
      -- ) as trans_count,
      count(distinct fee.card_id) unique_card_count,
      count(distinct fee.card_id) unique_customer_count
      -- (count(1) / count(distinct fee.card_id)) frequency_of_visits,
      -- (  sum(case when fee.calc_tran_type_description in
      -- ('REFUND','REFUND REVERSAL','PURCHASE','PURCHASE REVERSAL','PURCHASE WITH CASHOUT','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION','REDEMPTION REVERSAL') then fee.transaction_total else 0 end) 
      -- / (100*count(distinct fee.card_id))) spend_per_card_holder 
      from wpay_analytics.wpay_billing_engine_tran_dtl fee  
      inner join
      `pdh_ref_ds.ref_store_details` m 
      on LEFT(FEE.net_term_id, 5) = M.store_id
      --inner join
      --wpay_analytics.wpay_card_identity_model cim
      --on trim(cim.scdr_token) = trim(fee.scdr_token)
      where fee.calc_approval_flag = 'A'
	  and left(cast(fee.tstamp_trans as string),7) > '2022-07'
and fee.calc_tran_type_description in ('REFUND','REFUND REVERSAL','PURCHASE','PURCHASE REVERSAL','PURCHASE WITH CASHOUT','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION','REDEMPTION REVERSAL')
      group by 1,2,3,4,5,6,7,8,9,10,11,12
      ) cust_info
  on cust_info.company = state_bench_filt.company
    and cust_info.merchant_group = state_bench_filt.merchant_group
    and cust_info.division = state_bench_filt.division
    and cust_info.state = state_bench_filt.state
    and concat(state_bench_filt.Year,concat('-',state_bench_filt.Month)) = cust_info.Transaction_Date
    and cust_info.is_online = state_bench_filt.is_online
     and cust_info.scheme = state_bench_filt.scheme
     and cust_info.card_type = state_bench_filt.scheme_card_type
     and cust_info.invoice_group = state_bench_filt.invoice_group
    and cust_info.brand = state_bench_filt.brand
    and cust_info.merchant_portal_mg = state_bench_filt.merchant_portal_mg
    and cust_info.merchant_portal_cg = state_bench_filt.merchant_portal_cg
  order by state_bench_filt.Benchmark
)
-- end

--------------------------------------------------------------------------------------------------------------
-- END
--------------------------------------------------------------------------------------------------------------

--------------------------------------------------------------------------------------------------------------
-- Country
--------------------------------------------------------------------------------------------------------------
-- Store base table + mastercard linking and benchmark

,
 final_country_filt as 
(select
  *
from
(
select 
  stores.country,
  --stores.state,
  stores.division,
  stores.brand,
  stores.company,
  stores.industry,
  stores.industry_name,
  --distinct stores.store_id,
  --stores.site_name,
  --stores.suburb,
  --stores.postcode,
  --stores.caid,
  --stores.is_closed,
  --stores.company,
  stores.invoice_group,
  stores.merchant_group,
  stores.merchant_portal_mg,
  stores.merchant_portal_cg
  --stores.is_online,
from
  wpay_analytics.ref_store_ind stores
group by 1,2,3,4,5,6,7,8,9,10) store_info
left join
  (
  select 
    area,
    period,
   -- industry_name,
    industry_code,
    left(period,4) as Year,
    right(period,2) as Month,
    CASE 
      WHEN spend_growth_yoy = 99999
        THEN NULL
      ELSE spend_growth_yoy
      END AS spend_growth_yoy,
    CASE 
      WHEN spend_growth_mom = 99999
        THEN NULL
      ELSE spend_growth_mom
      END AS spend_growth_mom,
    CASE
      WHEN txn_growth_yoy = 99999
        THEN NULL
      ELSE txn_growth_yoy
      END AS txn_growth_yoy,
    CASE
      WHEN txn_growth_mom = 99999
        THEN NULL
      ELSE txn_growth_mom
      END AS txn_growth_mom,
    CASE
      WHEN spend_density_growth_yoy = 99999
        THEN NULL
      ELSE spend_density_growth_yoy
      END AS spend_density_growth_yoy,
    CASE
      WHEN spend_density_growth_mom = 99999
        THEN NULL
      ELSE spend_density_growth_mom
      END AS spend_density_growth_mom,
    CASE
      WHEN avg_ticket = 99999
        THEN NULL
      ELSE avg_ticket
      END AS avg_ticket,
    b_avg_ticket_LM,
    b_avg_ticket_LY,
    b_avg_ticket_mom,
    b_avg_ticket_yoy,
    CASE
      WHEN avg_visits_per_cust = 99999
        THEN NULL
      ELSE avg_visits_per_cust
      END AS avg_visits_per_cust,
    CASE
      WHEN avg_spend_per_cust = 99999
        THEN NULL
      ELSE avg_spend_per_cust
      END AS avg_spend_per_cust,
    CASE
      WHEN ecom_ind_rank = 99999
        THEN NULL
      ELSE ecom_ind_rank
      END AS ecom_ind_rank,
    CASE
      WHEN ecom_txns_share = 99999
        THEN NULL
      ELSE ecom_txns_share
      END AS ecom_txns_share,
    CASE
      WHEN ecom_ind_rank_share = 99999
        THEN NULL
      ELSE ecom_ind_rank_share
      END AS ecom_ind_rank_share
  from
    wpay_analytics.mmi_area_industry_edit
  where
    area_level = 'country'
  group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21
  ) ind_lga
on 
 cast(store_info.industry as string) = cast(ind_lga.industry_code as string)
)

----------------------------------------
-- wpay store transactions
,
 store_trans_country_filt as 
(select 
  amount.Year as Year,
  amount.Month as Month,
  amount.Country as Country,
  --amount.State as State,
  --amount.Scheme as Scheme,
  amount.is_online as is_online,
  amount.scheme as scheme,
  amount.scheme_card_type as scheme_card_type,
  --amount.Store as Store,
  amount.industry as Industry,
  amount.industry_name as industry_name,
  amount.Brand as Brand,
  amount.division as division,
  amount.company as company,
  amount.invoice_group as invoice_group,
  amount.merchant_group as merchant_group,
  amount.merchant_portal_mg as merchant_portal_mg,
  amount.merchant_portal_cg as merchant_portal_cg,
  --amount.Suburb as Suburb,
  amount.trans_amount as trans_amount,
  counts.trans_count as trans_count,
from
(
select
  left(cast(trans.transaction_date as string),4) as Year,
  substring(cast(trans.transaction_date as string),6,2) as Month,
 -- right(cast(trans.transaction_date as string),2) as Day,
  stores.country as Country,
  --trans.state as State,
  --trans.scheme as Scheme,
  trans.is_online as is_online,
  trans.scheme as scheme,
  trans.scheme_card_type as scheme_card_type,
  --trans.store_id as Store,
  stores.industry as Industry,
  stores.industry_name as industry_name,
  stores.brand as Brand,
  stores.division as division,
  stores.company as company,
  stores.invoice_group as invoice_group,
  stores.merchant_group as merchant_group,
  stores.merchant_portal_mg as merchant_portal_mg,
  stores.merchant_portal_cg as merchant_portal_cg,
  --stores.suburb as Suburb,
  --sum(trans.transaction_count) as trans_count,
  sum(trans.settlement_amount_inc_gst) as trans_amount,
from
  pdh_sf_mep_vw.wpay_all_merchants_daily_fees_summary_hist trans
left join 
  wpay_analytics.ref_store_ind stores
on
  trans.store_id = stores.store_id
where
  (trans.approval_flag = 'A' ) 
  and ((trans.tran_type_description = 'REFUND')
  or (trans.tran_type_description = 'REFUND REVERSAL')
  or (trans.tran_type_description = 'PURCHASE')
  or (trans.tran_type_description = 'PURCHASE REVERSAL')
  or (trans.tran_type_description = 'PURCHASE WITH CASHOUT')
  or (trans.tran_type_description = 'PURCHASE WITH CASHOUT REVERSAL')
  or (trans.tran_type_description = 'REDEMPTION')
  or (trans.tran_type_description = 'REDEMPTION REVERSAL')
  )
group by
  1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
) amount
left join
  (
    select
  left(cast(trans.transaction_date as string),4) as Year,
  substring(cast(trans.transaction_date as string),6,2) as Month,
  --right(cast(trans.transaction_date as string),2) as Day,
  stores.country as Country,
  --trans.state as State,
  --trans.scheme as Scheme,
  trans.is_online as is_online,
  trans.scheme as scheme,
  trans.scheme_card_type as scheme_card_type,
  --trans.store_id as Store,
  stores.industry as Industry,
  stores.brand as Brand,
  stores.division as division,
  stores.company as company,
  stores.invoice_group as invoice_group,
  stores.merchant_group as merchant_group,
  stores.merchant_portal_mg as merchant_portal_mg,
  stores.merchant_portal_cg as merchant_portal_cg,
  --stores.suburb as Suburb,
  --sum(trans.transaction_count) as trans_count,
  (sum(case when trans.tran_type_description in 
  ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL') then transaction_count else 0 end)
  -
  sum(case when trans.tran_type_description in 
  ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL') then transaction_count else 0 end)
  ) as trans_count
  --sum(trans.settlement_amount_inc_gst) as trans_amount,
from
  pdh_sf_mep_vw.wpay_all_merchants_daily_fees_summary_hist trans
left join 
  wpay_analytics.ref_store_ind  stores
on
  trans.store_id = stores.store_id
where
  (trans.approval_flag = 'A' ) 
  -- and ((trans.tran_type_description = 'PURCHASE')
  -- or (trans.tran_type_description = 'PURCHASE REVERSAL')
  -- or (trans.tran_type_description = 'PURCHASE WITH CASHOUT')
  -- or (trans.tran_type_description = 'PURCHASE WITH CASHOUT REVERSAL')
  -- or (trans.tran_type_description = 'REDEMPTION')
  -- or (trans.tran_type_description = 'REDEMPTION REVERSAL')
  -- )
  group by
  1,2,3,4,5,6,7,8,9,10,11,12,13,14) counts
on (amount.Year = counts.Year)
   and (amount.Month = counts.Month)
   --and (amount.State = counts.State)
   --and (amount.Scheme = counts.Scheme)
   and (amount.is_online = counts.is_online)
   and (amount.scheme = counts.scheme)
   --and (amount.Store = counts.Store)
   and (amount.Brand = counts.Brand)
   and (amount.division = counts.division)
   and (amount.company = counts.company)
   and (amount.invoice_group = counts.invoice_group)
   and (amount.merchant_group = counts.merchant_group)
   and (amount.merchant_portal_mg = counts.merchant_portal_mg)
   and (amount.merchant_portal_cg = counts.merchant_portal_cg)
   and (amount.scheme_card_type = counts.scheme_card_type)
   --and (amount.Suburb = counts.Suburb))
)
------------------------------------------------------------------------------------------------------------

, country_internal_filt as
(
  select 
    store_trans_country_filt.*,
    case when cast(prev_m.prev_Amount as int) <> 0
    then
    ((store_trans_country_filt.trans_amount - prev_m.prev_Amount)/prev_m.prev_Amount) 
    else 0 end
     as trans_amount_mom,
    case when cast(prev_y.prev_Amount as int) <> 0
    then
    ((store_trans_country_filt.trans_amount - prev_y.prev_Amount)/prev_y.prev_Amount) 
    else 0 end
     as trans_amount_yoy,
    case when cast(prev_m.prev_Count as int) <> 0
    then
    ((store_trans_country_filt.trans_count - prev_m.prev_Count)/prev_m.prev_Count) 
    else 0 end
     as trans_count_mom,
    case when cast(prev_y.prev_Count as int) <> 0
    then
    ((store_trans_country_filt.trans_count - prev_y.prev_Count)/prev_y.prev_Count) 
    else 0 end
     as trans_count_yoy,
    prev_m.prev_Amount as trans_amount_LM,
    prev_m.prev_Count as trans_count_LM,
    prev_y.prev_Amount as trans_amount_LY,
    prev_y.prev_Count as trans_count_LY,
    case when cast(store_trans_country_filt.trans_count as int) <> 0 then
    (store_trans_country_filt.trans_amount/store_trans_country_filt.trans_count) 
    else 0 end 
    as trans_value,
    case when cast(prev_m.prev_Count as int) <> 0
    then
    (prev_m.prev_Amount/prev_m.prev_Count)  
    else 0 end 
    as trans_value_LM,
    case when cast(prev_y.prev_Count as int) <> 0
    then
    (prev_y.prev_Amount/prev_y.prev_Count)  
    else 0 end 
    as trans_value_LY
from store_trans_country_filt
left join
(
select 
  curr.*,
  prev.Year as prev_year,
  prev.Month as prev_month,
  prev.trans_amount as prev_Amount,
  prev.trans_count as prev_Count
from store_trans_country_filt curr
left join 
  store_trans_country_filt prev
on  
    (curr.Brand = prev.Brand)
and    (curr.division = prev.division)
   and (curr.company = prev.company)
   and (curr.invoice_group = prev.invoice_group)
   and (curr.merchant_group = prev.merchant_group)
   and (curr.merchant_portal_mg = prev.merchant_portal_mg)
   and (curr.merchant_portal_cg = prev.merchant_portal_cg)
   and curr.Month = prev.Month
   and (cast(curr.Year as int)-1) = cast(prev.Year as int)
   and curr.is_online = prev.is_online
   and curr.scheme = prev.scheme
   and curr.scheme_card_type = prev.scheme_card_type
order by curr.Year,curr.Month
) prev_y
on
    (store_trans_country_filt.Brand = prev_y.Brand)
and    (store_trans_country_filt.division = prev_y.division)
   and (store_trans_country_filt.company = prev_y.company)
   and (store_trans_country_filt.invoice_group = prev_y.invoice_group)
   and (store_trans_country_filt.merchant_group = prev_y.merchant_group)
   and (store_trans_country_filt.merchant_portal_mg = prev_y.merchant_portal_mg)
   and (store_trans_country_filt.merchant_portal_cg = prev_y.merchant_portal_cg)
  and store_trans_country_filt.Year = prev_y.Year
  and store_trans_country_filt.Month = prev_y.Month
  and store_trans_country_filt.is_online = prev_y.is_online
   and store_trans_country_filt.scheme = prev_y.scheme
  and store_trans_country_filt.scheme_card_type = prev_y.scheme_card_type
left join
(
select 
  curr.*,
  prev.trans_amount as prev_Amount,
  prev.trans_count as prev_Count
from store_trans_country_filt curr
left join 
  store_trans_country_filt prev
on 
    (curr.Brand = prev.Brand)
and    (curr.division = prev.division)
   and (curr.company = prev.company)
   and (curr.invoice_group = prev.invoice_group)
   and (curr.merchant_group = prev.merchant_group)
   and (curr.merchant_portal_mg = prev.merchant_portal_mg)
   and (curr.merchant_portal_cg = prev.merchant_portal_cg)
    and left(cast(date_add(cast(concat(concat(concat(concat(curr.Year,'-'),curr.Month),'-'),'01') as date), interval -1 month) as string),4) = cast(prev.Year as string)
    and substring(cast(date_add(cast(concat(concat(concat(concat(curr.Year,'-'),curr.Month),'-'),'01') as date), interval -1 month) as string),6,2)  = prev.Month
   and curr.is_online = prev.is_online
   and curr.scheme = prev.scheme
   and curr.scheme_card_type = prev.scheme_card_type
order by curr.Year,curr.Month
) prev_m
on
    (store_trans_country_filt.Brand = prev_m.Brand)
and    (store_trans_country_filt.division = prev_m.division)
   and (store_trans_country_filt.company = prev_m.company)
   and (store_trans_country_filt.invoice_group = prev_m.invoice_group)
   and (store_trans_country_filt.merchant_group = prev_m.merchant_group)
   and (store_trans_country_filt.merchant_portal_mg = prev_m.merchant_portal_mg)
   and (store_trans_country_filt.merchant_portal_cg = prev_m.merchant_portal_cg)
  and store_trans_country_filt.Year = prev_m.Year
  and store_trans_country_filt.Month = prev_m.Month
  and store_trans_country_filt.is_online = prev_m.is_online
   and store_trans_country_filt.scheme = prev_m.scheme
  and store_trans_country_filt.scheme_card_type = prev_m.scheme_card_type
order by store_trans_country_filt.Year,store_trans_country_filt.Month
)

-----------------------------------------------------------------------------------------------------------

--- adding only LGA to wpay_trans

, wpay_country_filt as
 (select 
    country_internal_filt.*,
    final_country_filt.area,
    --final_country_filt.industry_name,
    final_country_filt.spend_growth_yoy,
    final_country_filt.spend_growth_mom,
    final_country_filt.txn_growth_yoy,
    final_country_filt.txn_growth_mom,
    final_country_filt.spend_density_growth_yoy,
    final_country_filt.spend_density_growth_mom,
    final_country_filt.avg_ticket,
    final_country_filt.b_avg_ticket_LM,
    final_country_filt.b_avg_ticket_LY,
    final_country_filt.b_avg_ticket_mom,
    final_country_filt.b_avg_ticket_yoy,
    final_country_filt.avg_visits_per_cust,
    final_country_filt.avg_spend_per_cust,
    final_country_filt.ecom_ind_rank,
    final_country_filt.ecom_txns_share,
    final_country_filt.ecom_ind_rank_share,
    --final_lga.Benchmark
  from 
    country_internal_filt
  left join 
    final_country_filt
  on 
    (country_internal_filt.Brand = final_country_filt.Brand)
    and (country_internal_filt.division = final_country_filt.division)
    and (country_internal_filt.company = final_country_filt.company)
    and (country_internal_filt.invoice_group = final_country_filt.invoice_group)
    and (country_internal_filt.merchant_group = final_country_filt.merchant_group)
    and (country_internal_filt.merchant_portal_mg = final_country_filt.merchant_portal_mg)
    and (country_internal_filt.merchant_portal_cg = final_country_filt.merchant_portal_cg)
    and (country_internal_filt.Year = final_country_filt.Year)
    --and (country_internal.Month = final_country.Month)
    and (cast(country_internal_filt.Month as int) = cast(final_country_filt.Month as int))
  --where country_internal_filt.Year ='2022'
  --    and country_internal_filt.Month ='07'
  --order by final_lga.Benchmark
 )

------------------------------------------------------------
---- adding benchmark 
,
country_bench_filt as 
(
  select *,
  'Country' as Benchmark
  from wpay_country_filt
)


--------------------------------------------
--- going over LGA and filling nulls with State
, country_benchmark_filt as
(
  select
    'Country' as Aggregation_Level,
    'All' as store_id,
    'All' as store_status,
    country_bench_filt.country,
    'All' as state,
    country_bench_filt.division,
    country_bench_filt.brand,
    country_bench_filt.industry,
    'All' as site_name,
    'All' as suburb,
    'All' as postcode,
    'All' as caid,
    --'All' as is_closed,
    country_bench_filt.company,
    country_bench_filt.invoice_group,
    country_bench_filt.merchant_group,
    country_bench_filt.merchant_portal_mg,
    country_bench_filt.merchant_portal_cg,
    cast(country_bench_filt.is_online as string) as is_online,
    cast(country_bench_filt.scheme as string) as scheme,
    cast(country_bench_filt.scheme_card_type as string) as card_type,
    'All' as LGA,
    'All' as LGA_id,
    country_bench_filt.area,
    country_bench_filt.industry_name,
    --final_lga.industry_code,
    --wpay_LGA.Period,
    concat(country_bench_filt.Year,concat('-',country_bench_filt.Month)) as Period,
    country_bench_filt.Year,
    country_bench_filt.Month,
    country_bench_filt.trans_amount as w_trans_amount,
    country_bench_filt.trans_count as w_trans_count,
    country_bench_filt.trans_value as w_trans_value,
    country_bench_filt.trans_amount_LM as w_trans_amount_LM,
    country_bench_filt.trans_count_LM as w_trans_count_LM,
    country_bench_filt.trans_value_LM as w_trans_value_LM,
    country_bench_filt.trans_amount_LY as w_trans_amount_LY,
    country_bench_filt.trans_count_LY as w_trans_count_LY,
    country_bench_filt.trans_value_LY as w_trans_value_LY,
    country_bench_filt.trans_amount_mom as w_trans_amount_mom,
    country_bench_filt.trans_amount_yoy as w_trans_amount_yoy,
    country_bench_filt.trans_count_mom as w_trans_count_mom,
    country_bench_filt.trans_count_yoy as w_trans_count_yoy,
    case when cast(country_bench_filt.trans_value_LM as int) <> 0
    then ((country_bench_filt.trans_value - country_bench_filt.trans_value_LM)/country_bench_filt.trans_value_LM)  
    else 0 end 
    as w_trans_value_mom,
    case when cast(country_bench_filt.trans_value_LY as int) <> 0
    then ((country_bench_filt.trans_value - country_bench_filt.trans_value_LY)/country_bench_filt.trans_value_LY)  
    else 0 end 
    as w_trans_value_yoy,
    case when cust_info.unique_customer_count <> 0 then
    country_bench_filt.trans_count/cust_info.unique_customer_count 
    else 0 end as avg_cust_visits,
    case when cust_info.unique_customer_count <> 0 then
    country_bench_filt.trans_amount/cust_info.unique_customer_count 
    else 0 end as avg_cust_spend,
    cust_info.unique_card_count as unique_cust,
    cust_info.unique_customer_count as unique_card,
    country_bench_filt.spend_growth_yoy as b_spend_growth_yoy,
    country_bench_filt.spend_growth_mom as b_spend_growth_mom,
    country_bench_filt.txn_growth_yoy as b_txn_growth_yoy,
    country_bench_filt.txn_growth_mom as b_txn_growth_mom,
    country_bench_filt.spend_density_growth_yoy as b_spend_density_growth_yoy,
    country_bench_filt.spend_density_growth_mom as b_spend_density_growth_mom,
    country_bench_filt.avg_ticket as b_avg_ticket,
    country_bench_filt.b_avg_ticket_LM as b_avg_ticket_LM,
    country_bench_filt.b_avg_ticket_LY as b_avg_ticket_LY,
    country_bench_filt.b_avg_ticket_mom as b_avg_ticket_mom,
    country_bench_filt.b_avg_ticket_yoy as b_avg_ticket_yoy,
    country_bench_filt.avg_visits_per_cust as b_avg_visits_per_cust,
    country_bench_filt.avg_spend_per_cust as b_avg_spend_per_cust,
    country_bench_filt.ecom_ind_rank as b_ecom_ind_rank,
    country_bench_filt.ecom_txns_share as b_ecom_txns_share,
    country_bench_filt.ecom_ind_rank_share as b_ecom_ind_rank_share,
    --country_bench_filt.Benchmark,
    country_bench_filt.industry_name as Benchmark,
    country_bench_filt.Benchmark as Benchmark_Location,
    'External' as Benchmark_lvl
  from 
    country_bench_filt
  left join
    (
  select 
      m.company,
      m.invoice_group,    
      m.merchant_group,
      m.division, 
      m.brand, 
      m.merchant_portal_mg,
      m.merchant_portal_cg,   
      --m.store_id,   
      --m.site_name,    
     -- m.suburb,   
      --m.state,    
      --m.postcode,
      m.country,
      m.is_online,
      fee.calc_scheme as scheme,
      fee.calc_card_type as card_type,
      left(cast(fee.switch_tran_date as string),7) Transaction_Date,
      -- sum(case when fee.calc_tran_type_description in
      -- ('REFUND','REFUND REVERSAL','PURCHASE','PURCHASE REVERSAL','PURCHASE WITH CASHOUT','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION','REDEMPTION REVERSAL') then fee.transaction_total else 0 end)/100 as trans_amount,
      -- (sum(case when fee.calc_tran_type_description in
      -- ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL') then 1 else 0 end)
      -- -
      -- sum(case when fee.calc_tran_type_description in 
      -- ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL') then 1 else 0 end)
      -- ) as trans_count,
      count(distinct fee.card_id) unique_card_count,
      count(distinct fee.card_id) unique_customer_count
      -- (count(1) / count(distinct fee.card_id)) frequency_of_visits,
      -- (  sum(case when fee.calc_tran_type_description in
      -- ('REFUND','REFUND REVERSAL','PURCHASE','PURCHASE REVERSAL','PURCHASE WITH CASHOUT','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION','REDEMPTION REVERSAL') then fee.transaction_total else 0 end) 
      -- / (100*count(distinct fee.card_id))) spend_per_card_holder 
      from wpay_analytics.wpay_billing_engine_tran_dtl fee  
      inner join
      `pdh_ref_ds.ref_store_details` m 
      on LEFT(FEE.net_term_id, 5) = M.store_id
      --inner join
      --wpay_analytics.wpay_card_identity_model cim
      --on trim(cim.scdr_token) = trim(fee.scdr_token)
      where fee.calc_approval_flag = 'A'
	  and left(cast(fee.tstamp_trans as string),7) > '2022-07'
and fee.calc_tran_type_description in ('REFUND','REFUND REVERSAL','PURCHASE','PURCHASE REVERSAL','PURCHASE WITH CASHOUT','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION','REDEMPTION REVERSAL')
      group by 1,2,3,4,5,6,7,8,9,10,11,12
      ) cust_info
  on cust_info.company = country_bench_filt.company
    and cust_info.merchant_group = country_bench_filt.merchant_group
    and cust_info.division = country_bench_filt.division
    and cust_info.country = country_bench_filt.country
    and concat(country_bench_filt.Year,concat('-',country_bench_filt.Month)) = cust_info.Transaction_Date
    and cust_info.is_online = country_bench_filt.is_online
    and cust_info.scheme = country_bench_filt.scheme
    and cust_info.invoice_group = country_bench_filt.invoice_group
    and cust_info.brand = country_bench_filt.brand
    and cust_info.merchant_portal_mg = country_bench_filt.merchant_portal_mg
    and cust_info.merchant_portal_cg = country_bench_filt.merchant_portal_cg
    and cust_info.card_type = country_bench_filt.scheme_card_type
)
-- end
select 
  *
  from lga_benchmark_filt
union all
select 
  *
  from state_benchmark_filt
union all
select 
  *
  from country_benchmark_filt

);
--------------------------------------------------------------------------------------------------------------
-- END
--------------------------------------------------------------------------------------------------------------


--------------------------------------------------------------------------------------------------------------
-- ONLINE + OFFLINE ONLY

create or replace table pdh_staging_ds.temp_online as (
-------------------------------------------------------------------------------------------------------------------
-- LGA/STATE
-------------------------------------------------------------------------------------------------------------------
-- Store base table + mastercard linking and benchmark

with
final_lga_online as 
(select
  *
from
(
select 
  distinct stores.store_id,
  stores.country,
  stores.state,
  stores.division,
  stores.brand,
  stores.industry,
  stores.industry_name,
  --distinct stores.store_id,
  stores.site_name,
  stores.suburb,
  stores.postcode,
  stores.caid,
  --stores.is_closed,
  stores.company,
  stores.invoice_group,
  stores.merchant_group,
  stores.merchant_portal_mg,
  stores.merchant_portal_cg,
  --stores.is_online,
  stores.LGA as LGA,
  stores.LGA_id as LGA_id
from
  wpay_analytics.ref_store_ind stores
left join
(
  select *
  from
  (select 
  distinct mmi_postcode.Merchant_Post_Code,
  mmi_postcode.City_Name,
  key.level3,
  upper(key.level2) as level2,
  key.level2_id as level2_id
from
  wpay_analytics.ref_merchant_mmi_postcode mmi_postcode
left join
  wpay_analytics.mmi_aus_mapping_key_edit key
on 
  (cast(mmi_postcode.Level_3_Id as string) = cast(key.level3_id as string))
  and (mmi_postcode.Province_Code = key.State)
  ) 
  --where level2 is not null
 ) mmi_location
on 
  (cast(stores.postcode as string) = cast(mmi_location.Merchant_Post_Code as string))
  --and (replace(upper(mmi_location.City_Name),' ','') = replace(upper(stores.suburb),' ',''))
) store_lga
left join
  (
  select 
    area,
    period,
   -- industry_name,
    industry_code,
    left(period,4) as Year,
    right(period,2) as Month,
    CASE 
      WHEN spend_growth_yoy = 99999
        THEN NULL
      ELSE spend_growth_yoy
      END AS spend_growth_yoy,
    CASE 
      WHEN spend_growth_mom = 99999
        THEN NULL
      ELSE spend_growth_mom
      END AS spend_growth_mom,
    CASE
      WHEN txn_growth_yoy = 99999
        THEN NULL
      ELSE txn_growth_yoy
      END AS txn_growth_yoy,
    CASE
      WHEN txn_growth_mom = 99999
        THEN NULL
      ELSE txn_growth_mom
      END AS txn_growth_mom,
    CASE
      WHEN spend_density_growth_yoy = 99999
        THEN NULL
      ELSE spend_density_growth_yoy
      END AS spend_density_growth_yoy,
    CASE
      WHEN spend_density_growth_mom = 99999
        THEN NULL
      ELSE spend_density_growth_mom
      END AS spend_density_growth_mom,
    CASE
      WHEN avg_ticket = 99999
        THEN NULL
      ELSE avg_ticket
      END AS avg_ticket,
    b_avg_ticket_LM,
    b_avg_ticket_LY,
    b_avg_ticket_mom,
    b_avg_ticket_yoy,
    CASE
      WHEN avg_visits_per_cust = 99999
        THEN NULL
      ELSE avg_visits_per_cust
      END AS avg_visits_per_cust,
    CASE
      WHEN avg_spend_per_cust = 99999
        THEN NULL
      ELSE avg_spend_per_cust
      END AS avg_spend_per_cust,
    CASE
      WHEN ecom_ind_rank = 99999
        THEN NULL
      ELSE ecom_ind_rank
      END AS ecom_ind_rank,
    CASE
      WHEN ecom_txns_share = 99999
        THEN NULL
      ELSE ecom_txns_share
      END AS ecom_txns_share,
    CASE
      WHEN ecom_ind_rank_share = 99999
        THEN NULL
      ELSE ecom_ind_rank_share
      END AS ecom_ind_rank_share
  from
    wpay_analytics.mmi_area_industry_edit
  where
    area_level = 'local_govt_area'
  ) ind_lga
on 
  ((cast(store_lga.LGA_id as string) = cast(ind_lga.area as string))
  and cast(store_lga.industry as string) = cast(ind_lga.industry_code as string)))

----------------------------------------
-- wpay store transactions
,
  store_trans_online as 
(select 
  amount.Year as Year,
  amount.Month as Month,
  amount.State as State,
  --amount.Scheme as Scheme,
  amount.is_online as is_online,
  --amount.Card_Type as Card_Type,
  amount.Store as Store,
  amount.Brand as Brand,
  amount.Suburb as Suburb,
  amount.trans_amount as trans_amount,
  counts.trans_count as trans_count,
from
(
select
  left(cast(trans.transaction_date as string),4) as Year,
  substring(cast(trans.transaction_date as string),6,2) as Month,
 -- right(cast(trans.transaction_date as string),2) as Day,
  trans.state as State,
  --trans.scheme as Scheme,
  trans.is_online as is_online,
  --trans.card_type as Card_Type,
  trans.store_id as Store,
  stores.brand as Brand,
  stores.suburb as Suburb,
  --sum(trans.transaction_count) as trans_count,
  sum(trans.settlement_amount_inc_gst) as trans_amount,
from
  pdh_sf_mep_vw.wpay_all_merchants_daily_fees_summary_hist trans
left join 
  wpay_analytics.ref_store_ind stores
on
  trans.store_id = stores.store_id
where
  (trans.approval_flag = 'A' ) 
  and ((trans.tran_type_description = 'REFUND')
  or (trans.tran_type_description = 'REFUND REVERSAL')
  or (trans.tran_type_description = 'PURCHASE')
  or (trans.tran_type_description = 'PURCHASE REVERSAL')
  or (trans.tran_type_description = 'PURCHASE WITH CASHOUT')
  or (trans.tran_type_description = 'PURCHASE WITH CASHOUT REVERSAL')
  or (trans.tran_type_description = 'REDEMPTION')
  or (trans.tran_type_description = 'REDEMPTION REVERSAL')
  )
group by
  1,2,3,4,5,6,7
) amount
left join
  (
    select
  left(cast(trans.transaction_date as string),4) as Year,
  substring(cast(trans.transaction_date as string),6,2) as Month,
  --right(cast(trans.transaction_date as string),2) as Day,
  trans.state as State,
  --trans.scheme as Scheme,
  trans.is_online as is_online,
  --trans.card_type as Card_Type,
  trans.store_id as Store,
  stores.brand as Brand,
  stores.suburb as Suburb,
  --sum(trans.transaction_count) as trans_count,
  (sum(case when trans.tran_type_description in 
  ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL') then transaction_count else 0 end)
  -
  sum(case when trans.tran_type_description in 
  ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL') then transaction_count else 0 end)
  ) as trans_count
  --sum(trans.settlement_amount_inc_gst) as trans_amount,
from
  pdh_sf_mep_vw.wpay_all_merchants_daily_fees_summary_hist trans
left join 
  wpay_analytics.ref_store_ind stores
on
  trans.store_id = stores.store_id
where
  (trans.approval_flag = 'A' ) 
  -- and ((trans.tran_type_description = 'PURCHASE')
  -- or (trans.tran_type_description = 'PURCHASE REVERSAL')
  -- or (trans.tran_type_description = 'PURCHASE WITH CASHOUT')
  -- or (trans.tran_type_description = 'PURCHASE WITH CASHOUT REVERSAL')
  -- or (trans.tran_type_description = 'REDEMPTION')
  -- or (trans.tran_type_description = 'REDEMPTION REVERSAL')
  -- )
  group by
  1,2,3,4,5,6,7) counts
on (amount.Year = counts.Year)
   and (amount.Month = counts.Month)
   and (amount.State = counts.State)
   --and (amount.Scheme = counts.Scheme)
   and (amount.is_online = counts.is_online)
   --and (amount.Card_Type = counts.Card_Type)
   and (amount.Store = counts.Store)
   and (amount.Brand = counts.Brand)
   and (amount.Suburb = counts.Suburb)
--order by Store,year,month
)

-------------------------------------------------------------------------------------------------------------

-- Joined on same table to get previous month/year

, store_internal_online as
(
  select 
    store_trans_online.*,
    case when cast(prev_m.prev_Amount as int) <> 0
    then
    ((store_trans_online.trans_amount - prev_m.prev_Amount)/prev_m.prev_Amount)  
    else 0 end 
    as trans_amount_mom,
    case when cast(prev_y.prev_Amount as int) <> 0
    then
    ((store_trans_online.trans_amount - prev_y.prev_Amount)/prev_y.prev_Amount)  
    else 0 end
    as trans_amount_yoy,
    case when cast(prev_m.prev_Count as int) <> 0
    then
    ((store_trans_online.trans_count - prev_m.prev_Count)/prev_m.prev_Count)  
    else 0 end
    as trans_count_mom,
    case when cast(prev_y.prev_Count as int) <> 0
    then
    ((store_trans_online.trans_count - prev_y.prev_Count)/prev_y.prev_Count)  
    else 0 end
    as trans_count_yoy,
    prev_m.prev_Amount as trans_amount_LM,
    prev_m.prev_Count as trans_count_LM,
    prev_y.prev_Amount as trans_amount_LY,
    prev_y.prev_Count as trans_count_LY,
    case when cast(store_trans_online.trans_count as int) <> 0 then
    (store_trans_online.trans_amount/store_trans_online.trans_count) 
    else 0 end 
    as trans_value,
    case when cast(prev_m.prev_Count as int) <> 0
    then
    (prev_m.prev_Amount/prev_m.prev_Count)  
    else 0 end 
    as trans_value_LM,
    case when cast(prev_y.prev_Count as int) <> 0
    then
    (prev_y.prev_Amount/prev_y.prev_Count)  
    else 0 end 
    as trans_value_LY
from store_trans_online
left join
(
select 
  curr.*,
  prev.Year as prev_year,
  prev.Month as prev_month,
  prev.trans_amount as prev_Amount,
  prev.trans_count as prev_Count
from store_trans_online curr
left join 
  store_trans_online prev
on curr.Store = prev.Store
    and curr.Month = prev.Month
    and (cast(curr.Year as int)-1) = cast(prev.Year as int)
    and curr.is_online = prev.is_online
    --and curr.Card_Type = prev.Card_Type
order by curr.Store,curr.Year,curr.Month
) prev_y
on
  store_trans_online.Store = prev_y.Store
  and store_trans_online.Year = prev_y.Year
  and store_trans_online.Month = prev_y.Month
  and store_trans_online.is_online = prev_y.is_online
  --and store_trans_online.Card_Type = prev_y.Card_Type
left join
(
select 
  curr.*,
  prev.trans_amount as prev_Amount,
  prev.trans_count as prev_Count
from store_trans_online curr
left join 
  store_trans_online prev
on curr.Store = prev.Store
    and left(cast(date_add(cast(concat(concat(concat(concat(curr.Year,'-'),curr.Month),'-'),'01') as date), interval -1 month) as string),4) = cast(prev.Year as string)
    and substring(cast(date_add(cast(concat(concat(concat(concat(curr.Year,'-'),curr.Month),'-'),'01') as date), interval -1 month) as string),6,2)  = prev.Month
    and curr.is_online = prev.is_online
    --and curr.Card_Type = prev.Card_Type
order by curr.Store,curr.Year,curr.Month
) prev_m
on
  store_trans_online.Store = prev_m.Store
  and store_trans_online.Year = prev_m.Year
  and store_trans_online.Month = prev_m.Month
  and store_trans_online.is_online = prev_m.is_online
  --and store_trans_online.Card_Type = prev_m.Card_Type
order by store_trans_online.Store, store_trans_online.Year,store_trans_online.Month
)

---------------------------------------------------------------------------------------------------------------
,
wpay_store_trans_online as
  (select 
    distinct stores.store_id,
    stores.country,
    stores.state,
    stores.division,
    stores.brand,
    stores.company,
    stores.invoice_group,
    stores.merchant_group,
    stores.merchant_portal_mg,
    stores.merchant_portal_cg,
    stores.industry,
    stores.industry_name as industry_name,
    --distinct stores.store_id,
    stores.site_name,
    stores.suburb,
    stores.postcode,
    stores.LGA,
    stores.LGA_id,
    stores.caid,
    --stores.is_closed,
    store_internal_online.is_online,
    --store_internal_online.Card_Type,
    store_internal_online.Year as Year,
    store_internal_online.Month as Month,
    store_internal_online.trans_amount as trans_amount,
    store_internal_online.trans_count as trans_count,
    store_internal_online.trans_amount_mom as trans_amount_mom,
    store_internal_online.trans_amount_yoy as trans_amount_yoy,
    store_internal_online.trans_count_mom as trans_count_mom,
    store_internal_online.trans_count_yoy as trans_count_yoy,
    store_internal_online.trans_amount_LM as trans_amount_LM,
    store_internal_online.trans_count_LM as trans_count_LM,
    store_internal_online.trans_amount_LY as trans_amount_LY,
    store_internal_online.trans_count_LY as trans_count_LY,
    store_internal_online.trans_value as trans_value,
    store_internal_online.trans_value_LM as trans_value_LM,
    store_internal_online.trans_value_LY as trans_value_LY
  from
    wpay_analytics.ref_store_ind stores
  inner join
    store_internal_online
  on stores.store_id = store_internal_online.Store
  --group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34
  order by store_id
  )

-------------------------------------------
--- adding only LGA to wpay_trans

, wpay_LGA_online as
 (select 
    wpay_store_trans_online.*,
    --final_lga_online.LGA,
    --final_lga_online.LGA_id,
    final_lga_online.area,
    --final_lga_online.industry_name,
    final_lga_online.spend_growth_yoy,
    final_lga_online.spend_growth_mom,
    final_lga_online.txn_growth_yoy,
    final_lga_online.txn_growth_mom,
    final_lga_online.spend_density_growth_yoy,
    final_lga_online.spend_density_growth_mom,
    final_lga_online.avg_ticket,
    final_lga_online.b_avg_ticket_LM,
    final_lga_online.b_avg_ticket_LY,
    final_lga_online.b_avg_ticket_mom,
    final_lga_online.b_avg_ticket_yoy,
    final_lga_online.avg_visits_per_cust,
    final_lga_online.avg_spend_per_cust,
    final_lga_online.ecom_ind_rank,
    final_lga_online.ecom_txns_share,
    final_lga_online.ecom_ind_rank_share,
    --final_lga.Benchmark
  from 
    wpay_store_trans_online
  left join 
    final_lga_online
  on (wpay_store_trans_online.store_id = final_lga_online.store_id)
    and (wpay_store_trans_online.Year = final_lga_online.Year)
    --and (wpay_store_trans.Month = final_lga.Month)
    and (cast(wpay_store_trans_online.Month as int) = cast(final_lga_online.Month as int))
 -- where wpay_store_trans_online.Year ='2022'
  --    and wpay_store_trans_online.Month ='07'
  --order by final_lga.Benchmark
 )

------------------------------------------------------------
---- adding benchmark 
,

MaxMMIDate as (
select max(Period) as max_date
from  wpay_analytics.mmi_area_industry_edit
),

lga_bench_online as 
(
  select *,
  case when 
    (coalesce(spend_growth_yoy,spend_growth_mom,txn_growth_yoy,txn_growth_mom) is null 
    and concat(concat(Year,'-'),Month) <= (select max_date from MaxMMIDate))
    then 'State'
    else 'LGA'
    end as Benchmark
    from wpay_LGA_online
)


--------------------------------------------
--- going over LGA and filling nulls with State
, lga_benchmark_online as
(
  select
    'Store' as Aggregation_Level,
    lga_bench_online.store_id,
    'All' as store_status,
    lga_bench_online.country,
    lga_bench_online.state,
    lga_bench_online.division,
    lga_bench_online.brand,
    lga_bench_online.industry,
    lga_bench_online.site_name,
    lga_bench_online.suburb,
    cast(lga_bench_online.postcode as string) as postcode,
    cast(lga_bench_online.caid as string)  as caid,
    --cast(lga_bench_filt.is_closed as string) as is_closed,
    lga_bench_online.company,
    lga_bench_online.invoice_group,
    lga_bench_online.merchant_group,
    lga_bench_online.merchant_portal_mg,
    lga_bench_online.merchant_portal_cg,
    cast(lga_bench_online.is_online as string) as is_online,
    'All' as scheme,
    'All' as card_type,
    lga_bench_online.LGA,
    cast(lga_bench_online.LGA_id as string) as LGA_id,
    lga_bench_online.area,
    lga_bench_online.industry_name,
    --final_lga.industry_code,
    --wpay_LGA.Period,
    concat(lga_bench_online.Year,concat('-',lga_bench_online.Month)) as Period,
    lga_bench_online.Year,
    lga_bench_online.Month,
    lga_bench_online.trans_amount as w_trans_amount,
    lga_bench_online.trans_count as w_trans_count,
    lga_bench_online.trans_value as w_trans_value,
    lga_bench_online.trans_amount_LM as w_trans_amount_LM,
    lga_bench_online.trans_count_LM as w_trans_count_LM,
    lga_bench_online.trans_value_LM as w_trans_value_LM,
    lga_bench_online.trans_amount_LY as w_trans_amount_LY,
    lga_bench_online.trans_count_LY as w_trans_count_LY,
    lga_bench_online.trans_value_LY as w_trans_value_LY,
    lga_bench_online.trans_amount_mom as w_trans_amount_mom,
    lga_bench_online.trans_amount_yoy as w_trans_amount_yoy,
    lga_bench_online.trans_count_mom as w_trans_count_mom,
    lga_bench_online.trans_count_yoy as w_trans_count_yoy,
    case when cast(lga_bench_online.trans_value_LM as int) <> 0
    then ((lga_bench_online.trans_value - lga_bench_online.trans_value_LM)/lga_bench_online.trans_value_LM)  
    else 0 end 
    as w_trans_value_mom,
    case when cast(lga_bench_online.trans_value_LY as int) <> 0
    then ((lga_bench_online.trans_value - lga_bench_online.trans_value_LY)/lga_bench_online.trans_value_LY)  
    else 0 end 
    as w_trans_value_yoy,
    case when cust_info.unique_customer_count <> 0 then
    lga_bench_online.trans_count/cust_info.unique_customer_count 
    else 0 end as avg_cust_visits,
    case when cust_info.unique_customer_count <> 0 then
    lga_bench_online.trans_amount/cust_info.unique_customer_count 
    else 0 end as avg_cust_spend,
    cust_info.unique_card_count as unique_cust,
    cust_info.unique_customer_count as unique_card,
    CASE 
      WHEN lga_bench_online.Benchmark = 'State'
        THEN ind_key.spend_growth_yoy
      ELSE lga_bench_online.spend_growth_yoy
      END AS b_spend_growth_yoy,
    CASE 
      WHEN lga_bench_online.Benchmark = 'State'
        THEN ind_key.spend_growth_mom
      ELSE lga_bench_online.spend_growth_mom
      END AS b_spend_growth_mom,
    CASE
      WHEN lga_bench_online.Benchmark = 'State'
        THEN ind_key.txn_growth_yoy
      ELSE lga_bench_online.txn_growth_yoy
      END AS b_txn_growth_yoy,
    CASE
      WHEN lga_bench_online.Benchmark = 'State'
        THEN ind_key.txn_growth_mom
      ELSE lga_bench_online.txn_growth_mom
      END AS b_txn_growth_mom,
    CASE
      WHEN lga_bench_online.Benchmark = 'State'
        THEN ind_key.spend_density_growth_yoy
      ELSE lga_bench_online.spend_density_growth_yoy
      END AS b_spend_density_growth_yoy,
    CASE
      WHEN lga_bench_online.Benchmark = 'State'
        THEN ind_key.spend_density_growth_mom
      ELSE lga_bench_online.spend_density_growth_mom
      END AS b_spend_density_growth_mom,
    CASE
      WHEN lga_bench_online.Benchmark = 'State'
        THEN ind_key.avg_ticket
      ELSE lga_bench_online.avg_ticket
      END AS b_avg_ticket,
    CASE
      WHEN lga_bench_online.Benchmark = 'State'
        THEN ind_key.b_avg_ticket_LM
      ELSE lga_bench_online.b_avg_ticket_LM
      END AS b_avg_ticket_LM,
    CASE
      WHEN lga_bench_online.Benchmark = 'State'
        THEN ind_key.b_avg_ticket_LY
      ELSE lga_bench_online.b_avg_ticket_LY
      END AS b_avg_ticket_LY,
    CASE
      WHEN lga_bench_online.Benchmark = 'State'
        THEN ind_key.b_avg_ticket_mom
      ELSE lga_bench_online.b_avg_ticket_mom
      END AS b_avg_ticket_mom,
    CASE
      WHEN lga_bench_online.Benchmark = 'State'
        THEN ind_key.b_avg_ticket_yoy
      ELSE lga_bench_online.b_avg_ticket_yoy
      END AS b_avg_ticket_yoy,
    CASE
      WHEN lga_bench_online.Benchmark = 'State'
        THEN ind_key.avg_visits_per_cust
      ELSE lga_bench_online.avg_visits_per_cust
      END AS b_avg_visits_per_cust,
    CASE
      WHEN lga_bench_online.Benchmark = 'State'
        THEN ind_key.avg_spend_per_cust
      ELSE lga_bench_online.avg_spend_per_cust
      END AS b_avg_spend_per_cust,
    CASE
      WHEN lga_bench_online.Benchmark = 'State'
        THEN ind_key.ecom_ind_rank
      ELSE lga_bench_online.ecom_ind_rank
      END AS b_ecom_ind_rank,
    CASE
      WHEN lga_bench_online.Benchmark = 'State'
        THEN ind_key.ecom_txns_share
      ELSE lga_bench_online.ecom_txns_share
      END AS b_ecom_txns_share,
    CASE
      WHEN lga_bench_online.Benchmark = 'State'
        THEN ind_key.ecom_ind_rank_share
      ELSE lga_bench_online.ecom_ind_rank_share
      END AS b_ecom_ind_rank_share,
    --lga_bench_online.Benchmark,
    lga_bench_online.industry_name as Benchmark,
    lga_bench_online.Benchmark as Benchmark_Location,
    'External' as Benchmark_lvl
  from 
    lga_bench_online
  left join
    ( select distinct ind.*, 
    key.State as state,
    left(ind.period,4) as Year,
    right(ind.period,2) as Month
    from
    wpay_analytics.mmi_area_industry_edit ind
    left join 
      wpay_analytics.mmi_aus_mapping_key_edit key
      on cast(ind.area as string) = cast(key.level1_id as string)
    where area_level = 'state') ind_key
  on cast(lga_bench_online.state as string) = cast(ind_key.state as string)
     and cast(lga_bench_online.industry as string) = cast(ind_key.industry_code as string) 
     --and (final_lga.period = ind_key.period ) )
     and cast(trim(lga_bench_online.Year) as string) = cast(trim(ind_key.Year) as string) 
     --and cast(trim(lga_bench.Month) as string) = cast(trim(ind_key.Month) as string) 
     and (cast(lga_bench_online.Month as int) = cast(ind_key.Month as int))
     left join
    (
  select 
      --m.company,
      --m.invoice_group,    
      --m.merchant_group,
      --m.division, 
      --m.brand,    
      m.store_id,   
      --m.site_name,    
     -- m.suburb,   
     -- m.state,    
      --m.postcode,
     -- m.country,
      m.is_online,
      --fee.calc_scheme as scheme,
      --fee.calc_card_type as card_type,
      left(cast(fee.switch_tran_date as string),7) Transaction_Date,
      -- sum(case when fee.calc_tran_type_description in
      -- ('REFUND','REFUND REVERSAL','PURCHASE','PURCHASE REVERSAL','PURCHASE WITH CASHOUT','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION','REDEMPTION REVERSAL') then fee.transaction_total else 0 end)/100 as trans_amount,
      -- (sum(case when fee.calc_tran_type_description in
      -- ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL') then 1 else 0 end)
      -- -
      -- sum(case when fee.calc_tran_type_description in 
      -- ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL') then 1 else 0 end)
      -- ) as trans_count,
      count(distinct fee.card_id) unique_card_count,
      count(distinct fee.card_id) unique_customer_count
      -- (count(1) / count(distinct fee.card_id)) frequency_of_visits,
      -- (  sum(case when fee.calc_tran_type_description in
      -- ('REFUND','REFUND REVERSAL','PURCHASE','PURCHASE REVERSAL','PURCHASE WITH CASHOUT','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION','REDEMPTION REVERSAL') then fee.transaction_total else 0 end) 
      -- / (100*count(distinct fee.card_id))) spend_per_card_holder 
      from wpay_analytics.wpay_billing_engine_tran_dtl fee  
      inner join
      `pdh_ref_ds.ref_store_details` m 
      on LEFT(FEE.net_term_id, 5) = M.store_id
      --inner join
      --wpay_analytics.wpay_card_identity_model cim
      --on trim(cim.scdr_token) = trim(fee.scdr_token)
      where fee.calc_approval_flag = 'A'
	  and left(cast(fee.tstamp_trans as string),7) > '2022-07'
and fee.calc_tran_type_description in ('REFUND','REFUND REVERSAL','PURCHASE','PURCHASE REVERSAL','PURCHASE WITH CASHOUT','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION','REDEMPTION REVERSAL')
      group by 1,2,3
      ) cust_info
  on cust_info.store_id = lga_bench_online.store_id
    and cust_info.is_online = lga_bench_online.is_online
     and concat(lga_bench_online.Year,concat('-',lga_bench_online.Month)) = cust_info.Transaction_Date
  order by lga_bench_online.Year, lga_bench_online.Month, lga_bench_online.store_id
)
-- end

--------------------------------------------------------------------------------------------------------------

--------------------------------------------------------------------------------------------------------------
-- STATE/AUS
--------------------------------------------------------------------------------------------------------------

-- Store base table + mastercard linking and benchmark

,
 final_state_online as 
(select
  *
from
(
select 
  stores.country,
  stores.state,
  stores.division,
  stores.brand,
  stores.company,
  stores.industry,
  stores.industry_name,
  --distinct stores.store_id,
  --stores.site_name,
  --stores.suburb,
  --stores.postcode,
  --stores.caid,
  --stores.is_closed,
  --stores.company,
  stores.invoice_group,
  stores.merchant_group,
  stores.merchant_portal_mg,
  stores.merchant_portal_cg
  --stores.is_online,
from
  wpay_analytics.ref_store_ind stores
group by 1,2,3,4,5,6,7,8,9,10,11) store_info
left join
  (
  select 
    area,
    period,
   -- industry_name,
    industry_code,
    left(period,4) as Year,
    right(period,2) as Month,
    key.State as state_mmi,
    CASE 
      WHEN spend_growth_yoy = 99999
        THEN NULL
      ELSE spend_growth_yoy
      END AS spend_growth_yoy,
    CASE 
      WHEN spend_growth_mom = 99999
        THEN NULL
      ELSE spend_growth_mom
      END AS spend_growth_mom,
    CASE
      WHEN txn_growth_yoy = 99999
        THEN NULL
      ELSE txn_growth_yoy
      END AS txn_growth_yoy,
    CASE
      WHEN txn_growth_mom = 99999
        THEN NULL
      ELSE txn_growth_mom
      END AS txn_growth_mom,
    CASE
      WHEN spend_density_growth_yoy = 99999
        THEN NULL
      ELSE spend_density_growth_yoy
      END AS spend_density_growth_yoy,
    CASE
      WHEN spend_density_growth_mom = 99999
        THEN NULL
      ELSE spend_density_growth_mom
      END AS spend_density_growth_mom,
    CASE
      WHEN avg_ticket = 99999
        THEN NULL
      ELSE avg_ticket
      END AS avg_ticket,
    b_avg_ticket_LM,
    b_avg_ticket_LY,
    b_avg_ticket_mom,
    b_avg_ticket_yoy,
    CASE
      WHEN avg_visits_per_cust = 99999
        THEN NULL
      ELSE avg_visits_per_cust
      END AS avg_visits_per_cust,
    CASE
      WHEN avg_spend_per_cust = 99999
        THEN NULL
      ELSE avg_spend_per_cust
      END AS avg_spend_per_cust,
    CASE
      WHEN ecom_ind_rank = 99999
        THEN NULL
      ELSE ecom_ind_rank
      END AS ecom_ind_rank,
    CASE
      WHEN ecom_txns_share = 99999
        THEN NULL
      ELSE ecom_txns_share
      END AS ecom_txns_share,
    CASE
      WHEN ecom_ind_rank_share = 99999
        THEN NULL
      ELSE ecom_ind_rank_share
      END AS ecom_ind_rank_share
  from
    wpay_analytics.mmi_area_industry_edit
  left join
    wpay_analytics.mmi_aus_mapping_key_edit key
    on cast(key.level1_id as string) = cast(area as string)
  where
    area_level = 'state'
  group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22
  ) ind_lga
on 
  cast(store_info.state as string) = cast(ind_lga.state_mmi as string)
  and cast(store_info.industry as string) = cast(ind_lga.industry_code as string)
)

----------------------------------------
-- wpay store transactions
,
 store_trans_state_online as 
(select 
  amount.Year as Year,
  amount.Month as Month,
  amount.Country as Country,
  amount.State as State,
  --amount.Scheme as Scheme,
  amount.is_online as is_online,
  --amount.Card_Type as Card_Type,
  --amount.Store as Store,
  amount.industry as Industry,
  amount.industry_name as industry_name,
  amount.Brand as Brand,
  amount.division as division,
  amount.company as company,
  amount.invoice_group as invoice_group,
  amount.merchant_group as merchant_group,
  amount.merchant_portal_mg as merchant_portal_mg,
  amount.merchant_portal_cg as merchant_portal_cg,
  --amount.Suburb as Suburb,
  amount.trans_amount as trans_amount,
  counts.trans_count as trans_count,
from
(
select
  left(cast(trans.transaction_date as string),4) as Year,
  substring(cast(trans.transaction_date as string),6,2) as Month,
 -- right(cast(trans.transaction_date as string),2) as Day,
  stores.country as Country,
  trans.state as State,
  --trans.scheme as Scheme,
  trans.is_online as is_online,
  --trans.card_type as Card_Type,
  --trans.store_id as Store,
  stores.industry as Industry,
  stores.industry_name as industry_name,
  stores.brand as Brand,
  stores.division as division,
  stores.company as company,
  stores.invoice_group as invoice_group,
  stores.merchant_group as merchant_group,
  stores.merchant_portal_mg as merchant_portal_mg,
  stores.merchant_portal_cg as merchant_portal_cg,
  --stores.suburb as Suburb,
  --sum(trans.transaction_count) as trans_count,
  sum(trans.settlement_amount_inc_gst) as trans_amount,
from
  pdh_sf_mep_vw.wpay_all_merchants_daily_fees_summary_hist trans
left join 
  wpay_analytics.ref_store_ind stores
on
  trans.store_id = stores.store_id
where
  (trans.approval_flag = 'A' ) 
  and ((trans.tran_type_description = 'REFUND')
  or (trans.tran_type_description = 'REFUND REVERSAL')
  or (trans.tran_type_description = 'PURCHASE')
  or (trans.tran_type_description = 'PURCHASE REVERSAL')
  or (trans.tran_type_description = 'PURCHASE WITH CASHOUT')
  or (trans.tran_type_description = 'PURCHASE WITH CASHOUT REVERSAL')
  or (trans.tran_type_description = 'REDEMPTION')
  or (trans.tran_type_description = 'REDEMPTION REVERSAL')
  )
group by
  1,2,3,4,5,6,7,8,9,10,11,12,13,14
) amount
left join
  (
    select
  left(cast(trans.transaction_date as string),4) as Year,
  substring(cast(trans.transaction_date as string),6,2) as Month,
  --right(cast(trans.transaction_date as string),2) as Day,
  stores.country as Country,
  trans.state as State,
  --trans.scheme as Scheme,
  trans.is_online as is_online,
  --trans.card_type as Card_Type,
  --trans.store_id as Store,
  stores.industry as Industry,
  stores.brand as Brand,
  stores.division as division,
  stores.company as company,
  stores.invoice_group as invoice_group,
  stores.merchant_group as merchant_group,
  stores.merchant_portal_mg as merchant_portal_mg,
  stores.merchant_portal_cg as merchant_portal_cg,
  --stores.suburb as Suburb,
  --sum(trans.transaction_count) as trans_count,
  (sum(case when trans.tran_type_description in 
  ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL') then transaction_count else 0 end)
  -
  sum(case when trans.tran_type_description in 
  ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL') then transaction_count else 0 end)
  ) as trans_count
  --sum(trans.settlement_amount_inc_gst) as trans_amount,
from
  pdh_sf_mep_vw.wpay_all_merchants_daily_fees_summary_hist trans
left join 
  wpay_analytics.ref_store_ind  stores
on
  trans.store_id = stores.store_id
where
  (trans.approval_flag = 'A' ) 
  -- and ((trans.tran_type_description = 'PURCHASE')
  -- or (trans.tran_type_description = 'PURCHASE REVERSAL')
  -- or (trans.tran_type_description = 'PURCHASE WITH CASHOUT')
  -- or (trans.tran_type_description = 'PURCHASE WITH CASHOUT REVERSAL')
  -- or (trans.tran_type_description = 'REDEMPTION')
  -- or (trans.tran_type_description = 'REDEMPTION REVERSAL')
  -- )
  group by
  1,2,3,4,5,6,7,8,9,10,11,12,13) counts
on (amount.Year = counts.Year)
   and (amount.Month = counts.Month)
   and (amount.State = counts.State)
   --and (amount.Scheme = counts.Scheme)
   --and (amount.Online = counts.Online)
   --and (amount.Card_Type = counts.Card_Type)
   --and (amount.Store = counts.Store)
   and (amount.Brand = counts.Brand)
   and (amount.division = counts.division)
   and (amount.company = counts.company)
   and (amount.invoice_group = counts.invoice_group)
   and (amount.merchant_group = counts.merchant_group)
   and (amount.merchant_portal_mg = counts.merchant_portal_mg)
   and (amount.merchant_portal_cg = counts.merchant_portal_cg)
   and (amount.is_online = counts.is_online)
   --and (amount.Card_Type = counts.Card_Type)
   --and (amount.Suburb = counts.Suburb))
)
---------------------------------------------------------------------------------------------------------

-- Joined on same table to get previous month/year

, state_internal_online as
(
  select 
    store_trans_state_online.*,
    case when prev_m.prev_Amount <> 0 then
    ((store_trans_state_online.trans_amount - prev_m.prev_Amount)/prev_m.prev_Amount) 
    else 0 end as trans_amount_mom,
    case when prev_y.prev_Amount <> 0 then
    ((store_trans_state_online.trans_amount - prev_y.prev_Amount)/prev_y.prev_Amount) 
    else 0 end as trans_amount_yoy,
    case when cast(prev_m.prev_Count as int) <> 0 then
    ((store_trans_state_online.trans_count - prev_m.prev_Count)/prev_m.prev_Count)  
    else 0 end
    as trans_count_mom,
    case when cast(prev_y.prev_Count as int) <> 0 then
    ((store_trans_state_online.trans_count - prev_y.prev_Count)/prev_y.prev_Count)  
    else 0 end
    as trans_count_yoy,
    prev_m.prev_Amount as trans_amount_LM,
    prev_m.prev_Count as trans_count_LM,
    prev_y.prev_Amount as trans_amount_LY,
    prev_y.prev_Count as trans_count_LY,
    case when cast(store_trans_state_online.trans_count as int) <> 0 then
    (store_trans_state_online.trans_amount/store_trans_state_online.trans_count) 
    else 0 end 
    as trans_value,
    case when cast(prev_m.prev_Count as int) <> 0
    then
    (prev_m.prev_Amount/prev_m.prev_Count)  
    else 0 end 
    as trans_value_LM,
    case when cast(prev_y.prev_Count as int) <> 0
    then
    (prev_y.prev_Amount/prev_y.prev_Count)  
    else 0 end 
    as trans_value_LY
from store_trans_state_online
left join
(
select 
  curr.*,
  prev.Year as prev_year,
  prev.Month as prev_month,
  prev.trans_amount as prev_Amount,
  prev.trans_count as prev_Count
from store_trans_state_online curr
left join 
  store_trans_state_online prev
on  (curr.State = prev.State)
   and (curr.Brand = prev.Brand)
   and (curr.division = prev.division)
   and (curr.company = prev.company)
   and (curr.invoice_group = prev.invoice_group)
   and (curr.merchant_group = prev.merchant_group)
   and (curr.merchant_portal_mg = prev.merchant_portal_mg)
   and (curr.merchant_portal_cg = prev.merchant_portal_cg)
  and curr.Month = prev.Month
  and (cast(curr.Year as int)-1) = cast(prev.Year as int)
  and (curr.is_online = prev.is_online)
  --and (curr.Card_Type = prev.Card_Type)
order by curr.State,curr.Year,curr.Month
) prev_y
on
  (store_trans_state_online.State = prev_y.State)
   and (store_trans_state_online.Brand = prev_y.Brand)
   and (store_trans_state_online.division = prev_y.division)
   and (store_trans_state_online.company = prev_y.company)
   and (store_trans_state_online.invoice_group = prev_y.invoice_group)
   and (store_trans_state_online.merchant_group = prev_y.merchant_group)
   and (store_trans_state_online.merchant_portal_mg = prev_y.merchant_portal_mg)
   and (store_trans_state_online.merchant_portal_cg = prev_y.merchant_portal_cg)
  and store_trans_state_online.Year = prev_y.Year
  and store_trans_state_online.Month = prev_y.Month
  and (store_trans_state_online.is_online = prev_y.is_online)
  --and (store_trans_state_online.Card_Type = prev_y.Card_Type)
left join
(
select 
  curr.*,
  prev.trans_amount as prev_Amount,
  prev.trans_count as prev_Count
from store_trans_state_online curr
left join 
  store_trans_state_online prev
on (curr.State = prev.State)
   and (curr.Brand = prev.Brand)
   and (curr.division = prev.division)
   and (curr.company = prev.company)
   and (curr.invoice_group = prev.invoice_group)
   and (curr.merchant_group = prev.merchant_group)
   and (curr.merchant_portal_mg = prev.merchant_portal_mg)
   and (curr.merchant_portal_cg = prev.merchant_portal_cg)
    and left(cast(date_add(cast(concat(concat(concat(concat(curr.Year,'-'),curr.Month),'-'),'01') as date), interval -1 month) as string),4) = cast(prev.Year as string)
    and substring(cast(date_add(cast(concat(concat(concat(concat(curr.Year,'-'),curr.Month),'-'),'01') as date), interval -1 month) as string),6,2)  = prev.Month
  and (curr.is_online = prev.is_online)
  --and (curr.Card_Type = prev.Card_Type)
order by curr.State,curr.Year,curr.Month
) prev_m
on
  (store_trans_state_online.State = prev_m.State)
   and (store_trans_state_online.Brand = prev_m.Brand)
   and (store_trans_state_online.division = prev_m.division)
   and (store_trans_state_online.company = prev_m.company)
   and (store_trans_state_online.invoice_group = prev_m.invoice_group)
   and (store_trans_state_online.merchant_group = prev_m.merchant_group)
   and (store_trans_state_online.merchant_portal_mg = prev_m.merchant_portal_mg)
   and (store_trans_state_online.merchant_portal_cg = prev_m.merchant_portal_cg)
  and store_trans_state_online.Year = prev_m.Year
  and store_trans_state_online.Month = prev_m.Month
  and (store_trans_state_online.is_online = prev_m.is_online)
  --and (store_trans_state_online.Card_Type = prev_m.Card_Type)
order by store_trans_state_online.State, store_trans_state_online.Year,store_trans_state_online.Month
)

---------------------------------------------------------------------------------------------------------

--- adding only LGA to wpay_trans

, wpay_State_online as
 (select 
    state_internal_online.*,
    final_state_online.area,
    --final_state_online.industry_name,
    final_state_online.spend_growth_yoy,
    final_state_online.spend_growth_mom,
    final_state_online.txn_growth_yoy,
    final_state_online.txn_growth_mom,
    final_state_online.spend_density_growth_yoy,
    final_state_online.spend_density_growth_mom,
    final_state_online.avg_ticket,
    final_state_online.b_avg_ticket_LM,
    final_state_online.b_avg_ticket_LY,
    final_state_online.b_avg_ticket_mom,
    final_state_online.b_avg_ticket_yoy,
    final_state_online.avg_visits_per_cust,
    final_state_online.avg_spend_per_cust,
    final_state_online.ecom_ind_rank,
    final_state_online.ecom_txns_share,
    final_state_online.ecom_ind_rank_share,
    --final_lga.Benchmark
  from 
    state_internal_online
  left join 
    final_state_online
  on (state_internal_online.state = final_state_online.state)
    and (state_internal_online.Brand = final_state_online.Brand)
    and (state_internal_online.division = final_state_online.division)
    and (state_internal_online.company = final_state_online.company)
    and (state_internal_online.invoice_group = final_state_online.invoice_group)
    and (state_internal_online.merchant_group = final_state_online.merchant_group)
    and (state_internal_online.merchant_portal_mg = final_state_online.merchant_portal_mg)
    and (state_internal_online.merchant_portal_cg = final_state_online.merchant_portal_cg)
    and (state_internal_online.Year = final_state_online.Year)
    --and (state_internal.Month = final_state.Month)
    and (cast(state_internal_online.Month as int) = cast(final_state_online.Month as int))
 -- where state_internal_online.Year ='2022'
  --    and state_internal_online.Month ='07'
  --order by final_lga.Benchmark
 )

------------------------------------------------------------
---- adding benchmark 
,
state_bench_online as 
(
  select *,
  case when 
    (coalesce(spend_growth_yoy,spend_growth_mom,txn_growth_yoy,txn_growth_mom) is null 
    and concat(concat(Year,'-'),Month) <= (select max_date from MaxMMIDate))
    then 'Country'
    else 'State'
    end as Benchmark
    from wpay_State_online
)


--------------------------------------------
--- going over LGA and filling nulls with State
, state_benchmark_online as
(
  select
    'State' as Aggregation_Level,
    'All' as store_id,
    'All' as store_status,
    state_bench_online.country,
    state_bench_online.state,
    state_bench_online.division,
    state_bench_online.brand,
    state_bench_online.industry,
    'All' as site_name,
    'All' as suburb,
    'All' as postcode,
    'All' as caid,
   -- 'All' as is_closed,
    state_bench_online.company,
    state_bench_online.invoice_group,
    state_bench_online.merchant_group,
    state_bench_online.merchant_portal_mg,
    state_bench_online.merchant_portal_cg,
    cast(state_bench_online.is_online as string) as is_online,
    'All' as scheme,
    'All' as card_type,
    'All' as LGA,
    'All' as LGA_id,
    state_bench_online.area,
    state_bench_online.industry_name,
    --final_lga.industry_code,
    --wpay_LGA.Period,
    concat(state_bench_online.Year,concat('-',state_bench_online.Month)) as Period,
    state_bench_online.Year,
    state_bench_online.Month,
    state_bench_online.trans_amount as w_trans_amount,
    state_bench_online.trans_count as w_trans_count,
    state_bench_online.trans_value as w_trans_value,
    state_bench_online.trans_amount_LM as w_trans_amount_LM,
    state_bench_online.trans_count_LM as w_trans_count_LM,
    state_bench_online.trans_value_LM as w_trans_value_LM,
    state_bench_online.trans_amount_LY as w_trans_amount_LY,
    state_bench_online.trans_count_LY as w_trans_count_LY,
    state_bench_online.trans_value_LY as w_trans_value_LY,
    state_bench_online.trans_amount_mom as w_trans_amount_mom,
    state_bench_online.trans_amount_yoy as w_trans_amount_yoy,
    state_bench_online.trans_count_mom as w_trans_count_mom,
    state_bench_online.trans_count_yoy as w_trans_count_yoy,
    case when cast(state_bench_online.trans_value_LM as int) <> 0
    then ((state_bench_online.trans_value - state_bench_online.trans_value_LM)/state_bench_online.trans_value_LM)  
    else 0 end 
    as w_trans_value_mom,
    case when cast(state_bench_online.trans_value_LY as int) <> 0
    then ((state_bench_online.trans_value - state_bench_online.trans_value_LY)/state_bench_online.trans_value_LY)  
    else 0 end 
    as w_trans_value_yoy,
    case when cust_info.unique_customer_count <> 0 then
    state_bench_online.trans_count/cust_info.unique_customer_count
    else 0 end as avg_cust_visits,
    case when cust_info.unique_customer_count <> 0 then
    state_bench_online.trans_amount/cust_info.unique_customer_count 
    else 0 end as avg_cust_spend,
    cust_info.unique_card_count as unique_cust,
    cust_info.unique_customer_count as unique_card,
    CASE 
      WHEN state_bench_online.Benchmark = 'Country'
        THEN ind_key.spend_growth_yoy
      ELSE state_bench_online.spend_growth_yoy
      END AS b_spend_growth_yoy,
    CASE 
      WHEN state_bench_online.Benchmark = 'Country'
        THEN ind_key.spend_growth_mom
      ELSE state_bench_online.spend_growth_mom
      END AS b_spend_growth_mom,
    CASE
      WHEN state_bench_online.Benchmark = 'Country'
        THEN ind_key.txn_growth_yoy
      ELSE state_bench_online.txn_growth_yoy
      END AS b_txn_growth_yoy,
    CASE
      WHEN state_bench_online.Benchmark = 'Country'
        THEN ind_key.txn_growth_mom
      ELSE state_bench_online.txn_growth_mom
      END AS b_txn_growth_mom,
    CASE
      WHEN state_bench_online.Benchmark = 'Country'
        THEN ind_key.spend_density_growth_yoy
      ELSE state_bench_online.spend_density_growth_yoy
      END AS b_spend_density_growth_yoy,
    CASE
      WHEN state_bench_online.Benchmark = 'Country'
        THEN ind_key.spend_density_growth_mom
      ELSE state_bench_online.spend_density_growth_mom
      END AS b_spend_density_growth_mom,
    CASE
      WHEN state_bench_online.Benchmark = 'Country'
        THEN ind_key.avg_ticket
      ELSE state_bench_online.avg_ticket
      END AS b_avg_ticket,
    CASE
      WHEN state_bench_online.Benchmark = 'Country'
        THEN ind_key.b_avg_ticket_LM
      ELSE state_bench_online.b_avg_ticket_LM
      END AS b_avg_ticket_LM,
    CASE
      WHEN state_bench_online.Benchmark = 'Country'
        THEN ind_key.b_avg_ticket_LY
      ELSE state_bench_online.b_avg_ticket_LY
      END AS b_avg_ticket_LY,
    CASE
      WHEN state_bench_online.Benchmark = 'Country'
        THEN ind_key.b_avg_ticket_mom
      ELSE state_bench_online.b_avg_ticket_mom
      END AS b_avg_ticket_mom,
    CASE
      WHEN state_bench_online.Benchmark = 'Country'
        THEN ind_key.b_avg_ticket_yoy
      ELSE state_bench_online.b_avg_ticket_yoy
      END AS b_avg_ticket_yoy,
    CASE
      WHEN state_bench_online.Benchmark = 'Country'
        THEN ind_key.avg_visits_per_cust
      ELSE state_bench_online.avg_visits_per_cust
      END AS b_avg_visits_per_cust,
    CASE
      WHEN state_bench_online.Benchmark = 'Country'
        THEN ind_key.avg_spend_per_cust
      ELSE state_bench_online.avg_spend_per_cust
      END AS b_avg_spend_per_cust,
    CASE
      WHEN state_bench_online.Benchmark = 'Country'
        THEN ind_key.ecom_ind_rank
      ELSE state_bench_online.ecom_ind_rank
      END AS b_ecom_ind_rank,
    CASE
      WHEN state_bench_online.Benchmark = 'Country'
        THEN ind_key.ecom_txns_share
      ELSE state_bench_online.ecom_txns_share
      END AS b_ecom_txns_share,
    CASE
      WHEN state_bench_online.Benchmark = 'Country'
        THEN ind_key.ecom_ind_rank_share
      ELSE state_bench_online.ecom_ind_rank_share
      END AS b_ecom_ind_rank_share,
    --state_bench_online.Benchmark,
    state_bench_online.industry_name as Benchmark,
    state_bench_online.Benchmark as Benchmark_Location,
    'External' as Benchmark_lvl
  from 
    state_bench_online
  left join
    ( select distinct ind.*, 
    left(ind.period,4) as Year,
    right(ind.period,2) as Month
    from
    wpay_analytics.mmi_area_industry_edit ind
    where area_level = 'Country') ind_key
  on cast(state_bench_online.industry as string) = cast(ind_key.industry_code as string) 
     --and (final_lga.period = ind_key.period ) )
     and cast(trim(state_bench_online.Year) as string) = cast(trim(ind_key.Year) as string) 
     --and cast(trim(state_bench.Month) as string) = cast(trim(ind_key.Month) as string)
     and (cast(state_bench_online.Month as int) = cast(ind_key.Month as int))
  left join
    (
  select 
      m.company,
      m.invoice_group,    
      m.merchant_group,
      m.division, 
      m.brand, 
      m.merchant_portal_mg,
      m.merchant_portal_cg,   
      --m.store_id,   
      --m.site_name,    
     -- m.suburb,   
      m.state,    
      --m.postcode,
     -- m.country,
      m.is_online,
      --fee.calc_scheme as scheme,
      --fee.calc_card_type as card_type,
      left(cast(fee.switch_tran_date as string),7) Transaction_Date,
      -- sum(case when fee.calc_tran_type_description in
      -- ('REFUND','REFUND REVERSAL','PURCHASE','PURCHASE REVERSAL','PURCHASE WITH CASHOUT','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION','REDEMPTION REVERSAL') then fee.transaction_total else 0 end)/100 as trans_amount,
      -- (sum(case when fee.calc_tran_type_description in
      -- ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL') then 1 else 0 end)
      -- -
      -- sum(case when fee.calc_tran_type_description in 
      -- ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL') then 1 else 0 end)
      -- ) as trans_count,
      count(distinct fee.card_id) unique_card_count,
      count(distinct fee.card_id) unique_customer_count
      -- (count(1) / count(distinct fee.card_id)) frequency_of_visits,
      -- (  sum(case when fee.calc_tran_type_description in
      -- ('REFUND','REFUND REVERSAL','PURCHASE','PURCHASE REVERSAL','PURCHASE WITH CASHOUT','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION','REDEMPTION REVERSAL') then fee.transaction_total else 0 end) 
      -- / (100*count(distinct fee.card_id))) spend_per_card_holder 
      from wpay_analytics.wpay_billing_engine_tran_dtl fee  
      inner join
      `pdh_ref_ds.ref_store_details` m 
      on LEFT(FEE.net_term_id, 5) = M.store_id
      --inner join
      --wpay_analytics.wpay_card_identity_model cim
      --on trim(cim.scdr_token) = trim(fee.scdr_token)
      where fee.calc_approval_flag = 'A'
	  and left(cast(fee.tstamp_trans as string),7) > '2022-07'
and fee.calc_tran_type_description in ('REFUND','REFUND REVERSAL','PURCHASE','PURCHASE REVERSAL','PURCHASE WITH CASHOUT','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION','REDEMPTION REVERSAL')
      group by 1,2,3,4,5,6,7,8,9,10
      ) cust_info
  on cust_info.company = state_bench_online.company
    and cust_info.merchant_group = state_bench_online.merchant_group
    and cust_info.division = state_bench_online.division
    and cust_info.state = state_bench_online.state
    and cust_info.is_online = state_bench_online.is_online
    and cust_info.invoice_group = state_bench_online.invoice_group
    and cust_info.brand = state_bench_online.brand
    and cust_info.merchant_portal_mg = state_bench_online.merchant_portal_mg
    and cust_info.merchant_portal_cg = state_bench_online.merchant_portal_cg
    and concat(state_bench_online.Year,concat('-',state_bench_online.Month)) = cust_info.Transaction_Date 
  order by state_bench_online.Benchmark
)
-- end

--------------------------------------------------------------------------------------------------------------
-- END
--------------------------------------------------------------------------------------------------------------

--------------------------------------------------------------------------------------------------------------
-- Country
--------------------------------------------------------------------------------------------------------------
-- Store base table + mastercard linking and benchmark

,
 final_country_online as 
(select
  *
from
(
select 
  stores.country,
  --stores.state,
  stores.division,
  stores.brand,
  stores.company,
  stores.industry,
  stores.industry_name,
  --distinct stores.store_id,
  --stores.site_name,
  --stores.suburb,
  --stores.postcode,
  --stores.caid,
  --stores.is_closed,
  --stores.company,
  stores.invoice_group,
  stores.merchant_group,
  stores.merchant_portal_mg,
  stores.merchant_portal_cg
  --stores.is_online,
from
  wpay_analytics.ref_store_ind stores
group by 1,2,3,4,5,6,7,8,9,10) store_info
left join
  (
  select 
    area,
    period,
   -- industry_name,
    industry_code,
    left(period,4) as Year,
    right(period,2) as Month,
    CASE 
      WHEN spend_growth_yoy = 99999
        THEN NULL
      ELSE spend_growth_yoy
      END AS spend_growth_yoy,
    CASE 
      WHEN spend_growth_mom = 99999
        THEN NULL
      ELSE spend_growth_mom
      END AS spend_growth_mom,
    CASE
      WHEN txn_growth_yoy = 99999
        THEN NULL
      ELSE txn_growth_yoy
      END AS txn_growth_yoy,
    CASE
      WHEN txn_growth_mom = 99999
        THEN NULL
      ELSE txn_growth_mom
      END AS txn_growth_mom,
    CASE
      WHEN spend_density_growth_yoy = 99999
        THEN NULL
      ELSE spend_density_growth_yoy
      END AS spend_density_growth_yoy,
    CASE
      WHEN spend_density_growth_mom = 99999
        THEN NULL
      ELSE spend_density_growth_mom
      END AS spend_density_growth_mom,
    CASE
      WHEN avg_ticket = 99999
        THEN NULL
      ELSE avg_ticket
      END AS avg_ticket,
    b_avg_ticket_LM,
    b_avg_ticket_LY,
    b_avg_ticket_mom,
    b_avg_ticket_yoy,
    CASE
      WHEN avg_visits_per_cust = 99999
        THEN NULL
      ELSE avg_visits_per_cust
      END AS avg_visits_per_cust,
    CASE
      WHEN avg_spend_per_cust = 99999
        THEN NULL
      ELSE avg_spend_per_cust
      END AS avg_spend_per_cust,
    CASE
      WHEN ecom_ind_rank = 99999
        THEN NULL
      ELSE ecom_ind_rank
      END AS ecom_ind_rank,
    CASE
      WHEN ecom_txns_share = 99999
        THEN NULL
      ELSE ecom_txns_share
      END AS ecom_txns_share,
    CASE
      WHEN ecom_ind_rank_share = 99999
        THEN NULL
      ELSE ecom_ind_rank_share
      END AS ecom_ind_rank_share
  from
    wpay_analytics.mmi_area_industry_edit
  where
    area_level = 'country'
  group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21
  ) ind_lga
on 
 cast(store_info.industry as string) = cast(ind_lga.industry_code as string)
)

----------------------------------------
-- wpay store transactions
,
 store_trans_country_online as 
(select 
  amount.Year as Year,
  amount.Month as Month,
  amount.Country as Country,
  --amount.State as State,
  --amount.Scheme as Scheme,
  amount.is_online as is_online,
  --amount.Card_Type as Card_Type,
  --amount.Store as Store,
  amount.industry as Industry,
  amount.industry_name as industry_name,
  amount.Brand as Brand,
  amount.division as division,
  amount.company as company,
  amount.invoice_group as invoice_group,
  amount.merchant_group as merchant_group,
  amount.merchant_portal_mg as merchant_portal_mg,
  amount.merchant_portal_cg as merchant_portal_cg,
  --amount.Suburb as Suburb,
  amount.trans_amount as trans_amount,
  counts.trans_count as trans_count,
from
(
select
  left(cast(trans.transaction_date as string),4) as Year,
  substring(cast(trans.transaction_date as string),6,2) as Month,
 -- right(cast(trans.transaction_date as string),2) as Day,
  stores.country as Country,
  --trans.state as State,
  --trans.scheme as Scheme,
  trans.is_online as is_online,
  --.card_type as Card_Type,
  --trans.store_id as Store,
  stores.industry as Industry,
  stores.industry_name as industry_name,
  stores.brand as Brand,
  stores.division as division,
  stores.company as company,
  stores.invoice_group as invoice_group,
  stores.merchant_group as merchant_group,
  stores.merchant_portal_mg as merchant_portal_mg,
  stores.merchant_portal_cg as merchant_portal_cg,
  --stores.suburb as Suburb,
  --sum(trans.transaction_count) as trans_count,
  sum(trans.settlement_amount_inc_gst) as trans_amount,
from
  pdh_sf_mep_vw.wpay_all_merchants_daily_fees_summary_hist trans
left join 
  wpay_analytics.ref_store_ind stores
on
  trans.store_id = stores.store_id
where
  (trans.approval_flag = 'A' ) 
  and ((trans.tran_type_description = 'REFUND')
  or (trans.tran_type_description = 'REFUND REVERSAL')
  or (trans.tran_type_description = 'PURCHASE')
  or (trans.tran_type_description = 'PURCHASE REVERSAL')
  or (trans.tran_type_description = 'PURCHASE WITH CASHOUT')
  or (trans.tran_type_description = 'PURCHASE WITH CASHOUT REVERSAL')
  or (trans.tran_type_description = 'REDEMPTION')
  or (trans.tran_type_description = 'REDEMPTION REVERSAL')
  )
group by
  1,2,3,4,5,6,7,8,9,10,11,12,13
) amount
left join
  (
    select
  left(cast(trans.transaction_date as string),4) as Year,
  substring(cast(trans.transaction_date as string),6,2) as Month,
  --right(cast(trans.transaction_date as string),2) as Day,
  stores.country as Country,
  --trans.state as State,
  --trans.scheme as Scheme,
  trans.is_online as is_online,
  --trans.card_type as Card_Type,
  --trans.store_id as Store,
  stores.industry as Industry,
  stores.brand as Brand,
  stores.division as division,
  stores.company as company,
  stores.invoice_group as invoice_group,
  stores.merchant_group as merchant_group,
  stores.merchant_portal_mg as merchant_portal_mg,
  stores.merchant_portal_cg as merchant_portal_cg,
  --stores.suburb as Suburb,
  --sum(trans.transaction_count) as trans_count,
  (sum(case when trans.tran_type_description in 
  ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL') then transaction_count else 0 end)
  -
  sum(case when trans.tran_type_description in 
  ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL') then transaction_count else 0 end)
  ) as trans_count
  --sum(trans.settlement_amount_inc_gst) as trans_amount,
from
  pdh_sf_mep_vw.wpay_all_merchants_daily_fees_summary_hist trans
left join 
  wpay_analytics.ref_store_ind  stores
on
  trans.store_id = stores.store_id
where
  (trans.approval_flag = 'A' ) 
  -- and ((trans.tran_type_description = 'PURCHASE')
  -- or (trans.tran_type_description = 'PURCHASE REVERSAL')
  -- or (trans.tran_type_description = 'PURCHASE WITH CASHOUT')
  -- or (trans.tran_type_description = 'PURCHASE WITH CASHOUT REVERSAL')
  -- or (trans.tran_type_description = 'REDEMPTION')
  -- or (trans.tran_type_description = 'REDEMPTION REVERSAL')
  -- )
  group by
  1,2,3,4,5,6,7,8,9,10,11,12) counts
on (amount.Year = counts.Year)
   and (amount.Month = counts.Month)
   --and (amount.State = counts.State)
   --and (amount.Scheme = counts.Scheme)
   and (amount.is_online = counts.is_online)
   --and (amount.Card_Type = counts.Card_Type)
   --and (amount.Store = counts.Store)
   and (amount.Brand = counts.Brand)
   and (amount.division = counts.division)
   and (amount.company = counts.company)
   and (amount.invoice_group = counts.invoice_group)
   and (amount.merchant_group = counts.merchant_group)
   and (amount.merchant_portal_mg = counts.merchant_portal_mg)
   and (amount.merchant_portal_cg = counts.merchant_portal_cg)
   --and (amount.Suburb = counts.Suburb))
)
------------------------------------------------------------------------------------------------------------

, country_internal_online as
(
  select 
    store_trans_country_online.*,
    case when prev_m.prev_Amount <> 0 then
    ((store_trans_country_online.trans_amount - prev_m.prev_Amount)/prev_m.prev_Amount) 
    else 0 end as trans_amount_mom,
    case when prev_y.prev_Amount <> 0 then
    ((store_trans_country_online.trans_amount - prev_y.prev_Amount)/prev_y.prev_Amount)
    else 0 end  as trans_amount_yoy,
    case when cast(prev_m.prev_Count as int) <> 0 then
    ((store_trans_country_online.trans_count - prev_m.prev_Count)/prev_m.prev_Count)  
    else 0 end
    as trans_count_mom,
    case when cast(prev_y.prev_Count as int) <> 0 then
    ((store_trans_country_online.trans_count - prev_y.prev_Count)/prev_y.prev_Count)  
    else 0 end
    as trans_count_yoy,
    prev_m.prev_Amount as trans_amount_LM,
    prev_m.prev_Count as trans_count_LM,
    prev_y.prev_Amount as trans_amount_LY,
    prev_y.prev_Count as trans_count_LY,
    case when cast(store_trans_country_online.trans_count as int) <> 0 then
    (store_trans_country_online.trans_amount/store_trans_country_online.trans_count) 
    else 0 end 
    as trans_value,
    case when cast(prev_m.prev_Count as int) <> 0
    then
    (prev_m.prev_Amount/prev_m.prev_Count)  
    else 0 end 
    as trans_value_LM,
    case when cast(prev_y.prev_Count as int) <> 0
    then
    (prev_y.prev_Amount/prev_y.prev_Count)  
    else 0 end 
    as trans_value_LY
from store_trans_country_online
left join
(
select 
  curr.*,
  prev.Year as prev_year,
  prev.Month as prev_month,
  prev.trans_amount as prev_Amount,
  prev.trans_count as prev_Count
from store_trans_country_online curr
left join 
  store_trans_country_online prev
on  
    (curr.Brand = prev.Brand)
and    (curr.division = prev.division)
   and (curr.company = prev.company)
   and (curr.invoice_group = prev.invoice_group)
   and (curr.merchant_group = prev.merchant_group)
   and (curr.merchant_portal_mg = prev.merchant_portal_mg)
   and (curr.merchant_portal_cg = prev.merchant_portal_cg)
   and curr.Month = prev.Month
   and (cast(curr.Year as int)-1) = cast(prev.Year as int)
   and curr.is_online = prev.is_online
   --and curr.card_type = prev.card_type
order by curr.Year,curr.Month
) prev_y
on
    (store_trans_country_online.Brand = prev_y.Brand)
and    (store_trans_country_online.division = prev_y.division)
   and (store_trans_country_online.company = prev_y.company)
   and (store_trans_country_online.invoice_group = prev_y.invoice_group)
   and (store_trans_country_online.merchant_group = prev_y.merchant_group)
   and (store_trans_country_online.merchant_portal_mg = prev_y.merchant_portal_mg)
   and (store_trans_country_online.merchant_portal_cg = prev_y.merchant_portal_cg)
  and store_trans_country_online.Year = prev_y.Year
  and store_trans_country_online.Month = prev_y.Month
  and store_trans_country_online.is_online = prev_y.is_online
  -- and store_trans_country_online.card_type = prev_y.card_type
left join
(
select 
  curr.*,
  prev.trans_amount as prev_Amount,
  prev.trans_count as prev_Count
from store_trans_country_online curr
left join 
  store_trans_country_online prev
on 
    (curr.Brand = prev.Brand)
and    (curr.division = prev.division)
   and (curr.company = prev.company)
   and (curr.invoice_group = prev.invoice_group)
   and (curr.merchant_group = prev.merchant_group)
   and (curr.merchant_portal_mg = prev.merchant_portal_mg)
   and (curr.merchant_portal_cg = prev.merchant_portal_cg)
    and left(cast(date_add(cast(concat(concat(concat(concat(curr.Year,'-'),curr.Month),'-'),'01') as date), interval -1 month) as string),4) = cast(prev.Year as string)
    and substring(cast(date_add(cast(concat(concat(concat(concat(curr.Year,'-'),curr.Month),'-'),'01') as date), interval -1 month) as string),6,2)  = prev.Month
   and curr.is_online = prev.is_online
  -- and curr.card_type = prev.card_type
order by curr.Year,curr.Month
) prev_m
on
    (store_trans_country_online.Brand = prev_m.Brand)
and    (store_trans_country_online.division = prev_m.division)
   and (store_trans_country_online.company = prev_m.company)
   and (store_trans_country_online.invoice_group = prev_m.invoice_group)
   and (store_trans_country_online.merchant_group = prev_m.merchant_group)
   and (store_trans_country_online.merchant_portal_mg = prev_m.merchant_portal_mg)
   and (store_trans_country_online.merchant_portal_cg = prev_m.merchant_portal_cg)
  and store_trans_country_online.Year = prev_m.Year
  and store_trans_country_online.Month = prev_m.Month
  and store_trans_country_online.is_online = prev_m.is_online
  -- and store_trans_country_online.card_type = prev_m.card_type
order by store_trans_country_online.Year,store_trans_country_online.Month
)

-----------------------------------------------------------------------------------------------------------

--- adding only LGA to wpay_trans

, wpay_country_online as
 (select 
    country_internal_online.*,
    final_country_online.area,
    --final_country_online.industry_name,
    final_country_online.spend_growth_yoy,
    final_country_online.spend_growth_mom,
    final_country_online.txn_growth_yoy,
    final_country_online.txn_growth_mom,
    final_country_online.spend_density_growth_yoy,
    final_country_online.spend_density_growth_mom,
    final_country_online.avg_ticket,
    final_country_online.b_avg_ticket_LM,
    final_country_online.b_avg_ticket_LY,
    final_country_online.b_avg_ticket_mom,
    final_country_online.b_avg_ticket_yoy,
    final_country_online.avg_visits_per_cust,
    final_country_online.avg_spend_per_cust,
    final_country_online.ecom_ind_rank,
    final_country_online.ecom_txns_share,
    final_country_online.ecom_ind_rank_share,
    --final_lga.Benchmark
  from 
    country_internal_online
  left join 
    final_country_online
  on 
    (country_internal_online.Brand = final_country_online.Brand)
    and (country_internal_online.division = final_country_online.division)
    and (country_internal_online.company = final_country_online.company)
    and (country_internal_online.invoice_group = final_country_online.invoice_group)
    and (country_internal_online.merchant_group = final_country_online.merchant_group)
    and (country_internal_online.merchant_portal_mg = final_country_online.merchant_portal_mg)
    and (country_internal_online.merchant_portal_cg = final_country_online.merchant_portal_cg)
    and (country_internal_online.Year = final_country_online.Year)
    --and (country_internal.Month = final_country.Month)
    and (cast(country_internal_online.Month as int) = cast(final_country_online.Month as int))
  --where country_internal_online.Year ='2022'
  --    and country_internal_online.Month ='07'
  --order by final_lga.Benchmark
 )

------------------------------------------------------------
---- adding benchmark 
,
country_bench_online as 
(
  select *,
  'Country' as Benchmark
  from wpay_country_online
)


--------------------------------------------
--- going over LGA and filling nulls with State
, country_benchmark_online as
(
  select
    'Country' as Aggregation_Level,
    'All' as store_id,
    'All' as store_status,
    country_bench_online.country,
    'All' as state,
    country_bench_online.division,
    country_bench_online.brand,
    country_bench_online.industry,
    'All' as site_name,
    'All' as suburb,
    'All' as postcode,
    'All' as caid,
    --'All' as is_closed,
    country_bench_online.company,
    country_bench_online.invoice_group,
    country_bench_online.merchant_group,
    country_bench_online.merchant_portal_mg,
    country_bench_online.merchant_portal_cg,
    cast(country_bench_online.is_online as string) as is_online,
    'All' as scheme,
    'All' as card_type,
    'All' as LGA,
    'All' as LGA_id,
    country_bench_online.area,
    country_bench_online.industry_name,
    --final_lga.industry_code,
    --wpay_LGA.Period,
    concat(country_bench_online.Year,concat('-',country_bench_online.Month)) as Period,
    country_bench_online.Year,
    country_bench_online.Month,
    country_bench_online.trans_amount as w_trans_amount,
    country_bench_online.trans_count as w_trans_count,
    country_bench_online.trans_value as w_trans_value,
    country_bench_online.trans_amount_LM as w_trans_amount_LM,
    country_bench_online.trans_count_LM as w_trans_count_LM,
    country_bench_online.trans_value_LM as w_trans_value_LM,
    country_bench_online.trans_amount_LY as w_trans_amount_LY,
    country_bench_online.trans_count_LY as w_trans_count_LY,
    country_bench_online.trans_value_LY as w_trans_value_LY,
    country_bench_online.trans_amount_mom as w_trans_amount_mom,
    country_bench_online.trans_amount_yoy as w_trans_amount_yoy,
    country_bench_online.trans_count_mom as w_trans_count_mom,
    country_bench_online.trans_count_yoy as w_trans_count_yoy,
    case when cast(country_bench_online.trans_value_LM as int) <> 0
    then ((country_bench_online.trans_value - country_bench_online.trans_value_LM)/country_bench_online.trans_value_LM)  
    else 0 end 
    as w_trans_value_mom,
    case when cast(country_bench_online.trans_value_LY as int) <> 0
    then ((country_bench_online.trans_value - country_bench_online.trans_value_LY)/country_bench_online.trans_value_LY)  
    else 0 end 
    as w_trans_value_yoy,
    case when cust_info.unique_customer_count <> 0 then
    country_bench_online.trans_count/cust_info.unique_customer_count 
    else 0 end as avg_cust_visits,
    case when cust_info.unique_customer_count <> 0 then
    country_bench_online.trans_amount/cust_info.unique_customer_count
    else 0 end as avg_cust_spend,
    cust_info.unique_card_count as unique_cust,
    cust_info.unique_customer_count as unique_card,
    country_bench_online.spend_growth_yoy as b_spend_growth_yoy,
    country_bench_online.spend_growth_mom as b_spend_growth_mom,
    country_bench_online.txn_growth_yoy as b_txn_growth_yoy,
    country_bench_online.txn_growth_yoy as b_txn_growth_yoy,
    country_bench_online.spend_density_growth_yoy as b_spend_density_growth_yoy,
    country_bench_online.spend_density_growth_mom as b_spend_density_growth_mom,
    country_bench_online.avg_ticket as b_avg_ticket,
    country_bench_online.b_avg_ticket_LM as b_avg_ticket_LM,
    country_bench_online.b_avg_ticket_LY as b_avg_ticket_LY,
    country_bench_online.b_avg_ticket_mom as b_avg_ticket_mom,
    country_bench_online.b_avg_ticket_yoy as b_avg_ticket_yoy,
    country_bench_online.avg_visits_per_cust as b_avg_visits_per_cust,
    country_bench_online.avg_spend_per_cust as b_avg_spend_per_cust,
    country_bench_online.ecom_ind_rank as b_ecom_ind_rank,
    country_bench_online.ecom_txns_share as b_ecom_txns_share,
    country_bench_online.ecom_ind_rank_share as b_ecom_ind_rank_share,
    --country_bench_online.Benchmark,
    country_bench_online.industry_name as Benchmark,
    country_bench_online.Benchmark as Benchmark_Location,
    'External' as Benchmark_lvl
  from 
    country_bench_online
  left join
    (
  select 
      m.company,
      m.invoice_group,    
      m.merchant_group,
      m.division, 
      m.brand, 
      m.merchant_portal_mg,
      m.merchant_portal_cg,   
      --m.store_id,   
      --m.site_name,    
     -- m.suburb,   
      --m.state,    
      --m.postcode,
      m.country,
      m.is_online,
      --fee.calc_scheme as scheme,
      --fee.calc_card_type as card_type,
      left(cast(fee.switch_tran_date as string),7) Transaction_Date,
      -- sum(case when fee.calc_tran_type_description in
      -- ('REFUND','REFUND REVERSAL','PURCHASE','PURCHASE REVERSAL','PURCHASE WITH CASHOUT','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION','REDEMPTION REVERSAL') then fee.transaction_total else 0 end)/100 as trans_amount,
      -- (sum(case when fee.calc_tran_type_description in
      -- ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL') then 1 else 0 end)
      -- -
      -- sum(case when fee.calc_tran_type_description in 
      -- ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL') then 1 else 0 end)
      -- ) as trans_count,
      count(distinct fee.card_id) unique_card_count,
      count(distinct fee.card_id) unique_customer_count
      -- (count(1) / count(distinct fee.card_id)) frequency_of_visits,
      -- (  sum(case when fee.calc_tran_type_description in
      -- ('REFUND','REFUND REVERSAL','PURCHASE','PURCHASE REVERSAL','PURCHASE WITH CASHOUT','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION','REDEMPTION REVERSAL') then fee.transaction_total else 0 end) 
      -- / (100*count(distinct fee.card_id))) spend_per_card_holder 
      from wpay_analytics.wpay_billing_engine_tran_dtl fee  
      inner join
      `pdh_ref_ds.ref_store_details` m 
      on LEFT(FEE.net_term_id, 5) = M.store_id
      --inner join
      --wpay_analytics.wpay_card_identity_model cim
      --on trim(cim.scdr_token) = trim(fee.scdr_token)
      where fee.calc_approval_flag = 'A'
	  and left(cast(fee.tstamp_trans as string),7) > '2022-07'
and fee.calc_tran_type_description in ('REFUND','REFUND REVERSAL','PURCHASE','PURCHASE REVERSAL','PURCHASE WITH CASHOUT','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION','REDEMPTION REVERSAL')
      group by 1,2,3,4,5,6,7,8,9,10
      ) cust_info
  on cust_info.company = country_bench_online.company
    and cust_info.merchant_group = country_bench_online.merchant_group
    and cust_info.division = country_bench_online.division
    and cust_info.country = country_bench_online.country
    and cust_info.is_online = country_bench_online.is_online
    and cust_info.invoice_group = country_bench_online.invoice_group
    and cust_info.brand = country_bench_online.brand
    and cust_info.merchant_portal_mg = country_bench_online.merchant_portal_mg
    and cust_info.merchant_portal_cg = country_bench_online.merchant_portal_cg
    and concat(country_bench_online.Year,concat('-',country_bench_online.Month)) = cust_info.Transaction_Date
)
-- end
select 
  *
  from lga_benchmark_online
union all
select 
  *
  from state_benchmark_online
union all
select 
  *
  from country_benchmark_online

);
-------------------------------------------------------------------------------------------------------------

--- SCHEME ONLY

create or replace table pdh_staging_ds.temp_scheme as (
-------------------------------------------------------------------------------------------------------------------
-- LGA/STATE
-------------------------------------------------------------------------------------------------------------------
-- Store base table + mastercard linking and benchmark
with
final_lga_card as 
(select
  *
from
(
select 
  distinct stores.store_id,
  stores.country,
  stores.state,
  stores.division,
  stores.brand,
  stores.industry,
  stores.industry_name,
  --distinct stores.store_id,
  stores.site_name,
  stores.suburb,
  stores.postcode,
  stores.caid,
  --stores.is_closed,
  stores.company,
  stores.invoice_group,
  stores.merchant_group,
  stores.merchant_portal_mg,
  stores.merchant_portal_cg,
  --stores.is_online,
  stores.LGA as LGA,
  stores.LGA_id as LGA_id
from
  wpay_analytics.ref_store_ind stores
left join
(
  select *
  from
  (select 
  distinct mmi_postcode.Merchant_Post_Code,
  mmi_postcode.City_Name,
  key.level3,
  upper(key.level2) as level2,
  key.level2_id as level2_id
from
  wpay_analytics.ref_merchant_mmi_postcode mmi_postcode
left join
  wpay_analytics.mmi_aus_mapping_key_edit key
on 
  (cast(mmi_postcode.Level_3_Id as string) = cast(key.level3_id as string))
  and (mmi_postcode.Province_Code = key.State)
  ) 
  --where level2 is not null
 ) mmi_location
on 
  (cast(stores.postcode as string) = cast(mmi_location.Merchant_Post_Code as string))
  --and (replace(upper(mmi_location.City_Name),' ','') = replace(upper(stores.suburb),' ',''))
) store_lga
left join
  (
  select 
    area,
    period,
   -- industry_name,
    industry_code,
    left(period,4) as Year,
    right(period,2) as Month,
    CASE 
      WHEN spend_growth_yoy = 99999
        THEN NULL
      ELSE spend_growth_yoy
      END AS spend_growth_yoy,
    CASE 
      WHEN spend_growth_mom = 99999
        THEN NULL
      ELSE spend_growth_mom
      END AS spend_growth_mom,
    CASE
      WHEN txn_growth_yoy = 99999
        THEN NULL
      ELSE txn_growth_yoy
      END AS txn_growth_yoy,
    CASE
      WHEN txn_growth_mom = 99999
        THEN NULL
      ELSE txn_growth_mom
      END AS txn_growth_mom,
    CASE
      WHEN spend_density_growth_yoy = 99999
        THEN NULL
      ELSE spend_density_growth_yoy
      END AS spend_density_growth_yoy,
    CASE
      WHEN spend_density_growth_mom = 99999
        THEN NULL
      ELSE spend_density_growth_mom
      END AS spend_density_growth_mom,
    CASE
      WHEN avg_ticket = 99999
        THEN NULL
      ELSE avg_ticket
      END AS avg_ticket,
    b_avg_ticket_LM,
    b_avg_ticket_LY,
    b_avg_ticket_mom,
    b_avg_ticket_yoy,
    CASE
      WHEN avg_visits_per_cust = 99999
        THEN NULL
      ELSE avg_visits_per_cust
      END AS avg_visits_per_cust,
    CASE
      WHEN avg_spend_per_cust = 99999
        THEN NULL
      ELSE avg_spend_per_cust
      END AS avg_spend_per_cust,
    CASE
      WHEN ecom_ind_rank = 99999
        THEN NULL
      ELSE ecom_ind_rank
      END AS ecom_ind_rank,
    CASE
      WHEN ecom_txns_share = 99999
        THEN NULL
      ELSE ecom_txns_share
      END AS ecom_txns_share,
    CASE
      WHEN ecom_ind_rank_share = 99999
        THEN NULL
      ELSE ecom_ind_rank_share
      END AS ecom_ind_rank_share
  from
    wpay_analytics.mmi_area_industry_edit
  where
    area_level = 'local_govt_area'
  ) ind_lga
on 
  ((cast(store_lga.LGA_id as string) = cast(ind_lga.area as string))
  and cast(store_lga.industry as string) = cast(ind_lga.industry_code as string)))

----------------------------------------
-- wpay store transactions
,
  store_trans_card as 
(select 
  amount.Year as Year,
  amount.Month as Month,
  amount.State as State,
  --amount.Scheme as Scheme,
  --amount.is_online as is_online,
  amount.scheme as scheme,
  amount.Store as Store,
  amount.Brand as Brand,
  amount.Suburb as Suburb,
  amount.trans_amount as trans_amount,
  counts.trans_count as trans_count,
from
(
select
  left(cast(trans.transaction_date as string),4) as Year,
  substring(cast(trans.transaction_date as string),6,2) as Month,
 -- right(cast(trans.transaction_date as string),2) as Day,
  trans.state as State,
  --trans.scheme as Scheme,
  --trans.is_online as is_online,
  trans.scheme as scheme,
  trans.store_id as Store,
  stores.brand as Brand,
  stores.suburb as Suburb,
  --sum(trans.transaction_count) as trans_count,
  sum(trans.settlement_amount_inc_gst) as trans_amount,
from
  pdh_sf_mep_vw.wpay_all_merchants_daily_fees_summary_hist trans
left join 
  wpay_analytics.ref_store_ind stores
on
  trans.store_id = stores.store_id
where
  (trans.approval_flag = 'A' ) 
  and ((trans.tran_type_description = 'REFUND')
  or (trans.tran_type_description = 'REFUND REVERSAL')
  or (trans.tran_type_description = 'PURCHASE')
  or (trans.tran_type_description = 'PURCHASE REVERSAL')
  or (trans.tran_type_description = 'PURCHASE WITH CASHOUT')
  or (trans.tran_type_description = 'PURCHASE WITH CASHOUT REVERSAL')
  or (trans.tran_type_description = 'REDEMPTION')
  or (trans.tran_type_description = 'REDEMPTION REVERSAL')
  )
group by
  1,2,3,4,5,6,7
) amount
left join
  (
    select
  left(cast(trans.transaction_date as string),4) as Year,
  substring(cast(trans.transaction_date as string),6,2) as Month,
  --right(cast(trans.transaction_date as string),2) as Day,
  trans.state as State,
  --trans.scheme as Scheme,
  --trans.is_online as is_online,
  trans.scheme as scheme,
  trans.store_id as Store,
  stores.brand as Brand,
  stores.suburb as Suburb,
  --sum(trans.transaction_count) as trans_count,
  (sum(case when trans.tran_type_description in 
  ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL') then transaction_count else 0 end)
  -
  sum(case when trans.tran_type_description in 
  ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL') then transaction_count else 0 end)
  ) as trans_count
  --sum(trans.settlement_amount_inc_gst) as trans_amount,
from
  pdh_sf_mep_vw.wpay_all_merchants_daily_fees_summary_hist trans
left join 
  wpay_analytics.ref_store_ind stores
on
  trans.store_id = stores.store_id
where
  (trans.approval_flag = 'A' ) 
  -- and ((trans.tran_type_description = 'PURCHASE')
  -- or (trans.tran_type_description = 'PURCHASE REVERSAL')
  -- or (trans.tran_type_description = 'PURCHASE WITH CASHOUT')
  -- or (trans.tran_type_description = 'PURCHASE WITH CASHOUT REVERSAL')
  -- or (trans.tran_type_description = 'REDEMPTION')
  -- or (trans.tran_type_description = 'REDEMPTION REVERSAL')
  -- )
  group by
  1,2,3,4,5,6,7) counts
on (amount.Year = counts.Year)
   and (amount.Month = counts.Month)
   and (amount.State = counts.State)
   --and (amount.Scheme = counts.Scheme)
  -- and (amount.is_online = counts.is_online)
   and (amount.scheme = counts.scheme)
   and (amount.Store = counts.Store)
   and (amount.Brand = counts.Brand)
   and (amount.Suburb = counts.Suburb)
order by Store,year,month)

-------------------------------------------------------------------------------------------------------------

-- Joined on same table to get previous month/year

, store_internal_card as
(
  select 
    store_trans_card.*,
    case when cast(prev_m.prev_Amount as int) <> 0
    then
    ((store_trans_card.trans_amount - prev_m.prev_Amount)/prev_m.prev_Amount)  
    else 0 end 
    as trans_amount_mom,
    case when cast(prev_y.prev_Amount as int) <> 0
    then
    ((store_trans_card.trans_amount - prev_y.prev_Amount)/prev_y.prev_Amount)  
    else 0 end
    as trans_amount_yoy,
    case when cast(prev_m.prev_Count as int) <> 0
    then
    ((store_trans_card.trans_count - prev_m.prev_Count)/prev_m.prev_Count)  
    else 0 end
    as trans_count_mom,
    case when cast(prev_y.prev_Count as int) <> 0
    then
    ((store_trans_card.trans_count - prev_y.prev_Count)/prev_y.prev_Count)  
    else 0 end
    as trans_count_yoy,
    prev_m.prev_Amount as trans_amount_LM,
    prev_m.prev_Count as trans_count_LM,
    prev_y.prev_Amount as trans_amount_LY,
    prev_y.prev_Count as trans_count_LY,
    case when cast(store_trans_card.trans_count as int) <> 0 then
    (store_trans_card.trans_amount/store_trans_card.trans_count) 
    else 0 end 
    as trans_value,
    case when cast(prev_m.prev_Count as int) <> 0
    then
    (prev_m.prev_Amount/prev_m.prev_Count)  
    else 0 end 
    as trans_value_LM,
    case when cast(prev_y.prev_Count as int) <> 0
    then
    (prev_y.prev_Amount/prev_y.prev_Count)  
    else 0 end 
    as trans_value_LY
from store_trans_card
left join
(
select 
  curr.*,
  prev.Year as prev_year,
  prev.Month as prev_month,
  prev.trans_amount as prev_Amount,
  prev.trans_count as prev_Count
from store_trans_card curr
left join 
  store_trans_card prev
on curr.Store = prev.Store
    and curr.Month = prev.Month
    and (cast(curr.Year as int)-1) = cast(prev.Year as int)
   -- and curr.is_online = prev.is_online
    and curr.scheme = prev.scheme
order by curr.Store,curr.Year,curr.Month
) prev_y
on
  store_trans_card.Store = prev_y.Store
  and store_trans_card.Year = prev_y.Year
  and store_trans_card.Month = prev_y.Month
--  and store_trans_card.is_online = prev_y.is_online
  and store_trans_card.scheme = prev_y.scheme
left join
(
select 
  curr.*,
  prev.trans_amount as prev_Amount,
  prev.trans_count as prev_Count
from store_trans_card curr
left join 
  store_trans_card prev
on curr.Store = prev.Store
    and left(cast(date_add(cast(concat(concat(concat(concat(curr.Year,'-'),curr.Month),'-'),'01') as date), interval -1 month) as string),4) = cast(prev.Year as string)
    and substring(cast(date_add(cast(concat(concat(concat(concat(curr.Year,'-'),curr.Month),'-'),'01') as date), interval -1 month) as string),6,2)  = prev.Month
   -- and curr.is_online = prev.is_online
    and curr.scheme = prev.scheme
order by curr.Store,curr.Year,curr.Month
) prev_m
on
  store_trans_card.Store = prev_m.Store
  and store_trans_card.Year = prev_m.Year
  and store_trans_card.Month = prev_m.Month
--  and store_trans_card.is_online = prev_m.is_online
  and store_trans_card.scheme = prev_m.scheme
order by store_trans_card.Store, store_trans_card.Year,store_trans_card.Month
)

---------------------------------------------------------------------------------------------------------------
,
wpay_store_trans_card as
  (select 
    distinct stores.store_id,
    stores.country,
    stores.state,
    stores.division,
    stores.brand,
    stores.company,
    stores.invoice_group,
    stores.merchant_group,
    stores.merchant_portal_mg,
    stores.merchant_portal_cg,
    stores.industry,
    stores.industry_name as industry_name,
    --distinct stores.store_id,
    stores.site_name,
    stores.suburb,
    stores.postcode,
    stores.LGA,
    stores.LGA_id,
    stores.caid,
    --stores.is_closed,
  --  store_internal_card.is_online,
    store_internal_card.scheme,
    store_internal_card.Year as Year,
    store_internal_card.Month as Month,
    store_internal_card.trans_amount as trans_amount,
    store_internal_card.trans_count as trans_count,
    store_internal_card.trans_amount_mom as trans_amount_mom,
    store_internal_card.trans_amount_yoy as trans_amount_yoy,
    store_internal_card.trans_count_mom as trans_count_mom,
    store_internal_card.trans_count_yoy as trans_count_yoy,
    store_internal_card.trans_amount_LM as trans_amount_LM,
    store_internal_card.trans_count_LM as trans_count_LM,
    store_internal_card.trans_amount_LY as trans_amount_LY,
    store_internal_card.trans_count_LY as trans_count_LY,
    store_internal_card.trans_value as trans_value,
    store_internal_card.trans_value_LM as trans_value_LM,
    store_internal_card.trans_value_LY as trans_value_LY
  from
    wpay_analytics.ref_store_ind stores
  inner join
    store_internal_card
  on stores.store_id = store_internal_card.Store
  --group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34
  order by store_id
  )

-------------------------------------------
--- adding only LGA to wpay_trans

, wpay_LGA_card as
 (select 
    wpay_store_trans_card.*,
    --final_lga_card.LGA,
    --final_lga_card.LGA_id,
    final_lga_card.area,
    --final_lga_card.industry_name,
    final_lga_card.spend_growth_yoy,
    final_lga_card.spend_growth_mom,
    final_lga_card.txn_growth_yoy,
    final_lga_card.txn_growth_mom,
    final_lga_card.spend_density_growth_yoy,
    final_lga_card.spend_density_growth_mom,
    final_lga_card.avg_ticket,
    final_lga_card.b_avg_ticket_LM,
    final_lga_card.b_avg_ticket_LY,
    final_lga_card.b_avg_ticket_mom,
    final_lga_card.b_avg_ticket_yoy,
    final_lga_card.avg_visits_per_cust,
    final_lga_card.avg_spend_per_cust,
    final_lga_card.ecom_ind_rank,
    final_lga_card.ecom_txns_share,
    final_lga_card.ecom_ind_rank_share,
    --final_lga.Benchmark
  from 
    wpay_store_trans_card
  left join 
    final_lga_card
  on (wpay_store_trans_card.store_id = final_lga_card.store_id)
    and (wpay_store_trans_card.Year = final_lga_card.Year)
    --and (wpay_store_trans.Month = final_lga.Month)
    and (cast(wpay_store_trans_card.Month as int) = cast(final_lga_card.Month as int))
  --where wpay_store_trans_card.Year ='2022'
  --    and wpay_store_trans_card.Month ='07'
  --order by final_lga.Benchmark
 )

------------------------------------------------------------
---- adding benchmark 
,

MaxMMIDate as (
select max(Period) as max_date
from  wpay_analytics.mmi_area_industry_edit
),

lga_bench_card as 
(
  select *,
  case when 
    (coalesce(spend_growth_yoy,spend_growth_mom,txn_growth_yoy,txn_growth_mom) is null 
    and concat(concat(Year,'-'),Month) <= (select max_date from MaxMMIDate))
    then 'State'
    else 'LGA'
    end as Benchmark
    from wpay_LGA_card
)


--------------------------------------------
--- going over LGA and filling nulls with State
, lga_benchmark_card as
(
  select
    'Store' as Aggregation_Level,
    lga_bench_card.store_id,
    'All' as store_status,
    lga_bench_card.country,
    lga_bench_card.state,
    lga_bench_card.division,
    lga_bench_card.brand,
    lga_bench_card.industry,
    lga_bench_card.site_name,
    lga_bench_card.suburb,
    cast(lga_bench_card.postcode as string) as postcode,
    cast(lga_bench_card.caid as string)  as caid,
    --cast(lga_bench_filt.is_closed as string) as is_closed,
    lga_bench_card.company,
    lga_bench_card.invoice_group,
    lga_bench_card.merchant_group,
    lga_bench_card.merchant_portal_mg,
    lga_bench_card.merchant_portal_cg,
    'All' as is_online,
    cast(lga_bench_card.scheme as string) as scheme,
    'All' as card_type,
    lga_bench_card.LGA,
    cast(lga_bench_card.LGA_id as string) as LGA_id,
    lga_bench_card.area,
    lga_bench_card.industry_name,
    --final_lga.industry_code,
    --wpay_LGA.Period,
    concat(lga_bench_card.Year,concat('-',lga_bench_card.Month)) as Period,
    lga_bench_card.Year,
    lga_bench_card.Month,
    lga_bench_card.trans_amount as w_trans_amount,
    lga_bench_card.trans_count as w_trans_count,
    lga_bench_card.trans_value as w_trans_value,
    lga_bench_card.trans_amount_LM as w_trans_amount_LM,
    lga_bench_card.trans_count_LM as w_trans_count_LM,
    lga_bench_card.trans_value_LM as w_trans_value_LM,
    lga_bench_card.trans_amount_LY as w_trans_amount_LY,
    lga_bench_card.trans_count_LY as w_trans_count_LY,
    lga_bench_card.trans_value_LY as w_trans_value_LY,
    lga_bench_card.trans_amount_mom as w_trans_amount_mom,
    lga_bench_card.trans_amount_yoy as w_trans_amount_yoy,
    lga_bench_card.trans_count_mom as w_trans_count_mom,
    lga_bench_card.trans_count_yoy as w_trans_count_yoy,
    case when cast(lga_bench_card.trans_value_LM as int) <> 0
    then ((lga_bench_card.trans_value - lga_bench_card.trans_value_LM)/lga_bench_card.trans_value_LM)  
    else 0 end 
    as w_trans_value_mom,
    case when cast(lga_bench_card.trans_value_LY as int) <> 0
    then ((lga_bench_card.trans_value - lga_bench_card.trans_value_LY)/lga_bench_card.trans_value_LY)  
    else 0 end 
    as w_trans_value_yoy,
    case when cust_info.unique_customer_count <> 0 then
    lga_bench_card.trans_count/cust_info.unique_customer_count
    else 0 end as avg_cust_visits,
    case when cust_info.unique_customer_count <> 0 then
    lga_bench_card.trans_amount/cust_info.unique_customer_count
    else 0 end as avg_cust_spend,
    cust_info.unique_card_count as unique_cust,
    cust_info.unique_customer_count as unique_card,
    CASE 
      WHEN lga_bench_card.Benchmark = 'State'
        THEN ind_key.spend_growth_yoy
      ELSE lga_bench_card.spend_growth_yoy
      END AS b_spend_growth_yoy,
    CASE 
      WHEN lga_bench_card.Benchmark = 'State'
        THEN ind_key.spend_growth_mom
      ELSE lga_bench_card.spend_growth_mom
      END AS b_spend_growth_mom,
    CASE
      WHEN lga_bench_card.Benchmark = 'State'
        THEN ind_key.txn_growth_yoy
      ELSE lga_bench_card.txn_growth_yoy
      END AS b_txn_growth_yoy,
    CASE
      WHEN lga_bench_card.Benchmark = 'State'
        THEN ind_key.txn_growth_mom
      ELSE lga_bench_card.txn_growth_mom
      END AS b_txn_growth_mom,
    CASE
      WHEN lga_bench_card.Benchmark = 'State'
        THEN ind_key.spend_density_growth_yoy
      ELSE lga_bench_card.spend_density_growth_yoy
      END AS b_spend_density_growth_yoy,
    CASE
      WHEN lga_bench_card.Benchmark = 'State'
        THEN ind_key.spend_density_growth_mom
      ELSE lga_bench_card.spend_density_growth_mom
      END AS b_spend_density_growth_mom,
    CASE
      WHEN lga_bench_card.Benchmark = 'State'
        THEN ind_key.avg_ticket
      ELSE lga_bench_card.avg_ticket
      END AS b_avg_ticket,
    CASE
      WHEN lga_bench_card.Benchmark = 'State'
        THEN ind_key.b_avg_ticket_LM
      ELSE lga_bench_card.b_avg_ticket_LM
      END AS b_avg_ticket_LM,
    CASE
      WHEN lga_bench_card.Benchmark = 'State'
        THEN ind_key.b_avg_ticket_LY
      ELSE lga_bench_card.b_avg_ticket_LY
      END AS b_avg_ticket_LY,
    CASE
      WHEN lga_bench_card.Benchmark = 'State'
        THEN ind_key.b_avg_ticket_mom
      ELSE lga_bench_card.b_avg_ticket_mom
      END AS b_avg_ticket_mom,
    CASE
      WHEN lga_bench_card.Benchmark = 'State'
        THEN ind_key.b_avg_ticket_yoy
      ELSE lga_bench_card.b_avg_ticket_yoy
      END AS b_avg_ticket_yoy,
    CASE
      WHEN lga_bench_card.Benchmark = 'State'
        THEN ind_key.avg_visits_per_cust
      ELSE lga_bench_card.avg_visits_per_cust
      END AS b_avg_visits_per_cust,
    CASE
      WHEN lga_bench_card.Benchmark = 'State'
        THEN ind_key.avg_spend_per_cust
      ELSE lga_bench_card.avg_spend_per_cust
      END AS b_avg_spend_per_cust,
    CASE
      WHEN lga_bench_card.Benchmark = 'State'
        THEN ind_key.ecom_ind_rank
      ELSE lga_bench_card.ecom_ind_rank
      END AS b_ecom_ind_rank,
    CASE
      WHEN lga_bench_card.Benchmark = 'State'
        THEN ind_key.ecom_txns_share
      ELSE lga_bench_card.ecom_txns_share
      END AS b_ecom_txns_share,
    CASE
      WHEN lga_bench_card.Benchmark = 'State'
        THEN ind_key.ecom_ind_rank_share
      ELSE lga_bench_card.ecom_ind_rank_share
      END AS b_ecom_ind_rank_share,
    --lga_bench_card.Benchmark,
    lga_bench_card.industry_name as Benchmark,
    lga_bench_card.Benchmark as Benchmark_Location,
    'External' as Benchmark_lvl
  from 
    lga_bench_card
  left join
    ( select distinct ind.*, 
    key.State as state,
    left(ind.period,4) as Year,
    right(ind.period,2) as Month
    from
    wpay_analytics.mmi_area_industry_edit ind
    left join 
      wpay_analytics.mmi_aus_mapping_key_edit key
      on cast(ind.area as string) = cast(key.level1_id as string)
    where area_level = 'state') ind_key
  on cast(lga_bench_card.state as string) = cast(ind_key.state as string)
     and cast(lga_bench_card.industry as string) = cast(ind_key.industry_code as string) 
     --and (final_lga.period = ind_key.period ) )
     and cast(trim(lga_bench_card.Year) as string) = cast(trim(ind_key.Year) as string) 
     --and cast(trim(lga_bench.Month) as string) = cast(trim(ind_key.Month) as string) 
     and (cast(lga_bench_card.Month as int) = cast(ind_key.Month as int))
  left join
    (
  select 
      --m.company,
      --m.invoice_group,    
      --m.merchant_group,
      --m.division, 
      --m.brand,    
      m.store_id,   
      --m.site_name,    
     -- m.suburb,   
     -- m.state,    
      --m.postcode,
     -- m.country,
      --m.is_online,
      fee.calc_scheme as scheme,
      --fee.calc_card_type as card_type,
      left(cast(fee.switch_tran_date as string),7) Transaction_Date,
      -- sum(case when fee.calc_tran_type_description in
      -- ('REFUND','REFUND REVERSAL','PURCHASE','PURCHASE REVERSAL','PURCHASE WITH CASHOUT','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION','REDEMPTION REVERSAL') then fee.transaction_total else 0 end)/100 as trans_amount,
      -- (sum(case when fee.calc_tran_type_description in
      -- ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL') then 1 else 0 end)
      -- -
      -- sum(case when fee.calc_tran_type_description in 
      -- ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL') then 1 else 0 end)
      -- ) as trans_count,
      count(distinct fee.card_id) unique_card_count,
      count(distinct fee.card_id) unique_customer_count
      -- (count(1) / count(distinct fee.card_id)) frequency_of_visits,
      -- (  sum(case when fee.calc_tran_type_description in
      -- ('REFUND','REFUND REVERSAL','PURCHASE','PURCHASE REVERSAL','PURCHASE WITH CASHOUT','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION','REDEMPTION REVERSAL') then fee.transaction_total else 0 end) 
      -- / (100*count(distinct fee.card_id))) spend_per_card_holder 
      from wpay_analytics.wpay_billing_engine_tran_dtl fee  
      inner join
      `pdh_ref_ds.ref_store_details` m 
      on LEFT(FEE.net_term_id, 5) = M.store_id
      --inner join
      --wpay_analytics.wpay_card_identity_model cim
      --on trim(cim.scdr_token) = trim(fee.scdr_token)
      where fee.calc_approval_flag = 'A'
	  and left(cast(fee.tstamp_trans as string),7) > '2022-07'
and fee.calc_tran_type_description in ('REFUND','REFUND REVERSAL','PURCHASE','PURCHASE REVERSAL','PURCHASE WITH CASHOUT','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION','REDEMPTION REVERSAL')
      group by 1,2,3
      ) cust_info
  on cust_info.store_id = lga_bench_card.store_id
  and cust_info.scheme = lga_bench_card.scheme
     and concat(lga_bench_card.Year,concat('-',lga_bench_card.Month)) = cust_info.Transaction_Date
  order by lga_bench_card.Year, lga_bench_card.Month, lga_bench_card.store_id
)
-- end

--------------------------------------------------------------------------------------------------------------

--------------------------------------------------------------------------------------------------------------
-- STATE/AUS
--------------------------------------------------------------------------------------------------------------

-- Store base table + mastercard linking and benchmark

,
 final_state_card as 
(select
  *
from
(
select 
  stores.country,
  stores.state,
  stores.division,
  stores.brand,
  stores.company,
  stores.industry,
  stores.industry_name,
  --distinct stores.store_id,
  --stores.site_name,
  --stores.suburb,
  --stores.postcode,
  --stores.caid,
  --stores.is_closed,
  --stores.company,
  stores.invoice_group,
  stores.merchant_group,
  stores.merchant_portal_mg,
  stores.merchant_portal_cg
  --stores.is_online,
from
  wpay_analytics.ref_store_ind stores
group by 1,2,3,4,5,6,7,8,9,10,11) store_info
left join
  (
  select 
    area,
    period,
    --industry_name,
    industry_code,
    left(period,4) as Year,
    right(period,2) as Month,
    key.State as state_mmi,
    CASE 
      WHEN spend_growth_yoy = 99999
        THEN NULL
      ELSE spend_growth_yoy
      END AS spend_growth_yoy,
    CASE 
      WHEN spend_growth_mom = 99999
        THEN NULL
      ELSE spend_growth_mom
      END AS spend_growth_mom,
    CASE
      WHEN txn_growth_yoy = 99999
        THEN NULL
      ELSE txn_growth_yoy
      END AS txn_growth_yoy,
    CASE
      WHEN txn_growth_mom = 99999
        THEN NULL
      ELSE txn_growth_mom
      END AS txn_growth_mom,
    CASE
      WHEN spend_density_growth_yoy = 99999
        THEN NULL
      ELSE spend_density_growth_yoy
      END AS spend_density_growth_yoy,
    CASE
      WHEN spend_density_growth_mom = 99999
        THEN NULL
      ELSE spend_density_growth_mom
      END AS spend_density_growth_mom,
    CASE
      WHEN avg_ticket = 99999
        THEN NULL
      ELSE avg_ticket
      END AS avg_ticket,
    b_avg_ticket_LM,
    b_avg_ticket_LY,
    b_avg_ticket_mom,
    b_avg_ticket_yoy,
    CASE
      WHEN avg_visits_per_cust = 99999
        THEN NULL
      ELSE avg_visits_per_cust
      END AS avg_visits_per_cust,
    CASE
      WHEN avg_spend_per_cust = 99999
        THEN NULL
      ELSE avg_spend_per_cust
      END AS avg_spend_per_cust,
    CASE
      WHEN ecom_ind_rank = 99999
        THEN NULL
      ELSE ecom_ind_rank
      END AS ecom_ind_rank,
    CASE
      WHEN ecom_txns_share = 99999
        THEN NULL
      ELSE ecom_txns_share
      END AS ecom_txns_share,
    CASE
      WHEN ecom_ind_rank_share = 99999
        THEN NULL
      ELSE ecom_ind_rank_share
      END AS ecom_ind_rank_share
  from
    wpay_analytics.mmi_area_industry_edit
  left join
    wpay_analytics.mmi_aus_mapping_key_edit key
    on cast(key.level1_id as string) = cast(area as string)
  where
    area_level = 'state'
  group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22
  ) ind_lga
on 
  cast(store_info.state as string) = cast(ind_lga.state_mmi as string)
  and cast(store_info.industry as string) = cast(ind_lga.industry_code as string)
)

----------------------------------------
-- wpay store transactions
,
 store_trans_state_card as 
(select 
  amount.Year as Year,
  amount.Month as Month,
  amount.Country as Country,
  amount.State as State,
  --amount.Scheme as Scheme,
  --amount.is_online as is_online,
  amount.scheme as scheme,
  --amount.Store as Store,
  amount.industry as Industry,
  amount.industry_name as industry_name,
  amount.Brand as Brand,
  amount.division as division,
  amount.company as company,
  amount.invoice_group as invoice_group,
  amount.merchant_group as merchant_group,
  amount.merchant_portal_mg as merchant_portal_mg,
  amount.merchant_portal_cg as merchant_portal_cg,
  --amount.Suburb as Suburb,
  amount.trans_amount as trans_amount,
  counts.trans_count as trans_count,
from
(
select
  left(cast(trans.transaction_date as string),4) as Year,
  substring(cast(trans.transaction_date as string),6,2) as Month,
 -- right(cast(trans.transaction_date as string),2) as Day,
  stores.country as Country,
  trans.state as State,
  --trans.scheme as Scheme,
  --trans.is_online as is_online,
  trans.scheme as scheme,
  --trans.store_id as Store,
  stores.industry as Industry,
  stores.industry_name as industry_name,
  stores.brand as Brand,
  stores.division as division,
  stores.company as company,
  stores.invoice_group as invoice_group,
  stores.merchant_group as merchant_group,
  stores.merchant_portal_mg as merchant_portal_mg,
  stores.merchant_portal_cg as merchant_portal_cg,
  --stores.suburb as Suburb,
  --sum(trans.transaction_count) as trans_count,
  sum(trans.settlement_amount_inc_gst) as trans_amount,
from
  pdh_sf_mep_vw.wpay_all_merchants_daily_fees_summary_hist trans
left join 
  wpay_analytics.ref_store_ind stores
on
  trans.store_id = stores.store_id
where
  (trans.approval_flag = 'A' ) 
  and ((trans.tran_type_description = 'REFUND')
  or (trans.tran_type_description = 'REFUND REVERSAL')
  or (trans.tran_type_description = 'PURCHASE')
  or (trans.tran_type_description = 'PURCHASE REVERSAL')
  or (trans.tran_type_description = 'PURCHASE WITH CASHOUT')
  or (trans.tran_type_description = 'PURCHASE WITH CASHOUT REVERSAL')
  or (trans.tran_type_description = 'REDEMPTION')
  or (trans.tran_type_description = 'REDEMPTION REVERSAL')
  )
group by
  1,2,3,4,5,6,7,8,9,10,11,12,13,14
) amount
left join
  (
    select
  left(cast(trans.transaction_date as string),4) as Year,
  substring(cast(trans.transaction_date as string),6,2) as Month,
  --right(cast(trans.transaction_date as string),2) as Day,
  stores.country as Country,
  trans.state as State,
  --trans.scheme as Scheme,
  --trans.is_online as is_online,
  trans.scheme as scheme,
  --trans.store_id as Store,
  stores.industry as Industry,
  stores.brand as Brand,
  stores.division as division,
  stores.company as company,
  stores.invoice_group as invoice_group,
  stores.merchant_group as merchant_group,
  stores.merchant_portal_mg as merchant_portal_mg,
  stores.merchant_portal_cg as merchant_portal_cg,
  --stores.suburb as Suburb,
  --sum(trans.transaction_count) as trans_count,
  (sum(case when trans.tran_type_description in 
  ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL') then transaction_count else 0 end)
  -
  sum(case when trans.tran_type_description in 
  ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL') then transaction_count else 0 end)
  ) as trans_count
  --sum(trans.settlement_amount_inc_gst) as trans_amount,
from
  pdh_sf_mep_vw.wpay_all_merchants_daily_fees_summary_hist trans
left join 
  wpay_analytics.ref_store_ind  stores
on
  trans.store_id = stores.store_id
where
  (trans.approval_flag = 'A' ) 
  -- and ((trans.tran_type_description = 'PURCHASE')
  -- or (trans.tran_type_description = 'PURCHASE REVERSAL')
  -- or (trans.tran_type_description = 'PURCHASE WITH CASHOUT')
  -- or (trans.tran_type_description = 'PURCHASE WITH CASHOUT REVERSAL')
  -- or (trans.tran_type_description = 'REDEMPTION')
  -- or (trans.tran_type_description = 'REDEMPTION REVERSAL')
  -- )
  group by
  1,2,3,4,5,6,7,8,9,10,11,12,13) counts
on (amount.Year = counts.Year)
   and (amount.Month = counts.Month)
   and (amount.State = counts.State)
   --and (amount.Scheme = counts.Scheme)
   --and (amount.Online = counts.Online)
   --and (amount.Card_Type = counts.Card_Type)
   --and (amount.Store = counts.Store)
   and (amount.Brand = counts.Brand)
   and (amount.division = counts.division)
   and (amount.company = counts.company)
   and (amount.invoice_group = counts.invoice_group)
   and (amount.merchant_group = counts.merchant_group)
   and (amount.merchant_portal_mg = counts.merchant_portal_mg)
   and (amount.merchant_portal_cg = counts.merchant_portal_cg)
  -- and (amount.is_online = counts.is_online)
   and (amount.scheme = counts.scheme)
   --and (amount.Suburb = counts.Suburb))
)
---------------------------------------------------------------------------------------------------------

-- Joined on same table to get previous month/year

, state_internal_card as
(
  select 
    store_trans_state_card.*,
    case when cast(prev_m.prev_Amount as int) <> 0 then
    ((store_trans_state_card.trans_amount - prev_m.prev_Amount)/prev_m.prev_Amount)  
    else 0 end as trans_amount_mom,
    case when cast(prev_y.prev_Amount as int) <> 0 then
    ((store_trans_state_card.trans_amount - prev_y.prev_Amount)/prev_y.prev_Amount)  
    else 0 end as trans_amount_yoy,
    case when cast(prev_m.prev_Count as int) <> 0 then
    ((store_trans_state_card.trans_count - prev_m.prev_Count)/prev_m.prev_Count)  
    else 0 end as trans_count_mom,
    case when cast(prev_y.prev_Count as int) <> 0 then
    ((store_trans_state_card.trans_count - prev_y.prev_Count)/prev_y.prev_Count)  
    else 0 end as trans_count_yoy,
    prev_m.prev_Amount as trans_amount_LM,
    prev_m.prev_Count as trans_count_LM,
    prev_y.prev_Amount as trans_amount_LY,
    prev_y.prev_Count as trans_count_LY,
    case when cast(store_trans_state_card.trans_count as int) <> 0 then
    (store_trans_state_card.trans_amount/store_trans_state_card.trans_count) 
    else 0 end 
    as trans_value,
    case when cast(prev_m.prev_Count as int) <> 0
    then
    (prev_m.prev_Amount/prev_m.prev_Count)  
    else 0 end 
    as trans_value_LM,
    case when cast(prev_y.prev_Count as int) <> 0
    then
    (prev_y.prev_Amount/prev_y.prev_Count)  
    else 0 end 
    as trans_value_LY
from store_trans_state_card
left join
(
select 
  curr.*,
  prev.Year as prev_year,
  prev.Month as prev_month,
  prev.trans_amount as prev_Amount,
  prev.trans_count as prev_Count
from store_trans_state_card curr
left join 
  store_trans_state_card prev
on  (curr.State = prev.State)
   and (curr.Brand = prev.Brand)
   and (curr.division = prev.division)
   and (curr.company = prev.company)
   and (curr.invoice_group = prev.invoice_group)
   and (curr.merchant_group = prev.merchant_group)
   and (curr.merchant_portal_mg = prev.merchant_portal_mg)
   and (curr.merchant_portal_cg = prev.merchant_portal_cg)
  and curr.Month = prev.Month
  and (cast(curr.Year as int)-1) = cast(prev.Year as int)
 -- and (curr.is_online = prev.is_online)
  and (curr.scheme = prev.scheme)
order by curr.State,curr.Year,curr.Month
) prev_y
on
  (store_trans_state_card.State = prev_y.State)
   and (store_trans_state_card.Brand = prev_y.Brand)
   and (store_trans_state_card.division = prev_y.division)
   and (store_trans_state_card.company = prev_y.company)
   and (store_trans_state_card.invoice_group = prev_y.invoice_group)
   and (store_trans_state_card.merchant_group = prev_y.merchant_group)
   and (store_trans_state_card.merchant_portal_mg = prev_y.merchant_portal_mg)
   and (store_trans_state_card.merchant_portal_cg = prev_y.merchant_portal_cg)
  and store_trans_state_card.Year = prev_y.Year
  and store_trans_state_card.Month = prev_y.Month
 -- and (store_trans_state_card.is_online = prev_y.is_online)
  and (store_trans_state_card.scheme = prev_y.scheme)
left join
(
select 
  curr.*,
  prev.trans_amount as prev_Amount,
  prev.trans_count as prev_Count
from store_trans_state_card curr
left join 
  store_trans_state_card prev
on (curr.State = prev.State)
   and (curr.Brand = prev.Brand)
   and (curr.division = prev.division)
   and (curr.company = prev.company)
   and (curr.invoice_group = prev.invoice_group)
   and (curr.merchant_group = prev.merchant_group)
   and (curr.merchant_portal_mg = prev.merchant_portal_mg)
   and (curr.merchant_portal_cg = prev.merchant_portal_cg)
    and left(cast(date_add(cast(concat(concat(concat(concat(curr.Year,'-'),curr.Month),'-'),'01') as date), interval -1 month) as string),4) = cast(prev.Year as string)
    and substring(cast(date_add(cast(concat(concat(concat(concat(curr.Year,'-'),curr.Month),'-'),'01') as date), interval -1 month) as string),6,2)  = prev.Month
 -- and (curr.is_online = prev.is_online)
  and (curr.scheme = prev.scheme)
order by curr.State,curr.Year,curr.Month
) prev_m
on
  (store_trans_state_card.State = prev_m.State)
   and (store_trans_state_card.Brand = prev_m.Brand)
   and (store_trans_state_card.division = prev_m.division)
   and (store_trans_state_card.company = prev_m.company)
   and (store_trans_state_card.invoice_group = prev_m.invoice_group)
   and (store_trans_state_card.merchant_group = prev_m.merchant_group)
   and (store_trans_state_card.merchant_portal_mg = prev_m.merchant_portal_mg)
   and (store_trans_state_card.merchant_portal_cg = prev_m.merchant_portal_cg)
  and store_trans_state_card.Year = prev_m.Year
  and store_trans_state_card.Month = prev_m.Month
 -- and (store_trans_state_card.is_online = prev_m.is_online)
  and (store_trans_state_card.scheme = prev_m.scheme)
order by store_trans_state_card.State, store_trans_state_card.Year,store_trans_state_card.Month
)

---------------------------------------------------------------------------------------------------------

--- adding only LGA to wpay_trans

, wpay_State_card as
 (select 
    state_internal_card.*,
    final_state_card.area,
    --final_state_card.industry_name,
    final_state_card.spend_growth_yoy,
    final_state_card.spend_growth_mom,
    final_state_card.txn_growth_yoy,
    final_state_card.txn_growth_mom,
    final_state_card.spend_density_growth_yoy,
    final_state_card.spend_density_growth_mom,
    final_state_card.avg_ticket,
    final_state_card.b_avg_ticket_LM,
    final_state_card.b_avg_ticket_LY,
    final_state_card.b_avg_ticket_mom,
    final_state_card.b_avg_ticket_yoy,
    final_state_card.avg_visits_per_cust,
    final_state_card.avg_spend_per_cust,
    final_state_card.ecom_ind_rank,
    final_state_card.ecom_txns_share,
    final_state_card.ecom_ind_rank_share,
    --final_lga.Benchmark
  from 
    state_internal_card
  left join 
    final_state_card
  on (state_internal_card.state = final_state_card.state)
    and (state_internal_card.Brand = final_state_card.Brand)
    and (state_internal_card.division = final_state_card.division)
    and (state_internal_card.company = final_state_card.company)
    and (state_internal_card.invoice_group = final_state_card.invoice_group)
    and (state_internal_card.merchant_group = final_state_card.merchant_group)
    and (state_internal_card.merchant_portal_mg = final_state_card.merchant_portal_mg)
    and (state_internal_card.merchant_portal_cg = final_state_card.merchant_portal_cg)
    and (state_internal_card.Year = final_state_card.Year)
    --and (state_internal.Month = final_state.Month)
    and (cast(state_internal_card.Month as int) = cast(final_state_card.Month as int))
  --where state_internal_card.Year ='2022'
  --    and state_internal_card.Month ='07'
  --order by final_lga.Benchmark
 )

------------------------------------------------------------
---- adding benchmark 
,
state_bench_card as 
(
  select *,
  case when 
    (coalesce(spend_growth_yoy,spend_growth_mom,txn_growth_yoy,txn_growth_mom) is null 
    and concat(concat(Year,'-'),Month) <= (select max_date from MaxMMIDate))
    then 'Country'
    else 'State'
    end as Benchmark
    from wpay_State_card
)


--------------------------------------------
--- going over LGA and filling nulls with State
, state_benchmark_card as
(
  select
    'State' as Aggregation_Level,
    'All' as store_id,
    'All' as store_status,
    state_bench_card.country,
    state_bench_card.state,
    state_bench_card.division,
    state_bench_card.brand,
    state_bench_card.industry,
    'All' as site_name,
    'All' as suburb,
    'All' as postcode,
    'All' as caid,
   -- 'All' as is_closed,
    state_bench_card.company,
    state_bench_card.invoice_group,
    state_bench_card.merchant_group,
    state_bench_card.merchant_portal_mg,
    state_bench_card.merchant_portal_cg,
    'All' as is_online,
    cast(state_bench_card.scheme as string) as scheme,
    'All' as card_type,
    'All' as LGA,
    'All' as LGA_id,
    state_bench_card.area,
    state_bench_card.industry_name,
    --final_lga.industry_code,
    --wpay_LGA.Period,
    concat(state_bench_card.Year,concat('-',state_bench_card.Month)) as Period,
    state_bench_card.Year,
    state_bench_card.Month,
    state_bench_card.trans_amount as w_trans_amount,
    state_bench_card.trans_count as w_trans_count,
    state_bench_card.trans_value as w_trans_value,
    state_bench_card.trans_amount_LM as w_trans_amount_LM,
    state_bench_card.trans_count_LM as w_trans_count_LM,
    state_bench_card.trans_value_LM as w_trans_value_LM,
    state_bench_card.trans_amount_LY as w_trans_amount_LY,
    state_bench_card.trans_count_LY as w_trans_count_LY,
    state_bench_card.trans_value_LY as w_trans_value_LY,
    state_bench_card.trans_amount_mom as w_trans_amount_mom,
    state_bench_card.trans_amount_yoy as w_trans_amount_yoy,
    state_bench_card.trans_count_mom as w_trans_count_mom,
    state_bench_card.trans_count_yoy as w_trans_count_yoy,
    case when cast(state_bench_card.trans_value_LM as int) <> 0
    then ((state_bench_card.trans_value - state_bench_card.trans_value_LM)/state_bench_card.trans_value_LM)  
    else 0 end 
    as w_trans_value_mom,
    case when cast(state_bench_card.trans_value_LY as int) <> 0
    then ((state_bench_card.trans_value - state_bench_card.trans_value_LY)/state_bench_card.trans_value_LY)  
    else 0 end 
    as w_trans_value_yoy,
    case when cust_info.unique_customer_count <> 0 then
    state_bench_card.trans_count/cust_info.unique_customer_count
    else 0 end as avg_cust_visits,
    case when cust_info.unique_customer_count <> 0 then
    state_bench_card.trans_amount/cust_info.unique_customer_count
    else 0 end as avg_cust_spend,
    cust_info.unique_card_count as unique_cust,
    cust_info.unique_customer_count as unique_card,
    CASE 
      WHEN state_bench_card.Benchmark = 'Country'
        THEN ind_key.spend_growth_yoy
      ELSE state_bench_card.spend_growth_yoy
      END AS b_spend_growth_yoy,
    CASE 
      WHEN state_bench_card.Benchmark = 'Country'
        THEN ind_key.spend_growth_mom
      ELSE state_bench_card.spend_growth_mom
      END AS b_spend_growth_mom,
    CASE
      WHEN state_bench_card.Benchmark = 'Country'
        THEN ind_key.txn_growth_yoy
      ELSE state_bench_card.txn_growth_yoy
      END AS b_txn_growth_yoy,
    CASE
      WHEN state_bench_card.Benchmark = 'Country'
        THEN ind_key.txn_growth_mom
      ELSE state_bench_card.txn_growth_mom
      END AS b_txn_growth_mom,
    CASE
      WHEN state_bench_card.Benchmark = 'Country'
        THEN ind_key.spend_density_growth_yoy
      ELSE state_bench_card.spend_density_growth_yoy
      END AS b_spend_density_growth_yoy,
    CASE
      WHEN state_bench_card.Benchmark = 'Country'
        THEN ind_key.spend_density_growth_mom
      ELSE state_bench_card.spend_density_growth_mom
      END AS b_spend_density_growth_mom,
    CASE
      WHEN state_bench_card.Benchmark = 'Country'
        THEN ind_key.avg_ticket
      ELSE state_bench_card.avg_ticket
      END AS b_avg_ticket,
    CASE
      WHEN state_bench_card.Benchmark = 'Country'
        THEN ind_key.b_avg_ticket_LM
      ELSE state_bench_card.b_avg_ticket_LM
      END AS b_avg_ticket_LM,
    CASE
      WHEN state_bench_card.Benchmark = 'Country'
        THEN ind_key.b_avg_ticket_LY
      ELSE state_bench_card.b_avg_ticket_LY
      END AS b_avg_ticket_LY,
    CASE
      WHEN state_bench_card.Benchmark = 'Country'
        THEN ind_key.b_avg_ticket_mom
      ELSE state_bench_card.b_avg_ticket_mom
      END AS b_avg_ticket_mom,
    CASE
      WHEN state_bench_card.Benchmark = 'Country'
        THEN ind_key.b_avg_ticket_yoy
      ELSE state_bench_card.b_avg_ticket_yoy
      END AS b_avg_ticket_yoy,
    CASE
      WHEN state_bench_card.Benchmark = 'Country'
        THEN ind_key.avg_visits_per_cust
      ELSE state_bench_card.avg_visits_per_cust
      END AS b_avg_visits_per_cust,
    CASE
      WHEN state_bench_card.Benchmark = 'Country'
        THEN ind_key.avg_spend_per_cust
      ELSE state_bench_card.avg_spend_per_cust
      END AS b_avg_spend_per_cust,
    CASE
      WHEN state_bench_card.Benchmark = 'Country'
        THEN ind_key.ecom_ind_rank
      ELSE state_bench_card.ecom_ind_rank
      END AS b_ecom_ind_rank,
    CASE
      WHEN state_bench_card.Benchmark = 'Country'
        THEN ind_key.ecom_txns_share
      ELSE state_bench_card.ecom_txns_share
      END AS b_ecom_txns_share,
    CASE
      WHEN state_bench_card.Benchmark = 'Country'
        THEN ind_key.ecom_ind_rank_share
      ELSE state_bench_card.ecom_ind_rank_share
      END AS b_ecom_ind_rank_share,
    --state_bench_card.Benchmark,
    state_bench_card.industry_name as Benchmark,
    state_bench_card.Benchmark as Benchmark_Location,
    'External' as Benchmark_lvl
  from 
    state_bench_card
  left join
    ( select distinct ind.*, 
    left(ind.period,4) as Year,
    right(ind.period,2) as Month
    from
    wpay_analytics.mmi_area_industry_edit ind
    where area_level = 'Country') ind_key
  on cast(state_bench_card.industry as string) = cast(ind_key.industry_code as string) 
     --and (final_lga.period = ind_key.period ) )
     and cast(trim(state_bench_card.Year) as string) = cast(trim(ind_key.Year) as string) 
     --and cast(trim(state_bench.Month) as string) = cast(trim(ind_key.Month) as string)
     and (cast(state_bench_card.Month as int) = cast(ind_key.Month as int)) 
  left join
    (
  select 
      m.company,
      m.invoice_group,    
      m.merchant_group,
      m.division, 
      m.brand, 
      m.merchant_portal_mg,
      m.merchant_portal_cg,   
      --m.store_id,   
      --m.site_name,    
     -- m.suburb,   
      m.state,    
      --m.postcode,
     -- m.country,
      --m.is_online,
      fee.calc_scheme as scheme,
      --fee.calc_card_type as card_type,
      left(cast(fee.switch_tran_date as string),7) Transaction_Date,
      -- sum(case when fee.calc_tran_type_description in
      -- ('REFUND','REFUND REVERSAL','PURCHASE','PURCHASE REVERSAL','PURCHASE WITH CASHOUT','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION','REDEMPTION REVERSAL') then fee.transaction_total else 0 end)/100 as trans_amount,
      -- (sum(case when fee.calc_tran_type_description in
      -- ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL') then 1 else 0 end)
      -- -
      -- sum(case when fee.calc_tran_type_description in 
      -- ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL') then 1 else 0 end)
      -- ) as trans_count,
      count(distinct fee.card_id) unique_card_count,
      count(distinct fee.card_id) unique_customer_count
      -- (count(1) / count(distinct fee.card_id)) frequency_of_visits,
      -- (  sum(case when fee.calc_tran_type_description in
      -- ('REFUND','REFUND REVERSAL','PURCHASE','PURCHASE REVERSAL','PURCHASE WITH CASHOUT','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION','REDEMPTION REVERSAL') then fee.transaction_total else 0 end) 
      -- / (100*count(distinct fee.card_id))) spend_per_card_holder 
      from wpay_analytics.wpay_billing_engine_tran_dtl fee  
      inner join
      `pdh_ref_ds.ref_store_details` m 
      on LEFT(FEE.net_term_id, 5) = M.store_id
      --inner join
      --wpay_analytics.wpay_card_identity_model cim
      --on trim(cim.scdr_token) = trim(fee.scdr_token)
      where fee.calc_approval_flag = 'A'
	  and left(cast(fee.tstamp_trans as string),7) > '2022-07'
and fee.calc_tran_type_description in ('REFUND','REFUND REVERSAL','PURCHASE','PURCHASE REVERSAL','PURCHASE WITH CASHOUT','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION','REDEMPTION REVERSAL')
      group by 1,2,3,4,5,6,7,8,9,10
      ) cust_info
  on cust_info.company = state_bench_card.company
    and cust_info.merchant_group = state_bench_card.merchant_group
    and cust_info.division = state_bench_card.division
    and cust_info.state = state_bench_card.state
    and cust_info.scheme = state_bench_card.scheme
    and cust_info.invoice_group = state_bench_card.invoice_group
    and cust_info.brand = state_bench_card.brand
    and cust_info.merchant_portal_mg = state_bench_card.merchant_portal_mg
    and cust_info.merchant_portal_cg = state_bench_card.merchant_portal_cg
    and concat(state_bench_card.Year,concat('-',state_bench_card.Month)) = cust_info.Transaction_Date
  order by state_bench_card.Benchmark
)
-- end

--------------------------------------------------------------------------------------------------------------
-- END
--------------------------------------------------------------------------------------------------------------

--------------------------------------------------------------------------------------------------------------
-- Country
--------------------------------------------------------------------------------------------------------------
-- Store base table + mastercard linking and benchmark

,
 final_country_card as 
(select
  *
from
(
select 
  stores.country,
  --stores.state,
  stores.division,
  stores.brand,
  stores.company,
  stores.industry,
  stores.industry_name,
  --distinct stores.store_id,
  --stores.site_name,
  --stores.suburb,
  --stores.postcode,
  --stores.caid,
  --stores.is_closed,
  --stores.company,
  stores.invoice_group,
  stores.merchant_group,
  stores.merchant_portal_mg,
  stores.merchant_portal_cg
  --stores.is_online,
from
  wpay_analytics.ref_store_ind stores
group by 1,2,3,4,5,6,7,8,9,10) store_info
left join
  (
  select 
    area,
    period,
    --industry_name,
    industry_code,
    left(period,4) as Year,
    right(period,2) as Month,
    CASE 
      WHEN spend_growth_yoy = 99999
        THEN NULL
      ELSE spend_growth_yoy
      END AS spend_growth_yoy,
    CASE 
      WHEN spend_growth_mom = 99999
        THEN NULL
      ELSE spend_growth_mom
      END AS spend_growth_mom,
    CASE
      WHEN txn_growth_yoy = 99999
        THEN NULL
      ELSE txn_growth_yoy
      END AS txn_growth_yoy,
    CASE
      WHEN txn_growth_mom = 99999
        THEN NULL
      ELSE txn_growth_mom
      END AS txn_growth_mom,
    CASE
      WHEN spend_density_growth_yoy = 99999
        THEN NULL
      ELSE spend_density_growth_yoy
      END AS spend_density_growth_yoy,
    CASE
      WHEN spend_density_growth_mom = 99999
        THEN NULL
      ELSE spend_density_growth_mom
      END AS spend_density_growth_mom,
    CASE
      WHEN avg_ticket = 99999
        THEN NULL
      ELSE avg_ticket
      END AS avg_ticket,
    b_avg_ticket_LM,
    b_avg_ticket_LY,
    b_avg_ticket_mom,
    b_avg_ticket_yoy,
    CASE
      WHEN avg_visits_per_cust = 99999
        THEN NULL
      ELSE avg_visits_per_cust
      END AS avg_visits_per_cust,
    CASE
      WHEN avg_spend_per_cust = 99999
        THEN NULL
      ELSE avg_spend_per_cust
      END AS avg_spend_per_cust,
    CASE
      WHEN ecom_ind_rank = 99999
        THEN NULL
      ELSE ecom_ind_rank
      END AS ecom_ind_rank,
    CASE
      WHEN ecom_txns_share = 99999
        THEN NULL
      ELSE ecom_txns_share
      END AS ecom_txns_share,
    CASE
      WHEN ecom_ind_rank_share = 99999
        THEN NULL
      ELSE ecom_ind_rank_share
      END AS ecom_ind_rank_share
  from
    wpay_analytics.mmi_area_industry_edit
  where
    area_level = 'country'
  group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21
  ) ind_lga
on 
 cast(store_info.industry as string) = cast(ind_lga.industry_code as string)
)

----------------------------------------
-- wpay store transactions
,
 store_trans_country_card as 
(select 
  amount.Year as Year,
  amount.Month as Month,
  amount.Country as Country,
  --amount.State as State,
  --amount.Scheme as Scheme,
  --amount.is_online as is_online,
  amount.scheme as scheme,
  --amount.Store as Store,
  amount.industry as Industry,
  amount.industry_name as industry_name,
  amount.Brand as Brand,
  amount.division as division,
  amount.company as company,
  amount.invoice_group as invoice_group,
  amount.merchant_group as merchant_group,
  amount.merchant_portal_mg as merchant_portal_mg,
  amount.merchant_portal_cg as merchant_portal_cg,
  --amount.Suburb as Suburb,
  amount.trans_amount as trans_amount,
  counts.trans_count as trans_count,
from
(
select
  left(cast(trans.transaction_date as string),4) as Year,
  substring(cast(trans.transaction_date as string),6,2) as Month,
 -- right(cast(trans.transaction_date as string),2) as Day,
  stores.country as Country,
  --trans.state as State,
  --trans.scheme as Scheme,
  --trans.is_online as is_online,
  trans.scheme as scheme,
  --trans.store_id as Store,
  stores.industry as Industry,
  stores.industry_name as industry_name,
  stores.brand as Brand,
  stores.division as division,
  stores.company as company,
  stores.invoice_group as invoice_group,
  stores.merchant_group as merchant_group,
  stores.merchant_portal_mg as merchant_portal_mg,
  stores.merchant_portal_cg as merchant_portal_cg,
  --stores.suburb as Suburb,
  --sum(trans.transaction_count) as trans_count,
  sum(trans.settlement_amount_inc_gst) as trans_amount,
from
  pdh_sf_mep_vw.wpay_all_merchants_daily_fees_summary_hist trans
left join 
  wpay_analytics.ref_store_ind stores
on
  trans.store_id = stores.store_id
where
  (trans.approval_flag = 'A' ) 
  and ((trans.tran_type_description = 'REFUND')
  or (trans.tran_type_description = 'REFUND REVERSAL')
  or (trans.tran_type_description = 'PURCHASE')
  or (trans.tran_type_description = 'PURCHASE REVERSAL')
  or (trans.tran_type_description = 'PURCHASE WITH CASHOUT')
  or (trans.tran_type_description = 'PURCHASE WITH CASHOUT REVERSAL')
  or (trans.tran_type_description = 'REDEMPTION')
  or (trans.tran_type_description = 'REDEMPTION REVERSAL')
  )
group by
  1,2,3,4,5,6,7,8,9,10,11,12,13
) amount
left join
  (
    select
  left(cast(trans.transaction_date as string),4) as Year,
  substring(cast(trans.transaction_date as string),6,2) as Month,
  --right(cast(trans.transaction_date as string),2) as Day,
  stores.country as Country,
  --trans.state as State,
  --trans.scheme as Scheme,
  --trans.is_online as is_online,
  trans.scheme as scheme,
  --trans.store_id as Store,
  stores.industry as Industry,
  stores.brand as Brand,
  stores.division as division,
  stores.company as company,
  stores.invoice_group as invoice_group,
  stores.merchant_group as merchant_group,
  stores.merchant_portal_mg as merchant_portal_mg,
  stores.merchant_portal_cg as merchant_portal_cg,
  --stores.suburb as Suburb,
  --sum(trans.transaction_count) as trans_count,
  (sum(case when trans.tran_type_description in 
  ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL') then transaction_count else 0 end)
  -
  sum(case when trans.tran_type_description in 
  ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL') then transaction_count else 0 end)
  ) as trans_count
  --sum(trans.settlement_amount_inc_gst) as trans_amount,
from
  pdh_sf_mep_vw.wpay_all_merchants_daily_fees_summary_hist trans
left join 
  wpay_analytics.ref_store_ind  stores
on
  trans.store_id = stores.store_id
where
  (trans.approval_flag = 'A' ) 
  -- and ((trans.tran_type_description = 'PURCHASE')
  -- or (trans.tran_type_description = 'PURCHASE REVERSAL')
  -- or (trans.tran_type_description = 'PURCHASE WITH CASHOUT')
  -- or (trans.tran_type_description = 'PURCHASE WITH CASHOUT REVERSAL')
  -- or (trans.tran_type_description = 'REDEMPTION')
  -- or (trans.tran_type_description = 'REDEMPTION REVERSAL')
  -- )
  group by
  1,2,3,4,5,6,7,8,9,10,11,12) counts
on (amount.Year = counts.Year)
   and (amount.Month = counts.Month)
   --and (amount.State = counts.State)
   --and (amount.Scheme = counts.Scheme)
   --and (amount.is_online = counts.is_online)
   and (amount.scheme = counts.scheme)
   --and (amount.Store = counts.Store)
   and (amount.Brand = counts.Brand)
   and (amount.division = counts.division)
   and (amount.company = counts.company)
   and (amount.invoice_group = counts.invoice_group)
   and (amount.merchant_group = counts.merchant_group)
   and (amount.merchant_portal_mg = counts.merchant_portal_mg)
   and (amount.merchant_portal_cg = counts.merchant_portal_cg)
   --and (amount.Suburb = counts.Suburb))
)
------------------------------------------------------------------------------------------------------------

, country_internal_card as
(
  select 
    store_trans_country_card.*,
    case when cast(prev_m.prev_Amount as int) <> 0 then
    ((store_trans_country_card.trans_amount - prev_m.prev_Amount)/prev_m.prev_Amount)  
    else 0 end as trans_amount_mom,
    case when cast(prev_y.prev_Amount as int) <> 0 then
    ((store_trans_country_card.trans_amount - prev_y.prev_Amount)/prev_y.prev_Amount)  
    else 0 end as trans_amount_yoy,
    case when cast(prev_m.prev_Count as int) <> 0 then
    ((store_trans_country_card.trans_count - prev_m.prev_Count)/prev_m.prev_Count)  
    else 0 end as trans_count_mom,
    case when cast(prev_y.prev_Count as int) <> 0 then
    ((store_trans_country_card.trans_count - prev_y.prev_Count)/prev_y.prev_Count)  
    else 0 end as trans_count_yoy,
    prev_m.prev_Amount as trans_amount_LM,
    prev_m.prev_Count as trans_count_LM,
    prev_y.prev_Amount as trans_amount_LY,
    prev_y.prev_Count as trans_count_LY,
    case when cast(store_trans_country_card.trans_count as int) <> 0 then
    (store_trans_country_card.trans_amount/store_trans_country_card.trans_count) 
    else 0 end 
    as trans_value,
    case when cast(prev_m.prev_Count as int) <> 0
    then
    (prev_m.prev_Amount/prev_m.prev_Count)  
    else 0 end 
    as trans_value_LM,
    case when cast(prev_y.prev_Count as int) <> 0
    then
    (prev_y.prev_Amount/prev_y.prev_Count)  
    else 0 end 
    as trans_value_LY
from store_trans_country_card
left join
(
select 
  curr.*,
  prev.Year as prev_year,
  prev.Month as prev_month,
  prev.trans_amount as prev_Amount,
  prev.trans_count as prev_Count
from store_trans_country_card curr
left join 
  store_trans_country_card prev
on  
    (curr.Brand = prev.Brand)
and    (curr.division = prev.division)
   and (curr.company = prev.company)
   and (curr.invoice_group = prev.invoice_group)
   and (curr.merchant_group = prev.merchant_group)
   and (curr.merchant_portal_mg = prev.merchant_portal_mg)
   and (curr.merchant_portal_cg = prev.merchant_portal_cg)
   and curr.Month = prev.Month
   and (cast(curr.Year as int)-1) = cast(prev.Year as int)
   --and curr.is_online = prev.is_online
   and curr.scheme = prev.scheme
order by curr.Year,curr.Month
) prev_y
on
    (store_trans_country_card.Brand = prev_y.Brand)
and    (store_trans_country_card.division = prev_y.division)
   and (store_trans_country_card.company = prev_y.company)
   and (store_trans_country_card.invoice_group = prev_y.invoice_group)
   and (store_trans_country_card.merchant_group = prev_y.merchant_group)
   and (store_trans_country_card.merchant_portal_mg = prev_y.merchant_portal_mg)
   and (store_trans_country_card.merchant_portal_cg = prev_y.merchant_portal_cg)
  and store_trans_country_card.Year = prev_y.Year
  and store_trans_country_card.Month = prev_y.Month
  --and store_trans_country_card.is_online = prev_y.is_online
   and store_trans_country_card.scheme = prev_y.scheme
left join
(
select 
  curr.*,
  prev.trans_amount as prev_Amount,
  prev.trans_count as prev_Count
from store_trans_country_card curr
left join 
  store_trans_country_card prev
on 
    (curr.Brand = prev.Brand)
and    (curr.division = prev.division)
   and (curr.company = prev.company)
   and (curr.invoice_group = prev.invoice_group)
   and (curr.merchant_group = prev.merchant_group)
   and (curr.merchant_portal_mg = prev.merchant_portal_mg)
   and (curr.merchant_portal_cg = prev.merchant_portal_cg)
    and left(cast(date_add(cast(concat(concat(concat(concat(curr.Year,'-'),curr.Month),'-'),'01') as date), interval -1 month) as string),4) = cast(prev.Year as string)
    and substring(cast(date_add(cast(concat(concat(concat(concat(curr.Year,'-'),curr.Month),'-'),'01') as date), interval -1 month) as string),6,2)  = prev.Month
  -- and curr.is_online = prev.is_online
   and curr.scheme = prev.scheme
order by curr.Year,curr.Month
) prev_m
on
    (store_trans_country_card.Brand = prev_m.Brand)
and    (store_trans_country_card.division = prev_m.division)
   and (store_trans_country_card.company = prev_m.company)
   and (store_trans_country_card.invoice_group = prev_m.invoice_group)
   and (store_trans_country_card.merchant_group = prev_m.merchant_group)
   and (store_trans_country_card.merchant_portal_mg = prev_m.merchant_portal_mg)
   and (store_trans_country_card.merchant_portal_cg = prev_m.merchant_portal_cg)
  and store_trans_country_card.Year = prev_m.Year
  and store_trans_country_card.Month = prev_m.Month
 -- and store_trans_country_card.is_online = prev_m.is_online
   and store_trans_country_card.scheme = prev_m.scheme
order by store_trans_country_card.Year,store_trans_country_card.Month
)

-----------------------------------------------------------------------------------------------------------

--- adding only LGA to wpay_trans

, wpay_country_card as
 (select 
    country_internal_card.*,
    final_country_card.area,
   -- final_country_card.industry_name,
    final_country_card.spend_growth_yoy,
    final_country_card.spend_growth_mom,
    final_country_card.txn_growth_yoy,
    final_country_card.txn_growth_mom,
    final_country_card.spend_density_growth_yoy,
    final_country_card.spend_density_growth_mom,
    final_country_card.avg_ticket,
    final_country_card.b_avg_ticket_LM,
    final_country_card.b_avg_ticket_LY,
    final_country_card.b_avg_ticket_mom,
    final_country_card.b_avg_ticket_yoy,
    final_country_card.avg_visits_per_cust,
    final_country_card.avg_spend_per_cust,
    final_country_card.ecom_ind_rank,
    final_country_card.ecom_txns_share,
    final_country_card.ecom_ind_rank_share,
    --final_lga.Benchmark
  from 
    country_internal_card
  left join 
    final_country_card
  on 
    (country_internal_card.Brand = final_country_card.Brand)
    and (country_internal_card.division = final_country_card.division)
    and (country_internal_card.company = final_country_card.company)
    and (country_internal_card.invoice_group = final_country_card.invoice_group)
    and (country_internal_card.merchant_group = final_country_card.merchant_group)
    and (country_internal_card.merchant_portal_mg = final_country_card.merchant_portal_mg)
    and (country_internal_card.merchant_portal_cg = final_country_card.merchant_portal_cg)
    and (country_internal_card.Year = final_country_card.Year)
    --and (country_internal.Month = final_country.Month)
    and (cast(country_internal_card.Month as int) = cast(final_country_card.Month as int))
  --where country_internal_card.Year ='2022'
  --    and country_internal_card.Month ='07'
  --order by final_lga.Benchmark
 )

------------------------------------------------------------
---- adding benchmark 
,
country_bench_card as 
(
  select *,
  'Country' as Benchmark
  from wpay_country_card
)


--------------------------------------------
--- going over LGA and filling nulls with State
, country_benchmark_card as
(
  select
    'Country' as Aggregation_Level,
    'All' as store_id,
    'All' as store_status,
    country_bench_card.country,
    'All' as state,
    country_bench_card.division,
    country_bench_card.brand,
    country_bench_card.industry,
    'All' as site_name,
    'All' as suburb,
    'All' as postcode,
    'All' as caid,
    --'All' as is_closed,
    country_bench_card.company,
    country_bench_card.invoice_group,
    country_bench_card.merchant_group,
    country_bench_card.merchant_portal_mg,
    country_bench_card.merchant_portal_cg,
    'All' as is_online,
    cast(country_bench_card.scheme as string) as scheme,
    'All' as card_type,
    'All' as LGA,
    'All' as LGA_id,
    country_bench_card.area,
    country_bench_card.industry_name,
    --final_lga.industry_code,
    --wpay_LGA.Period,
    concat(country_bench_card.Year,concat('-',country_bench_card.Month)) as Period,
    country_bench_card.Year,
    country_bench_card.Month,
    country_bench_card.trans_amount as w_trans_amount,
    country_bench_card.trans_count as w_trans_count,
    country_bench_card.trans_value as w_trans_value,
    country_bench_card.trans_amount_LM as w_trans_amount_LM,
    country_bench_card.trans_count_LM as w_trans_count_LM,
    country_bench_card.trans_value_LM as w_trans_value_LM,
    country_bench_card.trans_amount_LY as w_trans_amount_LY,
    country_bench_card.trans_count_LY as w_trans_count_LY,
    country_bench_card.trans_value_LY as w_trans_value_LY,
    country_bench_card.trans_amount_mom as w_trans_amount_mom,
    country_bench_card.trans_amount_yoy as w_trans_amount_yoy,
    country_bench_card.trans_count_mom as w_trans_count_mom,
    country_bench_card.trans_count_yoy as w_trans_count_yoy,
    case when cast(country_bench_card.trans_value_LM as int) <> 0
    then ((country_bench_card.trans_value - country_bench_card.trans_value_LM)/country_bench_card.trans_value_LM)  
    else 0 end 
    as w_trans_value_mom,
    case when cast(country_bench_card.trans_value_LY as int) <> 0
    then ((country_bench_card.trans_value - country_bench_card.trans_value_LY)/country_bench_card.trans_value_LY)  
    else 0 end 
    as w_trans_value_yoy,
    case when cust_info.unique_customer_count <> 0 then
    country_bench_card.trans_count/cust_info.unique_customer_count 
    else 0 end as avg_cust_visits,
    case when cust_info.unique_customer_count <> 0 then
    country_bench_card.trans_amount/cust_info.unique_customer_count
    else 0 end as avg_cust_spend,
    cust_info.unique_card_count as unique_cust,
    cust_info.unique_customer_count as unique_card,
    country_bench_card.spend_growth_yoy as b_spend_growth_yoy,
    country_bench_card.spend_growth_mom as b_spend_growth_mom,
    country_bench_card.txn_growth_yoy as b_txn_growth_yoy,
    country_bench_card.txn_growth_mom as b_txn_growth_mom,
    country_bench_card.spend_density_growth_yoy as b_spend_density_growth_yoy,
    country_bench_card.spend_density_growth_mom as b_spend_density_growth_mom,
    country_bench_card.avg_ticket as b_avg_ticket,
    country_bench_card.b_avg_ticket_LM as b_avg_ticket_LM,
    country_bench_card.b_avg_ticket_LY as b_avg_ticket_LY,
    country_bench_card.b_avg_ticket_mom as b_avg_ticket_mom,
    country_bench_card.b_avg_ticket_yoy as b_avg_ticket_yoy,
    country_bench_card.avg_visits_per_cust as b_avg_visits_per_cust,
    country_bench_card.avg_spend_per_cust as b_avg_spend_per_cust,
    country_bench_card.ecom_ind_rank as b_ecom_ind_rank,
    country_bench_card.ecom_txns_share as b_ecom_txns_share,
    country_bench_card.ecom_ind_rank_share as b_ecom_ind_rank_share,
    --country_bench_card.Benchmark,
    country_bench_card.industry_name as Benchmark,
    country_bench_card.Benchmark as Benchmark_Location,
    'External' as Benchmark_lvl
  from 
    country_bench_card
  left join
    (
  select 
      m.company,
      m.invoice_group,    
      m.merchant_group,
      m.division, 
      m.brand, 
      m.merchant_portal_mg,
      m.merchant_portal_cg,   
      --m.store_id,   
      --m.site_name,    
     -- m.suburb,   
      --m.state,    
      --m.postcode,
      m.country,
      --m.is_online,
      fee.calc_scheme as scheme,
      --fee.calc_card_type as card_type,
      left(cast(fee.switch_tran_date as string),7) Transaction_Date,
      -- sum(case when fee.calc_tran_type_description in
      -- ('REFUND','REFUND REVERSAL','PURCHASE','PURCHASE REVERSAL','PURCHASE WITH CASHOUT','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION','REDEMPTION REVERSAL') then fee.transaction_total else 0 end)/100 as trans_amount,
      -- (sum(case when fee.calc_tran_type_description in
      -- ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL') then 1 else 0 end)
      -- -
      -- sum(case when fee.calc_tran_type_description in 
      -- ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL') then 1 else 0 end)
      -- ) as trans_count,
      count(distinct fee.card_id) unique_card_count,
      count(distinct fee.card_id) unique_customer_count
      -- (count(1) / count(distinct fee.card_id)) frequency_of_visits,
      -- (  sum(case when fee.calc_tran_type_description in
      -- ('REFUND','REFUND REVERSAL','PURCHASE','PURCHASE REVERSAL','PURCHASE WITH CASHOUT','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION','REDEMPTION REVERSAL') then fee.transaction_total else 0 end) 
      -- / (100*count(distinct fee.card_id))) spend_per_card_holder 
      from wpay_analytics.wpay_billing_engine_tran_dtl fee  
      inner join
      `pdh_ref_ds.ref_store_details` m 
      on LEFT(FEE.net_term_id, 5) = M.store_id
      --inner join
      --wpay_analytics.wpay_card_identity_model cim
      --on trim(cim.scdr_token) = trim(fee.scdr_token)
      where fee.calc_approval_flag = 'A'
	  and left(cast(fee.tstamp_trans as string),7) > '2022-07'
and fee.calc_tran_type_description in ('REFUND','REFUND REVERSAL','PURCHASE','PURCHASE REVERSAL','PURCHASE WITH CASHOUT','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION','REDEMPTION REVERSAL')
      group by 1,2,3,4,5,6,7,8,9,10
      ) cust_info
  on cust_info.company = country_bench_card.company
    and cust_info.merchant_group = country_bench_card.merchant_group
    and cust_info.division = country_bench_card.division
    and cust_info.country = country_bench_card.country
    and cust_info.scheme = country_bench_card.scheme
    and cust_info.invoice_group = country_bench_card.invoice_group
    and cust_info.brand = country_bench_card.brand
    and cust_info.merchant_portal_mg = country_bench_card.merchant_portal_mg
    and cust_info.merchant_portal_cg = country_bench_card.merchant_portal_cg
    and concat(country_bench_card.Year,concat('-',country_bench_card.Month)) = cust_info.Transaction_Date
)
-- end
select 
  *
  from lga_benchmark_card
union all
select 
  *
  from state_benchmark_card
union all
select 
  *
  from country_benchmark_card

);

-------------------------------------------------------------------------------------------------------------

--- CARD TYPE ONLY

create or replace table pdh_staging_ds.temp_card as (
-------------------------------------------------------------------------------------------------------------------
-- LGA/STATE
-------------------------------------------------------------------------------------------------------------------
-- Store base table + mastercard linking and benchmark
with
final_lga_card as 
(select
  *
from
(
select 
  distinct stores.store_id,
  stores.country,
  stores.state,
  stores.division,
  stores.brand,
  stores.industry,
  stores.industry_name,
  --distinct stores.store_id,
  stores.site_name,
  stores.suburb,
  stores.postcode,
  stores.caid,
  --stores.is_closed,
  stores.company,
  stores.invoice_group,
  stores.merchant_group,
  stores.merchant_portal_mg,
  stores.merchant_portal_cg,
  --stores.is_online,
  stores.LGA as LGA,
  stores.LGA_id as LGA_id
from
  wpay_analytics.ref_store_ind stores
left join
(
  select *
  from
  (select 
  distinct mmi_postcode.Merchant_Post_Code,
  mmi_postcode.City_Name,
  key.level3,
  upper(key.level2) as level2,
  key.level2_id as level2_id
from
  wpay_analytics.ref_merchant_mmi_postcode mmi_postcode
left join
  wpay_analytics.mmi_aus_mapping_key_edit key
on 
  (cast(mmi_postcode.Level_3_Id as string) = cast(key.level3_id as string))
  and (mmi_postcode.Province_Code = key.State)
  ) 
  --where level2 is not null
 ) mmi_location
on 
  (cast(stores.postcode as string) = cast(mmi_location.Merchant_Post_Code as string))
  --and (replace(upper(mmi_location.City_Name),' ','') = replace(upper(stores.suburb),' ',''))
) store_lga
left join
  (
  select 
    area,
    period,
   -- industry_name,
    industry_code,
    left(period,4) as Year,
    right(period,2) as Month,
    CASE 
      WHEN spend_growth_yoy = 99999
        THEN NULL
      ELSE spend_growth_yoy
      END AS spend_growth_yoy,
    CASE 
      WHEN spend_growth_mom = 99999
        THEN NULL
      ELSE spend_growth_mom
      END AS spend_growth_mom,
    CASE
      WHEN txn_growth_yoy = 99999
        THEN NULL
      ELSE txn_growth_yoy
      END AS txn_growth_yoy,
    CASE
      WHEN txn_growth_mom = 99999
        THEN NULL
      ELSE txn_growth_mom
      END AS txn_growth_mom,
    CASE
      WHEN spend_density_growth_yoy = 99999
        THEN NULL
      ELSE spend_density_growth_yoy
      END AS spend_density_growth_yoy,
    CASE
      WHEN spend_density_growth_mom = 99999
        THEN NULL
      ELSE spend_density_growth_mom
      END AS spend_density_growth_mom,
    CASE
      WHEN avg_ticket = 99999
        THEN NULL
      ELSE avg_ticket
      END AS avg_ticket,
    b_avg_ticket_LM,
    b_avg_ticket_LY,
    b_avg_ticket_mom,
    b_avg_ticket_yoy,
    CASE
      WHEN avg_visits_per_cust = 99999
        THEN NULL
      ELSE avg_visits_per_cust
      END AS avg_visits_per_cust,
    CASE
      WHEN avg_spend_per_cust = 99999
        THEN NULL
      ELSE avg_spend_per_cust
      END AS avg_spend_per_cust,
    CASE
      WHEN ecom_ind_rank = 99999
        THEN NULL
      ELSE ecom_ind_rank
      END AS ecom_ind_rank,
    CASE
      WHEN ecom_txns_share = 99999
        THEN NULL
      ELSE ecom_txns_share
      END AS ecom_txns_share,
    CASE
      WHEN ecom_ind_rank_share = 99999
        THEN NULL
      ELSE ecom_ind_rank_share
      END AS ecom_ind_rank_share
  from
    wpay_analytics.mmi_area_industry_edit
  where
    area_level = 'local_govt_area'
  ) ind_lga
on 
  ((cast(store_lga.LGA_id as string) = cast(ind_lga.area as string))
  and cast(store_lga.industry as string) = cast(ind_lga.industry_code as string)))

----------------------------------------
-- wpay store transactions
,
  store_trans_card as 
(select 
  amount.Year as Year,
  amount.Month as Month,
  amount.State as State,
  --amount.scheme_card_type as scheme_card_type,
  --amount.is_online as is_online,
  amount.scheme_card_type as scheme_card_type,
  amount.Store as Store,
  amount.Brand as Brand,
  amount.Suburb as Suburb,
  amount.trans_amount as trans_amount,
  counts.trans_count as trans_count,
from
(
select
  left(cast(trans.transaction_date as string),4) as Year,
  substring(cast(trans.transaction_date as string),6,2) as Month,
 -- right(cast(trans.transaction_date as string),2) as Day,
  trans.state as State,
  --trans.scheme_card_type as scheme_card_type,
  --trans.is_online as is_online,
  trans.scheme_card_type as scheme_card_type,
  trans.store_id as Store,
  stores.brand as Brand,
  stores.suburb as Suburb,
  --sum(trans.transaction_count) as trans_count,
  sum(trans.settlement_amount_inc_gst) as trans_amount,
from
  pdh_sf_mep_vw.wpay_all_merchants_daily_fees_summary_hist trans
left join 
  wpay_analytics.ref_store_ind stores
on
  trans.store_id = stores.store_id
where
  (trans.approval_flag = 'A' ) 
  and ((trans.tran_type_description = 'REFUND')
  or (trans.tran_type_description = 'REFUND REVERSAL')
  or (trans.tran_type_description = 'PURCHASE')
  or (trans.tran_type_description = 'PURCHASE REVERSAL')
  or (trans.tran_type_description = 'PURCHASE WITH CASHOUT')
  or (trans.tran_type_description = 'PURCHASE WITH CASHOUT REVERSAL')
  or (trans.tran_type_description = 'REDEMPTION')
  or (trans.tran_type_description = 'REDEMPTION REVERSAL')
  )
group by
  1,2,3,4,5,6,7
) amount
left join
  (
    select
  left(cast(trans.transaction_date as string),4) as Year,
  substring(cast(trans.transaction_date as string),6,2) as Month,
  --right(cast(trans.transaction_date as string),2) as Day,
  trans.state as State,
  --trans.scheme_card_type as scheme_card_type,
  --trans.is_online as is_online,
  trans.scheme_card_type as scheme_card_type,
  trans.store_id as Store,
  stores.brand as Brand,
  stores.suburb as Suburb,
  --sum(trans.transaction_count) as trans_count,
  (sum(case when trans.tran_type_description in 
  ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL') then transaction_count else 0 end)
  -
  sum(case when trans.tran_type_description in 
  ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL') then transaction_count else 0 end)
  ) as trans_count
  --sum(trans.settlement_amount_inc_gst) as trans_amount,
from
  pdh_sf_mep_vw.wpay_all_merchants_daily_fees_summary_hist trans
left join 
  wpay_analytics.ref_store_ind stores
on
  trans.store_id = stores.store_id
where
  (trans.approval_flag = 'A' ) 
  -- and ((trans.tran_type_description = 'PURCHASE')
  -- or (trans.tran_type_description = 'PURCHASE REVERSAL')
  -- or (trans.tran_type_description = 'PURCHASE WITH CASHOUT')
  -- or (trans.tran_type_description = 'PURCHASE WITH CASHOUT REVERSAL')
  -- or (trans.tran_type_description = 'REDEMPTION')
  -- or (trans.tran_type_description = 'REDEMPTION REVERSAL')
  -- )
  group by
  1,2,3,4,5,6,7) counts
on (amount.Year = counts.Year)
   and (amount.Month = counts.Month)
   and (amount.State = counts.State)
   --and (amount.scheme_card_type = counts.scheme_card_type)
  -- and (amount.is_online = counts.is_online)
   and (amount.scheme_card_type = counts.scheme_card_type)
   and (amount.Store = counts.Store)
   and (amount.Brand = counts.Brand)
   and (amount.Suburb = counts.Suburb)
order by Store,year,month)

-------------------------------------------------------------------------------------------------------------

-- Joined on same table to get previous month/year

, store_internal_card as
(
  select 
    store_trans_card.*,
    case when cast(prev_m.prev_Amount as int) <> 0
    then
    ((store_trans_card.trans_amount - prev_m.prev_Amount)/prev_m.prev_Amount)  
    else 0 end 
    as trans_amount_mom,
    case when cast(prev_y.prev_Amount as int) <> 0
    then
    ((store_trans_card.trans_amount - prev_y.prev_Amount)/prev_y.prev_Amount)  
    else 0 end
    as trans_amount_yoy,
    case when cast(prev_m.prev_Count as int) <> 0
    then
    ((store_trans_card.trans_count - prev_m.prev_Count)/prev_m.prev_Count)  
    else 0 end
    as trans_count_mom,
    case when cast(prev_y.prev_Count as int) <> 0
    then
    ((store_trans_card.trans_count - prev_y.prev_Count)/prev_y.prev_Count)  
    else 0 end
    as trans_count_yoy,
    prev_m.prev_Amount as trans_amount_LM,
    prev_m.prev_Count as trans_count_LM,
    prev_y.prev_Amount as trans_amount_LY,
    prev_y.prev_Count as trans_count_LY,
    case when cast(store_trans_card.trans_count as int) <> 0 then
    (store_trans_card.trans_amount/store_trans_card.trans_count) 
    else 0 end 
    as trans_value,
    case when cast(prev_m.prev_Count as int) <> 0
    then
    (prev_m.prev_Amount/prev_m.prev_Count)  
    else 0 end 
    as trans_value_LM,
    case when cast(prev_y.prev_Count as int) <> 0
    then
    (prev_y.prev_Amount/prev_y.prev_Count)  
    else 0 end 
    as trans_value_LY
from store_trans_card
left join
(
select 
  curr.*,
  prev.Year as prev_year,
  prev.Month as prev_month,
  prev.trans_amount as prev_Amount,
  prev.trans_count as prev_Count
from store_trans_card curr
left join 
  store_trans_card prev
on curr.Store = prev.Store
    and curr.Month = prev.Month
    and (cast(curr.Year as int)-1) = cast(prev.Year as int)
   -- and curr.is_online = prev.is_online
    and curr.scheme_card_type = prev.scheme_card_type
order by curr.Store,curr.Year,curr.Month
) prev_y
on
  store_trans_card.Store = prev_y.Store
  and store_trans_card.Year = prev_y.Year
  and store_trans_card.Month = prev_y.Month
--  and store_trans_card.is_online = prev_y.is_online
  and store_trans_card.scheme_card_type = prev_y.scheme_card_type
left join
(
select 
  curr.*,
  prev.trans_amount as prev_Amount,
  prev.trans_count as prev_Count
from store_trans_card curr
left join 
  store_trans_card prev
on curr.Store = prev.Store
    and left(cast(date_add(cast(concat(concat(concat(concat(curr.Year,'-'),curr.Month),'-'),'01') as date), interval -1 month) as string),4) = cast(prev.Year as string)
    and substring(cast(date_add(cast(concat(concat(concat(concat(curr.Year,'-'),curr.Month),'-'),'01') as date), interval -1 month) as string),6,2)  = prev.Month
   -- and curr.is_online = prev.is_online
    and curr.scheme_card_type = prev.scheme_card_type
order by curr.Store,curr.Year,curr.Month
) prev_m
on
  store_trans_card.Store = prev_m.Store
  and store_trans_card.Year = prev_m.Year
  and store_trans_card.Month = prev_m.Month
--  and store_trans_card.is_online = prev_m.is_online
  and store_trans_card.scheme_card_type = prev_m.scheme_card_type
order by store_trans_card.Store, store_trans_card.Year,store_trans_card.Month
)

---------------------------------------------------------------------------------------------------------------
,
wpay_store_trans_card as
  (select 
    distinct stores.store_id,
    stores.country,
    stores.state,
    stores.division,
    stores.brand,
    stores.company,
    stores.invoice_group,
    stores.merchant_group,
    stores.merchant_portal_mg,
    stores.merchant_portal_cg,
    stores.industry,
    stores.industry_name as industry_name,
    --distinct stores.store_id,
    stores.site_name,
    stores.suburb,
    stores.postcode,
    stores.LGA,
    stores.LGA_id,
    stores.caid,
    --stores.is_closed,
  --  store_internal_card.is_online,
    store_internal_card.scheme_card_type,
    store_internal_card.Year as Year,
    store_internal_card.Month as Month,
    store_internal_card.trans_amount as trans_amount,
    store_internal_card.trans_count as trans_count,
    store_internal_card.trans_amount_mom as trans_amount_mom,
    store_internal_card.trans_amount_yoy as trans_amount_yoy,
    store_internal_card.trans_count_mom as trans_count_mom,
    store_internal_card.trans_count_yoy as trans_count_yoy,
    store_internal_card.trans_amount_LM as trans_amount_LM,
    store_internal_card.trans_count_LM as trans_count_LM,
    store_internal_card.trans_amount_LY as trans_amount_LY,
    store_internal_card.trans_count_LY as trans_count_LY,
    store_internal_card.trans_value as trans_value,
    store_internal_card.trans_value_LM as trans_value_LM,
    store_internal_card.trans_value_LY as trans_value_LY
  from
    wpay_analytics.ref_store_ind stores
  inner join
    store_internal_card
  on stores.store_id = store_internal_card.Store
  --group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34
  order by store_id
  )

-------------------------------------------
--- adding only LGA to wpay_trans

, wpay_LGA_card as
 (select 
    wpay_store_trans_card.*,
    --final_lga_card.LGA,
    --final_lga_card.LGA_id,
    final_lga_card.area,
    --final_lga_card.industry_name,
    final_lga_card.spend_growth_yoy,
    final_lga_card.spend_growth_mom,
    final_lga_card.txn_growth_yoy,
    final_lga_card.txn_growth_mom,
    final_lga_card.spend_density_growth_yoy,
    final_lga_card.spend_density_growth_mom,
    final_lga_card.avg_ticket,
    final_lga_card.b_avg_ticket_LM,
    final_lga_card.b_avg_ticket_LY,
    final_lga_card.b_avg_ticket_mom,
    final_lga_card.b_avg_ticket_yoy,
    final_lga_card.avg_visits_per_cust,
    final_lga_card.avg_spend_per_cust,
    final_lga_card.ecom_ind_rank,
    final_lga_card.ecom_txns_share,
    final_lga_card.ecom_ind_rank_share,
    --final_lga.Benchmark
  from 
    wpay_store_trans_card
  left join 
    final_lga_card
  on (wpay_store_trans_card.store_id = final_lga_card.store_id)
    and (wpay_store_trans_card.Year = final_lga_card.Year)
    --and (wpay_store_trans.Month = final_lga.Month)
    and (cast(wpay_store_trans_card.Month as int) = cast(final_lga_card.Month as int))
  --where wpay_store_trans_card.Year ='2022'
  --    and wpay_store_trans_card.Month ='07'
  --order by final_lga.Benchmark
 )

------------------------------------------------------------
---- adding benchmark 
,

MaxMMIDate as (
select max(Period) as max_date
from  wpay_analytics.mmi_area_industry_edit
),

lga_bench_card as 
(
  select *,
  case when 
    (coalesce(spend_growth_yoy,spend_growth_mom,txn_growth_yoy,txn_growth_mom) is null 
    and concat(concat(Year,'-'),Month) <= (select max_date from MaxMMIDate))
    then 'State'
    else 'LGA'
    end as Benchmark
    from wpay_LGA_card
)


--------------------------------------------
--- going over LGA and filling nulls with State
, lga_benchmark_card as
(
  select
    'Store' as Aggregation_Level,
    lga_bench_card.store_id,
    'All' as store_status,
    lga_bench_card.country,
    lga_bench_card.state,
    lga_bench_card.division,
    lga_bench_card.brand,
    lga_bench_card.industry,
    lga_bench_card.site_name,
    lga_bench_card.suburb,
    cast(lga_bench_card.postcode as string) as postcode,
    cast(lga_bench_card.caid as string)  as caid,
    --cast(lga_bench_filt.is_closed as string) as is_closed,
    lga_bench_card.company,
    lga_bench_card.invoice_group,
    lga_bench_card.merchant_group,
    lga_bench_card.merchant_portal_mg,
    lga_bench_card.merchant_portal_cg,
    'All' as is_online,
    'All' as scheme,
    cast(lga_bench_card.scheme_card_type as string) as card_type,
    lga_bench_card.LGA,
    cast(lga_bench_card.LGA_id as string) as LGA_id,
    lga_bench_card.area,
    lga_bench_card.industry_name,
    --final_lga.industry_code,
    --wpay_LGA.Period,
    concat(lga_bench_card.Year,concat('-',lga_bench_card.Month)) as Period,
    lga_bench_card.Year,
    lga_bench_card.Month,
    lga_bench_card.trans_amount as w_trans_amount,
    lga_bench_card.trans_count as w_trans_count,
    lga_bench_card.trans_value as w_trans_value,
    lga_bench_card.trans_amount_LM as w_trans_amount_LM,
    lga_bench_card.trans_count_LM as w_trans_count_LM,
    lga_bench_card.trans_value_LM as w_trans_value_LM,
    lga_bench_card.trans_amount_LY as w_trans_amount_LY,
    lga_bench_card.trans_count_LY as w_trans_count_LY,
    lga_bench_card.trans_value_LY as w_trans_value_LY,
    lga_bench_card.trans_amount_mom as w_trans_amount_mom,
    lga_bench_card.trans_amount_yoy as w_trans_amount_yoy,
    lga_bench_card.trans_count_mom as w_trans_count_mom,
    lga_bench_card.trans_count_yoy as w_trans_count_yoy,
    case when cast(lga_bench_card.trans_value_LM as int) <> 0
    then ((lga_bench_card.trans_value - lga_bench_card.trans_value_LM)/lga_bench_card.trans_value_LM)  
    else 0 end 
    as w_trans_value_mom,
    case when cast(lga_bench_card.trans_value_LY as int) <> 0
    then ((lga_bench_card.trans_value - lga_bench_card.trans_value_LY)/lga_bench_card.trans_value_LY)  
    else 0 end 
    as w_trans_value_yoy,
    case when cust_info.unique_customer_count <> 0 then
    lga_bench_card.trans_count/cust_info.unique_customer_count 
    else 0 end as avg_cust_visits,
    case when cust_info.unique_customer_count <> 0 then
    lga_bench_card.trans_amount/cust_info.unique_customer_count
    else 0 end as avg_cust_spend,
    cust_info.unique_card_count as unique_cust,
    cust_info.unique_customer_count as unique_card,
    CASE 
      WHEN lga_bench_card.Benchmark = 'State'
        THEN ind_key.spend_growth_yoy
      ELSE lga_bench_card.spend_growth_yoy
      END AS b_spend_growth_yoy,
    CASE 
      WHEN lga_bench_card.Benchmark = 'State'
        THEN ind_key.spend_growth_mom
      ELSE lga_bench_card.spend_growth_mom
      END AS b_spend_growth_mom,
    CASE
      WHEN lga_bench_card.Benchmark = 'State'
        THEN ind_key.txn_growth_yoy
      ELSE lga_bench_card.txn_growth_yoy
      END AS b_txn_growth_yoy,
    CASE
      WHEN lga_bench_card.Benchmark = 'State'
        THEN ind_key.txn_growth_mom
      ELSE lga_bench_card.txn_growth_mom
      END AS b_txn_growth_mom,
    CASE
      WHEN lga_bench_card.Benchmark = 'State'
        THEN ind_key.spend_density_growth_yoy
      ELSE lga_bench_card.spend_density_growth_yoy
      END AS b_spend_density_growth_yoy,
    CASE
      WHEN lga_bench_card.Benchmark = 'State'
        THEN ind_key.spend_density_growth_mom
      ELSE lga_bench_card.spend_density_growth_mom
      END AS b_spend_density_growth_mom,
    CASE
      WHEN lga_bench_card.Benchmark = 'State'
        THEN ind_key.avg_ticket
      ELSE lga_bench_card.avg_ticket
      END AS b_avg_ticket,
    CASE
      WHEN lga_bench_card.Benchmark = 'State'
        THEN ind_key.b_avg_ticket_LM
      ELSE lga_bench_card.b_avg_ticket_LM
      END AS b_avg_ticket_LM,
    CASE
      WHEN lga_bench_card.Benchmark = 'State'
        THEN ind_key.b_avg_ticket_LY
      ELSE lga_bench_card.b_avg_ticket_LY
      END AS b_avg_ticket_LY,
    CASE
      WHEN lga_bench_card.Benchmark = 'State'
        THEN ind_key.b_avg_ticket_mom
      ELSE lga_bench_card.b_avg_ticket_mom
      END AS b_avg_ticket_mom,
    CASE
      WHEN lga_bench_card.Benchmark = 'State'
        THEN ind_key.b_avg_ticket_yoy
      ELSE lga_bench_card.b_avg_ticket_yoy
      END AS b_avg_ticket_yoy,
    CASE
      WHEN lga_bench_card.Benchmark = 'State'
        THEN ind_key.avg_visits_per_cust
      ELSE lga_bench_card.avg_visits_per_cust
      END AS b_avg_visits_per_cust,
    CASE
      WHEN lga_bench_card.Benchmark = 'State'
        THEN ind_key.avg_spend_per_cust
      ELSE lga_bench_card.avg_spend_per_cust
      END AS b_avg_spend_per_cust,
    CASE
      WHEN lga_bench_card.Benchmark = 'State'
        THEN ind_key.ecom_ind_rank
      ELSE lga_bench_card.ecom_ind_rank
      END AS b_ecom_ind_rank,
    CASE
      WHEN lga_bench_card.Benchmark = 'State'
        THEN ind_key.ecom_txns_share
      ELSE lga_bench_card.ecom_txns_share
      END AS b_ecom_txns_share,
    CASE
      WHEN lga_bench_card.Benchmark = 'State'
        THEN ind_key.ecom_ind_rank_share
      ELSE lga_bench_card.ecom_ind_rank_share
      END AS b_ecom_ind_rank_share,
    --lga_bench_card.Benchmark,
    lga_bench_card.industry_name as Benchmark,
    lga_bench_card.Benchmark as Benchmark_Location,
    'External' as Benchmark_lvl
  from 
    lga_bench_card
  left join
    ( select distinct ind.*, 
    key.State as state,
    left(ind.period,4) as Year,
    right(ind.period,2) as Month
    from
    wpay_analytics.mmi_area_industry_edit ind
    left join 
      wpay_analytics.mmi_aus_mapping_key_edit key
      on cast(ind.area as string) = cast(key.level1_id as string)
    where area_level = 'state') ind_key
  on cast(lga_bench_card.state as string) = cast(ind_key.state as string)
     and cast(lga_bench_card.industry as string) = cast(ind_key.industry_code as string) 
     --and (final_lga.period = ind_key.period ) )
     and cast(trim(lga_bench_card.Year) as string) = cast(trim(ind_key.Year) as string) 
     --and cast(trim(lga_bench.Month) as string) = cast(trim(ind_key.Month) as string) 
     and (cast(lga_bench_card.Month as int) = cast(ind_key.Month as int))
  left join
    (
  select 
      --m.company,
      --m.invoice_group,    
      --m.merchant_group,
      --m.division, 
      --m.brand,    
      m.store_id,   
      --m.site_name,    
     -- m.suburb,   
     -- m.state,    
      --m.postcode,
     -- m.country,
      --m.is_online,
      --fee.calc_scheme as scheme,
      fee.calc_card_type as card_type,
      left(cast(fee.switch_tran_date as string),7) Transaction_Date,
      -- sum(case when fee.calc_tran_type_description in
      -- ('REFUND','REFUND REVERSAL','PURCHASE','PURCHASE REVERSAL','PURCHASE WITH CASHOUT','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION','REDEMPTION REVERSAL') then fee.transaction_total else 0 end)/100 as trans_amount,
      -- (sum(case when fee.calc_tran_type_description in
      -- ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL') then 1 else 0 end)
      -- -
      -- sum(case when fee.calc_tran_type_description in 
      -- ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL') then 1 else 0 end)
      -- ) as trans_count,
      count(distinct fee.card_id) unique_card_count,
      count(distinct fee.card_id) unique_customer_count
      -- (count(1) / count(distinct fee.card_id)) frequency_of_visits,
      -- (  sum(case when fee.calc_tran_type_description in
      -- ('REFUND','REFUND REVERSAL','PURCHASE','PURCHASE REVERSAL','PURCHASE WITH CASHOUT','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION','REDEMPTION REVERSAL') then fee.transaction_total else 0 end) 
      -- / (100*count(distinct fee.card_id))) spend_per_card_holder 
      from wpay_analytics.wpay_billing_engine_tran_dtl fee  
      inner join
      `pdh_ref_ds.ref_store_details` m 
      on LEFT(FEE.net_term_id, 5) = M.store_id
      --inner join
      --wpay_analytics.wpay_card_identity_model cim
      --on trim(cim.scdr_token) = trim(fee.scdr_token)
      where fee.calc_approval_flag = 'A'
	  and left(cast(fee.tstamp_trans as string),7) > '2022-07'
and fee.calc_tran_type_description in ('REFUND','REFUND REVERSAL','PURCHASE','PURCHASE REVERSAL','PURCHASE WITH CASHOUT','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION','REDEMPTION REVERSAL')
      group by 1,2,3
      ) cust_info
  on cust_info.store_id = lga_bench_card.store_id
    and cust_info.card_type = lga_bench_card.scheme_card_type
     and concat(lga_bench_card.Year,concat('-',lga_bench_card.Month)) = cust_info.Transaction_Date
  order by lga_bench_card.Year, lga_bench_card.Month, lga_bench_card.store_id
)
-- end

--------------------------------------------------------------------------------------------------------------

--------------------------------------------------------------------------------------------------------------
-- STATE/AUS
--------------------------------------------------------------------------------------------------------------

-- Store base table + mastercard linking and benchmark

,
 final_state_card as 
(select
  *
from
(
select 
  stores.country,
  stores.state,
  stores.division,
  stores.brand,
  stores.company,
  stores.industry,
  stores.industry_name,
  --distinct stores.store_id,
  --stores.site_name,
  --stores.suburb,
  --stores.postcode,
  --stores.caid,
  --stores.is_closed,
  --stores.company,
  stores.invoice_group,
  stores.merchant_group,
  stores.merchant_portal_mg,
  stores.merchant_portal_cg
  --stores.is_online,
from
  wpay_analytics.ref_store_ind stores
group by 1,2,3,4,5,6,7,8,9,10,11) store_info
left join
  (
  select 
    area,
    period,
    --industry_name,
    industry_code,
    left(period,4) as Year,
    right(period,2) as Month,
    key.State as state_mmi,
    CASE 
      WHEN spend_growth_yoy = 99999
        THEN NULL
      ELSE spend_growth_yoy
      END AS spend_growth_yoy,
    CASE 
      WHEN spend_growth_mom = 99999
        THEN NULL
      ELSE spend_growth_mom
      END AS spend_growth_mom,
    CASE
      WHEN txn_growth_yoy = 99999
        THEN NULL
      ELSE txn_growth_yoy
      END AS txn_growth_yoy,
    CASE
      WHEN txn_growth_mom = 99999
        THEN NULL
      ELSE txn_growth_mom
      END AS txn_growth_mom,
    CASE
      WHEN spend_density_growth_yoy = 99999
        THEN NULL
      ELSE spend_density_growth_yoy
      END AS spend_density_growth_yoy,
    CASE
      WHEN spend_density_growth_mom = 99999
        THEN NULL
      ELSE spend_density_growth_mom
      END AS spend_density_growth_mom,
    CASE
      WHEN avg_ticket = 99999
        THEN NULL
      ELSE avg_ticket
      END AS avg_ticket,
    b_avg_ticket_LM,
    b_avg_ticket_LY,
    b_avg_ticket_mom,
    b_avg_ticket_yoy,
    CASE
      WHEN avg_visits_per_cust = 99999
        THEN NULL
      ELSE avg_visits_per_cust
      END AS avg_visits_per_cust,
    CASE
      WHEN avg_spend_per_cust = 99999
        THEN NULL
      ELSE avg_spend_per_cust
      END AS avg_spend_per_cust,
    CASE
      WHEN ecom_ind_rank = 99999
        THEN NULL
      ELSE ecom_ind_rank
      END AS ecom_ind_rank,
    CASE
      WHEN ecom_txns_share = 99999
        THEN NULL
      ELSE ecom_txns_share
      END AS ecom_txns_share,
    CASE
      WHEN ecom_ind_rank_share = 99999
        THEN NULL
      ELSE ecom_ind_rank_share
      END AS ecom_ind_rank_share
  from
    wpay_analytics.mmi_area_industry_edit
  left join
    wpay_analytics.mmi_aus_mapping_key_edit key
    on cast(key.level1_id as string) = cast(area as string)
  where
    area_level = 'state'
  group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22
  ) ind_lga
on 
  cast(store_info.state as string) = cast(ind_lga.state_mmi as string)
  and cast(store_info.industry as string) = cast(ind_lga.industry_code as string)
)

----------------------------------------
-- wpay store transactions
,
 store_trans_state_card as 
(select 
  amount.Year as Year,
  amount.Month as Month,
  amount.Country as Country,
  amount.State as State,
  --amount.scheme_card_type as scheme_card_type,
  --amount.is_online as is_online,
  amount.scheme_card_type as scheme_card_type,
  --amount.Store as Store,
  amount.industry as Industry,
  amount.industry_name as industry_name,
  amount.Brand as Brand,
  amount.division as division,
  amount.company as company,
  amount.invoice_group as invoice_group,
  amount.merchant_group as merchant_group,
  amount.merchant_portal_mg as merchant_portal_mg,
  amount.merchant_portal_cg as merchant_portal_cg,
  --amount.Suburb as Suburb,
  amount.trans_amount as trans_amount,
  counts.trans_count as trans_count,
from
(
select
  left(cast(trans.transaction_date as string),4) as Year,
  substring(cast(trans.transaction_date as string),6,2) as Month,
 -- right(cast(trans.transaction_date as string),2) as Day,
  stores.country as Country,
  trans.state as State,
  --trans.scheme_card_type as scheme_card_type,
  --trans.is_online as is_online,
  trans.scheme_card_type as scheme_card_type,
  --trans.store_id as Store,
  stores.industry as Industry,
  stores.industry_name as industry_name,
  stores.brand as Brand,
  stores.division as division,
  stores.company as company,
  stores.invoice_group as invoice_group,
  stores.merchant_group as merchant_group,
  stores.merchant_portal_mg as merchant_portal_mg,
  stores.merchant_portal_cg as merchant_portal_cg,
  --stores.suburb as Suburb,
  --sum(trans.transaction_count) as trans_count,
  sum(trans.settlement_amount_inc_gst) as trans_amount,
from
  pdh_sf_mep_vw.wpay_all_merchants_daily_fees_summary_hist trans
left join 
  wpay_analytics.ref_store_ind stores
on
  trans.store_id = stores.store_id
where
  (trans.approval_flag = 'A' ) 
  and ((trans.tran_type_description = 'REFUND')
  or (trans.tran_type_description = 'REFUND REVERSAL')
  or (trans.tran_type_description = 'PURCHASE')
  or (trans.tran_type_description = 'PURCHASE REVERSAL')
  or (trans.tran_type_description = 'PURCHASE WITH CASHOUT')
  or (trans.tran_type_description = 'PURCHASE WITH CASHOUT REVERSAL')
  or (trans.tran_type_description = 'REDEMPTION')
  or (trans.tran_type_description = 'REDEMPTION REVERSAL')
  )
group by
  1,2,3,4,5,6,7,8,9,10,11,12,13,14
) amount
left join
  (
    select
  left(cast(trans.transaction_date as string),4) as Year,
  substring(cast(trans.transaction_date as string),6,2) as Month,
  --right(cast(trans.transaction_date as string),2) as Day,
  stores.country as Country,
  trans.state as State,
  --trans.scheme_card_type as scheme_card_type,
  --trans.is_online as is_online,
  trans.scheme_card_type as scheme_card_type,
  --trans.store_id as Store,
  stores.industry as Industry,
  stores.brand as Brand,
  stores.division as division,
  stores.company as company,
  stores.invoice_group as invoice_group,
  stores.merchant_group as merchant_group,
  stores.merchant_portal_mg as merchant_portal_mg,
  stores.merchant_portal_cg as merchant_portal_cg,
  --stores.suburb as Suburb,
  --sum(trans.transaction_count) as trans_count,
  (sum(case when trans.tran_type_description in 
  ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL') then transaction_count else 0 end)
  -
  sum(case when trans.tran_type_description in 
  ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL') then transaction_count else 0 end)
  ) as trans_count
  --sum(trans.settlement_amount_inc_gst) as trans_amount,
from
  pdh_sf_mep_vw.wpay_all_merchants_daily_fees_summary_hist trans
left join 
  wpay_analytics.ref_store_ind  stores
on
  trans.store_id = stores.store_id
where
  (trans.approval_flag = 'A' ) 
  -- and ((trans.tran_type_description = 'PURCHASE')
  -- or (trans.tran_type_description = 'PURCHASE REVERSAL')
  -- or (trans.tran_type_description = 'PURCHASE WITH CASHOUT')
  -- or (trans.tran_type_description = 'PURCHASE WITH CASHOUT REVERSAL')
  -- or (trans.tran_type_description = 'REDEMPTION')
  -- or (trans.tran_type_description = 'REDEMPTION REVERSAL')
  -- )
  group by
  1,2,3,4,5,6,7,8,9,10,11,12,13) counts
on (amount.Year = counts.Year)
   and (amount.Month = counts.Month)
   and (amount.State = counts.State)
   --and (amount.scheme_card_type = counts.scheme_card_type)
   --and (amount.Online = counts.Online)
   --and (amount.Card_Type = counts.Card_Type)
   --and (amount.Store = counts.Store)
   and (amount.Brand = counts.Brand)
   and (amount.division = counts.division)
   and (amount.company = counts.company)
   and (amount.invoice_group = counts.invoice_group)
   and (amount.merchant_group = counts.merchant_group)
   and (amount.merchant_portal_mg = counts.merchant_portal_mg)
   and (amount.merchant_portal_cg = counts.merchant_portal_cg)
  -- and (amount.is_online = counts.is_online)
   and (amount.scheme_card_type = counts.scheme_card_type)
   --and (amount.Suburb = counts.Suburb))
)
---------------------------------------------------------------------------------------------------------

-- Joined on same table to get previous month/year

, state_internal_card as
(
  select 
    store_trans_state_card.*,
    case when prev_m.prev_Amount <> 0 then
    ((store_trans_state_card.trans_amount - prev_m.prev_Amount)/prev_m.prev_Amount) 
    else 0 end as trans_amount_mom,
    case when prev_y.prev_Amount <> 0 then
    ((store_trans_state_card.trans_amount - prev_y.prev_Amount)/prev_y.prev_Amount) 
    else 0 end as trans_amount_yoy,
    case when cast(prev_m.prev_Count as int) <> 0 then
    ((store_trans_state_card.trans_count - prev_m.prev_Count)/prev_m.prev_Count)  
    else 0 end
    as trans_count_mom,
    case when cast(prev_y.prev_Count as int) <> 0 then
    ((store_trans_state_card.trans_count - prev_y.prev_Count)/prev_y.prev_Count)  
    else 0 end
    as trans_count_yoy,
    prev_m.prev_Amount as trans_amount_LM,
    prev_m.prev_Count as trans_count_LM,
    prev_y.prev_Amount as trans_amount_LY,
    prev_y.prev_Count as trans_count_LY,
    case when cast(store_trans_state_card.trans_count as int) <> 0 then
    (store_trans_state_card.trans_amount/store_trans_state_card.trans_count) 
    else 0 end 
    as trans_value,
    case when cast(prev_m.prev_Count as int) <> 0
    then
    (prev_m.prev_Amount/prev_m.prev_Count)  
    else 0 end 
    as trans_value_LM,
    case when cast(prev_y.prev_Count as int) <> 0
    then
    (prev_y.prev_Amount/prev_y.prev_Count)  
    else 0 end 
    as trans_value_LY
from store_trans_state_card
left join
(
select 
  curr.*,
  prev.Year as prev_year,
  prev.Month as prev_month,
  prev.trans_amount as prev_Amount,
  prev.trans_count as prev_Count
from store_trans_state_card curr
left join 
  store_trans_state_card prev
on  (curr.State = prev.State)
   and (curr.Brand = prev.Brand)
   and (curr.division = prev.division)
   and (curr.company = prev.company)
   and (curr.invoice_group = prev.invoice_group)
   and (curr.merchant_group = prev.merchant_group)
   and (curr.merchant_portal_mg = prev.merchant_portal_mg)
   and (curr.merchant_portal_cg = prev.merchant_portal_cg)
  and curr.Month = prev.Month
  and (cast(curr.Year as int)-1) = cast(prev.Year as int)
 -- and (curr.is_online = prev.is_online)
  and (curr.scheme_card_type = prev.scheme_card_type)
order by curr.State,curr.Year,curr.Month
) prev_y
on
  (store_trans_state_card.State = prev_y.State)
   and (store_trans_state_card.Brand = prev_y.Brand)
   and (store_trans_state_card.division = prev_y.division)
   and (store_trans_state_card.company = prev_y.company)
   and (store_trans_state_card.invoice_group = prev_y.invoice_group)
   and (store_trans_state_card.merchant_group = prev_y.merchant_group)
   and (store_trans_state_card.merchant_portal_mg = prev_y.merchant_portal_mg)
   and (store_trans_state_card.merchant_portal_cg = prev_y.merchant_portal_cg)
  and store_trans_state_card.Year = prev_y.Year
  and store_trans_state_card.Month = prev_y.Month
 -- and (store_trans_state_card.is_online = prev_y.is_online)
  and (store_trans_state_card.scheme_card_type = prev_y.scheme_card_type)
left join
(
select 
  curr.*,
  prev.trans_amount as prev_Amount,
  prev.trans_count as prev_Count
from store_trans_state_card curr
left join 
  store_trans_state_card prev
on (curr.State = prev.State)
   and (curr.Brand = prev.Brand)
   and (curr.division = prev.division)
   and (curr.company = prev.company)
   and (curr.invoice_group = prev.invoice_group)
   and (curr.merchant_group = prev.merchant_group)
   and (curr.merchant_portal_mg = prev.merchant_portal_mg)
   and (curr.merchant_portal_cg = prev.merchant_portal_cg)
    and left(cast(date_add(cast(concat(concat(concat(concat(curr.Year,'-'),curr.Month),'-'),'01') as date), interval -1 month) as string),4) = cast(prev.Year as string)
    and substring(cast(date_add(cast(concat(concat(concat(concat(curr.Year,'-'),curr.Month),'-'),'01') as date), interval -1 month) as string),6,2)  = prev.Month
 -- and (curr.is_online = prev.is_online)
  and (curr.scheme_card_type = prev.scheme_card_type)
order by curr.State,curr.Year,curr.Month
) prev_m
on
  (store_trans_state_card.State = prev_m.State)
   and (store_trans_state_card.Brand = prev_m.Brand)
   and (store_trans_state_card.division = prev_m.division)
   and (store_trans_state_card.company = prev_m.company)
   and (store_trans_state_card.invoice_group = prev_m.invoice_group)
   and (store_trans_state_card.merchant_group = prev_m.merchant_group)
   and (store_trans_state_card.merchant_portal_mg = prev_m.merchant_portal_mg)
   and (store_trans_state_card.merchant_portal_cg = prev_m.merchant_portal_cg)
  and store_trans_state_card.Year = prev_m.Year
  and store_trans_state_card.Month = prev_m.Month
 -- and (store_trans_state_card.is_online = prev_m.is_online)
  and (store_trans_state_card.scheme_card_type = prev_m.scheme_card_type)
order by store_trans_state_card.State, store_trans_state_card.Year,store_trans_state_card.Month
)

---------------------------------------------------------------------------------------------------------

--- adding only LGA to wpay_trans

, wpay_State_card as
 (select 
    state_internal_card.*,
    final_state_card.area,
    --final_state_card.industry_name,
    final_state_card.spend_growth_yoy,
    final_state_card.spend_growth_mom,
    final_state_card.txn_growth_yoy,
    final_state_card.txn_growth_mom,
    final_state_card.spend_density_growth_yoy,
    final_state_card.spend_density_growth_mom,
    final_state_card.avg_ticket,
    final_state_card.b_avg_ticket_LM,
    final_state_card.b_avg_ticket_LY,
    final_state_card.b_avg_ticket_mom,
    final_state_card.b_avg_ticket_yoy,
    final_state_card.avg_visits_per_cust,
    final_state_card.avg_spend_per_cust,
    final_state_card.ecom_ind_rank,
    final_state_card.ecom_txns_share,
    final_state_card.ecom_ind_rank_share,
    --final_lga.Benchmark
  from 
    state_internal_card
  left join 
    final_state_card
  on (state_internal_card.state = final_state_card.state)
    and (state_internal_card.Brand = final_state_card.Brand)
    and (state_internal_card.division = final_state_card.division)
    and (state_internal_card.company = final_state_card.company)
    and (state_internal_card.invoice_group = final_state_card.invoice_group)
    and (state_internal_card.merchant_group = final_state_card.merchant_group)
    and (state_internal_card.merchant_portal_mg = final_state_card.merchant_portal_mg)
    and (state_internal_card.merchant_portal_cg = final_state_card.merchant_portal_cg)
    and (state_internal_card.Year = final_state_card.Year)
    --and (state_internal.Month = final_state.Month)
    and (cast(state_internal_card.Month as int) = cast(final_state_card.Month as int))
  --where state_internal_card.Year ='2022'
  --    and state_internal_card.Month ='07'
  --order by final_lga.Benchmark
 )

------------------------------------------------------------
---- adding benchmark 
,
state_bench_card as 
(
  select *,
  case when 
    (coalesce(spend_growth_yoy,spend_growth_mom,txn_growth_yoy,txn_growth_mom) is null 
    and concat(concat(Year,'-'),Month) <= (select max_date from MaxMMIDate))
    then 'Country'
    else 'State'
    end as Benchmark
    from wpay_State_card
)


--------------------------------------------
--- going over LGA and filling nulls with State
, state_benchmark_card as
(
  select
    'State' as Aggregation_Level,
    'All' as store_id,
    'All' as store_status,
    state_bench_card.country,
    state_bench_card.state,
    state_bench_card.division,
    state_bench_card.brand,
    state_bench_card.industry,
    'All' as site_name,
    'All' as suburb,
    'All' as postcode,
    'All' as caid,
   -- 'All' as is_closed,
    state_bench_card.company,
    state_bench_card.invoice_group,
    state_bench_card.merchant_group,
    state_bench_card.merchant_portal_mg,
    state_bench_card.merchant_portal_cg,
    'All' as is_online,
    'All' as scheme,
    cast(state_bench_card.scheme_card_type as string) as card_type,
    'All' as LGA,
    'All' as LGA_id,
    state_bench_card.area,
    state_bench_card.industry_name,
    --final_lga.industry_code,
    --wpay_LGA.Period,
    concat(state_bench_card.Year,concat('-',state_bench_card.Month)) as Period,
    state_bench_card.Year,
    state_bench_card.Month,
    state_bench_card.trans_amount as w_trans_amount,
    state_bench_card.trans_count as w_trans_count,
    state_bench_card.trans_value as w_trans_value,
    state_bench_card.trans_amount_LM as w_trans_amount_LM,
    state_bench_card.trans_count_LM as w_trans_count_LM,
    state_bench_card.trans_value_LM as w_trans_value_LM,
    state_bench_card.trans_amount_LY as w_trans_amount_LY,
    state_bench_card.trans_count_LY as w_trans_count_LY,
    state_bench_card.trans_value_LY as w_trans_value_LY,
    state_bench_card.trans_amount_mom as w_trans_amount_mom,
    state_bench_card.trans_amount_yoy as w_trans_amount_yoy,
    state_bench_card.trans_count_mom as w_trans_count_mom,
    state_bench_card.trans_count_yoy as w_trans_count_yoy,
    case when cast(state_bench_card.trans_value_LM as int) <> 0
    then ((state_bench_card.trans_value - state_bench_card.trans_value_LM)/state_bench_card.trans_value_LM)  
    else 0 end 
    as w_trans_value_mom,
    case when cast(state_bench_card.trans_value_LY as int) <> 0
    then ((state_bench_card.trans_value - state_bench_card.trans_value_LY)/state_bench_card.trans_value_LY)  
    else 0 end 
    as w_trans_value_yoy,
    case when cust_info.unique_customer_count <> 0 then
    state_bench_card.trans_count/cust_info.unique_customer_count 
    else 0 end as avg_cust_visits,
    case when cust_info.unique_customer_count <> 0 then
    state_bench_card.trans_amount/cust_info.unique_customer_count 
    else 0 end as avg_cust_spend,
    cust_info.unique_card_count as unique_cust,
    cust_info.unique_customer_count as unique_card,
    CASE 
      WHEN state_bench_card.Benchmark = 'Country'
        THEN ind_key.spend_growth_yoy
      ELSE state_bench_card.spend_growth_yoy
      END AS b_spend_growth_yoy,
    CASE 
      WHEN state_bench_card.Benchmark = 'Country'
        THEN ind_key.spend_growth_mom
      ELSE state_bench_card.spend_growth_mom
      END AS b_spend_growth_mom,
    CASE
      WHEN state_bench_card.Benchmark = 'Country'
        THEN ind_key.txn_growth_yoy
      ELSE state_bench_card.txn_growth_yoy
      END AS b_txn_growth_yoy,
    CASE
      WHEN state_bench_card.Benchmark = 'Country'
        THEN ind_key.txn_growth_mom
      ELSE state_bench_card.txn_growth_mom
      END AS b_txn_growth_mom,
    CASE
      WHEN state_bench_card.Benchmark = 'Country'
        THEN ind_key.spend_density_growth_yoy
      ELSE state_bench_card.spend_density_growth_yoy
      END AS b_spend_density_growth_yoy,
    CASE
      WHEN state_bench_card.Benchmark = 'Country'
        THEN ind_key.spend_density_growth_mom
      ELSE state_bench_card.spend_density_growth_mom
      END AS b_spend_density_growth_mom,
    CASE
      WHEN state_bench_card.Benchmark = 'Country'
        THEN ind_key.avg_ticket
      ELSE state_bench_card.avg_ticket
      END AS b_avg_ticket,
    CASE
      WHEN state_bench_card.Benchmark = 'Country'
        THEN ind_key.b_avg_ticket_LM
      ELSE state_bench_card.b_avg_ticket_LM
      END AS b_avg_ticket_LM,
    CASE
      WHEN state_bench_card.Benchmark = 'Country'
        THEN ind_key.b_avg_ticket_LY
      ELSE state_bench_card.b_avg_ticket_LY
      END AS b_avg_ticket_LY,
    CASE
      WHEN state_bench_card.Benchmark = 'Country'
        THEN ind_key.b_avg_ticket_mom
      ELSE state_bench_card.b_avg_ticket_mom
      END AS b_avg_ticket_mom,
    CASE
      WHEN state_bench_card.Benchmark = 'Country'
        THEN ind_key.b_avg_ticket_yoy
      ELSE state_bench_card.b_avg_ticket_yoy
      END AS b_avg_ticket_yoy,
    CASE
      WHEN state_bench_card.Benchmark = 'Country'
        THEN ind_key.avg_visits_per_cust
      ELSE state_bench_card.avg_visits_per_cust
      END AS b_avg_visits_per_cust,
    CASE
      WHEN state_bench_card.Benchmark = 'Country'
        THEN ind_key.avg_spend_per_cust
      ELSE state_bench_card.avg_spend_per_cust
      END AS b_avg_spend_per_cust,
    CASE
      WHEN state_bench_card.Benchmark = 'Country'
        THEN ind_key.ecom_ind_rank
      ELSE state_bench_card.ecom_ind_rank
      END AS b_ecom_ind_rank,
    CASE
      WHEN state_bench_card.Benchmark = 'Country'
        THEN ind_key.ecom_txns_share
      ELSE state_bench_card.ecom_txns_share
      END AS b_ecom_txns_share,
    CASE
      WHEN state_bench_card.Benchmark = 'Country'
        THEN ind_key.ecom_ind_rank_share
      ELSE state_bench_card.ecom_ind_rank_share
      END AS b_ecom_ind_rank_share,
    --state_bench_card.Benchmark,
    state_bench_card.industry_name as Benchmark,
    state_bench_card.Benchmark as Benchmark_Location,
    'External' as Benchmark_lvl
  from 
    state_bench_card
  left join
    ( select distinct ind.*, 
    left(ind.period,4) as Year,
    right(ind.period,2) as Month
    from
    wpay_analytics.mmi_area_industry_edit ind
    where area_level = 'Country') ind_key
  on cast(state_bench_card.industry as string) = cast(ind_key.industry_code as string) 
     --and (final_lga.period = ind_key.period ) )
     and cast(trim(state_bench_card.Year) as string) = cast(trim(ind_key.Year) as string) 
     --and cast(trim(state_bench.Month) as string) = cast(trim(ind_key.Month) as string)
     and (cast(state_bench_card.Month as int) = cast(ind_key.Month as int)) 
  left join
    (
  select 
      m.company,
      m.invoice_group,    
      m.merchant_group,
      m.division, 
      m.brand, 
      m.merchant_portal_mg,
      m.merchant_portal_cg,   
      --m.store_id,   
      --m.site_name,    
     -- m.suburb,   
      m.state,    
      --m.postcode,
     -- m.country,
      --m.is_online,
      --fee.calc_scheme as scheme,
      fee.calc_card_type as card_type,
      left(cast(fee.switch_tran_date as string),7) Transaction_Date,
      -- sum(case when fee.calc_tran_type_description in
      -- ('REFUND','REFUND REVERSAL','PURCHASE','PURCHASE REVERSAL','PURCHASE WITH CASHOUT','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION','REDEMPTION REVERSAL') then fee.transaction_total else 0 end)/100 as trans_amount,
      -- (sum(case when fee.calc_tran_type_description in
      -- ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL') then 1 else 0 end)
      -- -
      -- sum(case when fee.calc_tran_type_description in 
      -- ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL') then 1 else 0 end)
      -- ) as trans_count,
      count(distinct fee.card_id) unique_card_count,
      count(distinct fee.card_id) unique_customer_count
      -- (count(1) / count(distinct fee.card_id)) frequency_of_visits,
      -- (  sum(case when fee.calc_tran_type_description in
      -- ('REFUND','REFUND REVERSAL','PURCHASE','PURCHASE REVERSAL','PURCHASE WITH CASHOUT','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION','REDEMPTION REVERSAL') then fee.transaction_total else 0 end) 
      -- / (100*count(distinct fee.card_id))) spend_per_card_holder 
      from wpay_analytics.wpay_billing_engine_tran_dtl fee  
      inner join
      `pdh_ref_ds.ref_store_details` m 
      on LEFT(FEE.net_term_id, 5) = M.store_id
      --inner join
      --wpay_analytics.wpay_card_identity_model cim
      --on trim(cim.scdr_token) = trim(fee.scdr_token)
      where fee.calc_approval_flag = 'A'
	  and left(cast(fee.tstamp_trans as string),7) > '2022-07'
and fee.calc_tran_type_description in ('REFUND','REFUND REVERSAL','PURCHASE','PURCHASE REVERSAL','PURCHASE WITH CASHOUT','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION','REDEMPTION REVERSAL')
      group by 1,2,3,4,5,6,7,8,9,10
      ) cust_info
  on cust_info.company = state_bench_card.company
    and cust_info.merchant_group = state_bench_card.merchant_group
    and cust_info.division = state_bench_card.division
    and cust_info.state = state_bench_card.state
    and cust_info.card_type = state_bench_card.scheme_card_type
    and cust_info.invoice_group = state_bench_card.invoice_group
    and cust_info.brand = state_bench_card.brand
    and cust_info.merchant_portal_mg = state_bench_card.merchant_portal_mg
    and cust_info.merchant_portal_cg = state_bench_card.merchant_portal_cg
    and concat(state_bench_card.Year,concat('-',state_bench_card.Month)) = cust_info.Transaction_Date
  order by state_bench_card.Benchmark
)
-- end

--------------------------------------------------------------------------------------------------------------
-- END
--------------------------------------------------------------------------------------------------------------

--------------------------------------------------------------------------------------------------------------
-- Country
--------------------------------------------------------------------------------------------------------------
-- Store base table + mastercard linking and benchmark

,
 final_country_card as 
(select
  *
from
(
select 
  stores.country,
  --stores.state,
  stores.division,
  stores.brand,
  stores.company,
  stores.industry,
  stores.industry_name,
  --distinct stores.store_id,
  --stores.site_name,
  --stores.suburb,
  --stores.postcode,
  --stores.caid,
  --stores.is_closed,
  --stores.company,
  stores.invoice_group,
  stores.merchant_group,
  stores.merchant_portal_mg,
  stores.merchant_portal_cg
  --stores.is_online,
from
  wpay_analytics.ref_store_ind stores
group by 1,2,3,4,5,6,7,8,9,10) store_info
left join
  (
  select 
    area,
    period,
    --industry_name,
    industry_code,
    left(period,4) as Year,
    right(period,2) as Month,
    CASE 
      WHEN spend_growth_yoy = 99999
        THEN NULL
      ELSE spend_growth_yoy
      END AS spend_growth_yoy,
    CASE 
      WHEN spend_growth_mom = 99999
        THEN NULL
      ELSE spend_growth_mom
      END AS spend_growth_mom,
    CASE
      WHEN txn_growth_yoy = 99999
        THEN NULL
      ELSE txn_growth_yoy
      END AS txn_growth_yoy,
    CASE
      WHEN txn_growth_mom = 99999
        THEN NULL
      ELSE txn_growth_mom
      END AS txn_growth_mom,
    CASE
      WHEN spend_density_growth_yoy = 99999
        THEN NULL
      ELSE spend_density_growth_yoy
      END AS spend_density_growth_yoy,
    CASE
      WHEN spend_density_growth_mom = 99999
        THEN NULL
      ELSE spend_density_growth_mom
      END AS spend_density_growth_mom,
    CASE
      WHEN avg_ticket = 99999
        THEN NULL
      ELSE avg_ticket
      END AS avg_ticket,
    b_avg_ticket_LM,
    b_avg_ticket_LY,
    b_avg_ticket_mom,
    b_avg_ticket_yoy,
    CASE
      WHEN avg_visits_per_cust = 99999
        THEN NULL
      ELSE avg_visits_per_cust
      END AS avg_visits_per_cust,
    CASE
      WHEN avg_spend_per_cust = 99999
        THEN NULL
      ELSE avg_spend_per_cust
      END AS avg_spend_per_cust,
    CASE
      WHEN ecom_ind_rank = 99999
        THEN NULL
      ELSE ecom_ind_rank
      END AS ecom_ind_rank,
    CASE
      WHEN ecom_txns_share = 99999
        THEN NULL
      ELSE ecom_txns_share
      END AS ecom_txns_share,
    CASE
      WHEN ecom_ind_rank_share = 99999
        THEN NULL
      ELSE ecom_ind_rank_share
      END AS ecom_ind_rank_share
  from
    wpay_analytics.mmi_area_industry_edit
  where
    area_level = 'country'
  group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21
  ) ind_lga
on 
 cast(store_info.industry as string) = cast(ind_lga.industry_code as string)
)

----------------------------------------
-- wpay store transactions
,
 store_trans_country_card as 
(select 
  amount.Year as Year,
  amount.Month as Month,
  amount.Country as Country,
  --amount.State as State,
  --amount.scheme_card_type as scheme_card_type,
  --amount.is_online as is_online,
  amount.scheme_card_type as scheme_card_type,
  --amount.Store as Store,
  amount.industry as Industry,
  amount.industry_name as industry_name,
  amount.Brand as Brand,
  amount.division as division,
  amount.company as company,
  amount.invoice_group as invoice_group,
  amount.merchant_group as merchant_group,
  amount.merchant_portal_mg as merchant_portal_mg,
  amount.merchant_portal_cg as merchant_portal_cg,
  --amount.Suburb as Suburb,
  amount.trans_amount as trans_amount,
  counts.trans_count as trans_count,
from
(
select
  left(cast(trans.transaction_date as string),4) as Year,
  substring(cast(trans.transaction_date as string),6,2) as Month,
 -- right(cast(trans.transaction_date as string),2) as Day,
  stores.country as Country,
  --trans.state as State,
  --trans.scheme_card_type as scheme_card_type,
  --trans.is_online as is_online,
  trans.scheme_card_type as scheme_card_type,
  --trans.store_id as Store,
  stores.industry as Industry,
  stores.industry_name as industry_name,
  stores.brand as Brand,
  stores.division as division,
  stores.company as company,
  stores.invoice_group as invoice_group,
  stores.merchant_group as merchant_group,
  stores.merchant_portal_mg as merchant_portal_mg,
  stores.merchant_portal_cg as merchant_portal_cg,
  --stores.suburb as Suburb,
  --sum(trans.transaction_count) as trans_count,
  sum(trans.settlement_amount_inc_gst) as trans_amount,
from
  pdh_sf_mep_vw.wpay_all_merchants_daily_fees_summary_hist trans
left join 
  wpay_analytics.ref_store_ind stores
on
  trans.store_id = stores.store_id
where
  (trans.approval_flag = 'A' ) 
  and ((trans.tran_type_description = 'REFUND')
  or (trans.tran_type_description = 'REFUND REVERSAL')
  or (trans.tran_type_description = 'PURCHASE')
  or (trans.tran_type_description = 'PURCHASE REVERSAL')
  or (trans.tran_type_description = 'PURCHASE WITH CASHOUT')
  or (trans.tran_type_description = 'PURCHASE WITH CASHOUT REVERSAL')
  or (trans.tran_type_description = 'REDEMPTION')
  or (trans.tran_type_description = 'REDEMPTION REVERSAL')
  )
group by
  1,2,3,4,5,6,7,8,9,10,11,12,13
) amount
left join
  (
    select
  left(cast(trans.transaction_date as string),4) as Year,
  substring(cast(trans.transaction_date as string),6,2) as Month,
  --right(cast(trans.transaction_date as string),2) as Day,
  stores.country as Country,
  --trans.state as State,
  --trans.scheme_card_type as scheme_card_type,
  --trans.is_online as is_online,
  trans.scheme_card_type as scheme_card_type,
  --trans.store_id as Store,
  stores.industry as Industry,
  stores.brand as Brand,
  stores.division as division,
  stores.company as company,
  stores.invoice_group as invoice_group,
  stores.merchant_group as merchant_group,
  stores.merchant_portal_mg as merchant_portal_mg,
  stores.merchant_portal_cg as merchant_portal_cg,
  --stores.suburb as Suburb,
  --sum(trans.transaction_count) as trans_count,
  (sum(case when trans.tran_type_description in 
  ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL') then transaction_count else 0 end)
  -
  sum(case when trans.tran_type_description in 
  ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL') then transaction_count else 0 end)
  ) as trans_count
  --sum(trans.settlement_amount_inc_gst) as trans_amount,
from
  pdh_sf_mep_vw.wpay_all_merchants_daily_fees_summary_hist trans
left join 
  wpay_analytics.ref_store_ind  stores
on
  trans.store_id = stores.store_id
where
  (trans.approval_flag = 'A' ) 
  -- and ((trans.tran_type_description = 'PURCHASE')
  -- or (trans.tran_type_description = 'PURCHASE REVERSAL')
  -- or (trans.tran_type_description = 'PURCHASE WITH CASHOUT')
  -- or (trans.tran_type_description = 'PURCHASE WITH CASHOUT REVERSAL')
  -- or (trans.tran_type_description = 'REDEMPTION')
  -- or (trans.tran_type_description = 'REDEMPTION REVERSAL')
  -- )
  group by
  1,2,3,4,5,6,7,8,9,10,11,12) counts
on (amount.Year = counts.Year)
   and (amount.Month = counts.Month)
   --and (amount.State = counts.State)
   --and (amount.scheme_card_type = counts.scheme_card_type)
   --and (amount.is_online = counts.is_online)
   and (amount.scheme_card_type = counts.scheme_card_type)
   --and (amount.Store = counts.Store)
   and (amount.Brand = counts.Brand)
   and (amount.division = counts.division)
   and (amount.company = counts.company)
   and (amount.invoice_group = counts.invoice_group)
   and (amount.merchant_group = counts.merchant_group)
   and (amount.merchant_portal_mg = counts.merchant_portal_mg)
   and (amount.merchant_portal_cg = counts.merchant_portal_cg)
   --and (amount.Suburb = counts.Suburb))
)
------------------------------------------------------------------------------------------------------------

, country_internal_card as
(
  select 
    store_trans_country_card.*,
    case when prev_m.prev_Amount <> 0 then
    ((store_trans_country_card.trans_amount - prev_m.prev_Amount)/prev_m.prev_Amount) 
    else 0 end as trans_amount_mom,
    case when prev_y.prev_Amount <> 0 then
    ((store_trans_country_card.trans_amount - prev_y.prev_Amount)/prev_y.prev_Amount) 
    else 0 end as trans_amount_yoy,
    case when cast(prev_m.prev_Count as int) <> 0 then
    ((store_trans_country_card.trans_count - prev_m.prev_Count)/prev_m.prev_Count)  
    else 0 end
    as trans_count_mom,
    case when cast(prev_y.prev_Count as int) <> 0 then
    ((store_trans_country_card.trans_count - prev_y.prev_Count)/prev_y.prev_Count)  
    else 0 end
    as trans_count_yoy,
    prev_m.prev_Amount as trans_amount_LM,
    prev_m.prev_Count as trans_count_LM,
    prev_y.prev_Amount as trans_amount_LY,
    prev_y.prev_Count as trans_count_LY,
    case when cast(store_trans_country_card.trans_count as int) <> 0 then
    (store_trans_country_card.trans_amount/store_trans_country_card.trans_count) 
    else 0 end 
    as trans_value,
    case when cast(prev_m.prev_Count as int) <> 0
    then
    (prev_m.prev_Amount/prev_m.prev_Count)  
    else 0 end 
    as trans_value_LM,
    case when cast(prev_y.prev_Count as int) <> 0
    then
    (prev_y.prev_Amount/prev_y.prev_Count)  
    else 0 end 
    as trans_value_LY
from store_trans_country_card
left join
(
select 
  curr.*,
  prev.Year as prev_year,
  prev.Month as prev_month,
  prev.trans_amount as prev_Amount,
  prev.trans_count as prev_Count
from store_trans_country_card curr
left join 
  store_trans_country_card prev
on  
    (curr.Brand = prev.Brand)
and    (curr.division = prev.division)
   and (curr.company = prev.company)
   and (curr.invoice_group = prev.invoice_group)
   and (curr.merchant_group = prev.merchant_group)
   and (curr.merchant_portal_mg = prev.merchant_portal_mg)
   and (curr.merchant_portal_cg = prev.merchant_portal_cg)
   and curr.Month = prev.Month
   and (cast(curr.Year as int)-1) = cast(prev.Year as int)
   --and curr.is_online = prev.is_online
   and curr.scheme_card_type = prev.scheme_card_type
order by curr.Year,curr.Month
) prev_y
on
    (store_trans_country_card.Brand = prev_y.Brand)
and    (store_trans_country_card.division = prev_y.division)
   and (store_trans_country_card.company = prev_y.company)
   and (store_trans_country_card.invoice_group = prev_y.invoice_group)
   and (store_trans_country_card.merchant_group = prev_y.merchant_group)
   and (store_trans_country_card.merchant_portal_mg = prev_y.merchant_portal_mg)
   and (store_trans_country_card.merchant_portal_cg = prev_y.merchant_portal_cg)
  and store_trans_country_card.Year = prev_y.Year
  and store_trans_country_card.Month = prev_y.Month
  --and store_trans_country_card.is_online = prev_y.is_online
   and store_trans_country_card.scheme_card_type = prev_y.scheme_card_type
left join
(
select 
  curr.*,
  prev.trans_amount as prev_Amount,
  prev.trans_count as prev_Count
from store_trans_country_card curr
left join 
  store_trans_country_card prev
on 
    (curr.Brand = prev.Brand)
and    (curr.division = prev.division)
   and (curr.company = prev.company)
   and (curr.invoice_group = prev.invoice_group)
   and (curr.merchant_group = prev.merchant_group)
   and (curr.merchant_portal_mg = prev.merchant_portal_mg)
   and (curr.merchant_portal_cg = prev.merchant_portal_cg)
    and left(cast(date_add(cast(concat(concat(concat(concat(curr.Year,'-'),curr.Month),'-'),'01') as date), interval -1 month) as string),4) = cast(prev.Year as string)
    and substring(cast(date_add(cast(concat(concat(concat(concat(curr.Year,'-'),curr.Month),'-'),'01') as date), interval -1 month) as string),6,2)  = prev.Month
  -- and curr.is_online = prev.is_online
   and curr.scheme_card_type = prev.scheme_card_type
order by curr.Year,curr.Month
) prev_m
on
    (store_trans_country_card.Brand = prev_m.Brand)
and    (store_trans_country_card.division = prev_m.division)
   and (store_trans_country_card.company = prev_m.company)
   and (store_trans_country_card.invoice_group = prev_m.invoice_group)
   and (store_trans_country_card.merchant_group = prev_m.merchant_group)
   and (store_trans_country_card.merchant_portal_mg = prev_m.merchant_portal_mg)
   and (store_trans_country_card.merchant_portal_cg = prev_m.merchant_portal_cg)
  and store_trans_country_card.Year = prev_m.Year
  and store_trans_country_card.Month = prev_m.Month
 -- and store_trans_country_card.is_online = prev_m.is_online
   and store_trans_country_card.scheme_card_type = prev_m.scheme_card_type
order by store_trans_country_card.Year,store_trans_country_card.Month
)

-----------------------------------------------------------------------------------------------------------

--- adding only LGA to wpay_trans

, wpay_country_card as
 (select 
    country_internal_card.*,
    final_country_card.area,
    --final_country_card.industry_name,
    final_country_card.spend_growth_yoy,
    final_country_card.spend_growth_mom,
    final_country_card.txn_growth_yoy,
    final_country_card.txn_growth_mom,
    final_country_card.spend_density_growth_yoy,
    final_country_card.spend_density_growth_mom,
    final_country_card.avg_ticket,
    final_country_card.b_avg_ticket_LM,
    final_country_card.b_avg_ticket_LY,
    final_country_card.b_avg_ticket_mom,
    final_country_card.b_avg_ticket_yoy,
    final_country_card.avg_visits_per_cust,
    final_country_card.avg_spend_per_cust,
    final_country_card.ecom_ind_rank,
    final_country_card.ecom_txns_share,
    final_country_card.ecom_ind_rank_share,
    --final_lga.Benchmark
  from 
    country_internal_card
  left join 
    final_country_card
  on 
    (country_internal_card.Brand = final_country_card.Brand)
    and (country_internal_card.division = final_country_card.division)
    and (country_internal_card.company = final_country_card.company)
    and (country_internal_card.invoice_group = final_country_card.invoice_group)
    and (country_internal_card.merchant_group = final_country_card.merchant_group)
    and (country_internal_card.merchant_portal_mg = final_country_card.merchant_portal_mg)
    and (country_internal_card.merchant_portal_cg = final_country_card.merchant_portal_cg)
    and (country_internal_card.Year = final_country_card.Year)
    --and (country_internal.Month = final_country.Month)
    and (cast(country_internal_card.Month as int) = cast(final_country_card.Month as int))
  --where country_internal_card.Year ='2022'
  --    and country_internal_card.Month ='07'
  --order by final_lga.Benchmark
 )

------------------------------------------------------------
---- adding benchmark 
,
country_bench_card as 
(
  select *,
  'Country' as Benchmark
  from wpay_country_card
)


--------------------------------------------
--- going over LGA and filling nulls with State
, country_benchmark_card as
(
  select
    'Country' as Aggregation_Level,
    'All' as store_id,
    'All' as store_status,
    country_bench_card.country,
    'All' as state,
    country_bench_card.division,
    country_bench_card.brand,
    country_bench_card.industry,
    'All' as site_name,
    'All' as suburb,
    'All' as postcode,
    'All' as caid,
    --'All' as is_closed,
    country_bench_card.company,
    country_bench_card.invoice_group,
    country_bench_card.merchant_group,
    country_bench_card.merchant_portal_mg,
    country_bench_card.merchant_portal_cg,
    'All' as is_online,
    'All' as scheme,
    cast(country_bench_card.scheme_card_type as string) as card_type,
    'All' as LGA,
    'All' as LGA_id,
    country_bench_card.area,
    country_bench_card.industry_name,
    --final_lga.industry_code,
    --wpay_LGA.Period,
    concat(country_bench_card.Year,concat('-',country_bench_card.Month)) as Period,
    country_bench_card.Year,
    country_bench_card.Month,
    country_bench_card.trans_amount as w_trans_amount,
    country_bench_card.trans_count as w_trans_count,
    country_bench_card.trans_value as w_trans_value,
    country_bench_card.trans_amount_LM as w_trans_amount_LM,
    country_bench_card.trans_count_LM as w_trans_count_LM,
    country_bench_card.trans_value_LM as w_trans_value_LM,
    country_bench_card.trans_amount_LY as w_trans_amount_LY,
    country_bench_card.trans_count_LY as w_trans_count_LY,
    country_bench_card.trans_value_LY as w_trans_value_LY,
    country_bench_card.trans_amount_mom as w_trans_amount_mom,
    country_bench_card.trans_amount_yoy as w_trans_amount_yoy,
    country_bench_card.trans_count_mom as w_trans_count_mom,
    country_bench_card.trans_count_yoy as w_trans_count_yoy,
    case when cast(country_bench_card.trans_value_LM as int) <> 0
    then ((country_bench_card.trans_value - country_bench_card.trans_value_LM)/country_bench_card.trans_value_LM)  
    else 0 end 
    as w_trans_value_mom,
    case when cast(country_bench_card.trans_value_LY as int) <> 0
    then ((country_bench_card.trans_value - country_bench_card.trans_value_LY)/country_bench_card.trans_value_LY)  
    else 0 end 
    as w_trans_value_yoy,
    case when cust_info.unique_customer_count <> 0 then
    country_bench_card.trans_count/cust_info.unique_customer_count 
    else 0 end as avg_cust_visits,
    case when cust_info.unique_customer_count <> 0 then
    country_bench_card.trans_amount/cust_info.unique_customer_count 
    else 0 end as avg_cust_spend,
    cust_info.unique_card_count as unique_cust,
    cust_info.unique_customer_count as unique_card,
    country_bench_card.spend_growth_yoy as b_spend_growth_yoy,
    country_bench_card.spend_growth_mom as b_spend_growth_mom,
    country_bench_card.txn_growth_yoy as b_txn_growth_yoy,
    country_bench_card.txn_growth_mom as b_txn_growth_mom,
    country_bench_card.spend_density_growth_yoy as b_spend_density_growth_yoy,
    country_bench_card.spend_density_growth_mom as b_spend_density_growth_mom,
    country_bench_card.avg_ticket as b_avg_ticket,
    country_bench_card.b_avg_ticket_LM as b_avg_ticket_LM,
    country_bench_card.b_avg_ticket_LY as b_avg_ticket_LY,
    country_bench_card.b_avg_ticket_mom as b_avg_ticket_mom,
    country_bench_card.b_avg_ticket_yoy as b_avg_ticket_yoy,
    country_bench_card.avg_visits_per_cust as b_avg_visits_per_cust,
    country_bench_card.avg_spend_per_cust as b_avg_spend_per_cust,
    country_bench_card.ecom_ind_rank as b_ecom_ind_rank,
    country_bench_card.ecom_txns_share as b_ecom_txns_share,
    country_bench_card.ecom_ind_rank_share as b_ecom_ind_rank_share,
    --country_bench_card.Benchmark,
    country_bench_card.industry_name as Benchmark,
    country_bench_card.Benchmark as Benchmark_Location,
    'External' as Benchmark_lvl
  from 
    country_bench_card
  left join
    (
  select 
      m.company,
      m.invoice_group,    
      m.merchant_group,
      m.division, 
      m.brand, 
      m.merchant_portal_mg,
      m.merchant_portal_cg,   
      --m.store_id,   
      --m.site_name,    
     -- m.suburb,   
      --m.state,    
      --m.postcode,
      m.country,
      --m.is_online,
      --fee.calc_scheme as scheme,
      fee.calc_card_type as card_type,
      left(cast(fee.switch_tran_date as string),7) Transaction_Date,
      -- sum(case when fee.calc_tran_type_description in
      -- ('REFUND','REFUND REVERSAL','PURCHASE','PURCHASE REVERSAL','PURCHASE WITH CASHOUT','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION','REDEMPTION REVERSAL') then fee.transaction_total else 0 end)/100 as trans_amount,
      -- (sum(case when fee.calc_tran_type_description in
      -- ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL') then 1 else 0 end)
      -- -
      -- sum(case when fee.calc_tran_type_description in 
      -- ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL') then 1 else 0 end)
      -- ) as trans_count,
      count(distinct fee.card_id) unique_card_count,
      count(distinct fee.card_id) unique_customer_count
      -- (count(1) / count(distinct fee.card_id)) frequency_of_visits,
      -- (  sum(case when fee.calc_tran_type_description in
      -- ('REFUND','REFUND REVERSAL','PURCHASE','PURCHASE REVERSAL','PURCHASE WITH CASHOUT','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION','REDEMPTION REVERSAL') then fee.transaction_total else 0 end) 
      -- / (100*count(distinct fee.card_id))) spend_per_card_holder 
      from wpay_analytics.wpay_billing_engine_tran_dtl fee  
      inner join
      `pdh_ref_ds.ref_store_details` m 
      on LEFT(FEE.net_term_id, 5) = M.store_id
      --inner join
      --wpay_analytics.wpay_card_identity_model cim
      --on trim(cim.scdr_token) = trim(fee.scdr_token)
      where fee.calc_approval_flag = 'A'
	  and left(cast(fee.tstamp_trans as string),7) > '2022-07'
and fee.calc_tran_type_description in ('REFUND','REFUND REVERSAL','PURCHASE','PURCHASE REVERSAL','PURCHASE WITH CASHOUT','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION','REDEMPTION REVERSAL')
      group by 1,2,3,4,5,6,7,8,9,10
      ) cust_info
  on cust_info.company = country_bench_card.company
    and cust_info.merchant_group = country_bench_card.merchant_group
    and cust_info.division = country_bench_card.division
    and cust_info.country = country_bench_card.country
    and cust_info.card_type = country_bench_card.scheme_card_type
    and cust_info.invoice_group = country_bench_card.invoice_group
    and cust_info.brand = country_bench_card.brand
    and cust_info.merchant_portal_mg = country_bench_card.merchant_portal_mg
    and cust_info.merchant_portal_cg = country_bench_card.merchant_portal_cg
    and concat(country_bench_card.Year,concat('-',country_bench_card.Month)) = cust_info.Transaction_Date
)
-- end
select 
  *
  from lga_benchmark_card
union all
select 
  *
  from state_benchmark_card
union all
select 
  *
  from country_benchmark_card

);

-------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------
-- FINAL EXTERNAL TABLE (union of store,state,country)
create or replace table pdh_staging_ds.temp_external as (
  select 
    Aggregation_Level,					
    store_id,
    store_status,
    country,
    state,
    division,
    brand,
    industry,
    site_name,
    suburb,
    postcode,
    caid,
    company,
    invoice_group,
    merchant_group,
    merchant_portal_mg,
    merchant_portal_cg,
    is_online,
    scheme,
    card_type,
    LGA,
    LGA_id,
    area,
    industry_name,
    Period,
    Year,
    Month,
    w_trans_amount,
    w_trans_count,				
    w_trans_value as w_avg_trans_value,				
    w_trans_amount_LM,		
    w_trans_count_LM,				
    w_trans_value_LM as w_avg_trans_value_LM,				
    w_trans_amount_LY,				
    w_trans_count_LY,				
    w_trans_value_LY as w_avg_trans_value_LY,					
    w_trans_amount_mom,					
    w_trans_amount_yoy,					
    w_trans_count_mom,					
    w_trans_count_yoy,					
    w_trans_value_mom as w_avg_trans_value_mom,					
    w_trans_value_yoy as w_avg_trans_value_yoy,					
    avg_cust_visits as w_avg_cust_visits,					
    avg_cust_spend as w_avg_cust_trans_value,	
    unique_cust as w_unique_cust,
    unique_card as w_unique_card,
    b_spend_growth_yoy as b_trans_amount_yoy,		
    b_spend_growth_mom as b_trans_amount_mom,					
    b_txn_growth_yoy as b_trans_count_yoy,					
    b_txn_growth_mom as b_trans_count_mom,					
    b_spend_density_growth_yoy,					
    b_spend_density_growth_mom,					
    b_avg_ticket as b_avg_trans_value,					
    b_avg_ticket_LM as b_avg_trans_value_LM,					
    b_avg_ticket_LY as b_avg_trans_value_LY,					
    b_avg_ticket_mom as b_avg_trans_value_mom,					
    b_avg_ticket_yoy as b_avg_trans_value_yoy,					
    b_avg_visits_per_cust as b_avg_cust_visits,					
    b_avg_spend_per_cust as b_avg_cust_trans_value,					
    b_ecom_ind_rank,					
    b_ecom_txns_share,					
    b_ecom_ind_rank_share,					
    Benchmark,
    Benchmark_Location,					
    Benchmark_lvl 
  from
  (
    select *
    from pdh_staging_ds.temp_location
    union all
    select *
    from pdh_staging_ds.temp_filters_combo
    union all
    select *
    from pdh_staging_ds.temp_online
    union all
    select *
    from pdh_staging_ds.temp_scheme
    union all
    select *
    from pdh_staging_ds.temp_card
  )
  where store_id is not null
)
;
-----------------------------------------------------------------------------------------------------------------
--INTERNAL BENCHMARKS
-------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------
-- BASE TABLE (ALL)
create or replace table pdh_staging_ds.temp_internal as 
(
-------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------
-- Country

with w_internal_country as 
(select 
  base.Aggregation_Level,					
  base.store_id,
  base.store_status,
  base.country,
  base.state,
  base.division,
  base.brand,
  base.industry,
  base.site_name,
  base.suburb,
  base.postcode,
  base.caid,
  base.company,
  base.invoice_group,
  base.merchant_group,
  base.merchant_portal_mg,
  base.merchant_portal_cg,
  base.is_online,
  base.scheme,
  base.card_type,
  base.LGA,
  base.LGA_id,
  base.area,
  base.industry_name,
  base.Period,
  base.Year,
  base.Month,
  base.w_trans_amount,
  base.w_trans_count,				
  base.w_avg_trans_value as w_avg_trans_value,				
  base.w_trans_amount_LM,		
  base.w_trans_count_LM,				
  base.w_avg_trans_value_LM as w_avg_trans_value_LM,				
  base.w_trans_amount_LY,				
  base.w_trans_count_LY,				
  base.w_avg_trans_value_LY as w_avg_trans_value_LY,					
  base.w_trans_amount_mom,					
  base.w_trans_amount_yoy,					
  base.w_trans_count_mom,					
  base.w_trans_count_yoy,					
  base.w_avg_trans_value_mom as w_avg_trans_value_mom,					
  base.w_avg_trans_value_yoy as w_avg_trans_value_yoy,					
  base.w_avg_cust_visits as w_avg_cust_visits,					
  base.w_avg_cust_trans_value as w_avg_cust_trans_value,
  base.w_unique_cust as w_unique_cust,
  base.w_unique_card as w_unique_card,					
  internal_lga.w_trans_amount_tot_yoy as b_trans_amount_yoy,		
  internal_lga.w_trans_amount_tot_mom as b_trans_amount_mom,					
  internal_lga.w_trans_count_tot_yoy as b_trans_count_yoy,					
  internal_lga.w_trans_count_tot_mom as b_trans_count_mom,					
  0 as b_spend_density_growth_yoy,					
  0 as b_spend_density_growth_mom,					
  internal_lga.w_trans_value_tot as b_avg_trans_value,					
  internal_lga.w_trans_value_LM_tot as b_avg_trans_value_LM,					
  internal_lga.w_trans_value_LY_tot as b_avg_trans_value_LY,					
  internal_lga.w_trans_value_tot_mom as b_avg_trans_value_mom,					
  internal_lga.w_trans_value_tot_yoy as b_avg_trans_value_yoy,					
  0 as b_avg_cust_visits,					
  0 as b_avg_cust_trans_value,					
  0 as b_ecom_ind_rank,					
  0 as b_ecom_txns_share,					
  0 as b_ecom_ind_rank_share,					
  --'Country' as Benchmark,
  base.merchant_group as Benchmark,			
  'Country' as Benchmark_Location,	
  'Internal' as Benchmark_lvl
from
  pdh_staging_ds.temp_external base
inner join
(
select
  store_status,
  country,
  is_online,
  scheme,
  card_type,
  --state,
  division,
  brand,
  merchant_portal_mg,
  merchant_portal_cg,
  company,
  invoice_group,
  merchant_group,
  --LGA_id,
  Period,
  Year,
  Month,
  sum(w_trans_amount) as w_trans_amount_tot,
  sum(w_trans_count) as w_trans_count_tot,

  case when sum(w_trans_count) <> 0
  then (sum(w_trans_amount)/sum(w_trans_count))
  else 0 end
  as w_trans_value_tot,

  sum(w_trans_amount_LM) as w_trans_amount_LM_tot,
  sum(w_trans_count_LM) as w_trans_count_LM_tot,

  case when sum(w_trans_count_LM) <> 0
  then (sum(w_trans_amount_LM)/sum(w_trans_count_LM))
  else 0 end
  as w_trans_value_LM_tot,

  sum(w_trans_amount_LY) as w_trans_amount_LY_tot,
  sum(w_trans_count_LY) as w_trans_count_LY_tot,

  case when sum(w_trans_count_LY) <> 0
  then (sum(w_trans_amount_LY)/sum(w_trans_count_LY))
  else 0 end
  as w_trans_value_LY_tot,

  case when sum(w_trans_amount_LM) <> 0
  then ((sum(w_trans_amount) - sum(w_trans_amount_LM))/(sum(w_trans_amount_LM)))
  else 0 end 
  as w_trans_amount_tot_mom,

  case when sum(w_trans_count_LM) <> 0
  then ((sum(w_trans_count) - sum(w_trans_count_LM))/(sum(w_trans_count_LM)))
  else 0 end 
  as w_trans_count_tot_mom,

  case when ((sum(w_trans_count) <>0 ) and (sum(w_trans_count_LM) <>0))
  then (((sum(w_trans_amount)/sum(w_trans_count)) - (sum(w_trans_amount_LM)/sum(w_trans_count_LM)))/((sum(w_trans_amount_LM)/sum(w_trans_count_LM))))
  else 0 end 
  as w_trans_value_tot_mom,

  case when sum(w_trans_amount_LY) <> 0
  then ((sum(w_trans_amount) - sum(w_trans_amount_LY))/(sum(w_trans_amount_LY)))
  else 0 end 
  as w_trans_amount_tot_yoy,

  case when sum(w_trans_count_LY) <> 0
  then ((sum(w_trans_count) - sum(w_trans_count_LY))/(sum(w_trans_count_LY)))
  else 0 end 
  as w_trans_count_tot_yoy,

  case when ((sum(w_trans_count) <>0 ) and (sum(w_trans_count_LY) <>0))
  then (((sum(w_trans_amount)/sum(w_trans_count)) - (sum(w_trans_amount_LY)/sum(w_trans_count_LY)))/((sum(w_trans_amount_LY)/sum(w_trans_count_LY))))
  else 0 end 
  as w_trans_value_tot_yoy
from
  pdh_staging_ds.temp_external
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15) internal_lga
on 
  base.store_status = internal_lga.store_status
  and base.country = internal_lga.country
  and base.is_online = internal_lga.is_online
  and base.scheme = internal_lga.scheme
  and base.card_type = internal_lga.card_type
  --and base.state = internal_lga.state
  and base.division = internal_lga.division
  and base.brand = internal_lga.brand
  and (base.merchant_portal_mg = internal_lga.merchant_portal_mg)
  and (base.merchant_portal_cg = internal_lga.merchant_portal_cg)
  and base.company = internal_lga.company
  and base.invoice_group = internal_lga.invoice_group
  and base.merchant_group = internal_lga.merchant_group
  --and base.LGA_id = internal_lga.LGA_id
  and base.Period = internal_lga.Period
  and base.Year = internal_lga.Year
  and base.Month = internal_lga.Month

where base.Aggregation_Level = 'State'
)

-------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------
-- STATE
,
 w_internal_state as 
(select 
  base.Aggregation_Level,					
  base.store_id,
  base.store_status,
  base.country,
  base.state,
  base.division,
  base.brand,
  base.industry,
  base.site_name,
  base.suburb,
  base.postcode,
  base.caid,
  base.company,
  base.invoice_group,
  base.merchant_group,
  base.merchant_portal_mg,
  base.merchant_portal_cg,
  base.is_online,
  base.scheme,
  base.card_type,
  base.LGA,
  base.LGA_id,
  base.area,
  base.industry_name,
  base.Period,
  base.Year,
  base.Month,
  base.w_trans_amount,
  base.w_trans_count,				
  base.w_avg_trans_value as w_avg_trans_value,				
  base.w_trans_amount_LM,		
  base.w_trans_count_LM,				
  base.w_avg_trans_value_LM as w_avg_trans_value_LM,				
  base.w_trans_amount_LY,				
  base.w_trans_count_LY,				
  base.w_avg_trans_value_LY as w_avg_trans_value_LY,					
  base.w_trans_amount_mom,					
  base.w_trans_amount_yoy,					
  base.w_trans_count_mom,					
  base.w_trans_count_yoy,					
  base.w_avg_trans_value_mom as w_avg_trans_value_mom,					
  base.w_avg_trans_value_yoy as w_avg_trans_value_yoy,					
  base.w_avg_cust_visits as w_avg_cust_visits,					
  base.w_avg_cust_trans_value as w_avg_cust_trans_value,
  base.w_unique_cust as w_unique_cust,
  base.w_unique_card as w_unique_card,				
  internal_lga.w_trans_amount_tot_yoy as b_trans_amount_yoy,		
  internal_lga.w_trans_amount_tot_mom as b_trans_amount_mom,					
  internal_lga.w_trans_count_tot_yoy as b_trans_count_yoy,					
  internal_lga.w_trans_count_tot_mom as b_trans_count_mom,					
  0 as b_spend_density_growth_yoy,					
  0 as b_spend_density_growth_mom,					
  internal_lga.w_trans_value_tot as b_avg_trans_value,					
  internal_lga.w_trans_value_LM_tot as b_avg_trans_value_LM,					
  internal_lga.w_trans_value_LY_tot as b_avg_trans_value_LY,					
  internal_lga.w_trans_value_tot_mom as b_avg_trans_value_mom,					
  internal_lga.w_trans_value_tot_yoy as b_avg_trans_value_yoy,					
  0 as b_avg_cust_visits,					
  0 as b_avg_cust_trans_value,					
  0 as b_ecom_ind_rank,					
  0 as b_ecom_txns_share,					
  0 as b_ecom_ind_rank_share,					
  --'State' as Benchmark,
  base.merchant_group as Benchmark,	
  'State' as Benchmark_Location,					
  'Internal' as Benchmark_lvl
from
  pdh_staging_ds.temp_external base
inner join
(
select
  store_status,
  country,
  is_online,
  scheme,
  card_type,
  state,
  division,
  brand,
  merchant_portal_mg,
  merchant_portal_cg,
  company,
  invoice_group,
  merchant_group,
  --LGA_id,
  Period,
  Year,
  Month,
  sum(w_trans_amount) as w_trans_amount_tot,
  sum(w_trans_count) as w_trans_count_tot,

  case when sum(w_trans_count) <> 0
  then (sum(w_trans_amount)/sum(w_trans_count))
  else 0 end
  as w_trans_value_tot,

  sum(w_trans_amount_LM) as w_trans_amount_LM_tot,
  sum(w_trans_count_LM) as w_trans_count_LM_tot,

  case when sum(w_trans_count_LM) <> 0
  then (sum(w_trans_amount_LM)/sum(w_trans_count_LM))
  else 0 end
  as w_trans_value_LM_tot,

  sum(w_trans_amount_LY) as w_trans_amount_LY_tot,
  sum(w_trans_count_LY) as w_trans_count_LY_tot,

  case when sum(w_trans_count_LY) <> 0
  then (sum(w_trans_amount_LY)/sum(w_trans_count_LY))
  else 0 end
  as w_trans_value_LY_tot,

  case when sum(w_trans_amount_LM) <> 0
  then ((sum(w_trans_amount) - sum(w_trans_amount_LM))/(sum(w_trans_amount_LM)))
  else 0 end 
  as w_trans_amount_tot_mom,

  case when sum(w_trans_count_LM) <> 0
  then ((sum(w_trans_count) - sum(w_trans_count_LM))/(sum(w_trans_count_LM)))
  else 0 end 
  as w_trans_count_tot_mom,

  case when ((sum(w_trans_count) <>0 ) and (sum(w_trans_count_LM) <>0))
  then (((sum(w_trans_amount)/sum(w_trans_count)) - (sum(w_trans_amount_LM)/sum(w_trans_count_LM)))/((sum(w_trans_amount_LM)/sum(w_trans_count_LM))))
  else 0 end 
  as w_trans_value_tot_mom,

  case when sum(w_trans_amount_LY) <> 0
  then ((sum(w_trans_amount) - sum(w_trans_amount_LY))/(sum(w_trans_amount_LY)))
  else 0 end 
  as w_trans_amount_tot_yoy,

  case when sum(w_trans_count_LY) <> 0
  then ((sum(w_trans_count) - sum(w_trans_count_LY))/(sum(w_trans_count_LY)))
  else 0 end 
  as w_trans_count_tot_yoy,

  case when ((sum(w_trans_count) <>0 ) and (sum(w_trans_count_LY) <>0))
  then (((sum(w_trans_amount)/sum(w_trans_count)) - (sum(w_trans_amount_LY)/sum(w_trans_count_LY)))/((sum(w_trans_amount_LY)/sum(w_trans_count_LY))))
  else 0 end 
  as w_trans_value_tot_yoy
from
  pdh_staging_ds.temp_external
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16) internal_lga
on 
  base.store_status = internal_lga.store_status
  and base.country = internal_lga.country
  and base.is_online = internal_lga.is_online
  and base.scheme = internal_lga.scheme
  and base.card_type = internal_lga.card_type
  and base.state = internal_lga.state
  and base.division = internal_lga.division
  and base.brand = internal_lga.brand
  and (base.merchant_portal_mg = internal_lga.merchant_portal_mg)
   and (base.merchant_portal_cg = internal_lga.merchant_portal_cg)
  and base.company = internal_lga.company
  and base.invoice_group = internal_lga.invoice_group
  and base.merchant_group = internal_lga.merchant_group
  --and base.LGA_id = internal_lga.LGA_id
  and base.Period = internal_lga.Period
  and base.Year = internal_lga.Year
  and base.Month = internal_lga.Month

where base.Aggregation_Level = 'Store'
)

-------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------

  select * from w_internal_state
  union all
  select * from w_internal_country
);
-------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------
-- END
--------------------------------------------------------------------------------------------------------------

---------------------------------------------------------------------------------------------------
--STABLE STORES

create or replace table wpay_analytics.ref_store_stable as
(
 with active as 
  (
  select 
    store_id,
    left(cast(transaction_date as string),7) as active_months
  from
    pdh_sf_mep_vw.wpay_all_merchants_daily_fees_summary_hist
  --where left(cast(transaction_date as string),7) > left(cast(date_add(current_date(), interval -1 year) as string),7)
  group by 1,2
  order by 1,2
  ) 
,
cal as 
(
select
  --concat(left(cast(transaction_date as string),7),'-01') as cal_months
  left(cast(transaction_date as string),7) as cal_months
from
  pdh_sf_mep_vw.wpay_all_merchants_daily_fees_summary_hist
group by 1
)


select
  *
from
  (
  select 
    output.cal_months,
    output.store_id,
    count(distinct active_months) as active_months
  from
  (
  select
    cal.cal_months,
    active.store_id,
    active.active_months
  from
    cal
  left join 
    active
  on cal.cal_months > active.active_months
  order by 1,2,3
  ) output
  group by 1,2
  )
where active_months >= 13
)
;
---------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------
-- FINAL EXTERNAL TABLE (union of store,state,country)
create or replace table pdh_staging_ds.temp_external_stable as (
  
with store_stable as
(
select 
  Aggregation_Level,
  store_id,
  store_status,
  country,
  state,
  division,
  brand,
  industry,
  site_name,
  suburb,
  postcode,
  caid,
  company,
  invoice_group,
  merchant_group,
  merchant_portal_mg,
  merchant_portal_cg,
  is_online,
  scheme,
  card_type,
  LGA,
  LGA_id,
  area,
  industry_name,
  Period,
  Year,
  Month,
  w_trans_amount,
  w_trans_count,
  w_avg_trans_value,
  w_trans_amount_LM,
  w_trans_count_LM,
  w_avg_trans_value_LM,
  w_trans_amount_LY,
  w_trans_count_LY,
  w_avg_trans_value_LY,
  w_trans_amount_mom,
  w_trans_amount_yoy,
  w_trans_count_mom,
  w_trans_count_yoy,
  w_avg_trans_value_mom,
  w_avg_trans_value_yoy,

  w_avg_cust_visits,
  w_avg_cust_trans_value,
  w_unique_cust,
  w_unique_card,

  b_trans_amount_yoy,
  b_trans_amount_mom,
  b_trans_count_yoy,
  b_trans_count_mom,
  b_spend_density_growth_yoy,
  b_spend_density_growth_mom,
  b_avg_trans_value,
  b_avg_trans_value_LM,
  b_avg_trans_value_LY,
  b_avg_trans_value_mom,
  b_avg_trans_value_yoy,
  b_avg_cust_visits,
  b_avg_cust_trans_value,
  b_ecom_ind_rank,
  b_ecom_txns_share,
  b_ecom_ind_rank_share,
  Benchmark,
  Benchmark_Location,
  Benchmark_lvl
from
  (
  select
    original.*
  from 
    pdh_staging_ds.temp_external original
  inner join
    wpay_analytics.ref_store_stable stores
  on
    original.store_id = stores.store_id
    and original.Period = stores.cal_months
  ) edit
)
,

-- STATE

state_stable as
(
 select 
  'State' as Aggregation_Level,
  'All' as store_id,
  'Stable' as store_status,
  base.country,
  base.state,
  base.division,
  base.brand,
  base.industry,
  'All' as site_name,
  'All' as suburb,
  'All' as postcode,
  'All' as caid,
  base.company,
  base.invoice_group,
  base.merchant_group,
  base.merchant_portal_mg,
  base.merchant_portal_cg,
  base.is_online,
  base.scheme,
  base.card_type,
  'All' as LGA,
  'All' as LGA_id,
  ind_lga.area,
  base.industry_name,
  base.Period,
  base.Year,
  base.Month,
  base.w_trans_amount,
  base.w_trans_count,
  base.w_avg_trans_value,
  base.w_trans_amount_LM,
  base.w_trans_count_LM,
  base.w_avg_trans_value_LM,
  base.w_trans_amount_LY,
  base.w_trans_count_LY,
  base.w_avg_trans_value_LY,
  base.w_trans_amount_mom,
  base.w_trans_amount_yoy,
  base.w_trans_count_mom,
  base.w_trans_count_yoy,
  base.w_avg_trans_value_mom,
  base.w_avg_trans_value_yoy,

    case when cust_info.w_unique_cust <> 0 
    then base.w_trans_count/cust_info.w_unique_cust
    else 0
    end as w_avg_cust_visits,

    case when cust_info.w_unique_cust <> 0 
    then base.w_trans_amount/cust_info.w_unique_cust
    else 0
    end as w_avg_cust_trans_value,

  cust_info.w_unique_cust,
  cust_info.w_unique_card,

  ind_lga.b_trans_amount_yoy,
  ind_lga.b_trans_amount_mom,
  ind_lga.b_trans_count_yoy,
  ind_lga.b_trans_count_mom,
  ind_lga.b_spend_density_growth_yoy,
  ind_lga.b_spend_density_growth_mom,
  ind_lga.b_avg_trans_value,
  ind_lga.b_avg_trans_value_LM,
  ind_lga.b_avg_trans_value_LY,
  ind_lga.b_avg_trans_value_mom,
  ind_lga.b_avg_trans_value_yoy,
  ind_lga.b_avg_cust_visits,
  ind_lga.b_avg_cust_trans_value,
  ind_lga.b_ecom_ind_rank,
  ind_lga.b_ecom_txns_share,
  ind_lga.b_ecom_ind_rank_share,
  --base.industry_name as Benchmark,
  --Benchmark_Location,
  'External' as Benchmark_lvl
 from
  (
  select 
    country,
    state,
    division,
    brand,
    industry,
    company,
    invoice_group,
    merchant_group,
    merchant_portal_mg,
    merchant_portal_cg,
    is_online,
    scheme,
    card_type,
    industry_name,
    Period,
    Year,
    Month,
    sum(w_trans_amount) as w_trans_amount,
    sum(w_trans_count) as w_trans_count,

    case when sum(w_trans_count) <> 0 
    then
    sum(w_trans_amount)/sum(w_trans_count)
    else 0
    end as w_avg_trans_value,

    sum(w_trans_amount_LM) as w_trans_amount_LM,
    sum(w_trans_count_LM) as w_trans_count_LM,

    case when sum(w_trans_count_LM) <> 0 
    then
    sum(w_trans_amount_LM)/sum(w_trans_count_LM) 
    else 0 
    end as w_avg_trans_value_LM,

    sum(w_trans_amount_LY) as w_trans_amount_LY,
    sum(w_trans_count_LY) as w_trans_count_LY,

    case when sum(w_trans_count_LY) <> 0 
    then
    sum(w_trans_amount_LY)/sum(w_trans_count_LY) 
    else 0 
    end as w_avg_trans_value_LY,

    case when sum(w_trans_amount_LM) <> 0
    then ((sum(w_trans_amount)-sum(w_trans_amount_LM))/(sum(w_trans_amount_LM)))
    else 0
    end as w_trans_amount_mom,

    case when sum(w_trans_amount_LY) <> 0
    then ((sum(w_trans_amount)-sum(w_trans_amount_LY))/(sum(w_trans_amount_LY)))
    else 0
    end as w_trans_amount_yoy,

    case when sum(w_trans_count_LM) <> 0
    then ((sum(w_trans_count)-sum(w_trans_count_LM))/(sum(w_trans_count_LM)))
    else 0
    end as w_trans_count_mom,

    case when sum(w_trans_count_LY) <> 0
    then ((sum(w_trans_count)-sum(w_trans_count_LY))/(sum(w_trans_count_LY)))
    else 0
    end as w_trans_count_yoy,

    case when ((sum(w_trans_amount_LM) <> 0) and (sum(w_trans_count) <> 0) and (sum(w_trans_count_LM) <> 0))
    then (((sum(w_trans_amount)/sum(w_trans_count))-(sum(w_trans_amount_LM)/sum(w_trans_count_LM)))/(sum(w_trans_amount_LM)/sum(w_trans_count_LM)))
    else 0
    end as w_avg_trans_value_mom,

    case when ((sum(w_trans_count_LY) <> 0) and (sum(w_trans_count) <> 0) and (sum(w_trans_count_LY) <> 0))
    then (((sum(w_trans_amount)/sum(w_trans_count))-(sum(w_trans_amount_LY)/sum(w_trans_count_LY)))/(sum(w_trans_amount_LY)/sum(w_trans_count_LY)))
    else 0
    end as w_avg_trans_value_yoy,
    
  from
    store_stable
  group by
  1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
  ) base
left join
  (
  select 
    area,
    period,
    --industry_name,
    industry_code,
    left(period,4) as Year,
    right(period,2) as Month,
    key.State as state_mmi,
    CASE 
      WHEN spend_growth_yoy = 99999
        THEN NULL
      ELSE spend_growth_yoy
      END AS b_trans_amount_yoy,
    CASE 
      WHEN spend_growth_mom = 99999
        THEN NULL
      ELSE spend_growth_mom
      END AS b_trans_amount_mom,
    CASE
      WHEN txn_growth_yoy = 99999
        THEN NULL
      ELSE txn_growth_yoy
      END AS b_trans_count_yoy,
    CASE
      WHEN txn_growth_mom = 99999
        THEN NULL
      ELSE txn_growth_mom
      END AS b_trans_count_mom,
    CASE
      WHEN spend_density_growth_yoy = 99999
        THEN NULL
      ELSE spend_density_growth_yoy
      END AS b_spend_density_growth_yoy,
    CASE
      WHEN spend_density_growth_mom = 99999
        THEN NULL
      ELSE spend_density_growth_mom
      END AS b_spend_density_growth_mom,
    CASE
      WHEN avg_ticket = 99999
        THEN NULL
      ELSE avg_ticket
      END AS b_avg_trans_value,
    b_avg_ticket_LM as b_avg_trans_value_LM,
    b_avg_ticket_LY as b_avg_trans_value_LY,
    b_avg_ticket_mom as b_avg_trans_value_mom,
    b_avg_ticket_yoy as b_avg_trans_value_yoy,
    CASE
      WHEN avg_visits_per_cust = 99999
        THEN NULL
      ELSE avg_visits_per_cust
      END AS b_avg_cust_visits,
    CASE
      WHEN avg_spend_per_cust = 99999
        THEN NULL
      ELSE avg_spend_per_cust
      END AS b_avg_cust_trans_value,
    CASE
      WHEN ecom_ind_rank = 99999
        THEN NULL
      ELSE ecom_ind_rank
      END AS b_ecom_ind_rank,
    CASE
      WHEN ecom_txns_share = 99999
        THEN NULL
      ELSE ecom_txns_share
      END AS b_ecom_txns_share,
    CASE
      WHEN ecom_ind_rank_share = 99999
        THEN NULL
      ELSE ecom_ind_rank_share
      END AS b_ecom_ind_rank_share
  from
    wpay_analytics.mmi_area_industry_edit
  left join
    wpay_analytics.mmi_aus_mapping_key_edit key
    on cast(key.level1_id as string) = cast(area as string)
  where
    area_level = 'state'
  group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22
  ) ind_lga
on 
  cast(base.state as string) = cast(ind_lga.state_mmi as string)
  and cast(base.industry as string) = cast(ind_lga.industry_code as string)
  and cast(base.period as string) = cast(ind_lga.period as string)

left join
      (
  select 
      m.company,
      m.invoice_group,    
      m.merchant_group,
      m.division, 
      m.brand, 
      m.merchant_portal_mg,
      m.merchant_portal_cg,   
      --m.store_id,   
      --m.site_name,    
     -- m.suburb,   
      m.state,    
      --m.postcode,
     -- m.country,
      --m.is_online,
      --fee.calc_scheme as scheme,
      --fee.calc_card_type as card_type,
      left(cast(fee.switch_tran_date as string),7) Transaction_Date,
      count(distinct fee.card_id) w_unique_card,
      count(distinct fee.card_id) w_unique_cust
      from wpay_analytics.wpay_billing_engine_tran_dtl fee  
      inner join
      `pdh_ref_ds.ref_store_details` m 
      on LEFT(FEE.net_term_id, 5) = M.store_id
      --inner join
      --wpay_analytics.wpay_card_identity_model cim
      --on trim(cim.scdr_token) = trim(fee.scdr_token)
      where fee.calc_approval_flag = 'A'
	  and left(cast(fee.tstamp_trans as string),7) > '2022-07'
and fee.calc_tran_type_description in ('REFUND','REFUND REVERSAL','PURCHASE','PURCHASE REVERSAL','PURCHASE WITH CASHOUT','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION','REDEMPTION REVERSAL')
      group by 1,2,3,4,5,6,7,8,9
      ) cust_info
  on cust_info.company = base.company
    and cust_info.merchant_group = base.merchant_group
    and cust_info.division = base.division
    and cust_info.state = base.state
    and cust_info.invoice_group = base.invoice_group
    and cust_info.brand = base.brand
    and cust_info.merchant_portal_mg = base.merchant_portal_mg
    and cust_info.merchant_portal_cg = base.merchant_portal_cg
    and base.period = cust_info.Transaction_Date

)
,

MaxMMIDate as (
select max(Period) as max_date
from  wpay_analytics.mmi_area_industry_edit
)

,
 state_stable_bench as 
 (
  select *,
  case when 
    (coalesce(b_trans_amount_yoy,b_trans_amount_mom,b_trans_count_yoy,b_trans_count_mom) is null 
    and concat(concat(Year,'-'),Month) <= (select max_date from MaxMMIDate))
    then 'Country'
    else 'State'
    end as Benchmark_Location
    from state_stable
)
,

 state_stable_final as
(
 select 
  'State' as Aggregation_Level,
  'All' as store_id,
  'Stable' as store_status,
  base.country,
  base.state,
  base.division,
  base.brand,
  base.industry,
  'All' as site_name,
  'All' as suburb,
  'All' as postcode,
  'All' as caid,
  base.company,
  base.invoice_group,
  base.merchant_group,
  base.merchant_portal_mg,
  base.merchant_portal_cg,
  base.is_online,
  base.scheme,
  base.card_type,
  'All' as LGA,
  'All' as LGA_id,
  base.area,
  base.industry_name,
  base.Period,
  base.Year,
  base.Month,
  base.w_trans_amount,
  base.w_trans_count,
  base.w_avg_trans_value,
  base.w_trans_amount_LM,
  base.w_trans_count_LM,
  base.w_avg_trans_value_LM,
  base.w_trans_amount_LY,
  base.w_trans_count_LY,
  base.w_avg_trans_value_LY,
  base.w_trans_amount_mom,
  base.w_trans_amount_yoy,
  base.w_trans_count_mom,
  base.w_trans_count_yoy,
  base.w_avg_trans_value_mom,
  base.w_avg_trans_value_yoy,
  base.w_avg_cust_visits,
  base.w_avg_cust_trans_value,
  base.w_unique_cust,
  base.w_unique_card,

  case when Benchmark_Location = 'Country'
  then ind_lga.b_trans_amount_yoy
  else base.b_trans_amount_yoy
  end as
  b_trans_amount_yoy,

  case when Benchmark_Location = 'Country'
  then ind_lga.b_trans_amount_mom
  else base.b_trans_amount_mom
  end as
  b_trans_amount_mom,

  case when Benchmark_Location = 'Country'
  then ind_lga.b_trans_count_yoy
  else base.b_trans_count_yoy
  end as
  b_trans_count_yoy,

  case when Benchmark_Location = 'Country'
  then ind_lga.b_trans_count_mom
  else base.b_trans_count_mom
  end as
  b_trans_count_mom,

  case when Benchmark_Location = 'Country'
  then ind_lga.b_spend_density_growth_yoy
  else base.b_spend_density_growth_yoy
  end as
  b_spend_density_growth_yoy,

  case when Benchmark_Location = 'Country'
  then ind_lga.b_spend_density_growth_mom
  else base.b_spend_density_growth_mom
  end as
  b_spend_density_growth_mom,

  case when Benchmark_Location = 'Country'
  then ind_lga.b_avg_trans_value
  else base.b_avg_trans_value
  end as
  b_avg_trans_value,

  case when Benchmark_Location = 'Country'
  then ind_lga.b_avg_trans_value_LM
  else base.b_avg_trans_value_LM
  end as
  b_avg_trans_value_LM,

  case when Benchmark_Location = 'Country'
  then ind_lga.b_avg_trans_value_LY
  else base.b_avg_trans_value_LY
  end as
  b_avg_trans_value_LY,

  case when Benchmark_Location = 'Country'
  then ind_lga.b_avg_trans_value_mom
  else base.b_avg_trans_value_mom
  end as
  b_avg_trans_value_mom,

  case when Benchmark_Location = 'Country'
  then ind_lga.b_avg_trans_value_yoy
  else base.b_avg_trans_value_yoy
  end as
  b_avg_trans_value_yoy,

  case when Benchmark_Location = 'Country'
  then ind_lga.b_avg_cust_visits
  else base.b_avg_cust_visits
  end as
  b_avg_cust_visits,

  case when Benchmark_Location = 'Country'
  then ind_lga.b_avg_cust_trans_value
  else base.b_avg_cust_trans_value
  end as
  b_avg_cust_trans_value,

  case when Benchmark_Location = 'Country'
  then ind_lga.b_ecom_ind_rank
  else base.b_ecom_ind_rank
  end as
  b_ecom_ind_rank,

  case when Benchmark_Location = 'Country'
  then ind_lga.b_ecom_txns_share
  else base.b_ecom_txns_share
  end as
  b_ecom_txns_share,

  case when Benchmark_Location = 'Country'
  then ind_lga.b_ecom_ind_rank_share
  else base.b_ecom_ind_rank_share
  end as
  b_ecom_ind_rank_share,

  base.industry_name as Benchmark,
  base.Benchmark_Location,
  'External' as Benchmark_lvl
from
  state_stable_bench base
left join
    (
  select 
    area,
    period,
    --industry_name,
    industry_code,
    left(period,4) as Year,
    right(period,2) as Month,
    key.State as state_mmi,
    CASE 
      WHEN spend_growth_yoy = 99999
        THEN NULL
      ELSE spend_growth_yoy
      END AS b_trans_amount_yoy,
    CASE 
      WHEN spend_growth_mom = 99999
        THEN NULL
      ELSE spend_growth_mom
      END AS b_trans_amount_mom,
    CASE
      WHEN txn_growth_yoy = 99999
        THEN NULL
      ELSE txn_growth_yoy
      END AS b_trans_count_yoy,
    CASE
      WHEN txn_growth_mom = 99999
        THEN NULL
      ELSE txn_growth_mom
      END AS b_trans_count_mom,
    CASE
      WHEN spend_density_growth_yoy = 99999
        THEN NULL
      ELSE spend_density_growth_yoy
      END AS b_spend_density_growth_yoy,
    CASE
      WHEN spend_density_growth_mom = 99999
        THEN NULL
      ELSE spend_density_growth_mom
      END AS b_spend_density_growth_mom,
    CASE
      WHEN avg_ticket = 99999
        THEN NULL
      ELSE avg_ticket
      END AS b_avg_trans_value,
    b_avg_ticket_LM as b_avg_trans_value_LM,
    b_avg_ticket_LY as b_avg_trans_value_LY,
    b_avg_ticket_mom as b_avg_trans_value_mom,
    b_avg_ticket_yoy as b_avg_trans_value_yoy,
    CASE
      WHEN avg_visits_per_cust = 99999
        THEN NULL
      ELSE avg_visits_per_cust
      END AS b_avg_cust_visits,
    CASE
      WHEN avg_spend_per_cust = 99999
        THEN NULL
      ELSE avg_spend_per_cust
      END AS b_avg_cust_trans_value,
    CASE
      WHEN ecom_ind_rank = 99999
        THEN NULL
      ELSE ecom_ind_rank
      END AS b_ecom_ind_rank,
    CASE
      WHEN ecom_txns_share = 99999
        THEN NULL
      ELSE ecom_txns_share
      END AS b_ecom_txns_share,
    CASE
      WHEN ecom_ind_rank_share = 99999
        THEN NULL
      ELSE ecom_ind_rank_share
      END AS b_ecom_ind_rank_share
  from
    wpay_analytics.mmi_area_industry_edit
  left join
    wpay_analytics.mmi_aus_mapping_key_edit key
    on cast(key.level1_id as string) = cast(area as string)
  where
    area_level = 'country'
  group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22
  ) ind_lga
on 
  --cast(base.country as string) = cast(ind_lga.area as string)
  cast(base.country as string) = left(cast(ind_lga.area as string),2) 
  and cast(base.industry as string) = cast(ind_lga.industry_code as string)
  and cast(base.period as string) = cast(ind_lga.period as string)
)
,
-------------------------------------------------------------------------------------------------------------------------------------------
-- COUNTRY STABLE 

country_stable as
(
 select 
  'Country' as Aggregation_Level,
  'All' as store_id,
  'Stable' as store_status,
  base.country,
  'All' as state,
  base.division,
  base.brand,
  base.industry,
  'All' as site_name,
  'All' as suburb,
  'All' as postcode,
  'All' as caid,
  base.company,
  base.invoice_group,
  base.merchant_group,
  base.merchant_portal_mg,
  base.merchant_portal_cg,
  base.is_online,
  base.scheme,
  base.card_type,
  'All' as LGA,
  'All' as LGA_id,
  ind_lga.area,
  base.industry_name,
  base.Period,
  base.Year,
  base.Month,
  base.w_trans_amount,
  base.w_trans_count,
  base.w_avg_trans_value,
  base.w_trans_amount_LM,
  base.w_trans_count_LM,
  base.w_avg_trans_value_LM,
  base.w_trans_amount_LY,
  base.w_trans_count_LY,
  base.w_avg_trans_value_LY,
  base.w_trans_amount_mom,
  base.w_trans_amount_yoy,
  base.w_trans_count_mom,
  base.w_trans_count_yoy,
  base.w_avg_trans_value_mom,
  base.w_avg_trans_value_yoy,

    case when cust_info.w_unique_cust <> 0 
    then base.w_trans_count/cust_info.w_unique_cust
    else 0
    end as w_avg_cust_visits,

    case when cust_info.w_unique_cust <> 0 
    then base.w_trans_amount/cust_info.w_unique_cust
    else 0
    end as w_avg_cust_trans_value,

  cust_info.w_unique_cust,
  cust_info.w_unique_card,

  ind_lga.b_trans_amount_yoy,
  ind_lga.b_trans_amount_mom,
  ind_lga.b_trans_count_yoy,
  ind_lga.b_trans_count_mom,
  ind_lga.b_spend_density_growth_yoy,
  ind_lga.b_spend_density_growth_mom,
  ind_lga.b_avg_trans_value,
  ind_lga.b_avg_trans_value_LM,
  ind_lga.b_avg_trans_value_LY,
  ind_lga.b_avg_trans_value_mom,
  ind_lga.b_avg_trans_value_yoy,
  ind_lga.b_avg_cust_visits,
  ind_lga.b_avg_cust_trans_value,
  ind_lga.b_ecom_ind_rank,
  ind_lga.b_ecom_txns_share,
  ind_lga.b_ecom_ind_rank_share,
  --base.industry_name as Benchmark,
  --Benchmark_Location,
  'External' as Benchmark_lvl
 from
  (
  select 
    country,
    --state,
    division,
    brand,
    industry,
    company,
    invoice_group,
    merchant_group,
    merchant_portal_mg,
    merchant_portal_cg,
    is_online,
    scheme,
    card_type,
    industry_name,
    Period,
    Year,
    Month,
    sum(w_trans_amount) as w_trans_amount,
    sum(w_trans_count) as w_trans_count,

    case when sum(w_trans_count) <> 0 
    then
    sum(w_trans_amount)/sum(w_trans_count)
    else 0
    end as w_avg_trans_value,

    sum(w_trans_amount_LM) as w_trans_amount_LM,
    sum(w_trans_count_LM) as w_trans_count_LM,

    case when sum(w_trans_count_LM) <> 0 
    then
    sum(w_trans_amount_LM)/sum(w_trans_count_LM) 
    else 0 
    end as w_avg_trans_value_LM,

    sum(w_trans_amount_LY) as w_trans_amount_LY,
    sum(w_trans_count_LY) as w_trans_count_LY,

    case when sum(w_trans_count_LY) <> 0 
    then
    sum(w_trans_amount_LY)/sum(w_trans_count_LY) 
    else 0 
    end as w_avg_trans_value_LY,

    case when sum(w_trans_amount_LM) <> 0
    then ((sum(w_trans_amount)-sum(w_trans_amount_LM))/(sum(w_trans_amount_LM)))
    else 0
    end as w_trans_amount_mom,

    case when sum(w_trans_amount_LY) <> 0
    then ((sum(w_trans_amount)-sum(w_trans_amount_LY))/(sum(w_trans_amount_LY)))
    else 0
    end as w_trans_amount_yoy,

    case when sum(w_trans_count_LM) <> 0
    then ((sum(w_trans_count)-sum(w_trans_count_LM))/(sum(w_trans_count_LM)))
    else 0
    end as w_trans_count_mom,

    case when sum(w_trans_count_LY) <> 0
    then ((sum(w_trans_count)-sum(w_trans_count_LY))/(sum(w_trans_count_LY)))
    else 0
    end as w_trans_count_yoy,

    case when ((sum(w_trans_amount_LM) <> 0) and (sum(w_trans_count) <> 0) and (sum(w_trans_count_LM) <> 0))
    then (((sum(w_trans_amount)/sum(w_trans_count))-(sum(w_trans_amount_LM)/sum(w_trans_count_LM)))/(sum(w_trans_amount_LM)/sum(w_trans_count_LM)))
    else 0
    end as w_avg_trans_value_mom,

    case when ((sum(w_trans_count_LY) <> 0) and (sum(w_trans_count) <> 0) and (sum(w_trans_count_LY) <> 0))
    then (((sum(w_trans_amount)/sum(w_trans_count))-(sum(w_trans_amount_LY)/sum(w_trans_count_LY)))/(sum(w_trans_amount_LY)/sum(w_trans_count_LY)))
    else 0
    end as w_avg_trans_value_yoy,
    
  from
    store_stable
  group by
  1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16
  ) base
left join
  (
  select 
    area,
    period,
    --industry_name,
    industry_code,
    left(period,4) as Year,
    right(period,2) as Month,
    key.State as state_mmi,
    CASE 
      WHEN spend_growth_yoy = 99999
        THEN NULL
      ELSE spend_growth_yoy
      END AS b_trans_amount_yoy,
    CASE 
      WHEN spend_growth_mom = 99999
        THEN NULL
      ELSE spend_growth_mom
      END AS b_trans_amount_mom,
    CASE
      WHEN txn_growth_yoy = 99999
        THEN NULL
      ELSE txn_growth_yoy
      END AS b_trans_count_yoy,
    CASE
      WHEN txn_growth_mom = 99999
        THEN NULL
      ELSE txn_growth_mom
      END AS b_trans_count_mom,
    CASE
      WHEN spend_density_growth_yoy = 99999
        THEN NULL
      ELSE spend_density_growth_yoy
      END AS b_spend_density_growth_yoy,
    CASE
      WHEN spend_density_growth_mom = 99999
        THEN NULL
      ELSE spend_density_growth_mom
      END AS b_spend_density_growth_mom,
    CASE
      WHEN avg_ticket = 99999
        THEN NULL
      ELSE avg_ticket
      END AS b_avg_trans_value,
    b_avg_ticket_LM as b_avg_trans_value_LM,
    b_avg_ticket_LY as b_avg_trans_value_LY,
    b_avg_ticket_mom as b_avg_trans_value_mom,
    b_avg_ticket_yoy as b_avg_trans_value_yoy,
    CASE
      WHEN avg_visits_per_cust = 99999
        THEN NULL
      ELSE avg_visits_per_cust
      END AS b_avg_cust_visits,
    CASE
      WHEN avg_spend_per_cust = 99999
        THEN NULL
      ELSE avg_spend_per_cust
      END AS b_avg_cust_trans_value,
    CASE
      WHEN ecom_ind_rank = 99999
        THEN NULL
      ELSE ecom_ind_rank
      END AS b_ecom_ind_rank,
    CASE
      WHEN ecom_txns_share = 99999
        THEN NULL
      ELSE ecom_txns_share
      END AS b_ecom_txns_share,
    CASE
      WHEN ecom_ind_rank_share = 99999
        THEN NULL
      ELSE ecom_ind_rank_share
      END AS b_ecom_ind_rank_share
  from
    wpay_analytics.mmi_area_industry_edit
  left join
    wpay_analytics.mmi_aus_mapping_key_edit key
    on cast(key.level1_id as string) = cast(area as string)
  where
    area_level = 'country'
  group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22
  ) ind_lga
on 
  --cast(base.country as string) = cast(ind_lga.area as string)
  cast(base.country as string) = left(cast(ind_lga.area as string),2)
  and cast(base.industry as string) = cast(ind_lga.industry_code as string)
  and cast(base.period as string) = cast(ind_lga.period as string)

left join
      (
  select 
      m.company,
      m.invoice_group,    
      m.merchant_group,
      m.division, 
      m.brand, 
      m.merchant_portal_mg,
      m.merchant_portal_cg,   
      --m.store_id,   
      --m.site_name,    
     -- m.suburb,   
      --m.state,    
      --m.postcode,
      m.country,
      --m.is_online,
      --fee.calc_scheme as scheme,
      --fee.calc_card_type as card_type,
      left(cast(fee.switch_tran_date as string),7) Transaction_Date,
      count(distinct fee.card_id) w_unique_card,
      count(distinct fee.card_id) w_unique_cust
      from wpay_analytics.wpay_billing_engine_tran_dtl fee  
      inner join
      `pdh_ref_ds.ref_store_details` m 
      on LEFT(FEE.net_term_id, 5) = M.store_id
      --inner join
      --wpay_analytics.wpay_card_identity_model cim
      --on trim(cim.scdr_token) = trim(fee.scdr_token)
      where fee.calc_approval_flag = 'A'
	  and left(cast(fee.tstamp_trans as string),7) > '2022-07'
and fee.calc_tran_type_description in ('REFUND','REFUND REVERSAL','PURCHASE','PURCHASE REVERSAL','PURCHASE WITH CASHOUT','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION','REDEMPTION REVERSAL')
      group by 1,2,3,4,5,6,7,8,9
      ) cust_info
  on cust_info.company = base.company
    and cust_info.merchant_group = base.merchant_group
    and cust_info.division = base.division
    and cust_info.country = base.country
    and cust_info.invoice_group = base.invoice_group
    and cust_info.brand = base.brand
    and cust_info.merchant_portal_mg = base.merchant_portal_mg
    and cust_info.merchant_portal_cg = base.merchant_portal_cg
    and base.period = cust_info.Transaction_Date

)
,

  country_stable_final as
(
 select 
  'Country' as Aggregation_Level,
  'All' as store_id,
  'Stable' as store_status,
  base.country,
  'All' as state,
  base.division,
  base.brand,
  base.industry,
  'All' as site_name,
  'All' as suburb,
  'All' as postcode,
  'All' as caid,
  base.company,
  base.invoice_group,
  base.merchant_group,
  base.merchant_portal_mg,
  base.merchant_portal_cg,
  base.is_online,
  base.scheme,
  base.card_type,
  'All' as LGA,
  'All' as LGA_id,
  base.area,
  base.industry_name,
  base.Period,
  base.Year,
  base.Month,
  base.w_trans_amount,
  base.w_trans_count,
  base.w_avg_trans_value,
  base.w_trans_amount_LM,
  base.w_trans_count_LM,
  base.w_avg_trans_value_LM,
  base.w_trans_amount_LY,
  base.w_trans_count_LY,
  base.w_avg_trans_value_LY,
  base.w_trans_amount_mom,
  base.w_trans_amount_yoy,
  base.w_trans_count_mom,
  base.w_trans_count_yoy,
  base.w_avg_trans_value_mom,
  base.w_avg_trans_value_yoy,
  base.w_avg_cust_visits,
  base.w_avg_cust_trans_value,
  base.w_unique_cust,
  base.w_unique_card,

  base.b_trans_amount_yoy,
  base.b_trans_amount_mom,
  base.b_trans_count_yoy,
  base.b_trans_count_mom,
  base.b_spend_density_growth_yoy,
  base.b_spend_density_growth_mom,
  base.b_avg_trans_value,
  base.b_avg_trans_value_LM,
  base.b_avg_trans_value_LY,
  base.b_avg_trans_value_mom,
  base.b_avg_trans_value_yoy,
  base.b_avg_cust_visits,
  base.b_avg_cust_trans_value,
  base.b_ecom_ind_rank,
  base.b_ecom_txns_share,
  base.b_ecom_ind_rank_share,

  base.industry_name as Benchmark,
  'Country' as Benchmark_Location,
  'External' as Benchmark_lvl
from
  country_stable base
)



select 
  *
from
  store_stable
union all
select 
  *
from
  state_stable_final
union all
select 
  *
from
  country_stable_final

)


;
-----------------------------------------------------------------------------------------------------------------
--INTERNAL BENCHMARKS
-------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------
-- BASE TABLE (ALL)
create or replace table pdh_staging_ds.temp_internal_stable as 
(
-------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------
-- Country

with w_internal_country as 
(select 
  base.Aggregation_Level,					
  base.store_id,
  base.store_status,
  base.country,
  base.state,
  base.division,
  base.brand,
  base.industry,
  base.site_name,
  base.suburb,
  base.postcode,
  base.caid,
  base.company,
  base.invoice_group,
  base.merchant_group,
  base.merchant_portal_mg,
  base.merchant_portal_cg,
  base.is_online,
  base.scheme,
  base.card_type,
  base.LGA,
  base.LGA_id,
  base.area,
  base.industry_name,
  base.Period,
  base.Year,
  base.Month,
  base.w_trans_amount,
  base.w_trans_count,				
  base.w_avg_trans_value as w_avg_trans_value,				
  base.w_trans_amount_LM,		
  base.w_trans_count_LM,				
  base.w_avg_trans_value_LM as w_avg_trans_value_LM,				
  base.w_trans_amount_LY,				
  base.w_trans_count_LY,				
  base.w_avg_trans_value_LY as w_avg_trans_value_LY,					
  base.w_trans_amount_mom,					
  base.w_trans_amount_yoy,					
  base.w_trans_count_mom,					
  base.w_trans_count_yoy,					
  base.w_avg_trans_value_mom as w_avg_trans_value_mom,					
  base.w_avg_trans_value_yoy as w_avg_trans_value_yoy,					
  base.w_avg_cust_visits as w_avg_cust_visits,					
  base.w_avg_cust_trans_value as w_avg_cust_trans_value,
  base.w_unique_cust as w_unique_cust,					
  base.w_unique_card as w_unique_card,			
  internal_lga.w_trans_amount_tot_yoy as b_trans_amount_yoy,		
  internal_lga.w_trans_amount_tot_mom as b_trans_amount_mom,					
  internal_lga.w_trans_count_tot_yoy as b_trans_count_yoy,					
  internal_lga.w_trans_count_tot_mom as b_trans_count_mom,					
  0 as b_spend_density_growth_yoy,					
  0 as b_spend_density_growth_mom,					
  internal_lga.w_trans_value_tot as b_avg_trans_value,					
  internal_lga.w_trans_value_LM_tot as b_avg_trans_value_LM,					
  internal_lga.w_trans_value_LY_tot as b_avg_trans_value_LY,					
  internal_lga.w_trans_value_tot_mom as b_avg_trans_value_mom,					
  internal_lga.w_trans_value_tot_yoy as b_avg_trans_value_yoy,					
  0 as b_avg_cust_visits,					
  0 as b_avg_cust_trans_value,					
  0 as b_ecom_ind_rank,					
  0 as b_ecom_txns_share,					
  0 as b_ecom_ind_rank_share,					
  --'Country' as Benchmark,
  base.merchant_group as Benchmark,		
  'Country' as Benchmark_Location,		
  'Internal' as Benchmark_lvl
from
  pdh_staging_ds.temp_external_stable base
inner join
(
select
  store_status,
  country,
  is_online,
  scheme,
  card_type,
  --state,
  division,
  brand,
  company,
  invoice_group,
  merchant_group,
  merchant_portal_mg,
  merchant_portal_cg,
  --LGA_id,
  Period,
  Year,
  Month,
  sum(w_trans_amount) as w_trans_amount_tot,
  sum(w_trans_count) as w_trans_count_tot,

  case when sum(w_trans_count) <> 0
  then (sum(w_trans_amount)/sum(w_trans_count))
  else 0 end
  as w_trans_value_tot,

  sum(w_trans_amount_LM) as w_trans_amount_LM_tot,
  sum(w_trans_count_LM) as w_trans_count_LM_tot,

  case when sum(w_trans_count_LM) <> 0
  then (sum(w_trans_amount_LM)/sum(w_trans_count_LM))
  else 0 end
  as w_trans_value_LM_tot,

  sum(w_trans_amount_LY) as w_trans_amount_LY_tot,
  sum(w_trans_count_LY) as w_trans_count_LY_tot,

  case when sum(w_trans_count_LY) <> 0
  then (sum(w_trans_amount_LY)/sum(w_trans_count_LY))
  else 0 end
  as w_trans_value_LY_tot,

  case when sum(w_trans_amount_LM) <> 0
  then ((sum(w_trans_amount) - sum(w_trans_amount_LM))/(sum(w_trans_amount_LM)))
  else 0 end 
  as w_trans_amount_tot_mom,

  case when sum(w_trans_count_LM) <> 0
  then ((sum(w_trans_count) - sum(w_trans_count_LM))/(sum(w_trans_count_LM)))
  else 0 end 
  as w_trans_count_tot_mom,

  case when ((sum(w_trans_count) <>0 ) and (sum(w_trans_count_LM) <>0))
  then (((sum(w_trans_amount)/sum(w_trans_count)) - (sum(w_trans_amount_LM)/sum(w_trans_count_LM)))/((sum(w_trans_amount_LM)/sum(w_trans_count_LM))))
  else 0 end 
  as w_trans_value_tot_mom,

  case when sum(w_trans_amount_LY) <> 0
  then ((sum(w_trans_amount) - sum(w_trans_amount_LY))/(sum(w_trans_amount_LY)))
  else 0 end 
  as w_trans_amount_tot_yoy,

  case when sum(w_trans_count_LY) <> 0
  then ((sum(w_trans_count) - sum(w_trans_count_LY))/(sum(w_trans_count_LY)))
  else 0 end 
  as w_trans_count_tot_yoy,

  case when ((sum(w_trans_count) <>0 ) and (sum(w_trans_count_LY) <>0))
  then (((sum(w_trans_amount)/sum(w_trans_count)) - (sum(w_trans_amount_LY)/sum(w_trans_count_LY)))/((sum(w_trans_amount_LY)/sum(w_trans_count_LY))))
  else 0 end 
  as w_trans_value_tot_yoy
from
  pdh_staging_ds.temp_external_stable
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15) internal_lga
on 
  base.store_status = internal_lga.store_status
  and base.country = internal_lga.country
  and base.is_online = internal_lga.is_online
  and base.scheme = internal_lga.scheme
  and base.card_type = internal_lga.card_type
  --and base.state = internal_lga.state
  and base.division = internal_lga.division
  and base.brand = internal_lga.brand
  and base.company = internal_lga.company
  and base.invoice_group = internal_lga.invoice_group
  and base.merchant_group = internal_lga.merchant_group
  and (base.merchant_portal_mg = internal_lga.merchant_portal_mg)
   and (base.merchant_portal_cg = internal_lga.merchant_portal_cg)
  --and base.LGA_id = internal_lga.LGA_id
  and base.Period = internal_lga.Period
  and base.Year = internal_lga.Year
  and base.Month = internal_lga.Month

where base.Aggregation_Level = 'State'
)

-------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------
-- STATE
,
 w_internal_state as 
(select 
  base.Aggregation_Level,					
  base.store_id,
  base.store_status,
  base.country,
  base.state,
  base.division,
  base.brand,
  base.industry,
  base.site_name,
  base.suburb,
  base.postcode,
  base.caid,
  base.company,
  base.invoice_group,
  base.merchant_group,
  base.merchant_portal_mg,
  base.merchant_portal_cg,
  base.is_online,
  base.scheme,
  base.card_type,
  base.LGA,
  base.LGA_id,
  base.area,
  base.industry_name,
  base.Period,
  base.Year,
  base.Month,
  base.w_trans_amount,
  base.w_trans_count,				
  base.w_avg_trans_value as w_avg_trans_value,				
  base.w_trans_amount_LM,		
  base.w_trans_count_LM,				
  base.w_avg_trans_value_LM as w_avg_trans_value_LM,				
  base.w_trans_amount_LY,				
  base.w_trans_count_LY,				
  base.w_avg_trans_value_LY as w_avg_trans_value_LY,					
  base.w_trans_amount_mom,					
  base.w_trans_amount_yoy,					
  base.w_trans_count_mom,					
  base.w_trans_count_yoy,					
  base.w_avg_trans_value_mom as w_avg_trans_value_mom,					
  base.w_avg_trans_value_yoy as w_avg_trans_value_yoy,					
  base.w_avg_cust_visits as w_avg_cust_visits,					
  base.w_avg_cust_trans_value as w_avg_cust_trans_value,
  base.w_unique_cust as w_unique_cust,					
  base.w_unique_card as w_unique_card,				
  internal_lga.w_trans_amount_tot_yoy as b_trans_amount_yoy,		
  internal_lga.w_trans_amount_tot_mom as b_trans_amount_mom,					
  internal_lga.w_trans_count_tot_yoy as b_trans_count_yoy,					
  internal_lga.w_trans_count_tot_mom as b_trans_count_mom,					
  0 as b_spend_density_growth_yoy,					
  0 as b_spend_density_growth_mom,					
  internal_lga.w_trans_value_tot as b_avg_trans_value,					
  internal_lga.w_trans_value_LM_tot as b_avg_trans_value_LM,					
  internal_lga.w_trans_value_LY_tot as b_avg_trans_value_LY,					
  internal_lga.w_trans_value_tot_mom as b_avg_trans_value_mom,					
  internal_lga.w_trans_value_tot_yoy as b_avg_trans_value_yoy,					
  0 as b_avg_cust_visits,					
  0 as b_avg_cust_trans_value,					
  0 as b_ecom_ind_rank,					
  0 as b_ecom_txns_share,					
  0 as b_ecom_ind_rank_share,					
  --'State' as Benchmark,
  base.merchant_group as Benchmark,		
  'State' as Benchmark_Location,		
  'Internal' as Benchmark_lvl
from
  pdh_staging_ds.temp_external_stable base
inner join
(
select
  store_status,
  country,
  is_online,
  scheme,
  card_type,
  state,
  division,
  brand,
  company,
  invoice_group,
  merchant_group,
  merchant_portal_mg,
  merchant_portal_cg,
  --LGA_id,
  Period,
  Year,
  Month,
  sum(w_trans_amount) as w_trans_amount_tot,
  sum(w_trans_count) as w_trans_count_tot,

  case when sum(w_trans_count) <> 0
  then (sum(w_trans_amount)/sum(w_trans_count))
  else 0 end
  as w_trans_value_tot,

  sum(w_trans_amount_LM) as w_trans_amount_LM_tot,
  sum(w_trans_count_LM) as w_trans_count_LM_tot,

  case when sum(w_trans_count_LM) <> 0
  then (sum(w_trans_amount_LM)/sum(w_trans_count_LM))
  else 0 end
  as w_trans_value_LM_tot,

  sum(w_trans_amount_LY) as w_trans_amount_LY_tot,
  sum(w_trans_count_LY) as w_trans_count_LY_tot,

  case when sum(w_trans_count_LY) <> 0
  then (sum(w_trans_amount_LY)/sum(w_trans_count_LY))
  else 0 end
  as w_trans_value_LY_tot,

  case when sum(w_trans_amount_LM) <> 0
  then ((sum(w_trans_amount) - sum(w_trans_amount_LM))/(sum(w_trans_amount_LM)))
  else 0 end 
  as w_trans_amount_tot_mom,

  case when sum(w_trans_count_LM) <> 0
  then ((sum(w_trans_count) - sum(w_trans_count_LM))/(sum(w_trans_count_LM)))
  else 0 end 
  as w_trans_count_tot_mom,

  case when ((sum(w_trans_count) <>0 ) and (sum(w_trans_count_LM) <>0))
  then (((sum(w_trans_amount)/sum(w_trans_count)) - (sum(w_trans_amount_LM)/sum(w_trans_count_LM)))/((sum(w_trans_amount_LM)/sum(w_trans_count_LM))))
  else 0 end 
  as w_trans_value_tot_mom,

  case when sum(w_trans_amount_LY) <> 0
  then ((sum(w_trans_amount) - sum(w_trans_amount_LY))/(sum(w_trans_amount_LY)))
  else 0 end 
  as w_trans_amount_tot_yoy,

  case when sum(w_trans_count_LY) <> 0
  then ((sum(w_trans_count) - sum(w_trans_count_LY))/(sum(w_trans_count_LY)))
  else 0 end 
  as w_trans_count_tot_yoy,

  case when ((sum(w_trans_count) <>0 ) and (sum(w_trans_count_LY) <>0))
  then (((sum(w_trans_amount)/sum(w_trans_count)) - (sum(w_trans_amount_LY)/sum(w_trans_count_LY)))/((sum(w_trans_amount_LY)/sum(w_trans_count_LY))))
  else 0 end 
  as w_trans_value_tot_yoy
from
  pdh_staging_ds.temp_external_stable
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16) internal_lga
on 
  base.store_status = internal_lga.store_status
  and base.country = internal_lga.country
  and base.is_online = internal_lga.is_online
  and base.scheme = internal_lga.scheme
  and base.card_type = internal_lga.card_type
  and base.state = internal_lga.state
  and base.division = internal_lga.division
  and base.brand = internal_lga.brand
  and base.company = internal_lga.company
  and base.invoice_group = internal_lga.invoice_group
  and base.merchant_group = internal_lga.merchant_group
  and (base.merchant_portal_mg = internal_lga.merchant_portal_mg)
   and (base.merchant_portal_cg = internal_lga.merchant_portal_cg)
  --and base.LGA_id = internal_lga.LGA_id
  and base.Period = internal_lga.Period
  and base.Year = internal_lga.Year
  and base.Month = internal_lga.Month

where base.Aggregation_Level = 'Store'
)

-------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------

  select * from w_internal_state
  union all
  select * from w_internal_country
);

---------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------

--------------------------------------------------------------------------------------------------------------
create or replace table pdh_staging_ds.temp_output as (
  select * from
  ( select * from
  pdh_staging_ds.temp_external
  union all 
  select * from
  pdh_staging_ds.temp_internal
  union all 
  select * from
  pdh_staging_ds.temp_external_stable
  union all 
  select * from
  pdh_staging_ds.temp_internal_stable)
);

----------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------

-- SEGMENTING CUSTOMERS
-- NEW CUSTOMERS/ONCE OFF
create or replace table pdh_staging_ds.temp_cust_count as (
with cust_count as
(
select
  -- m.company,
  m.invoice_group,    
  -- m.merchant_group,
  -- m.division, 
  -- m.brand,    
  -- m.store_id,   
  -- m.site_name,    
  -- m.suburb,   
  -- m.state,    
  -- m.postcode,
  -- m.country,
  --m.is_online,
  --fee.calc_scheme as scheme,
  --fee.calc_card_type as card_type,
  left(cast(fee.switch_tran_date as string),7) Transaction_Date,
  fee.card_id,
  (sum(case when fee.calc_tran_type_description in
  ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL') then 1 else 0 end)
  -
  sum(case when fee.calc_tran_type_description in 
  ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL') then 1 else 0 end)
  ) month_tran_count,

  (sum(case when fee.calc_tran_type_description in 
  ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL') then (fee.transaction_total/100) else 0 end)
  -
  sum(case when fee.calc_tran_type_description in 
  ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL','REFUND') then (fee.transaction_total/100) else 0 end)
  ) as month_tran_amount

from 
  wpay_analytics.wpay_billing_engine_tran_dtl fee
inner join
  `pdh_ref_ds.ref_store_details` m 
  on LEFT(FEE.net_term_id, 5) = M.store_id
where fee.calc_approval_flag = 'A'
and fee.calc_tran_type_description in ('REFUND','REFUND REVERSAL','PURCHASE','PURCHASE REVERSAL','PURCHASE WITH CASHOUT','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION','REDEMPTION REVERSAL')
and card_id is not null
group by 1,2,3
)
,

cust_visits as 
(
  select
    m.*,
    mt.month_tran_count as prev_m_one,
    mmt.month_tran_count as prev_m_two
  from
    cust_count m
  left join
    cust_count mt
  -- on m.company = mt.company
  on m.invoice_group = mt.invoice_group
  -- and m.merchant_group = mt.merchant_group
  -- and m.division = mt.division
  -- and m.brand = mt.brand
  -- and m.store_id  = mt.store_id
  -- and m.site_name  = mt.site_name
  -- and m.suburb = mt.suburb
  -- and m.state  = mt.state
  -- and  m.postcode = mt.postcode
  -- and m.country = mt.country
  --and m.is_online = mt.is_online
  --and m.scheme = mt.scheme
  --and m.card_type = mt.card_type
  and cast(date_add(cast(concat(m.Transaction_Date,'-01') as date), interval -1 month) as string) = cast(concat(mt.Transaction_Date,'-01') as string)
  and m.card_id = mt.card_id
  left join
    cust_count mmt
  -- on m.company = mmt.company
  on m.invoice_group = mmt.invoice_group
  -- and m.merchant_group = mmt.merchant_group
  -- and m.division = mmt.division
  -- and m.brand = mmt.brand
  -- and m.store_id  = mmt.store_id
  -- and m.site_name  = mmt.site_name
  -- and m.suburb = mmt.suburb
  -- and m.state  = mmt.state
  -- and  m.postcode = mmt.postcode
  -- and m.country = mmt.country
  --and m.is_online = mmt.is_online
  --and m.scheme = mmt.scheme
  --and m.card_type = mmt.card_type
  and cast(date_add(cast(concat(m.Transaction_Date,'-01') as date), interval -2 month) as string) = cast(concat(mmt.Transaction_Date,'-01') as string)
  and m.card_id = mmt.card_id
)
,

final_cust as
(
select 
  -- company,
  invoice_group,
  -- division,
  --store_id,
  Transaction_Date,
  sum(case when month_tran_count = 1 then 1 else 0 end) as once_off_cust,
  sum(case when month_tran_count > 1 then 1 else 0 end) as repeat_cust,
  sum(case when month_tran_count > 0 and (prev_m_one is null and prev_m_two is null) then 1 else 0 end) as new_cust,
  sum(case when month_tran_count > 0 and (prev_m_one is not null or prev_m_two is not null)  then 1 else 0 end) as return_cust,

  sum(case when month_tran_count = 1 then month_tran_count else 0 end) as once_off_cust_count,
  sum(case when month_tran_count > 1 then month_tran_count else 0 end) as repeat_cust_count,
  sum(case when month_tran_count > 0 and (prev_m_one is null and prev_m_two is null) then month_tran_count else 0 end) as new_cust_count,
  sum(case when month_tran_count > 0 and (prev_m_one is not null or prev_m_two is not null)  then month_tran_count else 0 end) as return_cust_count,

  sum(case when month_tran_count = 1 then month_tran_amount else 0 end) as once_off_cust_amount,
  sum(case when month_tran_count > 1 then month_tran_amount else 0 end) as repeat_cust_amount,
  sum(case when month_tran_count > 0 and (prev_m_one is null and prev_m_two is null) then month_tran_amount else 0 end) as new_cust_amount,
  sum(case when month_tran_count > 0 and (prev_m_one is not null or prev_m_two is not null)  then month_tran_amount else 0 end) as return_cust_amount
from
  cust_visits
group by 1,2
)

select  
  -- company,
  'Country' as Aggregation_Level,
  'External' as Benchmark_lvl,
  'All' as store_status,
  'All' as is_online,
  'All' as scheme,
  'All' as card_type,

  invoice_group,
  -- division,
  --store_id,
  Transaction_Date,
  once_off_cust,
  repeat_cust,
  new_cust,
  return_cust,

  once_off_cust_count,
  repeat_cust_count,
  new_cust_count,
  return_cust_count,

  once_off_cust_amount,
  repeat_cust_amount,
  new_cust_amount,
  return_cust_amount,
  sum(once_off_cust+repeat_cust) as cust_total
from
  final_cust
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20
);
----------------------------------------------------------------------------------------------------

----------------------------------------------------------------------------------------------------
-- SEGMENTING CUSTOMERS
-- ONLINE/BOTH
-- Displayed at a store level (online/offline at specific brand)
create or replace table pdh_staging_ds.temp_cust_count_online as (
with cust_count_online as
(
select
  -- m.company,
  m.invoice_group,    
  -- m.merchant_group,
  -- m.division, 
  -- m.brand,    
  -- m.store_id,   
  -- m.site_name,    
  -- m.suburb,   
  -- m.state,    
  -- m.postcode,
  -- m.country,
  m.is_online,
  --fee.calc_scheme as scheme,
  --fee.calc_card_type as card_type,
  left(cast(fee.switch_tran_date as string),7) Transaction_Date,
  fee.card_id,
  (sum(case when fee.calc_tran_type_description in
  ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL') then 1 else 0 end)
  -
  sum(case when fee.calc_tran_type_description in 
  ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL') then 1 else 0 end)
  ) month_tran_count,

  (sum(case when fee.calc_tran_type_description in 
  ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL') then (fee.transaction_total/100) else 0 end)
  -
  sum(case when fee.calc_tran_type_description in 
  ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL','REFUND') then (fee.transaction_total/100) else 0 end)
  ) as month_tran_amount

from 
  wpay_analytics.wpay_billing_engine_tran_dtl fee
inner join
  `pdh_ref_ds.ref_store_details` m 
  on LEFT(FEE.net_term_id, 5) = M.store_id
where fee.calc_approval_flag = 'A'
and fee.calc_tran_type_description in ('REFUND','REFUND REVERSAL','PURCHASE','PURCHASE REVERSAL','PURCHASE WITH CASHOUT','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION','REDEMPTION REVERSAL')
group by 1,2,3,4
order by 1,2,3,4
)

-- select 
--   transaction_date,
--   is_online,
--   count(distinct card_id)
-- from
--   cust_count_online
-- where 
--  invoice_group = 'EG Fuel'
-- and transaction_date = '2022-09'
-- and month_tran_count = 0
-- group by 1,2

,cust_trans as 
(
select 
  -- company,
  invoice_group,    
  -- merchant_group,
  -- division, 
  -- brand,    
  -- store_id,   
  -- site_name,    
  -- suburb,   
  -- state,    
  -- postcode,
  -- country,
  Transaction_Date,
  card_id,
  sum(case when (is_online = 'Online' and month_tran_count > 0) then 1 else 0 end) as online_cust,
  sum(case when (is_online = 'Not Online' and month_tran_count > 0) then 1 else 0 end) as offline_cust,

  sum(case when (is_online = 'Online' and month_tran_count > 0) then month_tran_count else 0 end) as on_cust_count,
  sum(case when (is_online = 'Not Online' and month_tran_count > 0) then month_tran_count else 0 end) as off_cust_count,

  sum(case when (is_online = 'Online' and month_tran_count > 0) then month_tran_amount else 0 end) as on_cust_amount,
  sum(case when (is_online = 'Not Online' and month_tran_count > 0) then month_tran_amount else 0 end) as off_cust_amount
from cust_count_online
--where store_id = 'F3490'
group by 1,2,3
order by 1,2,3
)

select 
  -- company,
  'Country' as Aggregation_Level,
  'External' as Benchmark_lvl,
  'All' as store_status,
  'All' as is_online,
  'All' as scheme,
  'All' as card_type,


  invoice_group,    
  --merchant_group,
  --division, 
  --brand,      
  -- country,
  Transaction_Date,
  --count(distinct scdr_token),
  sum(case when (online_cust > 0 and offline_cust = 0) then 1 else 0 end) as online_cust,
  sum(case when (online_cust = 0 and offline_cust > 0) then 1 else 0 end) as offline_cust,
  sum(case when (online_cust > 0 and offline_cust > 0) then 1 else 0 end) as both_cust,

  sum(case when (online_cust > 0 and offline_cust = 0) then on_cust_count else 0 end) as online_cust_count,
  sum(case when (online_cust = 0 and offline_cust > 0) then off_cust_count else 0 end) as offline_cust_count,
  sum(case when (online_cust > 0 and offline_cust > 0) then (on_cust_count+off_cust_count) else 0 end) as both_cust_count,

  sum(case when (online_cust > 0 and offline_cust = 0) then on_cust_amount else 0 end) as online_cust_amount,
  sum(case when (online_cust = 0 and offline_cust > 0) then off_cust_amount else 0 end) as offline_cust_amount,
  sum(case when (online_cust > 0 and offline_cust > 0) then (on_cust_amount+off_cust_amount) else 0 end)  as both_cust_amount
from cust_trans
--where store_id = 'F3490'
where card_id is not null
group by 1,2,3,4,5,6,7,8
order by 1,2,3,4,5,6,7,8
);
----------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------

--------------------------------------------------------------------------------------------------------------
-- RE-ARRANGING FIELDS
--------------------------------------------------------------------------------------------------------------

create or replace table pdh_staging_ds.temp_output_format as (
select distinct *
  from
(
  select 
    curr.Aggregation_Level,
    curr.Benchmark_lvl,
    curr.Benchmark,		
    curr.Benchmark_Location,
    curr.store_status,
    curr.is_online,
    curr.scheme,
    curr.card_type,
    curr.Period,
    cast(concat(curr.Period,'-01') as date) as Month_Start,
    curr.Year,
    curr.Month,
    curr.company,
    curr.division,
    curr.merchant_group,
    curr.invoice_group,
    curr.merchant_portal_mg,
    curr.merchant_portal_cg,
    curr.brand,
    curr.industry,
    curr.industry_name,
    curr.store_id,
    case when curr.site_name != 'All' then
    upper(curr.site_name)
    else 'All'
    end as site_name,
    curr.caid,
    case when curr.country != 'All' then
    upper(curr.country) 
    else 'All' end 
    as country,
    case when curr.state != 'All' then
    upper(curr.state) 
    else 'All' end 
    as state,
    case when curr.suburb != 'All' then
    upper(curr.suburb) 
    else 'All' end 
    as suburb,
    curr.postcode,
    curr.area,
    case when curr.LGA != 'All' then
    upper(curr.LGA) 
    else 'All' end 
    as LGA,
    curr.LGA_id,
    curr.w_trans_amount,
    curr.w_trans_count,				
    curr.w_avg_trans_value,				
    curr.w_trans_amount_LM,		
    curr.w_trans_count_LM,				
    curr.w_avg_trans_value_LM,				
    curr.w_trans_amount_LY,				
    curr.w_trans_count_LY,				
    curr.w_avg_trans_value_LY,					
    curr.w_trans_amount_mom,					
    curr.w_trans_amount_yoy,					
    curr.w_trans_count_mom,					
    curr.w_trans_count_yoy,					
    curr.w_avg_trans_value_mom,					
    curr.w_avg_trans_value_yoy,	

    cast(curr.w_avg_cust_visits as numeric) as w_avg_cust_visits,
    cast(prev_m.w_avg_cust_visits as numeric) as w_avg_cust_visits_LM,
    case when prev_m.w_avg_cust_visits <> 0 then
    ((curr.w_avg_cust_visits - prev_m.w_avg_cust_visits)/prev_m.w_avg_cust_visits)
    else 0
    end as 
    w_avg_cust_visits_mom,
    prev_y.w_avg_cust_visits as w_avg_cust_visits_LY,
    case when prev_y.w_avg_cust_visits <> 0 then
    ((curr.w_avg_cust_visits - prev_y.w_avg_cust_visits)/prev_y.w_avg_cust_visits)
    else 0
    end as 
    w_avg_cust_visits_yoy,

    curr.w_avg_cust_trans_value,
    prev_m.w_avg_cust_trans_value as w_avg_cust_trans_value_LM,	
    case when prev_m.w_avg_cust_trans_value <> 0 then
    ((curr.w_avg_cust_trans_value - prev_m.w_avg_cust_trans_value)/prev_m.w_avg_cust_trans_value)
    else 0
    end as 
    w_avg_cust_trans_value_mom,
    prev_y.w_avg_cust_trans_value as w_avg_cust_trans_value_LY,	
    case when prev_y.w_avg_cust_trans_value <> 0 then
    ((curr.w_avg_cust_trans_value - prev_y.w_avg_cust_trans_value)/prev_y.w_avg_cust_trans_value)
    else 0
    end as 
    w_avg_cust_trans_value_yoy,

    curr.w_unique_cust,	
    prev_m.w_unique_cust as w_unique_cust_LM,
    case when prev_m.w_unique_cust <> 0 then
    ((curr.w_unique_cust - prev_m.w_unique_cust)/prev_m.w_unique_cust)
    else 0
    end as 
    w_unique_cust_mom,	
    prev_y.w_unique_cust as w_unique_cust_LY,
    case when prev_y.w_unique_cust <> 0 then
    ((curr.w_unique_cust - prev_y.w_unique_cust)/prev_y.w_unique_cust)
    else 0
    end as 
    w_unique_cust_yoy,	

    curr.w_unique_card,	
    prev_m.w_unique_card as w_unique_card_LM,
    case when prev_m.w_unique_card <> 0 then
    ((curr.w_unique_card - prev_m.w_unique_card)/prev_m.w_unique_card)
    else 0
    end as 
    w_unique_card_mom,		
    prev_y.w_unique_card as w_unique_card_LY,
    case when prev_y.w_unique_card <> 0 then
    ((curr.w_unique_card - prev_y.w_unique_card)/prev_y.w_unique_card)
    else 0
    end as 
    w_unique_card_yoy,

    case when curr.is_online = 'Online'
    then 0
    else curr.b_trans_amount_yoy
    end as b_trans_amount_yoy,		
    case when curr.is_online = 'Online'
    then 0
    else curr.b_trans_amount_mom
    end as b_trans_amount_mom,				
    case when curr.is_online = 'Online'
    then 0
    else curr.b_trans_count_yoy
    end as b_trans_count_yoy,					
    case when curr.is_online = 'Online'
    then 0
    else curr.b_trans_count_mom
    end as b_trans_count_mom,					
    case when curr.is_online = 'Online'
    then 0
    else curr.b_spend_density_growth_yoy
    end as b_spend_density_growth_yoy,					
    case when curr.is_online = 'Online'
    then 0
    else curr.b_spend_density_growth_mom
    end as b_spend_density_growth_mom,					
    case when curr.is_online = 'Online'
    then 0
    else curr.b_avg_trans_value
    end as b_avg_trans_value,				
    case when curr.is_online = 'Online'
    then 0
    else curr.b_avg_trans_value_LM	
    end as b_avg_trans_value_LM	,
    case when curr.is_online = 'Online'
    then 0
    else curr.b_avg_trans_value_LY
    end as b_avg_trans_value_LY,
    case when curr.is_online = 'Online'
    then 0
    else curr.b_avg_trans_value_mom
    end as b_avg_trans_value_mom,
    case when curr.is_online = 'Online'
    then 0
    else curr.b_avg_trans_value_yoy
    end as b_avg_trans_value_yoy,

    case when curr.is_online = 'Online'
    then 0
    else curr.b_avg_cust_visits
    end as b_avg_cust_visits,
    case when curr.is_online = 'Online'
    then 0
    else prev_m.b_avg_cust_visits
    end as b_avg_cust_visits_LM,
    case when (curr.is_online != 'Online' and prev_m.b_avg_cust_visits <> 0 )
    then 
    ((curr.b_avg_cust_visits-prev_m.b_avg_cust_visits)/prev_m.b_avg_cust_visits)
    else 0
    end as 
    b_avg_cust_visits_mom,
    case when curr.is_online = 'Online'
    then 0
    else prev_y.b_avg_cust_visits
    end as b_avg_cust_visits_LY,
    case when (curr.is_online != 'Online' and prev_y.b_avg_cust_visits <> 0 )
    then 
    ((curr.b_avg_cust_visits-prev_y.b_avg_cust_visits)/prev_y.b_avg_cust_visits)
    else 0
    end as 
    b_avg_cust_visits_yoy,


    case when curr.is_online = 'Online'
    then 0
    else curr.b_avg_cust_trans_value
    end as b_avg_cust_trans_value,
    case when curr.is_online = 'Online'
    then 0
    else prev_m.b_avg_cust_trans_value
    end as b_avg_cust_trans_value_LM,
    case when (curr.is_online != 'Online' and prev_m.b_avg_cust_visits <> 0 )
    then 
    ((curr.b_avg_cust_trans_value-prev_m.b_avg_cust_trans_value)/prev_m.b_avg_cust_trans_value)
    else 0
    end as 
    b_avg_cust_trans_value_mom,
    case when curr.is_online = 'Online'
    then 0
    else prev_y.b_avg_cust_trans_value
    end as b_avg_cust_trans_value_LY,
    case when (curr.is_online != 'Online' and prev_y.b_avg_cust_visits <> 0 )
    then 
    ((curr.b_avg_cust_trans_value-prev_y.b_avg_cust_trans_value)/prev_y.b_avg_cust_trans_value)
    else 0
    end as 
    b_avg_cust_trans_value_yoy,


    curr.b_ecom_ind_rank,
    curr.b_ecom_txns_share,
    curr.b_ecom_ind_rank_share,

    cust_count.cust_total as w_cust_beh_cust_total,

    cust_count.once_off_cust as w_cust_beh_once_off_cust,
    cust_count.repeat_cust as w_cust_beh_repeat_cust,
    cust_count.once_off_cust_amount as w_cust_beh_once_off_amount,
    cust_count.repeat_cust_amount as w_cust_beh_repeat_amount,
    cust_count.once_off_cust_count as w_cust_beh_once_off_count,
    cust_count.repeat_cust_count as w_cust_beh_repeat_count,
    

    cust_count.new_cust as w_cust_beh_new_cust,
    cust_count.return_cust as w_cust_beh_returning_cust,
    cust_count.new_cust_amount as w_cust_beh_new_amount,
    cust_count.return_cust_amount as w_cust_beh_returning_amount,
    cust_count.new_cust_count as w_cust_beh_new_count,
    cust_count.return_cust_count as w_cust_beh_returning_count,


    cust_count_online.online_cust as w_cust_beh_online_cust,
    cust_count_online.offline_cust as w_cust_beh_in_store_cust,
    cust_count_online.both_cust as w_cust_beh_cross_channel_cust,
    cust_count_online.online_cust_amount as w_cust_beh_online_amount,
    cust_count_online.offline_cust_amount as w_cust_beh_in_store_amount,
    cust_count_online.both_cust_amount as w_cust_beh_cross_channel_amount,
    cust_count_online.online_cust_count as w_cust_beh_online_count,
    cust_count_online.offline_cust_count as w_cust_beh_in_store_count,
    cust_count_online.both_cust_count as w_cust_beh_cross_channel_count,


  from
  (
    select *
    from pdh_staging_ds.temp_output
  ) curr
  left join
    pdh_staging_ds.temp_output prev_m
  on
    curr.Aggregation_Level = prev_m.Aggregation_Level
    and curr.Benchmark_lvl = prev_m.Benchmark_lvl
    and curr.Benchmark = prev_m.Benchmark	
    and curr.store_status = prev_m.store_status
    and curr.is_online = prev_m.is_online
    and curr.scheme = prev_m.scheme
    and curr.card_type = prev_m.card_type
    --curr.Period = prev_m.Period
    and left(cast(date_add(cast(concat(concat(concat(concat(curr.Year,'-'),curr.Month),'-'),'01') as date), interval -1 month) as string),4) = cast(prev_m.Year as string)
    and substring(cast(date_add(cast(concat(concat(concat(concat(curr.Year,'-'),curr.Month),'-'),'01') as date), interval -1 month) as string),6,2)  = cast(prev_m.Month as string)
    and curr.company = prev_m.company
    and curr.division = prev_m.division
    and curr.merchant_group = prev_m.merchant_group
    and curr.invoice_group = prev_m.invoice_group
    and (curr.merchant_portal_mg = prev_m.merchant_portal_mg)
   and (curr.merchant_portal_cg = prev_m.merchant_portal_cg)
    and curr.industry = prev_m.industry
    and curr.industry_name = prev_m.industry_name
    and curr.store_id = prev_m.store_id
    and curr.site_name = prev_m.site_name
    and curr.caid = prev_m.caid
    and curr.country = prev_m.country
    and curr.state = prev_m.state
    and curr.suburb = prev_m.suburb
    and curr.postcode = prev_m.postcode
    and curr.brand = prev_m.brand
    and curr.LGA = prev_m.LGA
    and curr.LGA_id = prev_m.LGA_id
  left join
    pdh_staging_ds.temp_output prev_y
  on
    curr.Aggregation_Level = prev_y.Aggregation_Level
    and curr.Benchmark_lvl = prev_y.Benchmark_lvl
    and curr.Benchmark = prev_y.Benchmark
    and curr.store_status = prev_y.store_status
    and curr.is_online = prev_y.is_online
    and curr.scheme = prev_y.scheme
    and curr.card_type = prev_y.card_type
    --curr.Period = prev_y.Period
    and (cast(curr.Year as int)-1) = cast(prev_y.Year as int)
    and curr.Month = prev_y.Month
    and curr.company = prev_y.company
    and curr.division = prev_y.division
    and curr.merchant_group = prev_y.merchant_group
    and curr.invoice_group = prev_y.invoice_group
    and (curr.merchant_portal_mg = prev_y.merchant_portal_mg)
   and (curr.merchant_portal_cg = prev_y.merchant_portal_cg)
    and curr.industry = prev_y.industry
    and curr.industry_name = prev_y.industry_name
    and curr.store_id = prev_y.store_id
    and curr.site_name = prev_y.site_name
    and curr.caid = prev_y.caid
    and curr.country = prev_y.country
    and curr.state = prev_y.state
    and curr.suburb = prev_y.suburb
    and curr.postcode = prev_y.postcode
    and curr.brand = prev_y.brand
    and curr.LGA = prev_y.LGA
    and curr.LGA_id = prev_y.LGA_id

  left join
    pdh_staging_ds.temp_cust_count cust_count
  on
    curr.Aggregation_Level = cust_count.Aggregation_Level
    and curr.Benchmark_lvl = cust_count.Benchmark_lvl
    and curr.store_status = cust_count.store_status
    and curr.is_online = cust_count.is_online
    and curr.scheme = cust_count.scheme
    and curr.card_type = cust_count.card_type
    and curr.invoice_group = cust_count.invoice_group   
    and curr.Period = cust_count.Transaction_Date
  
  left join
    pdh_staging_ds.temp_cust_count_online cust_count_online
  on
    curr.Aggregation_Level = cust_count_online.Aggregation_Level
    and curr.Benchmark_lvl = cust_count_online.Benchmark_lvl
    and curr.store_status = cust_count_online.store_status
    and curr.is_online = cust_count_online.is_online
    and curr.scheme = cust_count_online.scheme
    and curr.card_type = cust_count_online.card_type
    and curr.invoice_group = cust_count_online.invoice_group    
    and curr.Period = cust_count_online.Transaction_Date
)
-- where curr.w_trans_amount is not null,
  --and curr.w_trans_count is not null,	
);

------------------------------------------------------------------------------------------------------------------------------

create or replace table wpay_analytics.wpay_all_merchants_performance_metrics as
(select * from pdh_staging_ds.temp_output_format);


create or replace table pdh_sf_mep_vw.wpay_all_merchants_performance_metrics as 
(
select *
from pdh_staging_ds.temp_output_format
where 
company in ('EG Fuel','Endeavour')
and cast(period as string)
  <=
    (
      (
        select max(period)
        from pdh_rd_external.mmi_area_industry
      )  
    )
)
;

--create or replace view pdh_sf_mep_vw.vw_wpay_all_merchants_performance_metrics
--as select * from 
--pdh_sf_mep_vw.wpay_all_merchants_performance_metrics
--;

drop table if exists pdh_staging_ds.temp_card;
drop table if exists pdh_staging_ds.temp_card_stable;
drop table if exists pdh_staging_ds.temp_external;
drop table if exists pdh_staging_ds.temp_external_stable;
drop table if exists pdh_staging_ds.temp_filters_combo;
drop table if exists pdh_staging_ds.temp_filters_combo_stable;
drop table if exists pdh_staging_ds.temp_internal;
drop table if exists pdh_staging_ds.temp_internal_stable;
drop table if exists pdh_staging_ds.temp_location;
drop table if exists pdh_staging_ds.temp_location_stable;
drop table if exists pdh_staging_ds.temp_online;
drop table if exists pdh_staging_ds.temp_online_stable;
drop table if exists pdh_staging_ds.temp_output;
drop table if exists pdh_staging_ds.temp_output_format;
drop table if exists pdh_staging_ds.temp_scheme;
drop table if exists pdh_staging_ds.temp_scheme_stable;
drop table if exists pdh_staging_ds.temp_cust_count_online;
drop table if exists pdh_staging_ds.temp_cust_count;

----------------------------------------------------------------------------------------------------------------

