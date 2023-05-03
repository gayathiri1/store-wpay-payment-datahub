-------------------------------------------------------------------------------------------------
create or replace table pdh_sf_mep_vw.auth_rates as
(
with auth_monthly as
(
select
  'Month' as timeframe,
  brand,
  merchant_group,
  store_id,
  --card_type,
  'All' as scheme,
  'All' as declined_reason,
  period,
  trans_count_approved,
  trans_count_declined,
  case when trans_count <> 0 then 
  trans_count_approved/trans_count 
  else 0 end 
  as auth_rate,
  case when trans_count <> 0 then 
  trans_count_declined/trans_count 
  else 0 end 
  as declined_rate,
  trans_amount_approved,
  case when trans_count_approved <> 0 then 
  trans_amount_approved/trans_count_approved 
  else 0 end 
  as avg_trans_value_approved,
  trans_amount_decline,
  case when trans_count_declined <> 0 then 
  trans_amount_decline/trans_count_declined 
  else 0 end 
  as avg_trans_value_declined
from
  (
  select
    stores.brand,
    stores.merchant_group,
    trans.store_id,
    --scheme,
    left(cast(transaction_date as string),7) as period,
    (sum(case when (trans.tran_type_description in 
    ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL')) then transaction_count else 0 end)
    -
    sum(case when (trans.tran_type_description in 
    ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL'))  then transaction_count else 0 end)
    ) as trans_count,
    (sum(case when (trans.tran_type_description in 
    ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL') and approval_flag = 'A') then transaction_count else 0 end)
    -
    sum(case when (trans.tran_type_description in 
    ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL') and approval_flag = 'A')  then transaction_count else 0 end)
    ) as trans_count_approved,
    (sum(case when (trans.tran_type_description in 
    ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL') and approval_flag = 'D') then transaction_count else 0 end)
    -
    sum(case when (trans.tran_type_description in 
    ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL') and approval_flag = 'D')  then transaction_count else 0 end)
    ) as trans_count_declined,
  sum(case when (trans.tran_type_description in ('REFUND','REFUND REVERSAL','PURCHASE','PURCHASE REVERSAL','PURCHASE WITH CASHOUT','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION','REDEMPTION REVERSAL') and approval_flag = 'A') then settlement_amount_inc_gst else 0 end) as trans_amount_approved,
  sum(case when (trans.tran_type_description in ('REFUND','REFUND REVERSAL','PURCHASE','PURCHASE REVERSAL','PURCHASE WITH CASHOUT','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION','REDEMPTION REVERSAL') and approval_flag = 'D') then settlement_amount_inc_gst else 0 end) as trans_amount_decline,
  from
    pdh_sf_mep_vw.wpay_all_merchants_daily_fees_summary_hist trans
  left join
    pdh_ref_ds.ref_store_details stores
    on stores.store_id = trans.store_id
  group by 1,2,3,4
  )
  order by 1,2,3,4,5,7
)
,

 auth_monthly_pre as
 (
  select 
    post.*,
    (post.auth_rate-pre.auth_rate) 
    as w_benchmark
  from auth_monthly post
  left join
    auth_monthly pre
  on
  left(cast(date_add(cast(concat(left(post.period,7),'-01') as date), interval -1 month) as string),4) = cast(left(pre.period,4) as string)
  and substring(cast(date_add(cast(concat(left(post.period,7),'-01') as date), interval -1 month) as string),6,2)  = right(left(pre.period,7),2)
  and post.brand = pre.brand
  and post.merchant_group = pre.merchant_group
  and post.store_id = pre.store_id
order by 1,2,3,4,5,7
)
,

  auth_daily as
(
select
  'Daily' as timeframe,
  brand,
  merchant_group,
  store_id,
  --card_type,
  'All' as scheme,
  'All' as declined_reason,
  period,
  trans_count_approved,
  trans_count_declined,
  case when trans_count <> 0 then 
  trans_count_approved/trans_count 
  else 0 end 
  as auth_rate,
  case when trans_count <> 0 then 
  trans_count_declined/trans_count 
  else 0 end 
  as declined_rate,
  trans_amount_approved,
  case when trans_count_approved <> 0 then 
  trans_amount_approved/trans_count_approved 
  else 0 end 
  as avg_trans_value_approved,
  trans_amount_decline,
  case when trans_count_declined <> 0 then 
  trans_amount_decline/trans_count_declined 
  else 0 end 
  as avg_trans_value_declined
from
  (
  select
    stores.brand,
    stores.merchant_group,
    trans.store_id,
    --scheme,
    cast(cast(transaction_date as date)as string) as period,
    (sum(case when (trans.tran_type_description in 
    ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL')) then transaction_count else 0 end)
    -
    sum(case when (trans.tran_type_description in 
    ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL'))  then transaction_count else 0 end)
    ) as trans_count,
    (sum(case when (trans.tran_type_description in 
    ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL') and approval_flag = 'A') then transaction_count else 0 end)
    -
    sum(case when (trans.tran_type_description in 
    ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL') and approval_flag = 'A')  then transaction_count else 0 end)
    ) as trans_count_approved,
    (sum(case when (trans.tran_type_description in 
    ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL') and approval_flag = 'D') then transaction_count else 0 end)
    -
    sum(case when (trans.tran_type_description in 
    ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL') and approval_flag = 'D')  then transaction_count else 0 end)
    ) as trans_count_declined,
  sum(case when (trans.tran_type_description in ('REFUND','REFUND REVERSAL','PURCHASE','PURCHASE REVERSAL','PURCHASE WITH CASHOUT','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION','REDEMPTION REVERSAL') and approval_flag = 'A') then settlement_amount_inc_gst else 0 end) as trans_amount_approved,
  sum(case when (trans.tran_type_description in ('REFUND','REFUND REVERSAL','PURCHASE','PURCHASE REVERSAL','PURCHASE WITH CASHOUT','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION','REDEMPTION REVERSAL') and approval_flag = 'D') then settlement_amount_inc_gst else 0 end) as trans_amount_decline,
  from
    pdh_sf_mep_vw.wpay_all_merchants_daily_fees_summary_hist trans
  left join
    pdh_ref_ds.ref_store_details stores
  on stores.store_id = trans.store_id
  group by 1,2,3,4
  )
)

,

 auth_daily_pre as
 (
  select 
    post.*,
    (post.auth_rate-pre.auth_rate) 
    as w_benchmark
  from auth_daily post
  left join
    auth_daily pre
  on
  cast(date_add(cast(post.period as date), interval -1 day) as string) = cast(pre.period as string)
  and post.brand = pre.brand
  and post.merchant_group = pre.merchant_group
  and post.store_id = pre.store_id
order by 1,2,3,4,5,7
)
,

 auth_yearly as
(
select
  'Year' as timeframe,
  brand,
  merchant_group,
  store_id,
  --card_type,
  'All' as scheme,
  'All' as declined_reason,
  period,
  trans_count_approved,
  trans_count_declined,
  case when trans_count <> 0 then 
  trans_count_approved/trans_count 
  else 0 end 
  as auth_rate,
  case when trans_count <> 0 then 
  trans_count_declined/trans_count 
  else 0 end 
  as declined_rate,
  trans_amount_approved,
  case when trans_count_approved <> 0 then 
  trans_amount_approved/trans_count_approved 
  else 0 end 
  as avg_trans_value_approved,
  trans_amount_decline,
  case when trans_count_declined <> 0 then 
  trans_amount_decline/trans_count_declined 
  else 0 end 
  as avg_trans_value_declined
from
  (
  select
    stores.brand,
    stores.merchant_group,
    trans.store_id,
    --scheme,
    left(cast(transaction_date as string),4) as period,
    (sum(case when (trans.tran_type_description in 
    ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL')) then transaction_count else 0 end)
    -
    sum(case when (trans.tran_type_description in 
    ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL'))  then transaction_count else 0 end)
    ) as trans_count,
    (sum(case when (trans.tran_type_description in 
    ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL') and approval_flag = 'A') then transaction_count else 0 end)
    -
    sum(case when (trans.tran_type_description in 
    ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL') and approval_flag = 'A')  then transaction_count else 0 end)
    ) as trans_count_approved,
    (sum(case when (trans.tran_type_description in 
    ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL') and approval_flag = 'D') then transaction_count else 0 end)
    -
    sum(case when (trans.tran_type_description in 
    ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL') and approval_flag = 'D')  then transaction_count else 0 end)
    ) as trans_count_declined,
  sum(case when (trans.tran_type_description in ('REFUND','REFUND REVERSAL','PURCHASE','PURCHASE REVERSAL','PURCHASE WITH CASHOUT','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION','REDEMPTION REVERSAL') and approval_flag = 'A') then settlement_amount_inc_gst else 0 end) as trans_amount_approved,
  sum(case when (trans.tran_type_description in ('REFUND','REFUND REVERSAL','PURCHASE','PURCHASE REVERSAL','PURCHASE WITH CASHOUT','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION','REDEMPTION REVERSAL') and approval_flag = 'D') then settlement_amount_inc_gst else 0 end) as trans_amount_decline,
  from
    pdh_sf_mep_vw.wpay_all_merchants_daily_fees_summary_hist trans
  left join
    pdh_ref_ds.ref_store_details stores
  on stores.store_id = trans.store_id
  group by 1,2,3,4
  )
)

,

 auth_yearly_pre as
 (
  select 
    post.*,
    (post.auth_rate-pre.auth_rate) 
    as w_benchmark
  from auth_yearly post
  left join
    auth_yearly pre
  on
  cast((cast(post.period as int)-1) as string) = cast(pre.period as string)
  and post.brand = pre.brand
  and post.merchant_group = pre.merchant_group
  and post.store_id = pre.store_id
order by 1,2,3,4,5,7
)
,

 auth_weekly as
(
select
  'Week' as timeframe,
  brand,
  merchant_group,
  store_id,
  --card_type,
  'All' as scheme,
  'All' as declined_reason,
  period,
  trans_count_approved,
  trans_count_declined,
  case when trans_count <> 0 then 
  trans_count_approved/trans_count 
  else 0 end 
  as auth_rate,
  case when trans_count <> 0 then 
  trans_count_declined/trans_count 
  else 0 end 
  as declined_rate,
  trans_amount_approved,
  case when trans_count_approved <> 0 then 
  trans_amount_approved/trans_count_approved 
  else 0 end 
  as avg_trans_value_approved,
  trans_amount_decline,
  case when trans_count_declined <> 0 then 
  trans_amount_decline/trans_count_declined 
  else 0 end 
  as avg_trans_value_declined
from
  (
  select
    stores.brand,
    stores.merchant_group,
    trans.store_id,
    --scheme,
    -- concat(left(cast(cal.clndr_date as string),4),concat('-',RIGHT(CONCAT('00', COALESCE(cast(cal.FiscalPeriodWeekNumber as string),'')), 2))) as period,
    cast(cal.fw_start_date as string) as period,
    (sum(case when (trans.tran_type_description in 
    ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL')) then transaction_count else 0 end)
    -
    sum(case when (trans.tran_type_description in 
    ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL'))  then transaction_count else 0 end)
    ) as trans_count,
    (sum(case when (trans.tran_type_description in 
    ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL') and approval_flag = 'A') then transaction_count else 0 end)
    -
    sum(case when (trans.tran_type_description in 
    ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL') and approval_flag = 'A')  then transaction_count else 0 end)
    ) as trans_count_approved,
    (sum(case when (trans.tran_type_description in 
    ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL') and approval_flag = 'D') then transaction_count else 0 end)
    -
    sum(case when (trans.tran_type_description in 
    ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL') and approval_flag = 'D')  then transaction_count else 0 end)
    ) as trans_count_declined,
  sum(case when (trans.tran_type_description in ('REFUND','REFUND REVERSAL','PURCHASE','PURCHASE REVERSAL','PURCHASE WITH CASHOUT','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION','REDEMPTION REVERSAL') and approval_flag = 'A') then settlement_amount_inc_gst else 0 end) as trans_amount_approved,
  sum(case when (trans.tran_type_description in ('REFUND','REFUND REVERSAL','PURCHASE','PURCHASE REVERSAL','PURCHASE WITH CASHOUT','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION','REDEMPTION REVERSAL') and approval_flag = 'D') then settlement_amount_inc_gst else 0 end) as trans_amount_decline,
  from
    pdh_sf_mep_vw.wpay_all_merchants_daily_fees_summary_hist trans
  left join
    pdh_ref_ds.ref_store_details stores
  on stores.store_id = trans.store_id
  left join
    pdh_ref_ds.dim_date cal
  on
    cast(trans.transaction_date as string) = cast(cal.clndr_date as string)
  group by 1,2,3,4
  order by 4
  )
)

 ,

  auth_weekly_pre as
 (
  select 
    post.*,
    (post.auth_rate-pre.auth_rate) 
    as w_benchmark
  from auth_weekly post
  left join
    auth_weekly pre
  on
  cast(date_add(cast(post.period as date), interval -7 day) as string) = cast(pre.period as string)
  and post.brand = pre.brand
  and post.merchant_group = pre.merchant_group
  and post.store_id = pre.store_id
order by 1,2,3,4,5,7
)


,

auth_total as 
(
  select * from
  -- (
  -- select * from
  --   auth_daily_pre
  -- )
  -- union all
  (
  select * from
    auth_monthly_pre
  where auth_rate > 0
  )
  -- union all
  -- (
  -- select * from
  --   auth_yearly_pre
  -- )
  union all
  (
  select * from
    auth_weekly_pre
  where auth_rate > 0
  )
)

,
------------------------------------------------------------------------------------------------------
 auth_monthly_scheme as
(
select
  'Month' as timeframe,
  brand,
  merchant_group,
  store_id,
  --card_type,
  scheme,
  'All' as declined_reason,
  period,
  trans_count_approved,
  trans_count_declined,
  case when trans_count <> 0 then 
  trans_count_approved/trans_count 
  else 0 end 
  as auth_rate,
  case when trans_count <> 0 then 
  trans_count_declined/trans_count 
  else 0 end 
  as declined_rate,
  trans_amount_approved,
  case when trans_count_approved <> 0 then 
  trans_amount_approved/trans_count_approved 
  else 0 end 
  as avg_trans_value_approved,
  trans_amount_decline,
  case when trans_count_declined <> 0 then 
  trans_amount_decline/trans_count_declined 
  else 0 end 
  as avg_trans_value_declined
from
  (
  select
    stores.brand,
    stores.merchant_group,
    trans.store_id,
    (
    case
    when scheme = 'VISA' then 'Visa'
    when scheme = 'MASTERCARD' then 'Mastercard'
    when scheme = 'GIFT CARDS' then 'Gift Cards'
    when scheme = 'PAYPAL' then 'Paypal'
    when scheme = 'AMEX' then 'Amex'
    when scheme = 'FUEL CARDS' then 'Fuel Cards'
    when scheme = 'DINERS' then 'Diners'
    when scheme = 'ALIPAY' then 'Alipay'
    when scheme = 'VERIFONE' then 'Verifone'
    else scheme
    end
    )
    as scheme,
    left(cast(transaction_date as string),7) as period,
    (sum(case when (trans.tran_type_description in 
    ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL')) then transaction_count else 0 end)
    -
    sum(case when (trans.tran_type_description in 
    ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL'))  then transaction_count else 0 end)
    ) as trans_count,
    (sum(case when (trans.tran_type_description in 
    ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL') and approval_flag = 'A') then transaction_count else 0 end)
    -
    sum(case when (trans.tran_type_description in 
    ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL') and approval_flag = 'A')  then transaction_count else 0 end)
    ) as trans_count_approved,
    (sum(case when (trans.tran_type_description in 
    ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL') and approval_flag = 'D') then transaction_count else 0 end)
    -
    sum(case when (trans.tran_type_description in 
    ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL') and approval_flag = 'D')  then transaction_count else 0 end)
    ) as trans_count_declined,
  sum(case when (trans.tran_type_description in ('REFUND','REFUND REVERSAL','PURCHASE','PURCHASE REVERSAL','PURCHASE WITH CASHOUT','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION','REDEMPTION REVERSAL') and approval_flag = 'A') then settlement_amount_inc_gst else 0 end) as trans_amount_approved,
  sum(case when (trans.tran_type_description in ('REFUND','REFUND REVERSAL','PURCHASE','PURCHASE REVERSAL','PURCHASE WITH CASHOUT','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION','REDEMPTION REVERSAL') and approval_flag = 'D') then settlement_amount_inc_gst else 0 end) as trans_amount_decline,
  from
    pdh_sf_mep_vw.wpay_all_merchants_daily_fees_summary_hist trans
  left join
    pdh_ref_ds.ref_store_details stores
  on stores.store_id = trans.store_id
  group by 1,2,3,4,5
  )
)
,

auth_monthly_scheme_pre as
 (
  select 
    post.*,
    (post.auth_rate-pre.auth_rate) 
    as w_benchmark
  from auth_monthly_scheme post
  left join
    auth_monthly_scheme pre
  on
  left(cast(date_add(cast(concat(left(post.period,7),'-01') as date), interval -1 month) as string),4) = cast(left(pre.period,4) as string)
  and substring(cast(date_add(cast(concat(left(post.period,7),'-01') as date), interval -1 month) as string),6,2)  = right(left(pre.period,7),2)
  and post.brand = pre.brand
  and post.merchant_group = pre.merchant_group
  and post.store_id = pre.store_id
  and post.scheme = pre.scheme
order by 1,2,3,4,5,7
)


,
 auth_daily_scheme as
(
select
  'Daily' as timeframe,
  brand,
  merchant_group,
  store_id,
  --card_type,
  scheme,
  'All' as declined_reason,
  period,
  trans_count_approved,
  trans_count_declined,
  case when trans_count <> 0 then 
  trans_count_approved/trans_count 
  else 0 end 
  as auth_rate,
  case when trans_count <> 0 then 
  trans_count_declined/trans_count 
  else 0 end 
  as declined_rate,
  trans_amount_approved,
  case when trans_count_approved <> 0 then 
  trans_amount_approved/trans_count_approved 
  else 0 end 
  as avg_trans_value_approved,
  trans_amount_decline,
  case when trans_count_declined <> 0 then 
  trans_amount_decline/trans_count_declined 
  else 0 end 
  as avg_trans_value_declined
from
  (
  select
    stores.brand,
    stores.merchant_group,
    trans.store_id,
    (
    case
    when scheme = 'VISA' then 'Visa'
    when scheme = 'MASTERCARD' then 'Mastercard'
    when scheme = 'GIFT CARDS' then 'Gift Cards'
    when scheme = 'PAYPAL' then 'Paypal'
    when scheme = 'AMEX' then 'Amex'
    when scheme = 'FUEL CARDS' then 'Fuel Cards'
    when scheme = 'DINERS' then 'Diners'
    when scheme = 'ALIPAY' then 'Alipay'
    when scheme = 'VERIFONE' then 'Verifone'
    else scheme
    end
    )
    as scheme,
    cast(cast(transaction_date as date)as string) as period,
    (sum(case when (trans.tran_type_description in 
    ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL')) then transaction_count else 0 end)
    -
    sum(case when (trans.tran_type_description in 
    ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL'))  then transaction_count else 0 end)
    ) as trans_count,
    (sum(case when (trans.tran_type_description in 
    ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL') and approval_flag = 'A') then transaction_count else 0 end)
    -
    sum(case when (trans.tran_type_description in 
    ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL') and approval_flag = 'A')  then transaction_count else 0 end)
    ) as trans_count_approved,
    (sum(case when (trans.tran_type_description in 
    ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL') and approval_flag = 'D') then transaction_count else 0 end)
    -
    sum(case when (trans.tran_type_description in 
    ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL') and approval_flag = 'D')  then transaction_count else 0 end)
    ) as trans_count_declined,
  sum(case when (trans.tran_type_description in ('REFUND','REFUND REVERSAL','PURCHASE','PURCHASE REVERSAL','PURCHASE WITH CASHOUT','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION','REDEMPTION REVERSAL') and approval_flag = 'A') then settlement_amount_inc_gst else 0 end) as trans_amount_approved,
  sum(case when (trans.tran_type_description in ('REFUND','REFUND REVERSAL','PURCHASE','PURCHASE REVERSAL','PURCHASE WITH CASHOUT','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION','REDEMPTION REVERSAL') and approval_flag = 'D') then settlement_amount_inc_gst else 0 end) as trans_amount_decline,
  from
    pdh_sf_mep_vw.wpay_all_merchants_daily_fees_summary_hist trans
  left join
    pdh_ref_ds.ref_store_details stores
  on stores.store_id = trans.store_id
  group by 1,2,3,4,5
  )
)
,


 auth_daily_scheme_pre as
 (
  select 
    post.*,
    (post.auth_rate-pre.auth_rate) 
    as w_benchmark
  from auth_daily_scheme post
  left join
    auth_daily_scheme pre
  on
  cast(date_add(cast(post.period as date), interval -1 day) as string) = cast(pre.period as string)
  and post.brand = pre.brand
  and post.merchant_group = pre.merchant_group
  and post.store_id = pre.store_id
  and post.scheme = pre.scheme
order by 1,2,3,4,5,7
)
,

 auth_yearly_scheme as
(
select
  'Year' as timeframe,
  brand,
  merchant_group,
  store_id,
  --card_type,
  scheme,
  'All' as declined_reason,
  period,
  trans_count_approved,
  trans_count_declined,
  case when trans_count <> 0 then 
  trans_count_approved/trans_count 
  else 0 end 
  as auth_rate,
  case when trans_count <> 0 then 
  trans_count_declined/trans_count 
  else 0 end 
  as declined_rate,
  trans_amount_approved,
  case when trans_count_approved <> 0 then 
  trans_amount_approved/trans_count_approved 
  else 0 end 
  as avg_trans_value_approved,
  trans_amount_decline,
  case when trans_count_declined <> 0 then 
  trans_amount_decline/trans_count_declined 
  else 0 end 
  as avg_trans_value_declined
from
  (
  select
    stores.brand,
    stores.merchant_group,
    trans.store_id,
    (
    case
    when scheme = 'VISA' then 'Visa'
    when scheme = 'MASTERCARD' then 'Mastercard'
    when scheme = 'GIFT CARDS' then 'Gift Cards'
    when scheme = 'PAYPAL' then 'Paypal'
    when scheme = 'AMEX' then 'Amex'
    when scheme = 'FUEL CARDS' then 'Fuel Cards'
    when scheme = 'DINERS' then 'Diners'
    when scheme = 'ALIPAY' then 'Alipay'
    when scheme = 'VERIFONE' then 'Verifone'
    else scheme
    end
    )
    as scheme,
    left(cast(transaction_date as string),4) as period,
    (sum(case when (trans.tran_type_description in 
    ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL')) then transaction_count else 0 end)
    -
    sum(case when (trans.tran_type_description in 
    ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL'))  then transaction_count else 0 end)
    ) as trans_count,
    (sum(case when (trans.tran_type_description in 
    ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL') and approval_flag = 'A') then transaction_count else 0 end)
    -
    sum(case when (trans.tran_type_description in 
    ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL') and approval_flag = 'A')  then transaction_count else 0 end)
    ) as trans_count_approved,
    (sum(case when (trans.tran_type_description in 
    ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL') and approval_flag = 'D') then transaction_count else 0 end)
    -
    sum(case when (trans.tran_type_description in 
    ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL') and approval_flag = 'D')  then transaction_count else 0 end)
    ) as trans_count_declined,
  sum(case when (trans.tran_type_description in ('REFUND','REFUND REVERSAL','PURCHASE','PURCHASE REVERSAL','PURCHASE WITH CASHOUT','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION','REDEMPTION REVERSAL') and approval_flag = 'A') then settlement_amount_inc_gst else 0 end) as trans_amount_approved,
  sum(case when (trans.tran_type_description in ('REFUND','REFUND REVERSAL','PURCHASE','PURCHASE REVERSAL','PURCHASE WITH CASHOUT','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION','REDEMPTION REVERSAL') and approval_flag = 'D') then settlement_amount_inc_gst else 0 end) as trans_amount_decline,
  from
    pdh_sf_mep_vw.wpay_all_merchants_daily_fees_summary_hist trans
  left join
    pdh_ref_ds.ref_store_details stores
  on stores.store_id = trans.store_id
  group by 1,2,3,4,5
  )
)
,

 auth_yearly_scheme_pre as
 (
  select 
    post.*,
    (post.auth_rate-pre.auth_rate) 
    as w_benchmark
  from auth_yearly_scheme post
  left join
    auth_yearly_scheme pre
  on
  cast((cast(post.period as int)-1) as string) = cast(pre.period as string)
  and post.brand = pre.brand
  and post.merchant_group = pre.merchant_group
  and post.store_id = pre.store_id
  and post.scheme = pre.scheme
order by 1,2,3,4,5,7
)
,

 auth_weekly_scheme as
(
select
  'Week' as timeframe,
  brand,
  merchant_group,
  store_id,
  --card_type,
  scheme as scheme,
  'All' as declined_reason,
  period,
  trans_count_approved,
  trans_count_declined,
  case when trans_count <> 0 then 
  trans_count_approved/trans_count 
  else 0 end 
  as auth_rate,
  case when trans_count <> 0 then 
  trans_count_declined/trans_count 
  else 0 end 
  as declined_rate,
  trans_amount_approved,
  case when trans_count_approved <> 0 then 
  trans_amount_approved/trans_count_approved 
  else 0 end 
  as avg_trans_value_approved,
  trans_amount_decline,
  case when trans_count_declined <> 0 then 
  trans_amount_decline/trans_count_declined 
  else 0 end 
  as avg_trans_value_declined
from
  (
  select
    stores.brand,
    stores.merchant_group,
    trans.store_id,
    (
    case
    when scheme = 'VISA' then 'Visa'
    when scheme = 'MASTERCARD' then 'Mastercard'
    when scheme = 'GIFT CARDS' then 'Gift Cards'
    when scheme = 'PAYPAL' then 'Paypal'
    when scheme = 'AMEX' then 'Amex'
    when scheme = 'FUEL CARDS' then 'Fuel Cards'
    when scheme = 'DINERS' then 'Diners'
    when scheme = 'ALIPAY' then 'Alipay'
    when scheme = 'VERIFONE' then 'Verifone'
    else scheme
    end
    )
    as scheme,
    -- concat(left(cast(cal.clndr_date as string),4),concat('-',RIGHT(CONCAT('00', COALESCE(cast(cal.FiscalPeriodWeekNumber as string),'')), 2))) as period,
    cast(cal.fw_start_date as string) as period,
    (sum(case when (trans.tran_type_description in 
    ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL')) then transaction_count else 0 end)
    -
    sum(case when (trans.tran_type_description in 
    ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL'))  then transaction_count else 0 end)
    ) as trans_count,
    (sum(case when (trans.tran_type_description in 
    ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL') and approval_flag = 'A') then transaction_count else 0 end)
    -
    sum(case when (trans.tran_type_description in 
    ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL') and approval_flag = 'A')  then transaction_count else 0 end)
    ) as trans_count_approved,
    (sum(case when (trans.tran_type_description in 
    ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL') and approval_flag = 'D') then transaction_count else 0 end)
    -
    sum(case when (trans.tran_type_description in 
    ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL') and approval_flag = 'D')  then transaction_count else 0 end)
    ) as trans_count_declined,
  sum(case when (trans.tran_type_description in ('REFUND','REFUND REVERSAL','PURCHASE','PURCHASE REVERSAL','PURCHASE WITH CASHOUT','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION','REDEMPTION REVERSAL') and approval_flag = 'A') then settlement_amount_inc_gst else 0 end) as trans_amount_approved,
  sum(case when (trans.tran_type_description in ('REFUND','REFUND REVERSAL','PURCHASE','PURCHASE REVERSAL','PURCHASE WITH CASHOUT','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION','REDEMPTION REVERSAL') and approval_flag = 'D') then settlement_amount_inc_gst else 0 end) as trans_amount_decline,
  from
    pdh_sf_mep_vw.wpay_all_merchants_daily_fees_summary_hist trans
  left join
    pdh_ref_ds.ref_store_details stores
  on stores.store_id = trans.store_id
  left join
    pdh_ref_ds.dim_date cal
  on
    cast(trans.transaction_date as string) = cast(cal.clndr_date as string)
  group by 1,2,3,4,5
  order by 4
  )
)

 ,

  auth_weekly_scheme_pre as
 (
  select 
    post.*,
    (post.auth_rate-pre.auth_rate) 
    as w_benchmark
  from auth_weekly_scheme post
  left join
    auth_weekly_scheme pre
  on
  cast(date_add(cast(post.period as date), interval -7 day) as string) = cast(pre.period as string)
  and post.scheme = pre.scheme
  and post.brand = pre.brand
  and post.merchant_group = pre.merchant_group
  and post.store_id = pre.store_id
order by 1,2,3,4,5,7
)

,
auth_total_scheme as 
(
  select * from
  -- (
  -- select * from
  --   auth_daily_scheme_pre
  -- )
  -- union all
  (
  select * from
    auth_monthly_scheme_pre
   )
  -- union all
  -- (
  -- select * from
  --   auth_yearly_scheme_pre
  -- )
    union all
  (
  select * from
    auth_weekly_scheme_pre
  )
)

,
------------------------------------------------------------------------------------------------------
-- DECLINE REASON

 auth_monthly_reason as
(
select
  'Month' as timeframe,
  brand,
  merchant_group,
  store_id,
  --card_type,
  'All' as scheme,
  declined_reason as declined_reason,
  period,
  trans_count_approved,
  trans_count_declined,
  case when trans_count <> 0 then 
  trans_count_approved/trans_count 
  else 0 end 
  as auth_rate,
  case when trans_count <> 0 then 
  trans_count_declined/trans_count 
  else 0 end 
  as declined_rate,
  trans_amount_approved,
  case when trans_count_approved <> 0 then 
  trans_amount_approved/trans_count_approved 
  else 0 end 
  as avg_trans_value_approved,
  trans_amount_decline,
  case when trans_count_declined <> 0 then 
  trans_amount_decline/trans_count_declined 
  else 0 end 
  as avg_trans_value_declined
from
  (
  select
    stores.brand,
    stores.merchant_group,
    stores.store_id,
    codes.description as declined_reason,
    left(cast(tstamp_trans as string),7) as period,
    (sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL')) then 1 else 0 end)
    -
    sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL'))  then 1 else 0 end)
    ) as trans_count,
    (sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL') and trans.approval_flag = 'Y') then 1 else 0 end)
    -
    sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL') and trans.approval_flag = 'Y')  then 1 else 0 end)
    ) as trans_count_approved,
    (sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL') and trans.approval_flag = 'N') then 1 else 0 end)
    -
    sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL') and trans.approval_flag = 'N')  then 1 else 0 end)
    ) as trans_count_declined,
  (sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL') and trans.approval_flag = 'Y') then transaction_total/100 else 0 end)
    -
    sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL') and trans.approval_flag = 'Y')  then transaction_total/100 else 0 end)
    ) as trans_amount_approved,
(sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL') and trans.approval_flag = 'N') then transaction_total/100 else 0 end)
    -
    sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL') and trans.approval_flag = 'N')  then transaction_total/100 else 0 end)
    ) as trans_amount_decline
  from
    wpay_analytics.wpay_billing_engine_tran_dtl trans
    left join
    pdh_ref_ds.ref_store_details stores
  on left(trans.net_term_id,5) = stores.store_id
  left join
    wpay_analytics.ref_response_code codes
  on
    codes.action_code = trans.act_code and
    codes.response_code = trans.response_code
  group by 1,2,3,4,5
  )
)
,

auth_monthly_reason_pre as
 (
  select 
    post.*,
    (post.auth_rate-pre.auth_rate) 
    as w_benchmark
  from auth_monthly_reason post
  left join
    auth_monthly_reason pre
  on
  left(cast(date_add(cast(concat(left(post.period,7),'-01') as date), interval -1 month) as string),4) = cast(left(pre.period,4) as string)
  and substring(cast(date_add(cast(concat(left(post.period,7),'-01') as date), interval -1 month) as string),6,2)  = right(left(pre.period,7),2)
  and post.brand = pre.brand
  and post.merchant_group = pre.merchant_group
  and post.store_id = pre.store_id
  and post.declined_reason = pre.declined_reason
order by 1,2,3,4,5,7
)


,
 auth_daily_reason as
(
select
  'Daily' as timeframe,
  brand,
  merchant_group,
  store_id,
  --card_type,
  'All' as scheme,
  declined_reason as declined_reason,
  period,
  trans_count_approved,
  trans_count_declined,
  case when trans_count <> 0 then 
  trans_count_approved/trans_count 
  else 0 end 
  as auth_rate,
  case when trans_count <> 0 then 
  trans_count_declined/trans_count 
  else 0 end 
  as declined_rate,
  trans_amount_approved,
  case when trans_count_approved <> 0 then 
  trans_amount_approved/trans_count_approved 
  else 0 end 
  as avg_trans_value_approved,
  trans_amount_decline,
  case when trans_count_declined <> 0 then 
  trans_amount_decline/trans_count_declined 
  else 0 end 
  as avg_trans_value_declined
from
  (
  select
    stores.brand,
    stores.merchant_group,
    stores.store_id,
    codes.description as declined_reason,
    cast(cast(tstamp_trans as date)as string) as period,
    (sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL')) then 1 else 0 end)
    -
    sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL'))  then 1 else 0 end)
    ) as trans_count,
    (sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL') and trans.approval_flag = 'Y') then 1 else 0 end)
    -
    sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL') and trans.approval_flag = 'Y')  then 1 else 0 end)
    ) as trans_count_approved,
    (sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL') and trans.approval_flag = 'N') then 1 else 0 end)
    -
    sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL') and trans.approval_flag = 'N')  then 1 else 0 end)
    ) as trans_count_declined,
  (sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL') and trans.approval_flag = 'Y') then transaction_total/100 else 0 end)
    -
    sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL') and trans.approval_flag = 'Y')  then transaction_total/100 else 0 end)
    ) as trans_amount_approved,
(sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL') and trans.approval_flag = 'N') then transaction_total/100 else 0 end)
    -
    sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL') and trans.approval_flag = 'N')  then transaction_total/100 else 0 end)
    ) as trans_amount_decline
  from
    wpay_analytics.wpay_billing_engine_tran_dtl trans
    left join
    pdh_ref_ds.ref_store_details stores
  on left(trans.net_term_id,5) = stores.store_id
  left join
    wpay_analytics.ref_response_code codes
  on
    codes.action_code = trans.act_code and
    codes.response_code = trans.response_code
  group by 1,2,3,4,5
  )
)
,


 auth_daily_reason_pre as
 (
  select 
    post.*,
    (post.auth_rate-pre.auth_rate) 
    as w_benchmark
  from auth_daily_reason post
  left join
    auth_daily_reason pre
  on
  cast(date_add(cast(post.period as date), interval -1 day) as string) = cast(pre.period as string)
  and post.brand = pre.brand
  and post.merchant_group = pre.merchant_group
  and post.store_id = pre.store_id
  and post.declined_reason = pre.declined_reason
order by 1,2,3,4,5,7
)
,

 auth_yearly_reason as
(
select
  'Year' as timeframe,
  brand,
  merchant_group,
  store_id,
  --card_type,
  'All' as scheme,
  declined_reason as declined_reason,
  period,
  trans_count_approved,
  trans_count_declined,
  case when trans_count <> 0 then 
  trans_count_approved/trans_count 
  else 0 end 
  as auth_rate,
  case when trans_count <> 0 then 
  trans_count_declined/trans_count 
  else 0 end 
  as declined_rate,
  trans_amount_approved,
  case when trans_count_approved <> 0 then 
  trans_amount_approved/trans_count_approved 
  else 0 end 
  as avg_trans_value_approved,
  trans_amount_decline,
  case when trans_count_declined <> 0 then 
  trans_amount_decline/trans_count_declined 
  else 0 end 
  as avg_trans_value_declined
from
  (
  select
    stores.brand,
    stores.merchant_group,
    stores.store_id,
    codes.description as declined_reason,
    left(cast(tstamp_trans as string),4) as period,
    (sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL')) then 1 else 0 end)
    -
    sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL'))  then 1 else 0 end)
    ) as trans_count,
    (sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL') and trans.approval_flag = 'Y') then 1 else 0 end)
    -
    sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL') and trans.approval_flag = 'Y')  then 1 else 0 end)
    ) as trans_count_approved,
    (sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL') and trans.approval_flag = 'N') then 1 else 0 end)
    -
    sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL') and trans.approval_flag = 'N')  then 1 else 0 end)
    ) as trans_count_declined,
  (sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL') and trans.approval_flag = 'Y') then transaction_total/100 else 0 end)
    -
    sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL') and trans.approval_flag = 'Y')  then transaction_total/100 else 0 end)
    ) as trans_amount_approved,
(sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL') and trans.approval_flag = 'N') then transaction_total/100 else 0 end)
    -
    sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL') and trans.approval_flag = 'N')  then transaction_total/100 else 0 end)
    ) as trans_amount_decline
  from
    wpay_analytics.wpay_billing_engine_tran_dtl trans
    left join
    pdh_ref_ds.ref_store_details stores
  on left(trans.net_term_id,5) = stores.store_id
  left join
    wpay_analytics.ref_response_code codes
  on
    codes.action_code = trans.act_code and
    codes.response_code = trans.response_code
  group by 1,2,3,4,5
  )
)
,

 auth_yearly_reason_pre as
 (
  select 
    post.*,
    (post.auth_rate-pre.auth_rate) 
    as w_benchmark
  from auth_yearly_reason post
  left join
    auth_yearly_reason pre
  on
  cast((cast(post.period as int)-1) as string) = cast(pre.period as string)
  and post.brand = pre.brand
  and post.merchant_group = pre.merchant_group
  and post.store_id = pre.store_id
  and post.declined_reason = pre.declined_reason
order by 1,2,3,4,5,7
)
,

 auth_weekly_reason as
(
select
  'Week' as timeframe,
  brand,
  merchant_group,
  store_id,
  --card_type,
  'All' as scheme,
  declined_reason as declined_reason,
  period,
  trans_count_approved,
  trans_count_declined,
  case when trans_count <> 0 then 
  trans_count_approved/trans_count 
  else 0 end 
  as auth_rate,
  case when trans_count <> 0 then 
  trans_count_declined/trans_count 
  else 0 end 
  as declined_rate,
  trans_amount_approved,
  case when trans_count_approved <> 0 then 
  trans_amount_approved/trans_count_approved 
  else 0 end 
  as avg_trans_value_approved,
  trans_amount_decline,
  case when trans_count_declined <> 0 then 
  trans_amount_decline/trans_count_declined 
  else 0 end 
  as avg_trans_value_declined
from
  (
  select
    stores.brand,
    stores.merchant_group,
    stores.store_id,
    codes.description as declined_reason,
    -- concat(left(cast(cal.clndr_date as string),4),concat('-',RIGHT(CONCAT('00', COALESCE(cast(cal.FiscalPeriodWeekNumber as string),'')), 2))) as period,
    cast(cal.fw_start_date as string) as period,
    (sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL')) then 1 else 0 end)
    -
    sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL'))  then 1 else 0 end)
    ) as trans_count,
    (sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL') and trans.approval_flag = 'Y') then 1 else 0 end)
    -
    sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL') and trans.approval_flag = 'Y')  then 1 else 0 end)
    ) as trans_count_approved,
    (sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL') and trans.approval_flag = 'N') then 1 else 0 end)
    -
    sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL') and trans.approval_flag = 'N')  then 1 else 0 end)
    ) as trans_count_declined,
  (sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL') and trans.approval_flag = 'Y') then transaction_total/100 else 0 end)
    -
    sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL') and trans.approval_flag = 'Y')  then transaction_total/100 else 0 end)
    ) as trans_amount_approved,
(sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL') and trans.approval_flag = 'N') then transaction_total/100 else 0 end)
    -
    sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL') and trans.approval_flag = 'N')  then transaction_total/100 else 0 end)
    ) as trans_amount_decline
  from
    wpay_analytics.wpay_billing_engine_tran_dtl trans

  left join
    pdh_ref_ds.dim_date cal
  on
    left(cast(trans.tstamp_trans as string),10) = cast(cal.clndr_date as string)
    left join
    pdh_ref_ds.ref_store_details stores
  on left(trans.net_term_id,5) = stores.store_id
  left join
    wpay_analytics.ref_response_code codes
  on
    codes.action_code = trans.act_code and
    codes.response_code = trans.response_code
  group by 1,2,3,4,5
  order by 4
  )
)

 ,

  auth_weekly_reason_pre as
 (
  select 
    post.*,
    (post.auth_rate-pre.auth_rate) 
    as w_benchmark
  from auth_weekly_reason post
  left join
    auth_weekly_reason pre
  on
  cast(date_add(cast(post.period as date), interval -7 day) as string) = cast(pre.period as string)
  and post.declined_reason = pre.declined_reason
  and post.brand = pre.brand
  and post.merchant_group = pre.merchant_group
  and post.store_id = pre.store_id
order by 1,2,3,4,5,7
)

,
auth_total_reason as 
(
  select * from
  -- (
  -- select * from
  --   auth_daily_reason_pre
  -- )
  -- union all
  (
  select * from
    auth_monthly_reason_pre
   )
  -- union all
  -- (
  -- select * from
  --   auth_yearly_reason_pre
  -- )
    union all
  (
  select * from
    auth_weekly_reason_pre
  )
)
,

-------------------------------------------------------------------------------------------
-- GROUP AT BRAND/MG LEVEL

 auth_weekly_tot as
(
select
  'Week' as timeframe,
  brand,
  merchant_group,
  'All' as store_id,
  --card_type,
  'All' as scheme,
  'All' as declined_reason,
  period,
  trans_count_approved,
  trans_count_declined,
  case when trans_count <> 0 then 
  trans_count_approved/trans_count 
  else 0 end 
  as auth_rate,
  case when trans_count <> 0 then 
  trans_count_declined/trans_count 
  else 0 end 
  as declined_rate,
  trans_amount_approved,
  case when trans_count_approved <> 0 then 
  trans_amount_approved/trans_count_approved 
  else 0 end 
  as avg_trans_value_approved,
  trans_amount_decline,
  case when trans_count_declined <> 0 then 
  trans_amount_decline/trans_count_declined 
  else 0 end 
  as avg_trans_value_declined
from
  (
  select
   stores.brand,
   stores.merchant_group,
    --store_id,
    --scheme,
    cast(cal.fw_start_date as string) as period,
    (sum(case when (trans.tran_type_description in 
    ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL')) then transaction_count else 0 end)
    -
    sum(case when (trans.tran_type_description in 
    ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL'))  then transaction_count else 0 end)
    ) as trans_count,
    (sum(case when (trans.tran_type_description in 
    ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL') and approval_flag = 'A') then transaction_count else 0 end)
    -
    sum(case when (trans.tran_type_description in 
    ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL') and approval_flag = 'A')  then transaction_count else 0 end)
    ) as trans_count_approved,
    (sum(case when (trans.tran_type_description in 
    ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL') and approval_flag = 'D') then transaction_count else 0 end)
    -
    sum(case when (trans.tran_type_description in 
    ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL') and approval_flag = 'D')  then transaction_count else 0 end)
    ) as trans_count_declined,
  sum(case when (trans.tran_type_description in ('REFUND','REFUND REVERSAL','PURCHASE','PURCHASE REVERSAL','PURCHASE WITH CASHOUT','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION','REDEMPTION REVERSAL') and approval_flag = 'A') then settlement_amount_inc_gst else 0 end) as trans_amount_approved,
  sum(case when (trans.tran_type_description in ('REFUND','REFUND REVERSAL','PURCHASE','PURCHASE REVERSAL','PURCHASE WITH CASHOUT','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION','REDEMPTION REVERSAL') and approval_flag = 'D') then settlement_amount_inc_gst else 0 end) as trans_amount_decline,
  from
    pdh_sf_mep_vw.wpay_all_merchants_daily_fees_summary_hist trans
  left join
    pdh_ref_ds.ref_store_details stores
  on stores.store_id = trans.store_id
      left join
    pdh_ref_ds.dim_date cal
  on
    cast(trans.transaction_date as string) = cast(cal.clndr_date as string)
  group by 1,2,3
  )
  order by 1,2,3,4,5,7
)
,

 auth_weekly_tot_pre as
 (
  select 
    post.*,
    (post.auth_rate-pre.auth_rate) 
    as w_benchmark
  from auth_weekly_tot post
  left join
    auth_weekly_tot pre
  on
  cast(date_add(cast(post.period as date), interval -7 day) as string) = cast(pre.period as string)
  and post.brand = pre.brand
  and post.merchant_group = pre.merchant_group
  --and post.store_id = pre.store_id
order by 1,2,3,4,5,7
)

,

 auth_monthly_tot as
(
select
  'Month' as timeframe,
  brand,
  merchant_group,
  'All' as store_id,
  --card_type,
  'All' as scheme,
  'All' as declined_reason,
  period,
  trans_count_approved,
  trans_count_declined,
  case when trans_count <> 0 then 
  trans_count_approved/trans_count 
  else 0 end 
  as auth_rate,
  case when trans_count <> 0 then 
  trans_count_declined/trans_count 
  else 0 end 
  as declined_rate,
  trans_amount_approved,
  case when trans_count_approved <> 0 then 
  trans_amount_approved/trans_count_approved 
  else 0 end 
  as avg_trans_value_approved,
  trans_amount_decline,
  case when trans_count_declined <> 0 then 
  trans_amount_decline/trans_count_declined 
  else 0 end 
  as avg_trans_value_declined
from
  (
  select
    stores.brand,
    stores.merchant_group,
    --store_id,
    --scheme,
    left(cast(transaction_date as string),7) as period,
    (sum(case when (trans.tran_type_description in 
    ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL')) then transaction_count else 0 end)
    -
    sum(case when (trans.tran_type_description in 
    ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL'))  then transaction_count else 0 end)
    ) as trans_count,
    (sum(case when (trans.tran_type_description in 
    ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL') and approval_flag = 'A') then transaction_count else 0 end)
    -
    sum(case when (trans.tran_type_description in 
    ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL') and approval_flag = 'A')  then transaction_count else 0 end)
    ) as trans_count_approved,
    (sum(case when (trans.tran_type_description in 
    ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL') and approval_flag = 'D') then transaction_count else 0 end)
    -
    sum(case when (trans.tran_type_description in 
    ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL') and approval_flag = 'D')  then transaction_count else 0 end)
    ) as trans_count_declined,
  sum(case when (trans.tran_type_description in ('REFUND','REFUND REVERSAL','PURCHASE','PURCHASE REVERSAL','PURCHASE WITH CASHOUT','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION','REDEMPTION REVERSAL') and approval_flag = 'A') then settlement_amount_inc_gst else 0 end) as trans_amount_approved,
  sum(case when (trans.tran_type_description in ('REFUND','REFUND REVERSAL','PURCHASE','PURCHASE REVERSAL','PURCHASE WITH CASHOUT','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION','REDEMPTION REVERSAL') and approval_flag = 'D') then settlement_amount_inc_gst else 0 end) as trans_amount_decline,
  from
    pdh_sf_mep_vw.wpay_all_merchants_daily_fees_summary_hist trans
  left join
    pdh_ref_ds.ref_store_details stores
  on stores.store_id = trans.store_id
  group by 1,2,3
  )
  order by 1,2,3,4,5,7
)
,

 auth_monthly_tot_pre as
 (
  select 
    post.*,
    (post.auth_rate-pre.auth_rate) 
    as w_benchmark
  from auth_monthly_tot post
  left join
    auth_monthly_tot pre
  on
  left(cast(date_add(cast(concat(left(post.period,7),'-01') as date), interval -1 month) as string),4) = cast(left(pre.period,4) as string)
  and substring(cast(date_add(cast(concat(left(post.period,7),'-01') as date), interval -1 month) as string),6,2)  = right(left(pre.period,7),2)
  and post.brand = pre.brand
  and post.merchant_group = pre.merchant_group
  --and post.store_id = pre.store_id
order by 1,2,3,4,5,7
)
,

  auth_daily_tot as
(
select
  'Daily' as timeframe,
  brand,
  merchant_group,
  'All' as store_id,
  --card_type,
  'All' as scheme,
  'All' as declined_reason,
  period,
  trans_count_approved,
  trans_count_declined,
  case when trans_count <> 0 then 
  trans_count_approved/trans_count 
  else 0 end 
  as auth_rate,
  case when trans_count <> 0 then 
  trans_count_declined/trans_count 
  else 0 end 
  as declined_rate,
  trans_amount_approved,
  case when trans_count_approved <> 0 then 
  trans_amount_approved/trans_count_approved 
  else 0 end 
  as avg_trans_value_approved,
  trans_amount_decline,
  case when trans_count_declined <> 0 then 
  trans_amount_decline/trans_count_declined 
  else 0 end 
  as avg_trans_value_declined
from
  (
  select
    stores.brand,
    stores.merchant_group,
    --store_id,
    --scheme,
    cast(cast(transaction_date as date)as string) as period,
    (sum(case when (trans.tran_type_description in 
    ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL')) then transaction_count else 0 end)
    -
    sum(case when (trans.tran_type_description in 
    ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL'))  then transaction_count else 0 end)
    ) as trans_count,
    (sum(case when (trans.tran_type_description in 
    ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL') and approval_flag = 'A') then transaction_count else 0 end)
    -
    sum(case when (trans.tran_type_description in 
    ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL') and approval_flag = 'A')  then transaction_count else 0 end)
    ) as trans_count_approved,
    (sum(case when (trans.tran_type_description in 
    ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL') and approval_flag = 'D') then transaction_count else 0 end)
    -
    sum(case when (trans.tran_type_description in 
    ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL') and approval_flag = 'D')  then transaction_count else 0 end)
    ) as trans_count_declined,
  sum(case when (trans.tran_type_description in ('REFUND','REFUND REVERSAL','PURCHASE','PURCHASE REVERSAL','PURCHASE WITH CASHOUT','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION','REDEMPTION REVERSAL') and approval_flag = 'A') then settlement_amount_inc_gst else 0 end) as trans_amount_approved,
  sum(case when (trans.tran_type_description in ('REFUND','REFUND REVERSAL','PURCHASE','PURCHASE REVERSAL','PURCHASE WITH CASHOUT','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION','REDEMPTION REVERSAL') and approval_flag = 'D') then settlement_amount_inc_gst else 0 end) as trans_amount_decline,
  from
    pdh_sf_mep_vw.wpay_all_merchants_daily_fees_summary_hist trans
  left join
    pdh_ref_ds.ref_store_details stores
  on stores.store_id = trans.store_id
  group by 1,2,3
  )
)

,

 auth_daily_tot_pre as
 (
  select 
    post.*,
    (post.auth_rate-pre.auth_rate) 
    as w_benchmark
  from auth_daily_tot post
  left join
    auth_daily_tot pre
  on
  cast(date_add(cast(post.period as date), interval -1 day) as string) = cast(pre.period as string)
  and post.brand = pre.brand
  and post.merchant_group = pre.merchant_group
  --and post.store_id = pre.store_id
order by 1,2,3,4,5,7
)
,

 auth_yearly_tot as
(
select
  'Year' as timeframe,
  brand,
  merchant_group,
  'All' as store_id,
  --card_type,
  'All' as scheme,
  'All' as declined_reason,
  period,
  trans_count_approved,
  trans_count_declined,
  case when trans_count <> 0 then 
  trans_count_approved/trans_count 
  else 0 end 
  as auth_rate,
  case when trans_count <> 0 then 
  trans_count_declined/trans_count 
  else 0 end 
  as declined_rate,
  trans_amount_approved,
  case when trans_count_approved <> 0 then 
  trans_amount_approved/trans_count_approved 
  else 0 end 
  as avg_trans_value_approved,
  trans_amount_decline,
  case when trans_count_declined <> 0 then 
  trans_amount_decline/trans_count_declined 
  else 0 end 
  as avg_trans_value_declined
from
  (
  select
    stores.brand,
    stores.merchant_group,
    --store_id,
    --scheme,
    left(cast(transaction_date as string),4) as period,
    (sum(case when (trans.tran_type_description in 
    ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL')) then transaction_count else 0 end)
    -
    sum(case when (trans.tran_type_description in 
    ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL'))  then transaction_count else 0 end)
    ) as trans_count,
    (sum(case when (trans.tran_type_description in 
    ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL') and approval_flag = 'A') then transaction_count else 0 end)
    -
    sum(case when (trans.tran_type_description in 
    ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL') and approval_flag = 'A')  then transaction_count else 0 end)
    ) as trans_count_approved,
    (sum(case when (trans.tran_type_description in 
    ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL') and approval_flag = 'D') then transaction_count else 0 end)
    -
    sum(case when (trans.tran_type_description in 
    ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL') and approval_flag = 'D')  then transaction_count else 0 end)
    ) as trans_count_declined,
  sum(case when (trans.tran_type_description in ('REFUND','REFUND REVERSAL','PURCHASE','PURCHASE REVERSAL','PURCHASE WITH CASHOUT','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION','REDEMPTION REVERSAL') and approval_flag = 'A') then settlement_amount_inc_gst else 0 end) as trans_amount_approved,
  sum(case when (trans.tran_type_description in ('REFUND','REFUND REVERSAL','PURCHASE','PURCHASE REVERSAL','PURCHASE WITH CASHOUT','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION','REDEMPTION REVERSAL') and approval_flag = 'D') then settlement_amount_inc_gst else 0 end) as trans_amount_decline,
  from
    pdh_sf_mep_vw.wpay_all_merchants_daily_fees_summary_hist trans
  left join
    pdh_ref_ds.ref_store_details stores
  on stores.store_id = trans.store_id
  group by 1,2,3
  )
)

,

 auth_yearly_tot_pre as
 (
  select 
    post.*,
    (post.auth_rate-pre.auth_rate) 
    as w_benchmark
  from auth_yearly_tot post
  left join
    auth_yearly_tot pre
  on
  cast((cast(post.period as int)-1) as string) = cast(pre.period as string)
  and post.brand = pre.brand
  and post.merchant_group = pre.merchant_group
  --and post.store_id = pre.store_id
order by 1,2,3,4,5,7
)


,

auth_total_tot as 
(
  select * from
  -- (
  -- select * from
  --   auth_daily_tot_pre
  -- )
  -- union all
  (
  select * from
    auth_monthly_tot_pre
  )
  -- union all
  -- (
  -- select * from
  --   auth_yearly_tot_pre
  -- )
    union all
  (
  select * from
    auth_weekly_tot_pre
  )
)
,
-------------------------------------------------------------------------------------------
-- GROUP AT BRAND/MG LEVEL -- SCHEME

 auth_weekly_tot_scheme as
(
select
  'Week' as timeframe,
  brand,
  merchant_group,
  'All' as store_id,
  --card_type,
  scheme as scheme,
  'All' as declined_reason,
  period,
  trans_count_approved,
  trans_count_declined,
  case when trans_count <> 0 then 
  trans_count_approved/trans_count 
  else 0 end 
  as auth_rate,
  case when trans_count <> 0 then 
  trans_count_declined/trans_count 
  else 0 end 
  as declined_rate,
  trans_amount_approved,
  case when trans_count_approved <> 0 then 
  trans_amount_approved/trans_count_approved 
  else 0 end 
  as avg_trans_value_approved,
  trans_amount_decline,
  case when trans_count_declined <> 0 then 
  trans_amount_decline/trans_count_declined 
  else 0 end 
  as avg_trans_value_declined
from
  (
  select
    stores.brand,
    stores.merchant_group,
    --store_id,
    (
    case
    when scheme = 'VISA' then 'Visa'
    when scheme = 'MASTERCARD' then 'Mastercard'
    when scheme = 'GIFT CARDS' then 'Gift Cards'
    when scheme = 'PAYPAL' then 'Paypal'
    when scheme = 'AMEX' then 'Amex'
    when scheme = 'FUEL CARDS' then 'Fuel Cards'
    when scheme = 'DINERS' then 'Diners'
    when scheme = 'ALIPAY' then 'Alipay'
    when scheme = 'VERIFONE' then 'Verifone'
    else scheme
    end
    )
    as scheme,
    cast(cal.fw_start_date as string) as period,
    (sum(case when (trans.tran_type_description in 
    ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL')) then transaction_count else 0 end)
    -
    sum(case when (trans.tran_type_description in 
    ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL'))  then transaction_count else 0 end)
    ) as trans_count,
    (sum(case when (trans.tran_type_description in 
    ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL') and approval_flag = 'A') then transaction_count else 0 end)
    -
    sum(case when (trans.tran_type_description in 
    ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL') and approval_flag = 'A')  then transaction_count else 0 end)
    ) as trans_count_approved,
    (sum(case when (trans.tran_type_description in 
    ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL') and approval_flag = 'D') then transaction_count else 0 end)
    -
    sum(case when (trans.tran_type_description in 
    ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL') and approval_flag = 'D')  then transaction_count else 0 end)
    ) as trans_count_declined,
  sum(case when (trans.tran_type_description in ('REFUND','REFUND REVERSAL','PURCHASE','PURCHASE REVERSAL','PURCHASE WITH CASHOUT','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION','REDEMPTION REVERSAL') and approval_flag = 'A') then settlement_amount_inc_gst else 0 end) as trans_amount_approved,
  sum(case when (trans.tran_type_description in ('REFUND','REFUND REVERSAL','PURCHASE','PURCHASE REVERSAL','PURCHASE WITH CASHOUT','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION','REDEMPTION REVERSAL') and approval_flag = 'D') then settlement_amount_inc_gst else 0 end) as trans_amount_decline,
  from
    pdh_sf_mep_vw.wpay_all_merchants_daily_fees_summary_hist trans
  left join
    pdh_ref_ds.ref_store_details stores
  on stores.store_id = trans.store_id
      left join
    pdh_ref_ds.dim_date cal
  on
    cast(trans.transaction_date as string) = cast(cal.clndr_date as string)
  group by 1,2,3,4
  )
  order by 1,2,3,4,5,7
)
,

 auth_weekly_tot_pre_scheme as
 (
  select 
    post.*,
    (post.auth_rate-pre.auth_rate) 
    as w_benchmark
  from auth_weekly_tot_scheme post
  left join
    auth_weekly_tot_scheme pre
  on
  cast(date_add(cast(post.period as date), interval -7 day) as string) = cast(pre.period as string)
  and post.brand = pre.brand
  and post.merchant_group = pre.merchant_group
  and post.scheme = pre.scheme
  --and post.store_id = pre.store_id
order by 1,2,3,4,5,7
)

,

 auth_monthly_tot_scheme as
(
select
  'Month' as timeframe,
  brand,
  merchant_group,
  'All' as store_id,
  --card_type,
  scheme as scheme,
  'All' as declined_reason,
  period,
  trans_count_approved,
  trans_count_declined,
  case when trans_count <> 0 then 
  trans_count_approved/trans_count 
  else 0 end 
  as auth_rate,
  case when trans_count <> 0 then 
  trans_count_declined/trans_count 
  else 0 end 
  as declined_rate,
  trans_amount_approved,
  case when trans_count_approved <> 0 then 
  trans_amount_approved/trans_count_approved 
  else 0 end 
  as avg_trans_value_approved,
  trans_amount_decline,
  case when trans_count_declined <> 0 then 
  trans_amount_decline/trans_count_declined 
  else 0 end 
  as avg_trans_value_declined
from
  (
  select
    stores.brand,
    stores.merchant_group,
    --store_id,
    (
    case
    when scheme = 'VISA' then 'Visa'
    when scheme = 'MASTERCARD' then 'Mastercard'
    when scheme = 'GIFT CARDS' then 'Gift Cards'
    when scheme = 'PAYPAL' then 'Paypal'
    when scheme = 'AMEX' then 'Amex'
    when scheme = 'FUEL CARDS' then 'Fuel Cards'
    when scheme = 'DINERS' then 'Diners'
    when scheme = 'ALIPAY' then 'Alipay'
    when scheme = 'VERIFONE' then 'Verifone'
    else scheme
    end
    )
    as scheme,
    left(cast(transaction_date as string),7) as period,
    (sum(case when (trans.tran_type_description in 
    ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL')) then transaction_count else 0 end)
    -
    sum(case when (trans.tran_type_description in 
    ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL'))  then transaction_count else 0 end)
    ) as trans_count,
    (sum(case when (trans.tran_type_description in 
    ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL') and approval_flag = 'A') then transaction_count else 0 end)
    -
    sum(case when (trans.tran_type_description in 
    ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL') and approval_flag = 'A')  then transaction_count else 0 end)
    ) as trans_count_approved,
    (sum(case when (trans.tran_type_description in 
    ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL') and approval_flag = 'D') then transaction_count else 0 end)
    -
    sum(case when (trans.tran_type_description in 
    ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL') and approval_flag = 'D')  then transaction_count else 0 end)
    ) as trans_count_declined,
  sum(case when (trans.tran_type_description in ('REFUND','REFUND REVERSAL','PURCHASE','PURCHASE REVERSAL','PURCHASE WITH CASHOUT','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION','REDEMPTION REVERSAL') and approval_flag = 'A') then settlement_amount_inc_gst else 0 end) as trans_amount_approved,
  sum(case when (trans.tran_type_description in ('REFUND','REFUND REVERSAL','PURCHASE','PURCHASE REVERSAL','PURCHASE WITH CASHOUT','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION','REDEMPTION REVERSAL') and approval_flag = 'D') then settlement_amount_inc_gst else 0 end) as trans_amount_decline,
  from
    pdh_sf_mep_vw.wpay_all_merchants_daily_fees_summary_hist trans
  left join
    pdh_ref_ds.ref_store_details stores
  on stores.store_id = trans.store_id
  group by 1,2,3,4
  )
  order by 1,2,3,4,5,7
)
,

 auth_monthly_tot_pre_scheme as
 (
  select 
    post.*,
    (post.auth_rate-pre.auth_rate) 
    as w_benchmark
  from auth_monthly_tot_scheme post
  left join
    auth_monthly_tot_scheme pre
  on
  left(cast(date_add(cast(concat(left(post.period,7),'-01') as date), interval -1 month) as string),4) = cast(left(pre.period,4) as string)
  and substring(cast(date_add(cast(concat(left(post.period,7),'-01') as date), interval -1 month) as string),6,2)  = right(left(pre.period,7),2)
  and post.brand = pre.brand
  and post.merchant_group = pre.merchant_group
  and post.scheme = pre.scheme
  --and post.store_id = pre.store_id
order by 1,2,3,4,5,7
)
,

  auth_daily_tot_scheme as
(
select
  'Daily' as timeframe,
  brand,
  merchant_group,
  'All' as store_id,
  --card_type,
  scheme as scheme,
  'All' as declined_reason,
  period,
  trans_count_approved,
  trans_count_declined,
  case when trans_count <> 0 then 
  trans_count_approved/trans_count 
  else 0 end 
  as auth_rate,
  case when trans_count <> 0 then 
  trans_count_declined/trans_count 
  else 0 end 
  as declined_rate,
  trans_amount_approved,
  case when trans_count_approved <> 0 then 
  trans_amount_approved/trans_count_approved 
  else 0 end 
  as avg_trans_value_approved,
  trans_amount_decline,
  case when trans_count_declined <> 0 then 
  trans_amount_decline/trans_count_declined 
  else 0 end 
  as avg_trans_value_declined
from
  (
  select
    stores.brand,
    stores.merchant_group,
    --store_id,
    (
    case
    when scheme = 'VISA' then 'Visa'
    when scheme = 'MASTERCARD' then 'Mastercard'
    when scheme = 'GIFT CARDS' then 'Gift Cards'
    when scheme = 'PAYPAL' then 'Paypal'
    when scheme = 'AMEX' then 'Amex'
    when scheme = 'FUEL CARDS' then 'Fuel Cards'
    when scheme = 'DINERS' then 'Diners'
    when scheme = 'ALIPAY' then 'Alipay'
    when scheme = 'VERIFONE' then 'Verifone'
    else scheme
    end
    )
    as scheme,
    cast(cast(transaction_date as date)as string) as period,
    (sum(case when (trans.tran_type_description in 
    ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL')) then transaction_count else 0 end)
    -
    sum(case when (trans.tran_type_description in 
    ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL'))  then transaction_count else 0 end)
    ) as trans_count,
    (sum(case when (trans.tran_type_description in 
    ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL') and approval_flag = 'A') then transaction_count else 0 end)
    -
    sum(case when (trans.tran_type_description in 
    ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL') and approval_flag = 'A')  then transaction_count else 0 end)
    ) as trans_count_approved,
    (sum(case when (trans.tran_type_description in 
    ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL') and approval_flag = 'D') then transaction_count else 0 end)
    -
    sum(case when (trans.tran_type_description in 
    ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL') and approval_flag = 'D')  then transaction_count else 0 end)
    ) as trans_count_declined,
  sum(case when (trans.tran_type_description in ('REFUND','REFUND REVERSAL','PURCHASE','PURCHASE REVERSAL','PURCHASE WITH CASHOUT','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION','REDEMPTION REVERSAL') and approval_flag = 'A') then settlement_amount_inc_gst else 0 end) as trans_amount_approved,
  sum(case when (trans.tran_type_description in ('REFUND','REFUND REVERSAL','PURCHASE','PURCHASE REVERSAL','PURCHASE WITH CASHOUT','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION','REDEMPTION REVERSAL') and approval_flag = 'D') then settlement_amount_inc_gst else 0 end) as trans_amount_decline,
  from
    pdh_sf_mep_vw.wpay_all_merchants_daily_fees_summary_hist trans
  left join
    pdh_ref_ds.ref_store_details stores
  on stores.store_id = trans.store_id
  group by 1,2,3,4
  )
)

,

 auth_daily_tot_pre_scheme as
 (
  select 
    post.*,
    (post.auth_rate-pre.auth_rate) 
    as w_benchmark
  from auth_daily_tot_scheme post
  left join
    auth_daily_tot_scheme pre
  on
  cast(date_add(cast(post.period as date), interval -1 day) as string) = cast(pre.period as string)
  and post.brand = pre.brand
  and post.merchant_group = pre.merchant_group
  and post.scheme = pre.scheme
  --and post.store_id = pre.store_id
order by 1,2,3,4,5,7
)
,

 auth_yearly_tot_scheme as
(
select
  'Year' as timeframe,
  brand,
  merchant_group,
  'All' as store_id,
  --card_type,
  scheme as scheme,
  'All' as declined_reason,
  period,
  trans_count_approved,
  trans_count_declined,
  case when trans_count <> 0 then 
  trans_count_approved/trans_count 
  else 0 end 
  as auth_rate,
  case when trans_count <> 0 then 
  trans_count_declined/trans_count 
  else 0 end 
  as declined_rate,
  trans_amount_approved,
  case when trans_count_approved <> 0 then 
  trans_amount_approved/trans_count_approved 
  else 0 end 
  as avg_trans_value_approved,
  trans_amount_decline,
  case when trans_count_declined <> 0 then 
  trans_amount_decline/trans_count_declined 
  else 0 end 
  as avg_trans_value_declined
from
  (
  select
    stores.brand,
    stores.merchant_group,
    --store_id,
    (
    case
    when scheme = 'VISA' then 'Visa'
    when scheme = 'MASTERCARD' then 'Mastercard'
    when scheme = 'GIFT CARDS' then 'Gift Cards'
    when scheme = 'PAYPAL' then 'Paypal'
    when scheme = 'AMEX' then 'Amex'
    when scheme = 'FUEL CARDS' then 'Fuel Cards'
    when scheme = 'DINERS' then 'Diners'
    when scheme = 'ALIPAY' then 'Alipay'
    when scheme = 'VERIFONE' then 'Verifone'
    else scheme
    end
    )
    as scheme,
    left(cast(transaction_date as string),4) as period,
    (sum(case when (trans.tran_type_description in 
    ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL')) then transaction_count else 0 end)
    -
    sum(case when (trans.tran_type_description in 
    ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL'))  then transaction_count else 0 end)
    ) as trans_count,
    (sum(case when (trans.tran_type_description in 
    ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL') and approval_flag = 'A') then transaction_count else 0 end)
    -
    sum(case when (trans.tran_type_description in 
    ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL') and approval_flag = 'A')  then transaction_count else 0 end)
    ) as trans_count_approved,
    (sum(case when (trans.tran_type_description in 
    ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL') and approval_flag = 'D') then transaction_count else 0 end)
    -
    sum(case when (trans.tran_type_description in 
    ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL') and approval_flag = 'D')  then transaction_count else 0 end)
    ) as trans_count_declined,
  sum(case when (trans.tran_type_description in ('REFUND','REFUND REVERSAL','PURCHASE','PURCHASE REVERSAL','PURCHASE WITH CASHOUT','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION','REDEMPTION REVERSAL') and approval_flag = 'A') then settlement_amount_inc_gst else 0 end) as trans_amount_approved,
  sum(case when (trans.tran_type_description in ('REFUND','REFUND REVERSAL','PURCHASE','PURCHASE REVERSAL','PURCHASE WITH CASHOUT','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION','REDEMPTION REVERSAL') and approval_flag = 'D') then settlement_amount_inc_gst else 0 end) as trans_amount_decline,
  from
    pdh_sf_mep_vw.wpay_all_merchants_daily_fees_summary_hist trans
  left join
    pdh_ref_ds.ref_store_details stores
  on stores.store_id = trans.store_id
  group by 1,2,3,4
  )
)

,

 auth_yearly_tot_pre_scheme as
 (
  select 
    post.*,
    (post.auth_rate-pre.auth_rate) 
    as w_benchmark
  from auth_yearly_tot_scheme post
  left join
    auth_yearly_tot_scheme pre
  on
  cast((cast(post.period as int)-1) as string) = cast(pre.period as string)
  and post.brand = pre.brand
  and post.scheme = pre.scheme
  and post.merchant_group = pre.merchant_group
  --and post.store_id = pre.store_id
order by 1,2,3,4,5,7
)


,

auth_total_tot_scheme as 
(
  select * from
  -- (
  -- select * from
  --   auth_daily_tot_pre_scheme
  -- )
  -- union all
  (
  select * from
    auth_monthly_tot_pre_scheme
  )
  -- union all
  -- (
  -- select * from
  --   auth_yearly_tot_pre_scheme
  -- )
    union all
  (
  select * from
    auth_weekly_tot_pre_scheme
  )
)
,
------------------------------------------------------------------------------------------------------

-- GROUP AT BRAND/MG LEVEL -- REASON

 auth_weekly_tot_reason as
(
select
  'Week' as timeframe,
  brand,
  merchant_group,
  'All' as store_id,
  --card_type,
  'All' as scheme,
  declined_reason as declined_reason,
  period,
  trans_count_approved,
  trans_count_declined,
  case when trans_count <> 0 then 
  trans_count_approved/trans_count 
  else 0 end 
  as auth_rate,
  case when trans_count <> 0 then 
  trans_count_declined/trans_count 
  else 0 end 
  as declined_rate,
  trans_amount_approved,
  case when trans_count_approved <> 0 then 
  trans_amount_approved/trans_count_approved 
  else 0 end 
  as avg_trans_value_approved,
  trans_amount_decline,
  case when trans_count_declined <> 0 then 
  trans_amount_decline/trans_count_declined 
  else 0 end 
  as avg_trans_value_declined
from
  (
  select
    stores.brand,
    stores.merchant_group,
    --store_id,
    codes.description as declined_reason,
    cast(cal.fw_start_date as string) as period,
    (sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL')) then 1 else 0 end)
    -
    sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL'))  then 1 else 0 end)
    ) as trans_count,
    (sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL') and trans.approval_flag = 'Y') then 1 else 0 end)
    -
    sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL') and trans.approval_flag = 'Y')  then 1 else 0 end)
    ) as trans_count_approved,
    (sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL') and trans.approval_flag = 'N') then 1 else 0 end)
    -
    sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL') and trans.approval_flag = 'N')  then 1 else 0 end)
    ) as trans_count_declined,
  (sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL') and trans.approval_flag = 'Y') then transaction_total/100 else 0 end)
    -
    sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL') and trans.approval_flag = 'Y')  then transaction_total/100 else 0 end)
    ) as trans_amount_approved,
(sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL') and trans.approval_flag = 'N') then transaction_total/100 else 0 end)
    -
    sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL') and trans.approval_flag = 'N')  then transaction_total/100 else 0 end)
    ) as trans_amount_decline
  from
    wpay_analytics.wpay_billing_engine_tran_dtl trans
      left join
    pdh_ref_ds.dim_date cal
  on
    left(cast(trans.tstamp_trans as string),10) = cast(cal.clndr_date as string)
  left join
    pdh_ref_ds.ref_store_details stores
  on left(trans.net_term_id,5) = stores.store_id
  left join
    wpay_analytics.ref_response_code codes
  on
    codes.action_code = trans.act_code and
    codes.response_code = trans.response_code
  group by 1,2,3,4
  )
  order by 1,2,3,4,5,7
)
,

 auth_weekly_tot_pre_reason as
 (
  select 
    post.*,
    (post.auth_rate-pre.auth_rate) 
    as w_benchmark
  from auth_weekly_tot_reason post
  left join
    auth_weekly_tot_reason pre
  on
  cast(date_add(cast(post.period as date), interval -7 day) as string) = cast(pre.period as string)
  and post.brand = pre.brand
  and post.merchant_group = pre.merchant_group
  and post.declined_reason = pre.declined_reason
  --and post.store_id = pre.store_id
order by 1,2,3,4,5,7
)

,

 auth_monthly_tot_reason as
(
select
  'Month' as timeframe,
  brand,
  merchant_group,
  'All' as store_id,
  --card_type,
  'All' as scheme,
  declined_reason as declined_reason,
  period,
  trans_count_approved,
  trans_count_declined,
  case when trans_count <> 0 then 
  trans_count_approved/trans_count 
  else 0 end 
  as auth_rate,
  case when trans_count <> 0 then 
  trans_count_declined/trans_count 
  else 0 end 
  as declined_rate,
  trans_amount_approved,
  case when trans_count_approved <> 0 then 
  trans_amount_approved/trans_count_approved 
  else 0 end 
  as avg_trans_value_approved,
  trans_amount_decline,
  case when trans_count_declined <> 0 then 
  trans_amount_decline/trans_count_declined 
  else 0 end 
  as avg_trans_value_declined
from
  (
  select
    stores.brand,
    stores.merchant_group,
    --store_id,
    codes.description as declined_reason,
    left(cast(tstamp_trans as string),7) as period,

    (sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL')) then 1 else 0 end)
    -
    sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL'))  then 1 else 0 end)
    ) as trans_count,
    (sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL') and trans.approval_flag = 'Y') then 1 else 0 end)
    -
    sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL') and trans.approval_flag = 'Y')  then 1 else 0 end)
    ) as trans_count_approved,
    (sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL') and trans.approval_flag = 'N') then 1 else 0 end)
    -
    sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL') and trans.approval_flag = 'N')  then 1 else 0 end)
    ) as trans_count_declined,
  (sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL') and trans.approval_flag = 'Y') then transaction_total/100 else 0 end)
    -
    sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL') and trans.approval_flag = 'Y')  then transaction_total/100 else 0 end)
    ) as trans_amount_approved,
(sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL') and trans.approval_flag = 'N') then transaction_total/100 else 0 end)
    -
    sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL') and trans.approval_flag = 'N')  then transaction_total/100 else 0 end)
    ) as trans_amount_decline
  from
    wpay_analytics.wpay_billing_engine_tran_dtl trans
    left join
    pdh_ref_ds.ref_store_details stores
  on left(trans.net_term_id,5) = stores.store_id
  left join
    wpay_analytics.ref_response_code codes
  on
    codes.action_code = trans.act_code and
    codes.response_code = trans.response_code


  group by 1,2,3,4
  )
  order by 1,2,3,4,5,7
)
,

 auth_monthly_tot_pre_reason as
 (
  select 
    post.*,
    (post.auth_rate-pre.auth_rate) 
    as w_benchmark
  from auth_monthly_tot_reason post
  left join
    auth_monthly_tot_reason pre
  on
  left(cast(date_add(cast(concat(left(post.period,7),'-01') as date), interval -1 month) as string),4) = cast(left(pre.period,4) as string)
  and substring(cast(date_add(cast(concat(left(post.period,7),'-01') as date), interval -1 month) as string),6,2)  = right(left(pre.period,7),2)
  and post.brand = pre.brand
  and post.merchant_group = pre.merchant_group
  and post.declined_reason = pre.declined_reason
  --and post.store_id = pre.store_id
order by 1,2,3,4,5,7
)
,

  auth_daily_tot_reason as
(
select
  'Daily' as timeframe,
  brand,
  merchant_group,
  'All' as store_id,
  --card_type,
  'All' as scheme,
  declined_reason as declined_reason,
  period,
  trans_count_approved,
  trans_count_declined,
  case when trans_count <> 0 then 
  trans_count_approved/trans_count 
  else 0 end 
  as auth_rate,
  case when trans_count <> 0 then 
  trans_count_declined/trans_count 
  else 0 end 
  as declined_rate,
  trans_amount_approved,
  case when trans_count_approved <> 0 then 
  trans_amount_approved/trans_count_approved 
  else 0 end 
  as avg_trans_value_approved,
  trans_amount_decline,
  case when trans_count_declined <> 0 then 
  trans_amount_decline/trans_count_declined 
  else 0 end 
  as avg_trans_value_declined
from
  (
  select
    stores.brand,
    stores.merchant_group,
    --store_id,
    codes.description as declined_reason,
    cast(cast(tstamp_trans as date)as string) as period,
    (sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL')) then 1 else 0 end)
    -
    sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL'))  then 1 else 0 end)
    ) as trans_count,
    (sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL') and trans.approval_flag = 'Y') then 1 else 0 end)
    -
    sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL') and trans.approval_flag = 'Y')  then 1 else 0 end)
    ) as trans_count_approved,
    (sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL') and trans.approval_flag = 'N') then 1 else 0 end)
    -
    sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL') and trans.approval_flag = 'N')  then 1 else 0 end)
    ) as trans_count_declined,
  (sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL') and trans.approval_flag = 'Y') then transaction_total/100 else 0 end)
    -
    sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL') and trans.approval_flag = 'Y')  then transaction_total/100 else 0 end)
    ) as trans_amount_approved,
(sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL') and trans.approval_flag = 'N') then transaction_total/100 else 0 end)
    -
    sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL') and trans.approval_flag = 'N')  then transaction_total/100 else 0 end)
    ) as trans_amount_decline
  from
    wpay_analytics.wpay_billing_engine_tran_dtl trans
    left join
    pdh_ref_ds.ref_store_details stores
  on left(trans.net_term_id,5) = stores.store_id
  left join
    wpay_analytics.ref_response_code codes
  on
    codes.action_code = trans.act_code and
    codes.response_code = trans.response_code

  group by 1,2,3,4
  )
)

,

 auth_daily_tot_pre_reason as
 (
  select 
    post.*,
    (post.auth_rate-pre.auth_rate) 
    as w_benchmark
  from auth_daily_tot_reason post
  left join
    auth_daily_tot_reason pre
  on
  cast(date_add(cast(post.period as date), interval -1 day) as string) = cast(pre.period as string)
  and post.brand = pre.brand
  and post.merchant_group = pre.merchant_group
  and post.declined_reason = pre.declined_reason
  --and post.store_id = pre.store_id
order by 1,2,3,4,5,7
)
,

 auth_yearly_tot_reason as
(
select
  'Year' as timeframe,
  brand,
  merchant_group,
  'All' as store_id,
  --card_type,
  'All' as scheme,
  declined_reason as declined_reason,
  period,
  trans_count_approved,
  trans_count_declined,
  case when trans_count <> 0 then 
  trans_count_approved/trans_count 
  else 0 end 
  as auth_rate,
  case when trans_count <> 0 then 
  trans_count_declined/trans_count 
  else 0 end 
  as declined_rate,
  trans_amount_approved,
  case when trans_count_approved <> 0 then 
  trans_amount_approved/trans_count_approved 
  else 0 end 
  as avg_trans_value_approved,
  trans_amount_decline,
  case when trans_count_declined <> 0 then 
  trans_amount_decline/trans_count_declined 
  else 0 end 
  as avg_trans_value_declined
from
  (
  select
    stores.brand,
    stores.merchant_group,
    --store_id,
    codes.description as declined_reason,
    left(cast(tstamp_trans as string),4) as period,
    (sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL')) then 1 else 0 end)
    -
    sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL'))  then 1 else 0 end)
    ) as trans_count,
    (sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL') and trans.approval_flag = 'Y') then 1 else 0 end)
    -
    sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL') and trans.approval_flag = 'Y')  then 1 else 0 end)
    ) as trans_count_approved,
    (sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL') and trans.approval_flag = 'N') then 1 else 0 end)
    -
    sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL') and trans.approval_flag = 'N')  then 1 else 0 end)
    ) as trans_count_declined,
  (sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL') and trans.approval_flag = 'Y') then transaction_total/100 else 0 end)
    -
    sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL') and trans.approval_flag = 'Y')  then transaction_total/100 else 0 end)
    ) as trans_amount_approved,
(sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL') and trans.approval_flag = 'N') then transaction_total/100 else 0 end)
    -
    sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL') and trans.approval_flag = 'N')  then transaction_total/100 else 0 end)
    ) as trans_amount_decline
  from
    wpay_analytics.wpay_billing_engine_tran_dtl trans
    left join
    pdh_ref_ds.ref_store_details stores
  on left(trans.net_term_id,5) = stores.store_id
  left join
    wpay_analytics.ref_response_code codes
  on
    codes.action_code = trans.act_code and
    codes.response_code = trans.response_code
  group by 1,2,3,4
  )
)

,

 auth_yearly_tot_pre_reason as
 (
  select 
    post.*,
    (post.auth_rate-pre.auth_rate) 
    as w_benchmark
  from auth_yearly_tot_reason post
  left join
    auth_yearly_tot_reason pre
  on
  cast((cast(post.period as int)-1) as string) = cast(pre.period as string)
  and post.brand = pre.brand
  and post.declined_reason = pre.declined_reason
  and post.merchant_group = pre.merchant_group
  --and post.store_id = pre.store_id
order by 1,2,3,4,5,7
)


,

auth_total_tot_reason as 
(
  select * from
  -- (
  -- select * from
  --   auth_daily_tot_pre_reason
  -- )
  -- union all
  (
  select * from
    auth_monthly_tot_pre_reason
  )
  -- union all
  -- (
  -- select * from
  --   auth_yearly_tot_pre_reason
  -- )
    union all
  (
  select * from
    auth_weekly_tot_pre_reason
  )
)
,
------------------------------------------------------------------------------------------------------
-- FILTER COMBINATION (REASON + SCHEME)

 auth_monthly_filter as
(
select
  'Month' as timeframe,
  brand,
  merchant_group,
  store_id,
  --card_type,
  scheme as scheme,
  declined_reason as declined_reason,
  period,
  trans_count_approved,
  trans_count_declined,
  case when trans_count <> 0 then 
  trans_count_approved/trans_count 
  else 0 end 
  as auth_rate,
  case when trans_count <> 0 then 
  trans_count_declined/trans_count 
  else 0 end 
  as declined_rate,
  trans_amount_approved,
  case when trans_count_approved <> 0 then 
  trans_amount_approved/trans_count_approved 
  else 0 end 
  as avg_trans_value_approved,
  trans_amount_decline,
  case when trans_count_declined <> 0 then 
  trans_amount_decline/trans_count_declined 
  else 0 end 
  as avg_trans_value_declined
from
  (
  select
    stores.brand,
    stores.merchant_group,
    stores.store_id,
    (
    case
    when trans.calc_scheme = 'VISA' then 'Visa'
    when trans.calc_scheme = 'MASTERCARD' then 'Mastercard'
    when trans.calc_scheme = 'GIFT CARDS' then 'Gift Cards'
    when trans.calc_scheme = 'PAYPAL' then 'Paypal'
    when trans.calc_scheme = 'AMEX' then 'Amex'
    when trans.calc_scheme = 'FUEL CARDS' then 'Fuel Cards'
    when trans.calc_scheme = 'DINERS' then 'Diners'
    when trans.calc_scheme = 'ALIPAY' then 'Alipay'
    when trans.calc_scheme = 'VERIFONE' then 'Verifone'
    else trans.calc_scheme
    end
    )
    as scheme,
    codes.description as declined_reason,
    left(cast(tstamp_trans as string),7) as period,
    (sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL')) then 1 else 0 end)
    -
    sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL'))  then 1 else 0 end)
    ) as trans_count,
    (sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL') and trans.approval_flag = 'Y') then 1 else 0 end)
    -
    sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL') and trans.approval_flag = 'Y')  then 1 else 0 end)
    ) as trans_count_approved,
    (sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL') and trans.approval_flag = 'N') then 1 else 0 end)
    -
    sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL') and trans.approval_flag = 'N')  then 1 else 0 end)
    ) as trans_count_declined,
  (sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL') and trans.approval_flag = 'Y') then transaction_total/100 else 0 end)
    -
    sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL') and trans.approval_flag = 'Y')  then transaction_total/100 else 0 end)
    ) as trans_amount_approved,
(sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL') and trans.approval_flag = 'N') then transaction_total/100 else 0 end)
    -
    sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL') and trans.approval_flag = 'N')  then transaction_total/100 else 0 end)
    ) as trans_amount_decline
  from
    wpay_analytics.wpay_billing_engine_tran_dtl trans
    left join
    pdh_ref_ds.ref_store_details stores
  on left(trans.net_term_id,5) = stores.store_id
  left join
    wpay_analytics.ref_response_code codes
  on
    codes.action_code = trans.act_code and
    codes.response_code = trans.response_code
  group by 1,2,3,4,5,6
  )
)
,

auth_monthly_filter_pre as
 (
  select 
    post.*,
    (post.auth_rate-pre.auth_rate) 
    as w_benchmark
  from auth_monthly_filter post
  left join
    auth_monthly_filter pre
  on
  left(cast(date_add(cast(concat(left(post.period,7),'-01') as date), interval -1 month) as string),4) = cast(left(pre.period,4) as string)
  and substring(cast(date_add(cast(concat(left(post.period,7),'-01') as date), interval -1 month) as string),6,2)  = right(left(pre.period,7),2)
  and post.brand = pre.brand
  and post.merchant_group = pre.merchant_group
  and post.store_id = pre.store_id
  and post.declined_reason = pre.declined_reason
  and post.scheme = pre.scheme
order by 1,2,3,4,5,7
)


,
 auth_daily_filter as
(
select
  'Daily' as timeframe,
  brand,
  merchant_group,
  store_id,
  --card_type,
  scheme as scheme,
  declined_reason as declined_reason,
  period,
  trans_count_approved,
  trans_count_declined,
  case when trans_count <> 0 then 
  trans_count_approved/trans_count 
  else 0 end 
  as auth_rate,
  case when trans_count <> 0 then 
  trans_count_declined/trans_count 
  else 0 end 
  as declined_rate,
  trans_amount_approved,
  case when trans_count_approved <> 0 then 
  trans_amount_approved/trans_count_approved 
  else 0 end 
  as avg_trans_value_approved,
  trans_amount_decline,
  case when trans_count_declined <> 0 then 
  trans_amount_decline/trans_count_declined 
  else 0 end 
  as avg_trans_value_declined
from
  (
  select
    stores.brand,
    stores.merchant_group,
    stores.store_id,
    codes.description as declined_reason,
    (
    case
    when trans.calc_scheme = 'VISA' then 'Visa'
    when trans.calc_scheme = 'MASTERCARD' then 'Mastercard'
    when trans.calc_scheme = 'GIFT CARDS' then 'Gift Cards'
    when trans.calc_scheme = 'PAYPAL' then 'Paypal'
    when trans.calc_scheme = 'AMEX' then 'Amex'
    when trans.calc_scheme = 'FUEL CARDS' then 'Fuel Cards'
    when trans.calc_scheme = 'DINERS' then 'Diners'
    when trans.calc_scheme = 'ALIPAY' then 'Alipay'
    when trans.calc_scheme = 'VERIFONE' then 'Verifone'
    else trans.calc_scheme
    end
    )
    as scheme,
    cast(cast(tstamp_trans as date)as string) as period,
    (sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL')) then 1 else 0 end)
    -
    sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL'))  then 1 else 0 end)
    ) as trans_count,
    (sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL') and trans.approval_flag = 'Y') then 1 else 0 end)
    -
    sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL') and trans.approval_flag = 'Y')  then 1 else 0 end)
    ) as trans_count_approved,
    (sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL') and trans.approval_flag = 'N') then 1 else 0 end)
    -
    sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL') and trans.approval_flag = 'N')  then 1 else 0 end)
    ) as trans_count_declined,
  (sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL') and trans.approval_flag = 'Y') then transaction_total/100 else 0 end)
    -
    sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL') and trans.approval_flag = 'Y')  then transaction_total/100 else 0 end)
    ) as trans_amount_approved,
(sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL') and trans.approval_flag = 'N') then transaction_total/100 else 0 end)
    -
    sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL') and trans.approval_flag = 'N')  then transaction_total/100 else 0 end)
    ) as trans_amount_decline
  from
    wpay_analytics.wpay_billing_engine_tran_dtl trans
    left join
    pdh_ref_ds.ref_store_details stores
  on left(trans.net_term_id,5) = stores.store_id
  left join
    wpay_analytics.ref_response_code codes
  on
    codes.action_code = trans.act_code and
    codes.response_code = trans.response_code
  group by 1,2,3,4,5,6
  )
)
,


 auth_daily_filter_pre as
 (
  select 
    post.*,
    (post.auth_rate-pre.auth_rate) 
    as w_benchmark
  from auth_daily_filter post
  left join
    auth_daily_filter pre
  on
  cast(date_add(cast(post.period as date), interval -1 day) as string) = cast(pre.period as string)
  and post.brand = pre.brand
  and post.merchant_group = pre.merchant_group
  and post.store_id = pre.store_id
  and post.declined_reason = pre.declined_reason
  and post.scheme = pre.scheme
order by 1,2,3,4,5,7
)
,

 auth_yearly_filter as
(
select
  'Year' as timeframe,
  brand,
  merchant_group,
  store_id,
  --card_type,
  scheme as scheme,
  declined_reason as declined_reason,
  period,
  trans_count_approved,
  trans_count_declined,
  case when trans_count <> 0 then 
  trans_count_approved/trans_count 
  else 0 end 
  as auth_rate,
  case when trans_count <> 0 then 
  trans_count_declined/trans_count 
  else 0 end 
  as declined_rate,
  trans_amount_approved,
  case when trans_count_approved <> 0 then 
  trans_amount_approved/trans_count_approved 
  else 0 end 
  as avg_trans_value_approved,
  trans_amount_decline,
  case when trans_count_declined <> 0 then 
  trans_amount_decline/trans_count_declined 
  else 0 end 
  as avg_trans_value_declined
from
  (
  select
    stores.brand,
    stores.merchant_group,
    stores.store_id,
    codes.description as declined_reason,
    (
    case
    when trans.calc_scheme = 'VISA' then 'Visa'
    when trans.calc_scheme = 'MASTERCARD' then 'Mastercard'
    when trans.calc_scheme = 'GIFT CARDS' then 'Gift Cards'
    when trans.calc_scheme = 'PAYPAL' then 'Paypal'
    when trans.calc_scheme = 'AMEX' then 'Amex'
    when trans.calc_scheme = 'FUEL CARDS' then 'Fuel Cards'
    when trans.calc_scheme = 'DINERS' then 'Diners'
    when trans.calc_scheme = 'ALIPAY' then 'Alipay'
    when trans.calc_scheme = 'VERIFONE' then 'Verifone'
    else trans.calc_scheme
    end
    )
    as scheme,
    left(cast(tstamp_trans as string),4) as period,
    (sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL')) then 1 else 0 end)
    -
    sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL'))  then 1 else 0 end)
    ) as trans_count,
    (sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL') and trans.approval_flag = 'Y') then 1 else 0 end)
    -
    sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL') and trans.approval_flag = 'Y')  then 1 else 0 end)
    ) as trans_count_approved,
    (sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL') and trans.approval_flag = 'N') then 1 else 0 end)
    -
    sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL') and trans.approval_flag = 'N')  then 1 else 0 end)
    ) as trans_count_declined,
  (sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL') and trans.approval_flag = 'Y') then transaction_total/100 else 0 end)
    -
    sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL') and trans.approval_flag = 'Y')  then transaction_total/100 else 0 end)
    ) as trans_amount_approved,
(sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL') and trans.approval_flag = 'N') then transaction_total/100 else 0 end)
    -
    sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL') and trans.approval_flag = 'N')  then transaction_total/100 else 0 end)
    ) as trans_amount_decline
  from
    wpay_analytics.wpay_billing_engine_tran_dtl trans
    left join
    pdh_ref_ds.ref_store_details stores
  on left(trans.net_term_id,5) = stores.store_id
  left join
    wpay_analytics.ref_response_code codes
  on
    codes.action_code = trans.act_code and
    codes.response_code = trans.response_code
  group by 1,2,3,4,5,6
  )
)
,

 auth_yearly_filter_pre as
 (
  select 
    post.*,
    (post.auth_rate-pre.auth_rate) 
    as w_benchmark
  from auth_yearly_filter post
  left join
    auth_yearly_filter pre
  on
  cast((cast(post.period as int)-1) as string) = cast(pre.period as string)
  and post.brand = pre.brand
  and post.merchant_group = pre.merchant_group
  and post.store_id = pre.store_id
  and post.declined_reason = pre.declined_reason
  and post.scheme = pre.scheme
order by 1,2,3,4,5,7
)
,

 auth_weekly_filter as
(
select
  'Week' as timeframe,
  brand,
  merchant_group,
  store_id,
  --card_type,
  scheme as scheme,
  declined_reason as declined_reason,
  period,
  trans_count_approved,
  trans_count_declined,
  case when trans_count <> 0 then 
  trans_count_approved/trans_count 
  else 0 end 
  as auth_rate,
  case when trans_count <> 0 then 
  trans_count_declined/trans_count 
  else 0 end 
  as declined_rate,
  trans_amount_approved,
  case when trans_count_approved <> 0 then 
  trans_amount_approved/trans_count_approved 
  else 0 end 
  as avg_trans_value_approved,
  trans_amount_decline,
  case when trans_count_declined <> 0 then 
  trans_amount_decline/trans_count_declined 
  else 0 end 
  as avg_trans_value_declined
from
  (
  select
    stores.brand,
    stores.merchant_group,
    stores.store_id,
    codes.description as declined_reason,
    (
    case
    when trans.calc_scheme = 'VISA' then 'Visa'
    when trans.calc_scheme = 'MASTERCARD' then 'Mastercard'
    when trans.calc_scheme = 'GIFT CARDS' then 'Gift Cards'
    when trans.calc_scheme = 'PAYPAL' then 'Paypal'
    when trans.calc_scheme = 'AMEX' then 'Amex'
    when trans.calc_scheme = 'FUEL CARDS' then 'Fuel Cards'
    when trans.calc_scheme = 'DINERS' then 'Diners'
    when trans.calc_scheme = 'ALIPAY' then 'Alipay'
    when trans.calc_scheme = 'VERIFONE' then 'Verifone'
    else trans.calc_scheme
    end
    )
    as scheme,
    -- concat(left(cast(cal.clndr_date as string),4),concat('-',RIGHT(CONCAT('00', COALESCE(cast(cal.FiscalPeriodWeekNumber as string),'')), 2))) as period,
    cast(cal.fw_start_date as string) as period,
    (sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL')) then 1 else 0 end)
    -
    sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL'))  then 1 else 0 end)
    ) as trans_count,
    (sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL') and trans.approval_flag = 'Y') then 1 else 0 end)
    -
    sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL') and trans.approval_flag = 'Y')  then 1 else 0 end)
    ) as trans_count_approved,
    (sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL') and trans.approval_flag = 'N') then 1 else 0 end)
    -
    sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL') and trans.approval_flag = 'N')  then 1 else 0 end)
    ) as trans_count_declined,
  (sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL') and trans.approval_flag = 'Y') then transaction_total/100 else 0 end)
    -
    sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL') and trans.approval_flag = 'Y')  then transaction_total/100 else 0 end)
    ) as trans_amount_approved,
(sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL') and trans.approval_flag = 'N') then transaction_total/100 else 0 end)
    -
    sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL') and trans.approval_flag = 'N')  then transaction_total/100 else 0 end)
    ) as trans_amount_decline
  from
    wpay_analytics.wpay_billing_engine_tran_dtl trans

  left join
    pdh_ref_ds.dim_date cal
  on
    left(cast(trans.tstamp_trans as string),10) = cast(cal.clndr_date as string)
    left join
    pdh_ref_ds.ref_store_details stores
  on left(trans.net_term_id,5) = stores.store_id
  left join
    wpay_analytics.ref_response_code codes
  on
    codes.action_code = trans.act_code and
    codes.response_code = trans.response_code
  group by 1,2,3,4,5,6
  order by 4
  )
)

 ,

  auth_weekly_filter_pre as
 (
  select 
    post.*,
    (post.auth_rate-pre.auth_rate) 
    as w_benchmark
  from auth_weekly_filter post
  left join
    auth_weekly_filter pre
  on
  cast(date_add(cast(post.period as date), interval -7 day) as string) = cast(pre.period as string)
  and post.declined_reason = pre.declined_reason
  and post.brand = pre.brand
  and post.merchant_group = pre.merchant_group
  and post.store_id = pre.store_id
  and post.scheme = pre.scheme
order by 1,2,3,4,5,7
)

,
auth_total_filter as 
(
  select * from
  -- (
  -- select * from
  --   auth_daily_filter_pre
  -- )
  -- union all
  (
  select * from
    auth_monthly_filter_pre
  )
  -- union all
  -- (
  -- select * from
  --   auth_yearly_filter_pre
  -- )
    union all
  (
  select * from
    auth_weekly_filter_pre
  )
)
,

-------------------------------------------------------------------------------------------
-- FILTER COMBINATION MG + BRAND LEVEL


 auth_weekly_tot_filter as
(
select
  'Week' as timeframe,
  brand,
  merchant_group,
  'All' as store_id,
  --card_type,
  scheme as scheme,
  declined_reason as declined_reason,
  period,
  trans_count_approved,
  trans_count_declined,
  case when trans_count <> 0 then 
  trans_count_approved/trans_count 
  else 0 end 
  as auth_rate,
  case when trans_count <> 0 then 
  trans_count_declined/trans_count 
  else 0 end 
  as declined_rate,
  trans_amount_approved,
  case when trans_count_approved <> 0 then 
  trans_amount_approved/trans_count_approved 
  else 0 end 
  as avg_trans_value_approved,
  trans_amount_decline,
  case when trans_count_declined <> 0 then 
  trans_amount_decline/trans_count_declined 
  else 0 end 
  as avg_trans_value_declined
from
  (
  select
    stores.brand,
    stores.merchant_group,
    --store_id,
    codes.description as declined_reason,
    (
    case
    when trans.calc_scheme = 'VISA' then 'Visa'
    when trans.calc_scheme = 'MASTERCARD' then 'Mastercard'
    when trans.calc_scheme = 'GIFT CARDS' then 'Gift Cards'
    when trans.calc_scheme = 'PAYPAL' then 'Paypal'
    when trans.calc_scheme = 'AMEX' then 'Amex'
    when trans.calc_scheme = 'FUEL CARDS' then 'Fuel Cards'
    when trans.calc_scheme = 'DINERS' then 'Diners'
    when trans.calc_scheme = 'ALIPAY' then 'Alipay'
    when trans.calc_scheme = 'VERIFONE' then 'Verifone'
    else trans.calc_scheme
    end
    )
    as scheme,
    cast(cal.fw_start_date as string) as period,
    (sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL')) then 1 else 0 end)
    -
    sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL'))  then 1 else 0 end)
    ) as trans_count,
    (sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL') and trans.approval_flag = 'Y') then 1 else 0 end)
    -
    sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL') and trans.approval_flag = 'Y')  then 1 else 0 end)
    ) as trans_count_approved,
    (sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL') and trans.approval_flag = 'N') then 1 else 0 end)
    -
    sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL') and trans.approval_flag = 'N')  then 1 else 0 end)
    ) as trans_count_declined,
  (sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL') and trans.approval_flag = 'Y') then transaction_total/100 else 0 end)
    -
    sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL') and trans.approval_flag = 'Y')  then transaction_total/100 else 0 end)
    ) as trans_amount_approved,
(sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL') and trans.approval_flag = 'N') then transaction_total/100 else 0 end)
    -
    sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL') and trans.approval_flag = 'N')  then transaction_total/100 else 0 end)
    ) as trans_amount_decline
  from
    wpay_analytics.wpay_billing_engine_tran_dtl trans
      left join
    pdh_ref_ds.dim_date cal
  on
    left(cast(trans.tstamp_trans as string),10) = cast(cal.clndr_date as string)
  left join
    pdh_ref_ds.ref_store_details stores
  on left(trans.net_term_id,5) = stores.store_id
  left join
    wpay_analytics.ref_response_code codes
  on
    codes.action_code = trans.act_code and
    codes.response_code = trans.response_code
  group by 1,2,3,4,5
  )
  order by 1,2,3,4,5,7
)
,

 auth_weekly_tot_pre_filter as
 (
  select 
    post.*,
    (post.auth_rate-pre.auth_rate) 
    as w_benchmark
  from auth_weekly_tot_filter post
  left join
    auth_weekly_tot_filter pre
  on
  cast(date_add(cast(post.period as date), interval -7 day) as string) = cast(pre.period as string)
  and post.brand = pre.brand
  and post.merchant_group = pre.merchant_group
  and post.declined_reason = pre.declined_reason
  and post.scheme = pre.scheme
  --and post.store_id = pre.store_id
order by 1,2,3,4,5,7
)

,

 auth_monthly_tot_filter as
(
select
  'Month' as timeframe,
  brand,
  merchant_group,
  'All' as store_id,
  --card_type,
  scheme as scheme,
  declined_reason as declined_reason,
  period,
  trans_count_approved,
  trans_count_declined,
  case when trans_count <> 0 then 
  trans_count_approved/trans_count 
  else 0 end 
  as auth_rate,
  case when trans_count <> 0 then 
  trans_count_declined/trans_count 
  else 0 end 
  as declined_rate,
  trans_amount_approved,
  case when trans_count_approved <> 0 then 
  trans_amount_approved/trans_count_approved 
  else 0 end 
  as avg_trans_value_approved,
  trans_amount_decline,
  case when trans_count_declined <> 0 then 
  trans_amount_decline/trans_count_declined 
  else 0 end 
  as avg_trans_value_declined
from
  (
  select
    stores.brand,
    stores.merchant_group,
    --store_id,
    codes.description as declined_reason,
    (
    case
    when trans.calc_scheme = 'VISA' then 'Visa'
    when trans.calc_scheme = 'MASTERCARD' then 'Mastercard'
    when trans.calc_scheme = 'GIFT CARDS' then 'Gift Cards'
    when trans.calc_scheme = 'PAYPAL' then 'Paypal'
    when trans.calc_scheme = 'AMEX' then 'Amex'
    when trans.calc_scheme = 'FUEL CARDS' then 'Fuel Cards'
    when trans.calc_scheme = 'DINERS' then 'Diners'
    when trans.calc_scheme = 'ALIPAY' then 'Alipay'
    when trans.calc_scheme = 'VERIFONE' then 'Verifone'
    else trans.calc_scheme
    end
    )
    as scheme,
    left(cast(tstamp_trans as string),7) as period,

    (sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL')) then 1 else 0 end)
    -
    sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL'))  then 1 else 0 end)
    ) as trans_count,
    (sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL') and trans.approval_flag = 'Y') then 1 else 0 end)
    -
    sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL') and trans.approval_flag = 'Y')  then 1 else 0 end)
    ) as trans_count_approved,
    (sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL') and trans.approval_flag = 'N') then 1 else 0 end)
    -
    sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL') and trans.approval_flag = 'N')  then 1 else 0 end)
    ) as trans_count_declined,
  (sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL') and trans.approval_flag = 'Y') then transaction_total/100 else 0 end)
    -
    sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL') and trans.approval_flag = 'Y')  then transaction_total/100 else 0 end)
    ) as trans_amount_approved,
(sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL') and trans.approval_flag = 'N') then transaction_total/100 else 0 end)
    -
    sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL') and trans.approval_flag = 'N')  then transaction_total/100 else 0 end)
    ) as trans_amount_decline
  from
    wpay_analytics.wpay_billing_engine_tran_dtl trans
    left join
    pdh_ref_ds.ref_store_details stores
  on left(trans.net_term_id,5) = stores.store_id
  left join
    wpay_analytics.ref_response_code codes
  on
    codes.action_code = trans.act_code and
    codes.response_code = trans.response_code


  group by 1,2,3,4,5
  )
  order by 1,2,3,4,5,7
)
,

 auth_monthly_tot_pre_filter as
 (
  select 
    post.*,
    (post.auth_rate-pre.auth_rate) 
    as w_benchmark
  from auth_monthly_tot_filter post
  left join
    auth_monthly_tot_filter pre
  on
  left(cast(date_add(cast(concat(left(post.period,7),'-01') as date), interval -1 month) as string),4) = cast(left(pre.period,4) as string)
  and substring(cast(date_add(cast(concat(left(post.period,7),'-01') as date), interval -1 month) as string),6,2)  = right(left(pre.period,7),2)
  and post.brand = pre.brand
  and post.merchant_group = pre.merchant_group
  and post.declined_reason = pre.declined_reason
  and post.scheme = pre.scheme
  --and post.store_id = pre.store_id
order by 1,2,3,4,5,7
)
,

  auth_daily_tot_filter as
(
select
  'Daily' as timeframe,
  brand,
  merchant_group,
  'All' as store_id,
  --card_type,
  scheme as scheme,
  declined_reason as declined_reason,
  period,
  trans_count_approved,
  trans_count_declined,
  case when trans_count <> 0 then 
  trans_count_approved/trans_count 
  else 0 end 
  as auth_rate,
  case when trans_count <> 0 then 
  trans_count_declined/trans_count 
  else 0 end 
  as declined_rate,
  trans_amount_approved,
  case when trans_count_approved <> 0 then 
  trans_amount_approved/trans_count_approved 
  else 0 end 
  as avg_trans_value_approved,
  trans_amount_decline,
  case when trans_count_declined <> 0 then 
  trans_amount_decline/trans_count_declined 
  else 0 end 
  as avg_trans_value_declined
from
  (
  select
    stores.brand,
    stores.merchant_group,
    --store_id,
    codes.description as declined_reason,
    (
    case
    when trans.calc_scheme = 'VISA' then 'Visa'
    when trans.calc_scheme = 'MASTERCARD' then 'Mastercard'
    when trans.calc_scheme = 'GIFT CARDS' then 'Gift Cards'
    when trans.calc_scheme = 'PAYPAL' then 'Paypal'
    when trans.calc_scheme = 'AMEX' then 'Amex'
    when trans.calc_scheme = 'FUEL CARDS' then 'Fuel Cards'
    when trans.calc_scheme = 'DINERS' then 'Diners'
    when trans.calc_scheme = 'ALIPAY' then 'Alipay'
    when trans.calc_scheme = 'VERIFONE' then 'Verifone'
    else trans.calc_scheme
    end
    )
    as scheme,
    cast(cast(tstamp_trans as date)as string) as period,
    (sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL')) then 1 else 0 end)
    -
    sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL'))  then 1 else 0 end)
    ) as trans_count,
    (sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL') and trans.approval_flag = 'Y') then 1 else 0 end)
    -
    sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL') and trans.approval_flag = 'Y')  then 1 else 0 end)
    ) as trans_count_approved,
    (sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL') and trans.approval_flag = 'N') then 1 else 0 end)
    -
    sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL') and trans.approval_flag = 'N')  then 1 else 0 end)
    ) as trans_count_declined,
  (sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL') and trans.approval_flag = 'Y') then transaction_total/100 else 0 end)
    -
    sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL') and trans.approval_flag = 'Y')  then transaction_total/100 else 0 end)
    ) as trans_amount_approved,
(sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL') and trans.approval_flag = 'N') then transaction_total/100 else 0 end)
    -
    sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL') and trans.approval_flag = 'N')  then transaction_total/100 else 0 end)
    ) as trans_amount_decline
  from
    wpay_analytics.wpay_billing_engine_tran_dtl trans
    left join
    pdh_ref_ds.ref_store_details stores
  on left(trans.net_term_id,5) = stores.store_id
  left join
    wpay_analytics.ref_response_code codes
  on
    codes.action_code = trans.act_code and
    codes.response_code = trans.response_code

  group by 1,2,3,4,5
  )
)

,

 auth_daily_tot_pre_filter as
 (
  select 
    post.*,
    (post.auth_rate-pre.auth_rate) 
    as w_benchmark
  from auth_daily_tot_filter post
  left join
    auth_daily_tot_filter pre
  on
  cast(date_add(cast(post.period as date), interval -1 day) as string) = cast(pre.period as string)
  and post.brand = pre.brand
  and post.merchant_group = pre.merchant_group
  and post.declined_reason = pre.declined_reason
  and post.scheme = pre.scheme
  --and post.store_id = pre.store_id
order by 1,2,3,4,5,7
)
,

 auth_yearly_tot_filter as
(
select
  'Year' as timeframe,
  brand,
  merchant_group,
  'All' as store_id,
  --card_type,
  scheme as scheme,
  declined_reason as declined_reason,
  period,
  trans_count_approved,
  trans_count_declined,
  case when trans_count <> 0 then 
  trans_count_approved/trans_count 
  else 0 end 
  as auth_rate,
  case when trans_count <> 0 then 
  trans_count_declined/trans_count 
  else 0 end 
  as declined_rate,
  trans_amount_approved,
  case when trans_count_approved <> 0 then 
  trans_amount_approved/trans_count_approved 
  else 0 end 
  as avg_trans_value_approved,
  trans_amount_decline,
  case when trans_count_declined <> 0 then 
  trans_amount_decline/trans_count_declined 
  else 0 end 
  as avg_trans_value_declined
from
  (
  select
    stores.brand,
    stores.merchant_group,
    --store_id,
    codes.description as declined_reason,
    (
    case
    when trans.calc_scheme = 'VISA' then 'Visa'
    when trans.calc_scheme = 'MASTERCARD' then 'Mastercard'
    when trans.calc_scheme = 'GIFT CARDS' then 'Gift Cards'
    when trans.calc_scheme = 'PAYPAL' then 'Paypal'
    when trans.calc_scheme = 'AMEX' then 'Amex'
    when trans.calc_scheme = 'FUEL CARDS' then 'Fuel Cards'
    when trans.calc_scheme = 'DINERS' then 'Diners'
    when trans.calc_scheme = 'ALIPAY' then 'Alipay'
    when trans.calc_scheme = 'VERIFONE' then 'Verifone'
    else trans.calc_scheme
    end
    )
    as scheme,
    left(cast(tstamp_trans as string),4) as period,
    (sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL')) then 1 else 0 end)
    -
    sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL'))  then 1 else 0 end)
    ) as trans_count,
    (sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL') and trans.approval_flag = 'Y') then 1 else 0 end)
    -
    sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL') and trans.approval_flag = 'Y')  then 1 else 0 end)
    ) as trans_count_approved,
    (sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL') and trans.approval_flag = 'N') then 1 else 0 end)
    -
    sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL') and trans.approval_flag = 'N')  then 1 else 0 end)
    ) as trans_count_declined,
  (sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL') and trans.approval_flag = 'Y') then transaction_total/100 else 0 end)
    -
    sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL') and trans.approval_flag = 'Y')  then transaction_total/100 else 0 end)
    ) as trans_amount_approved,
(sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE','PURCHASE WITH CASHOUT','REDEMPTION','REFUND REVERSAL') and trans.approval_flag = 'N') then transaction_total/100 else 0 end)
    -
    sum(case when (trans.calc_tran_type_description in 
    ('PURCHASE REVERSAL','PURCHASE WITH CASHOUT REVERSAL','REDEMPTION REVERSAL') and trans.approval_flag = 'N')  then transaction_total/100 else 0 end)
    ) as trans_amount_decline
  from
    wpay_analytics.wpay_billing_engine_tran_dtl trans
    left join
    pdh_ref_ds.ref_store_details stores
  on left(trans.net_term_id,5) = stores.store_id
  left join
    wpay_analytics.ref_response_code codes
  on
    codes.action_code = trans.act_code and
    codes.response_code = trans.response_code
  group by 1,2,3,4,5
  )
)

,

 auth_yearly_tot_pre_filter as
 (
  select 
    post.*,
    (post.auth_rate-pre.auth_rate) 
    as w_benchmark
  from auth_yearly_tot_filter post
  left join
    auth_yearly_tot_filter pre
  on
  cast((cast(post.period as int)-1) as string) = cast(pre.period as string)
  and post.brand = pre.brand
  and post.declined_reason = pre.declined_reason
  and post.merchant_group = pre.merchant_group
  and post.scheme = pre.scheme
  --and post.store_id = pre.store_id
order by 1,2,3,4,5,7
)


,

auth_total_tot_filter as 
(
  select * from
  -- (
  -- select * from
  --   auth_daily_tot_pre_filter
  -- )
  -- union all
  (
  select * from
    auth_monthly_tot_pre_filter
  )
  -- union all
  -- (
  -- select * from
  --   auth_yearly_tot_pre_filter
  -- )
    union all
  (
  select * from
    auth_weekly_tot_pre_filter
  )
)

-------------------------------------------------------------------------------------------------------------

------------------------------------------------------------------------------------------------------
  ,
  final_output as
  (select * from
  (
  select * from
    auth_total_scheme
  )
    union all
  (
  select * from
    auth_total
  )
    union all
  (
  select * from
    auth_total_tot
  )
      union all
  (
  select * from
    auth_total_tot_scheme
  )
        union all
  (
  select * from
    auth_total_tot_reason
  )
          union all
  (
  select * from
    auth_total_reason
  )
          union all
  (
  select * from
    auth_total_tot_filter
  )
          union all
  (
  select * from
    auth_total_filter
  )
  )


select
  final.*,
  case when final.store_id = 'All' then 'All' else store.site_name end as site_name,
  store2.company,
  store2.division,
  store2.invoice_group,
  store2.merchant_portal_mg,
  store2.merchant_portal_cg,

  cast(case when timeframe = 'Month' then concat(period,'-01')
       when timeframe = 'Year' then concat(period,'-01-01')
       else period end as date)
  as month_start

from
  final_output final
left join
  wpay_analytics.ref_store_ind store
on store.store_id = final.store_id
left join
  (
    select
      brand,
      merchant_group,
      company,
      division,
      invoice_group,
      merchant_portal_mg,
      merchant_portal_cg
    from
      wpay_analytics.ref_store_ind
    group by 
      1,2,3,4,5,6,7
    order by 1
  )
   store2
on store2.brand = final.brand
   and store2.merchant_group = final.merchant_group

-- where
--   final.trans_count_declined > 0
)
;
