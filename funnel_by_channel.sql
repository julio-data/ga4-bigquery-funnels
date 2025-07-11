-- Step 1: Aggregate daily unique users per funnel step
with main_query as(
select
PARSE_DATE('%Y%m%d', event_date) AS period,
traffic_source.source as channel,
count(distinct if(event_name='session_start',user_pseudo_id,null))as session_start,
count(distinct if(event_name='view_item',user_pseudo_id,null))as view_item,
count(distinct if(event_name='add_to_cart',user_pseudo_id,null))as add_to_cart,
count(distinct if(event_name='begin_checkout',user_pseudo_id,null))as begin_checkout,
count(distinct if(event_name='purchase',user_pseudo_id,null))as purchase
from bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_20210131
group by event_date, traffic_source.source
)

-- Step 2: Calculate conversion rates between funnel steps
select
channel,
session_start,
concat(round(SAFE_DIVIDE(view_item,session_start)*100,2),'%') as step_0_view_item,
view_item,
concat(round(SAFE_DIVIDE(add_to_cart,view_item)*100,2),'%') as step_1_add_to_cart,
add_to_cart,
concat(round(SAFE_DIVIDE(begin_checkout,add_to_cart)*100,2),'%') as step_2_begin_checkout,
begin_checkout,
concat(round(SAFE_DIVIDE(purchase,begin_checkout)*100,2),'%') as step_3_purchase,
purchase,
concat(round(SAFE_DIVIDE(purchase,session_start)*100,2),'%') as total_CR
from main_query
where period between  '2021-01-31' AND '2021-01-31'
