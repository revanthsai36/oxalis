with sales as (
    select * from {{ ref('stg_sales') }}
)
select
    sale_date,
    store_id,
    product_id,
    count(*) as transactions,
    sum(quantity) as units_sold,
    sum(total_amount) as revenue,
    avg(unit_price) as avg_unit_price
from sales
group by 1,2,3
