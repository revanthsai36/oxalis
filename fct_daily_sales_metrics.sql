{{ config(
    materialized='incremental',
    unique_key='sale_date',
    incremental_strategy='delete+insert'
) }}

with daily as (
    select * from {{ ref('int_daily_sales') }}
), rolled as (
    select
        sale_date,
        sum(transactions) as transactions,
        sum(units_sold) as units_sold,
        sum(revenue) as revenue,
        avg(avg_unit_price) as avg_unit_price
    from daily
    group by sale_date
)
select * from rolled

{% if is_incremental() %}
  where sale_date > (select coalesce(max(sale_date), '1900-01-01') from {{ this }})
{% endif %}
