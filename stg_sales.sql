with source as (
    select
        sale_id,
        sale_ts::timestamp as sale_ts,
        sale_date::date as sale_date,
        store_id,
        product_id,
        quantity::int as quantity,
        unit_price::numeric(12,2) as unit_price,
        currency,
        total_amount::numeric(14,2) as total_amount
    from raw.sales
), dedup as (
    select *
    from (
        select
            *,
            row_number() over (partition by sale_id order by sale_ts desc) as rn
        from source
    ) x
    where rn = 1
)
select * from dedup
