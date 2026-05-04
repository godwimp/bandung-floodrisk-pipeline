with source as (
    select
        kelurahan_id,
        date,
        precipitation,
        rain_sum,
        source
    from {{ source('public', 'rainfall_daily') }}
    where precipitation is not null
),
enriched as (
    select
        kelurahan_id,
        date,
        precipitation,
        rain_sum,
        source,
        sum(precipitation) over (
            partition by kelurahan_id
            order by date
            rows between 6 preceding and current row
        )                   as rainfall_7day,
        avg(precipitation) over (
            partition by kelurahan_id
            order by date
            rows between 29 preceding and current row
        )                   as rainfall_30day_avg,
        max(precipitation) over (
            partition by kelurahan_id
            order by date
            rows between 364 preceding and current row
        )                   as rainfall_annual_max
    from source
)
select * from enriched
