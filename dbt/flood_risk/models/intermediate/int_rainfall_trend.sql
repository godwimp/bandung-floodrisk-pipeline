with monthly as (
    select
        kelurahan_id,
        date_trunc('month', date)       as month,
        sum(precipitation)              as monthly_precipitation,
        avg(precipitation)              as daily_avg,
        max(precipitation)              as daily_max,
        count(*)                        as days_with_data
    from {{ ref('stg_rainfall') }}
    group by kelurahan_id, date_trunc('month', date)
)
select
    kelurahan_id,
    month,
    monthly_precipitation,
    daily_avg,
    daily_max,
    days_with_data,
    avg(monthly_precipitation) over (
        partition by kelurahan_id
        order by month
        rows between 2 preceding and current row
    )                                   as precipitation_3mo_avg
from monthly
