with trend as (
    select * from {{ ref('int_rainfall_trend') }}
),
latest_6mo as (
    select
        kelurahan_id,
        sum(monthly_precipitation)  as total_precip_6mo,
        avg(daily_max)              as avg_daily_max_6mo,
        max(daily_max)              as peak_daily_max_6mo
    from trend
    where month >= date_trunc('month', current_date) - interval '6 months'
    group by kelurahan_id
),
risk as (
    select * from {{ ref('mart_flood_risk') }}
)
select
    r.*,
    l.total_precip_6mo,
    l.avg_daily_max_6mo,
    l.peak_daily_max_6mo
from risk r
left join latest_6mo l on l.kelurahan_id = r.kelurahan_id
order by r.risk_index desc nulls last
