with base as (
    select * from {{ ref('int_kelurahan_enriched') }}
),
risk as (
    select
        kelurahan_id,
        risk_index,
        risk_level,
        period_date,
        scoring_method
    from {{ source('public', 'flood_risk_index') }}
    where scoring_method = 'rule_based'
      and period_date = (
          select max(period_date)
          from {{ source('public', 'flood_risk_index') }}
          where scoring_method = 'rule_based'
      )
)
select
    b.kelurahan_id,
    b.nama_kelurahan,
    b.nama_kecamatan,
    b.population,
    b.luas_km2,
    b.pop_density,
    b.mean_elevation,
    b.mean_slope,
    b.dist_to_river_m,
    b.rainfall_7day,
    b.rainfall_30day_avg,
    b.flood_count,
    b.last_flood_date,
    r.risk_index,
    r.risk_level,
    r.period_date
from base b
left join risk r on r.kelurahan_id = b.kelurahan_id
