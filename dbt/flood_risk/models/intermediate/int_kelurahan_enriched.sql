with kelurahan as (
	select * from {{ ref('stg_kelurahan') }}
),
elevation as (
	select * from {{ ref('stg_elevation') }}
),
flood_freq as (
	select * from {{ ref('stg_flood_events') }}
),
rainfall_latest as (
	select distinct on (kelurahan_id)
		kelurahan_id,
		rainfall_7day,
		rainfall_30day_avg,
		rainfall_annual_max
	from {{ ref('stg_rainfall') }}
	order by kelurahan_id, date desc
)
select
	k.kelurahan_id,
	k.nama_kelurahan,
	k.nama_kecamatan,
	k.population,
	k.luas_km2,
	k.pop_density,
	e.mean_elevation,
	e.mean_slope,
	e.dist_to_river_m,
	coalesce(r.rainfall_7day, 0) as rainfall_7day,
	coalesce(r.rainfall_30day_avg, 0) as rainfall_30day_avg,
	coalesce(r.rainfall_annual_max, 0) as rainfall_annual_max,
	coalesce(f.flood_count, 0) as flood_count,
	f.last_flood_date
from kelurahan k
left join elevation e on e.kelurahan_id = k.kelurahan_id
left join rainfall_latest r on r.kelurahan_id = k.kelurahan_id
left join flood_freq f on f.kelurahan_id = k.kelurahan_id
