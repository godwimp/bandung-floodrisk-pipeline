with source as(
	select
		id	as kelurahan_id,
		nama_kelurahan,
		nama_kecamatan,
		population,
		luas_km2,
		case
			when luas_km2 > 0 then population::float / luas_km2
			else null
		end		as pop_density,
		geom
	from {{source('public', 'kelurahan') }}
	where id not in ('BDG-PUSAT', 'BDG-UTARA', 'BDG-SELATAN', 'BDG-TIMUR', 'BDG-BARAT')
		and luas_km2 is not null
		and population is not null
)
select * from source
