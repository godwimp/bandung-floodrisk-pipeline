select
    kelurahan_id,
    mean_elevation,
    min_elevation,
    max_elevation,
    mean_slope,
    dist_to_river_m,
    updated_at
from {{ source('public', 'elevation_stats') }}
where mean_elevation is not null
