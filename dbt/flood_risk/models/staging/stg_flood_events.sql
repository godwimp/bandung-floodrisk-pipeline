with source as (
    select
        kelurahan_id,
        event_date,
        source,
        severity,
        geom
    from {{ source('public', 'flood_events') }}
    where kelurahan_id is not null
),
aggregated as (
    select
        kelurahan_id,
        count(*)            as flood_count,
        max(event_date)     as last_flood_date,
        min(event_date)     as first_flood_date
    from source
    group by kelurahan_id
)
select * from aggregated
