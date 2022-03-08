    {{ config(materialized='table') }}

with fhv_trips as (
    select
        *,
        'fhv' as service_type
    from {{ ref('stg_fhv_tripdata') }}
)
, d_zones as (
    select * from {{ ref('d_zones') }}
    where borough <> 'Unknown'
)

select
    t.tripid,
    t.dispatching_base_num,
    t.pickup_locationid,
    pz.borough as pickup_borough,
    pz.zone as pickup_zone,
    t.dropoff_locationid,
    dz.borough as dropoff_borough,
    dz.zone as dropoff_zone,
    t.pickup_datetime,
    t.dropoff_datetime,
    t.sr_flag,
    t.affiliated_base_number
from
    fhv_trips t
left join
    d_zones pz
on
    t.pickup_locationid = pz.locationid
left join
    d_zones dz
on
    t.pickup_locationid = dz.locationid
