    {{ config(materialized='table') }}

with green_trips as (
    select 
        *,
        'green' as service_type
    from {{ ref('stg_green_tripdata') }}
)
, yellow_trips as (
    select 
        *,
        'yellow' as service_type
    from {{ ref('stg_yellow_tripdata') }}
)
, unionised as (
    select * from green_trips
    union all
    select * from yellow_trips
)
, d_zones as (
    select * from {{ ref('d_zones') }}
    where borough <> 'Unknown'
)

select
    u.tripid,
    u.vendorid,
    u.ratecodeid,
    u.pickup_locationid,
    pz.borough as pickup_borough,
    pz.zone as pickup_zone,
    u.dropoff_locationid,
    dz.borough as dropoff_borough,
    dz.zone as dropoff_zone,
    u.pickup_datetime,
    u.dropoff_datetime,
    u.store_and_fwd_flag,
    u.passenger_count,
    u.trip_distance,
    u.trip_type,
    u.fare_amount,
    u.extra,
    u.mta_tax,
    u.tip_amount,
    u.tolls_amount,
    u.ehail_fee,
    u.improvement_surcharge,
    u.total_amount,
    u.payment_type,
    u.payment_type_description,
    u.congestion_surcharge
from
    unionised u
left join
    d_zones pz
on
    u.pickup_locationid = pz.locationid
left join
    d_zones dz
on
    u.pickup_locationid = dz.locationid
