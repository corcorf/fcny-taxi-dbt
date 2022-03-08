{{ config(materialized='view') }}

select
    -- identifiers
    {{ dbt_utils.surrogate_key(['dispatching_base_num', 'pickup_datetime']) }} as tripid,
    cast(dispatching_base_num as string) as dispatching_base_num,
    cast(pulocationid as integer) as  pickup_locationid,
    cast(dolocationid as integer) as dropoff_locationid,

    -- timestamps
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropoff_datetime as timestamp) as dropoff_datetime,

    -- trip info
    cast(sr_flag as integer) sr_flag,
    affiliated_base_number
from `fcny-taxi`.`trips_data_all`.`fhv_tripdata`
where dispatching_base_num IS NOT NULL

{% if var("is_test_run", default=true) %}

limit 100

{% endif %}
