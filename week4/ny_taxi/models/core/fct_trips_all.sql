{{
    config(
        materialized='table'
    )
}}

with green_tripdata as (
    select *, 
        'Green' as service_type
    from {{ ref('stg_green_tripdata') }}
), 
yellow_tripdata as (
    select *, 
        'Yellow' as service_type
    from {{ ref('stg_yellow_tripdata') }}
), 
fhv_tripdata as (
    select *,
    'FHV' as service_type
    from {{ref('stg_fhv_data')}}
),
trips_unioned as (
    select green_tripdata.tripid,
        green_tripdata.pickup_datetime,
        green_tripdata.dropoff_datetime,
        green_tripdata.pickup_locationid,
        green_tripdata.dropoff_locationid
            from green_tripdata
    union all 
    select yellow_tripdata.tripid,
        yellow_tripdata.pickup_datetime,
        yellow_tripdata.dropoff_datetime,
        yellow_tripdata.pickup_locationid,
        yellow_tripdata.dropoff_locationid from yellow_tripdata
    union all 
    select fhv_tripdata.tripid,
        fhv_tripdata.pickup_datetime,
        fhv_tripdata.dropoff_datetime,
        fhv_tripdata.pickup_locationid,
        fhv_tripdata.dropoff_locationid from fhv_tripdata
    
), 
dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)
select trips_unioned.tripid,
        trips_unioned.pickup_datetime,
        trips_unioned.dropoff_datetime,
        trips_unioned.pickup_locationid,
        pickup_zone.borough as pickup_borough,
        pickup_zone.zone as pickup_zone,
        trips_unioned.dropoff_locationid,
        dropoff_zone.borough as dropoff_borough, 
        dropoff_zone.zone as dropoff_zone
from trips_unioned
inner join dim_zones as pickup_zone
on trips_unioned.pickup_locationid = pickup_zone.locationid
inner join dim_zones as dropoff_zone
on trips_unioned.dropoff_locationid = dropoff_zone.locationid