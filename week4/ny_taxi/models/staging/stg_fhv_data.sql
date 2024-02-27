with 

source as (

    select *   from {{ source('staging', 'fhv_data') }}
     
),

renamed as (

    select
        {{ dbt_utils.generate_surrogate_key(['pulocationid', 'pickup_datetime']) }} as tripid,
        dispatching_base_num,
        -- timestamps
        cast(pickup_datetime as timestamp) as pickup_datetime,
        cast(dropoff_datetime as timestamp) as dropoff_datetime,
        {{ date_trunc("year", "pickup_datetime") }}  as pickup_year,
        pulocationid as pickup_locationid,
        dolocationid as dropoff_locationid,
        sr_flag,
        affiliated_base_number

    from source
    where {{ date_trunc("year", "pickup_datetime") }} >= '2019-01-01' and {{ date_trunc("year", "pickup_datetime") }} < '2020-01-01'

)

select * from renamed

-- dbt build --select <model_name> --vars '{'is_test_run': 'false'}'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}
