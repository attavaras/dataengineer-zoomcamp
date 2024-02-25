with 

source as (

    select *   from {{ source('staging', 'fhv_data') }}
     
),

renamed as (

    select
        dispatching_base_num,
        pickup_datetime,
        {{ date_trunc("year", "pickup_datetime") }}  as pickup_year,
        dropoff_datetime,
        pulocationid,
        dolocationid,
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
