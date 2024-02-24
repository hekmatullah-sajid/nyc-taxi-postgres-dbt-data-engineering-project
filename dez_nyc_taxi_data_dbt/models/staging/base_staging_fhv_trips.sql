{{ config(materialized='view') }}

with source as (
      select * from {{ source('staging', 'fhv_trips') }}
),
renamed as (
    select
        {{ dbt_utils.generate_surrogate_key(["dispatching_base_num", "pickup_datetime"]) }} as tripid,
        {{ adapter.quote("index") }},
        {{ adapter.quote("dispatching_base_num") }},
        {{ adapter.quote("pickup_datetime") }},
        {{ adapter.quote("dropOff_datetime") }} as dropoff_datetime,
        {{ adapter.quote("PUlocationID") }} as pu_locationid,
        {{ adapter.quote("DOlocationID") }} as do_locationid,
        {{ adapter.quote("SR_Flag") }} as sr_flag,
        {{ adapter.quote("Affiliated_base_number") }} as affiliated_base_number

    from source
)
select * from renamed

-- dbt build --select <model_name> --vars '{'is_test_run': 'false'}'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}
  