{{ config(materialized='view') }}

with source as (
      select *,
        ROW_NUMBER() OVER(PARTITION BY "VendorID", lpep_pickup_datetime ORDER BY "VendorID", lpep_pickup_datetime) AS rn
      from {{ source('staging', 'green_taxi_trips') }}
      where "VendorID" is not null 
),
renamed as (
    select
        {{ dbt_utils.generate_surrogate_key(["\"VendorID\"", "lpep_pickup_datetime"]) }} as tripid,
        {{ dbt.safe_cast("index", api.Column.translate_type("integer")) }} as index,
        {{ dbt.safe_cast(adapter.quote("VendorID"), api.Column.translate_type("integer")) }} as vendorid,
        cast(lpep_pickup_datetime as timestamp) as pickup_datetime,
        cast(lpep_dropoff_datetime as timestamp) as dropoff_datetime,
        {{ adapter.quote("store_and_fwd_flag") }},
        {{ dbt.safe_cast(adapter.quote("RatecodeID"), api.Column.translate_type("integer")) }} as ratecodeid,
        {{ dbt.safe_cast(adapter.quote("PULocationID"), api.Column.translate_type("integer")) }} as pickup_locationid,
        {{ dbt.safe_cast(adapter.quote("DOLocationID"), api.Column.translate_type("integer")) }} as dropoff_locationid,
        {{ dbt.safe_cast("passenger_count", api.Column.translate_type("integer")) }} as passenger_count,
        cast(trip_distance as numeric) as trip_distance,
        cast(fare_amount as numeric) as fare_amount,
        cast(extra as numeric) as extra,
        cast(mta_tax as numeric) as mta_tax,
        cast(tip_amount as numeric) as tip_amount,
        cast(tolls_amount as numeric) as tolls_amount,
        cast(ehail_fee as numeric) as ehail_fee,
        cast(improvement_surcharge as numeric) as improvement_surcharge,
        cast(total_amount as numeric) as total_amount,
        coalesce({{ dbt.safe_cast("payment_type", api.Column.translate_type("integer")) }},0) as payment_type,
        {{ get_payment_type_description(adapter.quote("payment_type")) }} as payment_type_descripted,
        {{ dbt.safe_cast("trip_type", api.Column.translate_type("integer")) }} as trip_type,
        {{ adapter.quote("congestion_surcharge") }}

    from source
    where rn = 1
)
select * from renamed

  
-- dbt build --select <model_name> --vars '{'is_test_run': 'false'}'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}