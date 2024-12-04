{{ config(
        materialized='incremental',
        unique_key = 'booking_id',
        on_schema_change='fail'
    ) 
}}

with 

source as (

    select * from {{ source('hotels_schema', 'bookings') }}

{% if is_incremental() %}

    WHERE _fivetran_synced > (SELECT MAX(datetimeload_utc) FROM {{ this }} )

{% endif %}

),

renamed as (

    select
        {{ dbt_utils.generate_surrogate_key(['booking_id']) }}                as booking_id,
        {{ dbt_utils.generate_surrogate_key(['customer_id']) }}               as customer_id,
        {{ dbt_utils.generate_surrogate_key(['hotel_id']) }}                  as hotel_id,
        {{ dbt_utils.generate_surrogate_key(['hotel_id','room_number']) }}    as room_id,
        {{ dbt_utils.generate_surrogate_key(['agent_id']) }}                  as agent_id,
        no_of_adults::INT                                                     as no_of_adults,
        no_of_children::INT                                                   as no_of_children,
        AS_BOOLEAN(required_car_parking)                                      as required_car_parking,
        check_in_date::DATE                                                   as checkInDate,
        check_out_date::DATE                                                  as checkOutDate,
        base_price::DECIMAL(10,2)                                             as base_price_euros,
        final_price::DECIMAL(10,2)                                            as final_price_euros,
        discount_code::VARCHAR(20)                                            as discount_code,
        discount_rate::DECIMAL(10,2)                                          as discount_rate,
        created_at::DATE                                                      as created_at,
        _fivetran_synced::TIMESTAMP_TZ                                        as datetimeload_utc

    from source

)

select * from renamed

