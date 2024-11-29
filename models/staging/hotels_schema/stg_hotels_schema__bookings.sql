{{ config(
    materialized='incremental',
    unique_key = 'booking_id'
    ) 
}}

with 
source as (

    select * from {{ ref('base_hotels_schema__bookings') }}

{% if is_incremental() %}

    WHERE datetimeload_utc > (SELECT MAX(datetimeload_utc) FROM {{ this }} )

{% endif %}

),

renamed as (

    select
        booking_id,
        customer_id,
        hotel_id,
        room_id,
        agent_id,
        no_of_adults,
        no_of_children,
        required_car_parking,
        checkInDate,
        checkOutDate,
        created_at,
        datetimeload_utc
    from source
    
)

select * from renamed
