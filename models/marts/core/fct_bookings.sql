{{ config(
    materialized='incremental',
    unique_key = 'booking_id'
    ) 
}}

with 
stg_bookings as (

    select * from {{ ref('stg_hotels_schema__bookings') }}

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
        --Campo que me permite saber que tipo de cliente es
        CASE
            WHEN no_of_children = 0 AND no_of_adults = 1 THEN 'Solo Traveler'
            WHEN no_of_children = 0 AND no_of_adults = 2 THEN 'Couple'
            WHEN no_of_children = 0 AND no_of_adults >= 3 THEN 'Group'
            WHEN no_of_children >= 1 AND no_of_adults >= 1 THEN 'Family'
            ELSE 'Unknown'
        END AS customer_type,
        required_car_parking,
        --Si necesita parking marca como 1, se facilita mas tarde la suma total de este campo.
        (CASE WHEN required_car_parking = TRUE THEN 1 ELSE 0 END) AS car_parking_requested_flag,
        checkInDate,
        checkOutDate,
        --Calculo de total de dias que pasará el cliente en el hotel
        DATEDIFF('day', check_in_date, check_out_date) AS stay_duration
        created_at,
    from stg_bookings
    
)

select * from renamed
