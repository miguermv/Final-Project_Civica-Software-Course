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
    --BOOKINGS

        booking_id,
        customer_id,
        hotel_id,
        room_id,
        agent_id,
        no_of_adults,
        no_of_children,
        CASE  --Campo que me permite saber que tipo de cliente es
            WHEN no_of_children = 0 AND no_of_adults = 1 THEN 'Solo Traveler'
            WHEN no_of_children = 0 AND no_of_adults = 2 THEN 'Couple'
            WHEN no_of_children = 0 AND no_of_adults >= 3 THEN 'Group'
            WHEN no_of_children >= 1 AND no_of_adults >= 1 THEN 'Family'
            ELSE 'Unknown'
        END AS customer_type,
        required_car_parking,
        (CASE WHEN required_car_parking = TRUE THEN 1 ELSE 0 END) AS car_parking_requested_flag, --Si necesita parking marca como 1, se facilita mas tarde la suma total de este campo.
        checkInDate,
        checkOutDate,
        DATEDIFF('day', checkindate, checkoutdate) AS stay_duration,  --Calculo de total de dias que pasará el cliente en el hotel
        booking_created_at,

    --PAYMENTS
        discount_id,
        base_price_euros,
        final_price_euros,
        (base_price_euros - final_price_euros) as discount_applied,
        payment_date,
        payment_amount,
        payment_method,
        payment_status,

    --REVIEWS
        review_id,
        review,
        {{ cortex_sentiment('review') }} as sentiment_category, --Negative, Positive or Neutral
        rating,
        review_date
    from stg_bookings
    
)

select * from renamed
