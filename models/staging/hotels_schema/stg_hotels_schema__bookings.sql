{{ config(
    materialized='incremental',
    unique_key = 'booking_id'
    ) 
}}

with 

base_bookings as (

    select * from {{ ref('base_hotels_schema__bookings') }}

),

base_reviews as (

    select * from {{ ref('base_hotels_schema__reviews') }}



),

base_payments as (

    select * from {{ ref('base_hotels_schema__payments') }}



),

renamed as (

    select
        b.booking_id,
        b.customer_id,
        b.hotel_id,
        room_id,
        agent_id,
        no_of_adults,
        no_of_children,
        required_car_parking,
        checkInDate,
        checkOutDate,
        created_at as booking_created_at,
        payment_id,
        {{ control_null_or_empty('discount_code', "'NODISCOUNT'") }} as discount_id,
        base_price_euros,
        final_price_euros,
        payment_date,
        payment_amount,
        payment_method,
        payment_status,
        review_id,
        review,
        rating,
        review_date,
        -- Obtener el datetimeload_utc más reciente entre las tres tablas, se actualiza si cambia algun registro de una de las tres tablas
        GREATEST_IGNORE_NULLS(b.datetimeload_utc, r.datetimeload_utc, p.datetimeload_utc) as datetimeload_utc
    from base_bookings b
    LEFT JOIN base_reviews r
    ON b.booking_id = r.booking_id
    LEFT JOIN base_payments p
    ON b.booking_id = p.booking_id

    
)

select * from renamed

{% if is_incremental() %}

    WHERE datetimeload_utc > (SELECT MAX(datetimeload_utc) FROM {{ this }} )

{% endif %}