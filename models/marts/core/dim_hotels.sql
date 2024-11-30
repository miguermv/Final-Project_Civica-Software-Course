
with 

hotels as (

    select * from {{ ref('stg_hotels_schema__hotels') }}

),

bookings as (

    select 
        hotel_id,
        SUM(base_price_euros * (comission_rate / 100)) as total_hotel_revenue
    from {{ ref('stg_hotels_schema__bookings') }} b
    JOIN {{ref("stg_hotels_schema__agents")}} a
    ON b.agent_id = a.agent_id
    group by hotel_id

),

renamed as (

    select
        h.hotel_id,
        hotel_name,
        hotel_address,
        hotel_phone_number,
        hotel_email,
        hotel_website,
        hotel_stars,
        checkInTime,
        checkOutTime,
        total_hotel_revenue::DECIMAL(10,2) as total_hotel_revenue
    from hotels h
    LEFT JOIN bookings b
        ON h.hotel_id = b.hotel_id


)

select * from renamed
