
with 

hotels as (

    select * from {{ ref('stg_hotels_schema__hotels') }}

),


renamed as (

    select
        hotel_id,
        hotel_name,
        hotel_address,
        hotel_phone_number,
        hotel_email,
        hotel_website,
        hotel_stars,
        checkInTime,
        checkOutTime
        -- Cálculo del total ganado por hotel
        -- total_hotel_revenue::DECIMAL(10,2) as total_hotel_revenue
    from hotels 



)

select * from renamed
