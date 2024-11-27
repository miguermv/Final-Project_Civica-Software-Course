{{ config(materialized='view') }}

with 

source as (

    select * from {{ source('hotels_schema', 'hotels') }}

),

renamed as (

    select
        {{ dbt_utils.generate_surrogate_key(['hotel_id']) }}    as hotel_id,
        name::VARCHAR(200)                                      as hotel_name,
        address::VARCHAR(250)                                   as hotel_address,
        TRIM(phone_number)::VARCHAR(15)                         as hotel_phone_number,
        email::VARCHAR(200)                                     as hotel_email,
        website::VARCHAR(255)                                   as hotel_website,
        stars::INT                                              as hotel_stars,
        checkintime::TIME                                       as checkInTime,
        checkouttime::TIME                                      as checkOutTime
    from source

)

select * from renamed
