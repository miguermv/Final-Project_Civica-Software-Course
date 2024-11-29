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
        check_in_time::TIME                                     as checkInTime,
        check_out_time::TIME                                    as checkOutTime,
        _fivetran_synced::TIMESTAMP_TZ                          as datetimeload_utc
    from source

)

select * from renamed
