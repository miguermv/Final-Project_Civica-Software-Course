{{ config(materialized='view') }}

with

source as (

    select * from {{ source('hotels_schema', 'bookings') }}

),

renamed as (

    select
        {{ dbt_utils.generate_surrogate_key(['booking_id']) }}  as booking_id,
        {{ dbt_utils.generate_surrogate_key(['customer_id']) }} as customer_id,
        {{ dbt_utils.generate_surrogate_key(['hotel_id']) }}    as hotel_id,
        {{ dbt_utils.generate_surrogate_key(['room_id']) }}     as room_id,
        {{ dbt_utils.generate_surrogate_key(['agent_id']) }}    as agent_id,
        checkindate::DATE                                       as checkInDate,
        checkoutdate::DATE                                      as checkOutDate,
        totalprice::DECIMAL(10,2)                               as totalPrice,
        created_at::DATE                                        as created_at
    from source
    
)

select * from renamed
