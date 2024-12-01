{{ config(
    materialized='incremental',
    unique_key = 'payment_id'
    ) 
}}

with 

payments as (

    --select * from {{ ref('stg_hotels_schema__payments') }}

    {% if is_incremental() %}

        where datetimeload_utc > (select MAX(datetimeload_utc) from {{ this }} )

    {% endif %}

),


bookings as (

    select  * from {{ ref('stg_hotels_schema__bookings') }}

),

agents as (

    select  * from {{ ref('stg_hotels_schema__agents') }}
),


renamed as (

    select
        payment_id,
        b.hotel_id,
        b.room_id,
        (p.base_price_euros / DATEDIFF('day', checkindate, checkoutdate))::DECIMAL(10,2) as price_per_night,
        (final_price_euros * (comission_rate / 100))::DECIMAL(10,2) as total_revenue_euros

    from payments p
    inner join bookings b 
        on p.booking_id = b.booking_id
    inner join agents a 
        on b.agent_id = a.agent_id

)

select * from renamed
