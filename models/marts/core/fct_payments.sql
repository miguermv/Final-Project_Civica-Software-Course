{{ config(
    materialized='incremental',
    unique_key = 'booking_id'
    ) 
}}

with 

payments as (

    select * from {{ ref('stg_hotels_schema__payments') }}

{% if is_incremental() %}

    WHERE datetimeload_utc > (SELECT MAX(datetimeload_utc) FROM {{ this }} )

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
        p.booking_id,
        p.customer_id,
        discount_id,
        p.base_price_euros,
        final_price_euros,
        (p.base_price_euros - final_price_euros) as discount_applied,
        payment_date,
        payment_amount,
        payment_method,
        payment_status,
        (payment_amount * (comission_rate / 100))::DECIMAL(10,2) as total_revenue_euros
        --datetimeload_utc
    from payments p
    INNER JOIN bookings b 
        ON p.booking_id = b.booking_id
    INNER JOIN agents a 
        ON b.agent_id = a.agent_id

)

select * from renamed
