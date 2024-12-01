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


renamed as (

    select
        payment_id,
        booking_id,
        customer_id,
        discount_id,
        base_price_euros,
        final_price_euros,
        (base_price_euros - final_price_euros) as discount_applied,
        payment_date,
        payment_amount,
        payment_method,
        payment_status
    from payments 

)

select * from renamed
