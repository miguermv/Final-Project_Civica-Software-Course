{{ config(materialized='view') }}

with 

source as (

    select * from {{ ref('base_hotels_schema__bookings') }}

),

renamed as (

    select
        {{ dbt_utils.generate_surrogate_key(['booking_id', 'customer_id']) }} as payment_id,
        booking_id,
        customer_id,
        {{ dbt_utils.generate_surrogate_key(['discount_code']) }} as discount_id,
        base_price_euros,
        final_price_euros,
        payment_date,
        payment_amount,
        payment_method,
        payment_status,
        datetimeload_utc
    from source

)

select * from renamed
