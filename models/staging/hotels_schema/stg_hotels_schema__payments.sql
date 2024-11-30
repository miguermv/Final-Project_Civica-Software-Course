{{ config(
    materialized='incremental',
    unique_key = 'booking_id'
    ) 
}}

with 

source as (

    select * from {{ ref('base_hotels_schema__bookings') }}

{% if is_incremental() %}

    WHERE datetimeload_utc > (SELECT MAX(datetimeload_utc) FROM {{ this }} )

{% endif %}

),

renamed as (

    select
        {{ dbt_utils.generate_surrogate_key(['booking_id', 'customer_id']) }} as payment_id,
        booking_id,
        customer_id,
        {{ control_null_or_empty('discount_code', "'NODISCOUNT'") }} as discount_id,
        base_price_euros,
        final_price_euros,
        payment_date,
        {{ control_null_or_empty('payment_amount', '0') }} as payment_amount,
        payment_method,
        payment_status,
        datetimeload_utc
    from source

)

select * from renamed
