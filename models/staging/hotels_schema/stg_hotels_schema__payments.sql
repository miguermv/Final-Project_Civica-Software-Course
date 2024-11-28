{{ config(materialized='view') }}

with 

source as (

    select * from {{ source('hotels_schema', 'bookings') }}

),

renamed as (

    select
        {{ dbt_utils.generate_surrogate_key(['booking_id', 'customer_id']) }}  as payment_id,
        {{ dbt_utils.generate_surrogate_key(['booking_id']) }}  as booking_id,
        {{ dbt_utils.generate_surrogate_key(['customer_id']) }} as customer_id,
        payment_date::DATE                                      as payment_date,
        payment_amount::DECIMAL(10,2)                           as payment_amount,
        payment_method::VARCHAR(50)                             as payment_method,
        payment_status::VARCHAR(20)                             as payment_status
    from source

)

select * from renamed
