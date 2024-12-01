{{ config(
    materialized='incremental',
    unique_key = 'payment_id'
    ) 
}}

with 

source as (

    select * from {{ source('hotels_schema', 'payments') }}
    
{% if is_incremental() %}

    WHERE _fivetran_synced > (SELECT MAX(_fivetran_synced) FROM {{ this }} )

{% endif %}

),

renamed as (

    select
        {{ dbt_utils.generate_surrogate_key(['payment_id']) }}                as payment_id,
        {{ dbt_utils.generate_surrogate_key(['booking_id']) }}                as booking_id,
        payment_date::DATE                                                    as payment_date,
        payment_amount::DECIMAL(10,2)                                         as payment_amount,
        payment_method::VARCHAR(50)                                           as payment_method,
        payment_status::VARCHAR(20)                                           as payment_status,
        _fivetran_synced::TIMESTAMP_TZ                                        as datetimeload_utc

    from source

)

select * from renamed