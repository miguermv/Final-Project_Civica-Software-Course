{{ config(
    materialized='incremental',
    unique_key = 'review_id'
    ) 
}}

with 

source as (

    select * from {{ source('hotels_schema', 'reviews') }}

{% if is_incremental() %}

    WHERE _fivetran_synced > (SELECT MAX(_fivetran_synced) FROM {{ this }} )

{% endif %}

),

renamed as (

    select
        {{ dbt_utils.generate_surrogate_key(['review_id']) }}   as review_id,
        {{ dbt_utils.generate_surrogate_key(['customer_id']) }} as customer_id,
        {{ dbt_utils.generate_surrogate_key(['hotel_id']) }}    as hotel_id,
        review::VARCHAR(10000)                                   as review,
        rating::INT                                             as rating,
        review_date::DATE                                       as review_date,
        _fivetran_synced::TIMESTAMP_TZ                          as datetimeload_utc
    from source

)

select * from renamed
