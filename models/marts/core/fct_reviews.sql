{{
    config(
        materialized='incremental',
        unique_key = 'review_id',
        pre_hook="ALTER ACCOUNT SET CORTEX_ENABLED_CROSS_REGION = 'AWS_EU';"
    )
}}

with 

reviews as (

    select * from {{ ref('stg_hotels_schema__reviews') }}

{% if is_incremental() %}

    WHERE datetimeload_utc > (SELECT MAX(datetimeload_utc) FROM {{ this }} )

{% endif %}

),

renamed as (

    select
        review_id,
        customer_id,
        hotel_id,
        review,
        {{ cortex_sentiment('review') }} as sentiment_category,
        rating,
        review_date
    from reviews

)

select * from renamed