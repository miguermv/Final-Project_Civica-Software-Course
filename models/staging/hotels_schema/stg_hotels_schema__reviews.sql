{{ config(materialized='view') }}

with 
source as (

    select * from {{ source('hotels_schema', 'reviews') }}

),

renamed as (

    select
        {{ dbt_utils.generate_surrogate_key(['review_id']) }}   as review_id,
        {{ dbt_utils.generate_surrogate_key(['customer_id']) }} as customer_id,
        {{ dbt_utils.generate_surrogate_key(['hotel_id']) }}    as hotel_id,
        review::VARCHAR(2000)                                   as review,
        rating::INT                                             as rating,
        review_date::DATE                                       as review_date
    from source

)

select * from renamed
