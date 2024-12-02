
with 

customers as (

    select * from {{ ref('stg_hotels_schema__customers') }}

),

renamed as (

    select
        customer_id,
        first_name,
        last_name,
        email,
        phone_number,
        birth_country,
        address,
        YEAR(CURRENT_DATE) - YEAR(dateofbirth) as customer_age,
        dateofbirth,
        gender,
        valid_from,
        valid_to
    from customers

)

select * from renamed
