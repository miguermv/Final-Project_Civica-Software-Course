
with 

source as (

    select * from {{ ref('snap_hotels_schema__customers') }}

),

renamed as (

    select
        {{ dbt_utils.generate_surrogate_key(['customer_id']) }} as customer_id,
        first_name::VARCHAR(30)                                 as first_name,
        last_name::VARCHAR(100)                                 as last_name,
        email::VARCHAR(320)                                     as email,
        TRIM(phone_number)::VARCHAR(15)                         as phone_number,
        birth_country::VARCHAR(100)                             as birth_country,
        address::VARCHAR(200)                                   as address,
        TO_DATE(dateofbirth,'DD/MM/YYYY')                       as dateofbirth,
        gender::VARCHAR(6)                                      as gender,
        dbt_valid_from                                          as valid_from,
        dbt_valid_to                                            as valid_to
    from source

)

select * from renamed
