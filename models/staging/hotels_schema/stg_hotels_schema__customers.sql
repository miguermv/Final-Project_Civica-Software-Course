
with 

source as (

    select * from {{ source('hotels_schema', 'customers') }}

),

renamed as (

    select
       {{ dbt_utils.generate_surrogate_key(['customer_id']) }}  as customer_id,
        first_name::VARCHAR(30)                                 as first_name,
        last_name::VARCHAR(100)                                 as last_name,
        email::VARCHAR(320)                                     as email,
        TRIM(phone_number)::VARCHAR(15)                         as phone_number,
        address::VARCHAR(200)                                   as address,
        TO_DATE(dateofbirth,'DD/MM/YYYY')                       as dateofbirth,
        gender::VARCHAR(6)                                      as gender
    from source

)

select * from renamed
