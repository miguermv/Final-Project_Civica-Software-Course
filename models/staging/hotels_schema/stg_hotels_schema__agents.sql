with 

source as (

    select * from {{ source('hotels_schema', 'agents') }}

),

renamed as (

    select
        {{ dbt_utils.generate_surrogate_key(['agent_id']) }}    as agent_id,
        name::VARCHAR(300)                                      as name,
        type::VARCHAR(30)                                       as type,
        comission_rate::DECIMAL(10,2)                           as comission_rate,
        active_status::BOOLEAN                                  as active_status,
        _fivestran_synced::TIMESTAMP_TZ                         as datetimeload_utc
    from source
    
)

select * from renamed