with 

source as (

    select * from {{ source('utilities', 'holidays_es') }}

),

renamed as (

    select
        TO_DATE(fecha,'DD/MM/YYYY')                                                       as holiday_id,
        dia_semana::VARCHAR(10)                                                           as day_of_week_name_es,
        --Si no se determina que tipo de dia es (Laborable, festivo...) se da por hecho que es laborable
        {{ replace_null_or_value('laborable_festivo_domingo', "'sabado'", "'laboral'") }} as laborable_festivo_domingo,
        tipo_festivo::VARCHAR(50)                                                         as holiday_type,
        festividad::VARCHAR(200)                                                          as holiday_desc
    from source

)

select * from renamed