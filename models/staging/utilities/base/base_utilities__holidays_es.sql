with 

source as (

    select * from {{ source('utilities', 'holidays_es') }}

),

renamed as (

    select
        TO_DATE(fecha,'DD/MM/YYYY')                                         as holiday_id,
        dia_semana::VARCHAR(10)                                             as day_of_week_name_es,
        {{ ifvalue_to_bool('laborable_festivo_domingo', "'festivo'") }}     as is_holiday,
        {{ ifvalue_to_bool('laborable_festivo_domingo', "'laborable'") }}   as is_workday,
        tipo_festivo::VARCHAR(50)                                           as holiday_type,
        festividad::VARCHAR(200)                                            as holiday_desc
    from source

)

select * from renamed