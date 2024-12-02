{{
    config(
        materialized='table',
    )
}}

WITH 

dates AS (

    select * from {{ ref('stg_utilities__dates') }}

),

dim_dates AS (

    select 
        date_id,
        day_of_week_name,
        day_of_week_name_short,
        day_of_week,
        day_of_month,
        day_of_year,
        week_start_date,
        week_end_date,
        month_of_year,
        month_name,
        month_name_short,
        month_start_date,
        month_end_date,
        quarter_of_year,
        quarter_start_date,
        quarter_end_date,
        year_number,
        is_weekend,
        is_holiday,
        is_workday,
        holiday_type_en,
        holiday_desc_en,
        {{ get_season_type('date_id') }} AS season_type

    FROM dates

)

SELECT * FROM dim_dates
