{{
    config(
        materialized='table',
        pre_hook="ALTER ACCOUNT SET CORTEX_ENABLED_CROSS_REGION = 'AWS_EU';"
    )
}}

WITH 

base_dates AS (

    select * from {{ ref('base_utilities__dates') }}

),

base_holidays AS (

    select * from {{ ref('base_utilities__holidays_es') }}

),

stg_utilities__dates AS (

    select 
        date_id,
        day_of_week_name,
        day_of_week_name_short,
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
        year_number
        is_holiday,
        is_workday,
        {{ cortex_translate('holiday_type') }} as holiday_type_en,
        {{ cortex_translate('holiday_desc') }} as holiday_desc_en,

    FROM base_dates dates
    LEFT JOIN base_holidays holidays
    ON dates.date_id = holidays.holiday_id
)

SELECT * FROM stg_utilities__dates
