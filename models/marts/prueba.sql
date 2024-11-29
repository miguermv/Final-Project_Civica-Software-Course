

WITH 

dates AS (

    select * from {{ ref('stg_utilities__dates') }}

),

payments AS (

    select * from {{ ref('stg_hotels_schema__payments') }}

),

stg_utilities__dates AS (

    select 
      date_id,
      payment_date

    FROM dates d
    INNER JOIN payments p
    ON d.date_id = p.payment_date
)

SELECT * FROM stg_utilities__dates
