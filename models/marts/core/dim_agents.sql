with 

agents as (

    select * from {{ ref('stg_hotels_schema__agents') }}

),

bookings as (

    select * from {{ ref('stg_hotels_schema__bookings') }}

),

total_bookings AS (
    -- Cálculo del total de reservas
    SELECT COUNT(*) AS total_bookings
    FROM bookings
),

payments as (

    select * from {{ ref('stg_hotels_schema__payments') }}

),

renamed as (

    select
        a.agent_id,
        name,
        type,
        comission_rate,
        active_status,
        SUM(p.payment_amount * (a.comission_rate / 100))::DECIMAL(10,2) as agent_revenue,
        (COUNT(b.booking_id) / tb.total_bookings) * 100 AS booking_success_rate
    from agents a
    JOIN bookings b
        ON a.agent_id = b.agent_id
    JOIN payments p
        ON p.booking_id = b.booking_id
    CROSS JOIN total_bookings tb
    GROUP BY 
        a.agent_id, a.name, a.type, a.comission_rate, a.active_status, tb.total_bookings
    
)

select * from renamed