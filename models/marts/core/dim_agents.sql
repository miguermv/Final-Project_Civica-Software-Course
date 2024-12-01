with 

agents as (

    select * from {{ ref('stg_hotels_schema__agents') }}

),


renamed as (

    select
        agent_id,
        name,
        type,
        comission_rate,
        active_status,
        -- SUM(p.payment_amount * (a.comission_rate / 100))::DECIMAL(10,2) as agent_revenue,
        -- (COUNT(b.booking_id) / tb.total_bookings) * 100 AS booking_success_rate
    from agents 

    
)

select * from renamed