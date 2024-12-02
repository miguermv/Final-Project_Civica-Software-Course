WITH


fct_bookings AS (
    SELECT * FROM {{ ref('fct_bookings_summary') }}
),

dim_rooms AS (
    SELECT * FROM {{ ref('dim_rooms') }}
),


dim_hotels AS (
    SELECT * FROM {{ ref('dim_hotels') }}
),

dim_agents AS (
    SELECT * FROM {{ ref('dim_agents') }}

),


total_revenue AS (
        SELECT 
            --Cuanto se he ganado en total sumando todos los hoteles
            SUM(r.price_per_night * b.stay_duration * (1 - COALESCE(a.comission_rate / 100.0, 0))) as total_revenue 
        FROM fct_bookings b
        JOIN dim_agents a ON a.agent_id = b.agent_id
        JOIN dim_rooms r ON b.room_id = r.room_id
        JOIN dim_hotels h ON r.hotel_id = h.hotel_id
),



hotels_summary AS (

    SELECT 
        h.hotel_name,
        COUNT(DISTINCT b.booking_id) AS total_reservations,  -- Total de reservas
        (SUM(r.price_per_night) / COUNT(r.room_id))::DECIMAL(10,2) AS avg_price_per_room,  -- Precio medio por habitación
        SUM(r.price_per_night * b.stay_duration)::DECIMAL(10,2) AS pre_commission_revenue, -- Ingresos totales
        SUM(r.price_per_night * b.stay_duration * (1 - COALESCE(a.comission_rate / 100.0, 0)))::DECIMAL(10,2) AS hotel_net_revenue , -- Ingresos totales menos comisiónes
        ((hotel_net_revenue/total_revenue) * 100)::DECIMAL(10,2) AS hotel_profit_share --Que porcentaje del beneficio total se ha generado en este hotel
    FROM dim_hotels h
    LEFT JOIN fct_bookings b ON h.hotel_id = b.hotel_id
    RIGHT JOIN dim_rooms r ON r.room_id = b.room_id
    RIGHT JOIN dim_agents a ON a.agent_id = b.agent_id
    CROSS JOIN total_revenue
    GROUP BY h.hotel_name, total_revenue
    ORDER BY h.hotel_name

)

SELECT * FROM hotels_summary