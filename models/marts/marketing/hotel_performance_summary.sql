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



hotels_summary AS (

    SELECT 
        h.hotel_name,
        COUNT(DISTINCT r.room_id) AS total_rooms,  -- Total de habitaciones
        SUM(r.price_per_night) / COUNT(r.room_id)::DECIMAL(10,2) AS avg_price_per_room,  -- Precio medio por habitación
        COUNT(DISTINCT b.booking_id) AS total_reservations,  -- Total de reservas
        AVG(stay_duration)::DECIMAL(10,2) AS avg_stay_duration, -- Media de duración de las estancias  
        COUNT(review_id) AS total_reviews,  -- Total de reseñas
        AVG(rating)::DECIMAL(10,2) AS avg_rating,  -- Media de las calificaciones
        SUM(r.price_per_night * b.stay_duration * (1 - COALESCE(a.comission_rate / 100.0, 0)))::DECIMAL(10,2) AS total_revenue, -- Ingresos totales menos comisiónes
    FROM dim_hotels h
    JOIN dim_rooms r ON h.hotel_id = r.hotel_id
    JOIN fct_bookings b ON h.hotel_id = b.hotel_id
    JOIN dim_agents a ON a.agent_id = b.agent_id
    GROUP BY h.hotel_name
    ORDER BY h.hotel_name

)

SELECT * FROM hotels_summary

