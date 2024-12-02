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

dim_customers AS (
    SELECT * FROM {{ ref('dim_customers') }}

),



hotels_summary AS (

    SELECT 
        h.hotel_name,
        COUNT(DISTINCT r.room_id) AS total_rooms,  -- Total de habitaciones
        COUNT(DISTINCT b.booking_id) AS total_reservations,  -- Total de reservas
        COUNT(DISTINCT CASE WHEN gender = 'Male' THEN c.customer_id ELSE NULL END) AS male_customers,
        COUNT(DISTINCT CASE WHEN gender = 'Female' THEN c.customer_id ELSE NULL END) AS female_customers,
        COUNT(DISTINCT CASE WHEN gender IS NULL OR gender = 'Other' THEN c.customer_id ELSE NULL END) AS other_customers,
        AVG(stay_duration)::DECIMAL(10,2) AS avg_stay_duration, -- Media de duración de las estancias  
        COUNT(DISTINCT review_id) AS total_reviews,  -- Total de reseñas
        COUNT(DISTINCT CASE WHEN sentiment_category = 'Positive' THEN review_id ELSE NULL END) AS positive_reviews,
        COUNT(DISTINCT CASE WHEN sentiment_category = 'Positive' THEN review_id ELSE NULL END) AS negative_reviews,
        AVG(rating)::DECIMAL(10,2) AS avg_rating,  -- Media de las calificaciones
    FROM dim_hotels h
    INNER JOIN fct_bookings b ON h.hotel_id = b.hotel_id
    JOIN dim_rooms r ON h.hotel_id = r.hotel_id
    INNER JOIN dim_customers c ON c.customer_id = b.customer_id
    GROUP BY h.hotel_name
    ORDER BY h.hotel_name

)

SELECT * FROM hotels_summary

