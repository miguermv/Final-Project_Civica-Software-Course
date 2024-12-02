WITH

fct_bookings AS (
    SELECT * FROM {{ ref('fct_bookings_summary') }}
),

dim_rooms AS (
    SELECT * FROM {{ ref('dim_rooms') }}
),

dim_agents AS (
    SELECT * FROM {{ ref('dim_agents') }}
),

dim_dates AS (
    SELECT * FROM {{ ref('dim_dates') }}
),

dim_hotels AS (
    SELECT * FROM {{ ref('dim_hotels') }}
),

years AS (
    SELECT DISTINCT year_number
    FROM dim_dates
    WHERE year_number IN (2023, 2024)
),



room_operations AS (
    SELECT
        hotel_name,
        room_number,
        room_type,
        d.year_number,
        (SUM(COALESCE(b.stay_duration, 0)) / 365)::DECIMAL(10,2) AS occupancy_percentage,
        SUM(COALESCE(b.stay_duration, 0))  AS occuped_days,
        COALESCE(SUM(price_per_night * stay_duration * (1 - COALESCE(a.comission_rate / 100.0, 0))),0)::DECIMAL(10,2) AS revenue_per_room,
    FROM dim_rooms r
    LEFT JOIN dim_hotels h
        ON r.hotel_id = h.hotel_id
    LEFT JOIN fct_bookings b
        ON r.room_id = b.room_id
    JOIN dim_dates d
        ON d.date_id = b.checkindate 
    LEFT JOIN dim_agents a 
        ON a.agent_id = b.agent_id
    

    GROUP BY r.room_number, h.hotel_name, d.year_number, room_type
    ORDER BY  r.room_number,  d.year_number,h.hotel_name


)

SELECT * FROM room_operations
