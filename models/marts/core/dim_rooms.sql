
WITH 

rooms AS (

    SELECT * FROM {{ ref('stg_hotels_schema__rooms') }}

),

bookings AS (

    SELECT * FROM {{ ref('stg_hotels_schema__bookings') }}

),

agents AS (

    SELECT * FROM {{ ref('stg_hotels_schema__agents') }}

),

renamed AS (
    SELECT
        r.room_id,
        r.hotel_id,
        r.room_number,
        r.price_per_night,
        r.room_type,
        r.room_status,
        r.valid_from,
        r.valid_to,
        -- Cálculo del total ganado por habitación
        (DATEDIFF('day', b.checkindate, b.checkoutdate) * r.price_per_night * 
            (1 - COALESCE(a.comission_rate, 0) / 100))::DECIMAL(10,2) AS total_earned_per_room
    FROM 
        rooms r
    LEFT JOIN bookings b 
        ON r.room_id = b.room_id
    LEFT JOIN agents a 
        ON b.agent_id = a.agent_id

)

SELECT * FROM renamed
