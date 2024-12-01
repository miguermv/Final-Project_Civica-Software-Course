
WITH 

rooms AS (

    SELECT * FROM {{ ref('stg_hotels_schema__rooms') }}

),


renamed AS (
    SELECT
        room_id,
        hotel_id,
        room_number,
        price_per_night,
        room_type,
        room_status,
        valid_from,
        valid_to
        -- Cálculo del total ganado por habitación
        --(DATEDIFF('day', b.checkindate, b.checkoutdate) * r.price_per_night * 
        --    (1 - COALESCE(a.comission_rate, 0) / 100))::DECIMAL(10,2) AS total_earned_per_room
    FROM rooms 


)

SELECT * FROM renamed
