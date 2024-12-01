with

dim_customers as (

    select * from {{ ref('dim_customers') }}

),

fct_bookings as(

    select * from {{ ref('fct_bookings_summary') }}

),

dim_agents as(

    select * from {{ ref('dim_agents') }}

),

dim_discounts as(

    select * from {{ ref('dim_discounts') }}

),

--CTE que selecciona el Agent mas usado agrupando por cada cliente.
preferred_channel AS (
    SELECT 
        customer_id,
        preferred_agent
    FROM (
        SELECT 
            c.customer_id as customer_id,
            a.name as preferred_agent,
            COUNT(b.booking_id) AS channel_usage,
            ROW_NUMBER() OVER (PARTITION BY c.customer_id ORDER BY COUNT(b.booking_id) DESC) AS rank
        FROM dim_customers c
        JOIN fct_bookings b
        ON c.customer_id = b.customer_id
        JOIN dim_agents a
        ON a.agent_id = b.agent_id
        GROUP BY c.customer_id, a.name
    ) subquery
    WHERE rank = 1
),

customer_segmentation as (

    SELECT
        c.customer_id,
        c.customer_age,
        c.birth_country,
        COUNT(b.booking_id) as total_reservations,
        SUM(b.stay_duration) as total_nights,
        pc.preferred_agent,
        --Ganancia por cliente menos comisión
        SUM(b.final_price_euros * (1 - (a.comission_rate / 100)))::DECIMAL(10,2) AS customer_revenue, 
        --Porcentaje reservas usnado un descuento
        (CAST(SUM(CASE WHEN d.discount_percentage > 0 THEN 1 ELSE 0 END) AS FLOAT) / COUNT(b.booking_id) * 100)::INT AS discount_usage_rate, 
        --Porcentaje reservas con descuento
        AVG(b.final_price_euros/stay_duration)::DECIMAL(10,2) AS avg_price_per_night 
    FROM dim_customers c
    JOIN fct_bookings b
    ON c.customer_id = b.customer_id
    JOIN dim_agents a
    ON a.agent_id = b.agent_id
    JOIN dim_discounts d
    ON d.discount_id = b.discount_id
    LEFT JOIN preferred_channel pc
    ON c.customer_id = pc.customer_id
    GROUP BY ALL

)

SELECT * FROM customer_segmentation