WITH


fct_bookings AS (
    SELECT * FROM {{ ref('fct_bookings_summary') }}
),


dim_agents AS (
    SELECT * FROM {{ ref('dim_agents') }}
),


dim_dates AS (
    SELECT * FROM {{ ref('dim_dates') }}
),


-- Analisis de ganancias según el mes del año
seasonal_analysis AS (


    SELECT 
        d.year_number,
        d.month_name,
        COUNT(DISTINCT b.customer_id) AS unique_customers, -- Cantidad de clientes distintos por mes
        SUM(b.final_price_euros) AS total_revenue_euros, -- Total de ganancias por mes
        (SUM(b.final_price_euros) / NULLIF(COUNT(DISTINCT b.customer_id), 0))::DECIMAL(10,2) AS avg_revenue_per_customer, -- Media de ingresos por cliente
        -- Porcentaje de cambio en ganancias respecto al mes anterior
        CASE
            WHEN LAG(total_revenue_euros) OVER (ORDER BY year_number, month_of_year) IS NULL THEN 0
            ELSE ROUND(
                ((total_revenue_euros - LAG(total_revenue_euros) OVER (ORDER BY year_number, month_of_year)) 
                / LAG(total_revenue_euros) OVER (ORDER BY year_number, month_of_year)) * 100, 2)
        END AS revenue_change_percentage
    FROM fct_bookings b
    RIGHT JOIN dim_dates d
    ON b.checkoutdate = d.date_id 
    JOIN dim_agents a
    ON b.agent_id = a.agent_id
    GROUP BY  d.month_name, year_number, month_of_year
    ORDER BY year_number

)

SELECT * FROM seasonal_analysis