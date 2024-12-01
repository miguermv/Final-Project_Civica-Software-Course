WITH

-- CTE para obtener reservas
fct_bookings AS (
    SELECT * FROM {{ ref('fct_bookings_summary') }}
),

-- CTE para obtener habitaciones
dim_rooms AS (
    SELECT * FROM {{ ref('dim_rooms') }}
),

-- CTE para obtener agentes
dim_agents AS (
    SELECT * FROM {{ ref('dim_agents') }}
),

-- CTE para obtener información de fechas (días festivos)
dim_dates AS (
    SELECT 
        date_id,
        is_holiday 
    FROM {{ ref('dim_dates') }}
),

-- CTE para calcular el total de ganancias, cantidad de clientes distintos y otros análisis por mes
monthly_analysis AS (
    SELECT
        EXTRACT(MONTH FROM b.booking_created_at) AS month,
        EXTRACT(YEAR FROM b.booking_created_at) AS year,
        SUM(b.final_price_euros) AS total_revenue_euros, -- Total de ganancias por mes
        COUNT(DISTINCT b.customer_id) AS unique_customers, -- Cantidad de clientes distintos por mes
    FROM fct_bookings b
    GROUP BY EXTRACT(MONTH FROM b.booking_created_at), EXTRACT(YEAR FROM b.booking_created_at)
)

-- Selección final con los análisis mensuales + Ganancia media por cliente
SELECT 
    ma.year,
    ma.month,
    ma.unique_customers,
    ma.total_revenue_euros,
    (ma.total_revenue_euros / NULLIF(ma.unique_customers, 0))::DECIMAL(10,2) AS avg_revenue_per_customer, -- Promedio de ingresos por cliente
    -- Porcentaje de cambio en ganancias respecto al mes anterior
    CASE
        WHEN LAG(ma.total_revenue_euros) OVER (ORDER BY ma.year, ma.month) IS NULL THEN 0
        ELSE ROUND(((ma.total_revenue_euros - LAG(ma.total_revenue_euros) OVER (ORDER BY ma.year, ma.month)) / LAG(ma.total_revenue_euros) OVER (ORDER BY ma.year, ma.month)) * 100, 2)
    END AS revenue_change_percentage
FROM monthly_analysis ma
ORDER BY ma.year, ma.month
