{{
    config(
        materialized='table'
    )
}}

WITH quarterly_revenue AS (
    SELECT
        service_type,
        EXTRACT(YEAR FROM pickup_datetime) AS year,
        EXTRACT(QUARTER FROM pickup_datetime) AS quarter,
        SUM(total_amount) AS revenue
    FROM {{ ref('fact_trips') }}
    WHERE EXTRACT(YEAR FROM pickup_datetime) IN (2019, 2020)
    GROUP BY service_type, year, quarter
),

quarterly_growth AS (
    SELECT 
        year,
        quarter,
        CONCAT(CAST(year AS STRING), '/Q', CAST(quarter AS STRING)) AS year_quarter,
        service_type,
        revenue,
        LAG(revenue) OVER (PARTITION BY service_type, quarter ORDER BY year) AS prev_year_revenue,
        ROUND(
            ((revenue - LAG(revenue) OVER (PARTITION BY service_type, quarter ORDER BY year)) /
             NULLIF(LAG(revenue) OVER (PARTITION BY service_type, quarter ORDER BY year), 0)) * 100, 2
        ) AS yoy_growth
    FROM quarterly_revenue
)
SELECT *
FROM quarterly_growth
ORDER BY service_type, year DESC, quarter DESC


