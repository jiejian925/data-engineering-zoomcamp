{{
    config(
        materialized='table'
    )
}}

with dim_fhv_trips_timestamp_diff as(
    select 
        *,
        TIMESTAMP_DIFF(pickup_datetime, dropoff_datetime, MINUTE) as trip_duration
    from {{ ref('dim_fhv_trips') }}
),

percentile AS (
    SELECT 
        *,
        PERCENTILE_CONT(trip_duration, 0.9) OVER (PARTITION BY Year, Month, PUlocationID,DOlocationID) AS trip_duration_p90
    FROM dim_fhv_trips_timestamp_diff
)

select * from percentile