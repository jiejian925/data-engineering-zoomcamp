
{{
    config(
        materialized='table'
    )
}}

with dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)
select stg.*,
    EXTRACT( year FROM pickup_datetime) as Year,
    EXTRACT( month FROM pickup_datetime) as Month,
    pickup_zone.borough as pickup_borough, 
    pickup_zone.zone as pickup_zone, 
    dropoff_zone.borough as dropoff_borough, 
    dropoff_zone.zone as dropoff_zone  
from {{ ref('stg_fhv') }} as stg
inner join dim_zones as pickup_zone
on stg.PUlocationID = pickup_zone.locationid
inner join dim_zones as dropoff_zone
on stg.DOlocationID = dropoff_zone.locationid
