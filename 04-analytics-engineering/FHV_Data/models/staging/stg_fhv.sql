{{
    config(
        materialized='view'
    )
}}

with fhvdata as 
(
  select *
  from {{ source('staging','fhv_2019') }}
  where dispatching_base_num is not null 
)
select * from fhvdata


-- dbt build --select <model_name> --vars '{'is_test_run': 'false'}'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}