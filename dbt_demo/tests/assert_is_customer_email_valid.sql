{{ config( severity = "warn") }}

select
    *
from {{ ref('dim_customers' )}} 
where email not like '%_@__%.__%'