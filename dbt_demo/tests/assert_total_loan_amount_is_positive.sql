select
    customer_id,
    sum(amount)
from {{ ref('fct_loans' )}}
group by 1
having sum(amount)  < 0