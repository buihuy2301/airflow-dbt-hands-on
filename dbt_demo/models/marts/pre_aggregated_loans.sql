select 
    customer_id,
    min(start_date) as first_recent_loan_date,
    max(start_date) as most_recent_loan_date,
    sum(loan_amount) as lifetime_value,
    count(loan_id) as number_of_loans
from {{ ref('stg_loans')}}
group by 1