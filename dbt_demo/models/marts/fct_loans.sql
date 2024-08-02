select 
   loan_id,
   customer_id,
   start_date,
   end_date,
   coalesce(loan_amount, 0) as amount,
   loan_intent,
   credit_score,
   loan_term,
   loan_grade,
   repayment_method,
   collateral_value,
   loan_purpose
from {{ ref('stg_loans' )}}
{% if is_incremental() %}
where start_date >= (select coalesce(max(start_date),'1900-01-01') from {{ this }} )
{% endif %}

