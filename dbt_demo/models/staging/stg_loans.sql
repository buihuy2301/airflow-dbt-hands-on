select
    loan_id,
    customer_id,
    start_date,
    end_date,
    status,
    loan_amount,
    loan_intent,
    credit_score,
    loan_term,
    loan_grade,
    repayment_method,
    collateral_value,
    loan_purpose
  from {{ source('demo', 'loans') }}