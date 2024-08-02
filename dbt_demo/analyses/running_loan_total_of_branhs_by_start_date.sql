with customers as (select *
                   from {{ ref('dim_customers') }}),
     loans as (select *
               from {{ ref('fct_loans') }}),
     pre_agg as
         (select start_date,
                 branch_name,
                 count( distinct customers.customer_id) as numbers_of_customer,
                 sum(loans.amount) as loan_total
          from customers
                   join loans using (customer_id)
          where loans.start_date is not null
          group by start_date, branch_name)
select
    start_date,
       branch_name,
       sum(numbers_of_customer)
       over (partition by branch_name order by start_date) as customer_cummulative,
       sum(loan_total)
       over (partition by branch_name order by start_date)  as  running_loan_total_by_start_date
from pre_agg
order by start_date desc