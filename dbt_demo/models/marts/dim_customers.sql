with customers as (
   select * from {{ ref('stg_customers')}}
),
loans as (
   select * from {{ ref('pre_aggregated_loans')}}
),
branchs as (
   select * from {{ ref('branchs')}}
),
final as (
   select
      customers.customer_id,
      customers.name,
      customers.gender,
      extract(year from current_date) - extract(year from customers.date_of_birth) as age,
      customers.address,
      customers.city,
      customers.country,
      customers.phone_number,
      customers.email,
      customers.income,
      customers.employment_status,
      customers.years_of_employment,
      customers.cb_person_default_on_file,
      customers.cb_preson_cred_hist_length,
      customers.education_level,
      branchs.branch_name,
      loans.first_recent_loan_date,
      loans.most_recent_loan_date,
      loans.lifetime_value,
      loans.number_of_loans
   from customers
   left join loans using (customer_id)
   left join branchs using (branch_code)
)
select * from final