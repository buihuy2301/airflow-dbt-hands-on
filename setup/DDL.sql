create table if not exists dwh.demo.customers (
  "customer_id" varchar,
  "name" varchar,
  "gender" varchar,
  "sector" varchar,
  "date_of_birth" date,
  "address" varchar,
  "city" varchar,
  "country" varchar,
  "phone_number" varchar,
  "email" varchar,
  "income" double precision,
  "employment_status" varchar,
  "years_of_employment" integer,
  "cb_person_default_on_file" varchar,
  "cb_preson_cred_hist_length" integer,
  "education_level" varchar,
  "reg_date" date,
  "branch_code" varchar
);

create table if not exists dwh.demo.loans (
  "loan_id" uuid,
  "customer_id" varchar,
  "loan_amount" integer,
  "interest_rate" float,
  "start_date" date,
  "end_date" date,
  "status" varchar,
  "loan_intent" varchar,
  "credit_score" integer,
  "loan_term" integer,
  "loan_grade" varchar,
  "repayment_method" varchar,
  "collateral_value" integer,
  "loan_purpose" varchar,
  "created_at" timestamp
);