{% snapshot customers_snapshot %}

{{
    config(
      target_database='dwh',
      target_schema='snapshots',
      unique_key='customer_id',

      strategy='check',
      updated_at='reg_date',
      check_cols='all',
      invalidate_hard_deletes=True,
    )
}}

select * from {{ ref('stg_customers') }}

{% endsnapshot %}