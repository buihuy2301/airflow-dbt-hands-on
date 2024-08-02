{{ 
    config(
        pre_hook="""
        {% if execute %}
            {% set table_exists_query %}
                SELECT 1
                FROM information_schema.tables 
                WHERE table_schema = '{{ target.schema }}' 
                  AND table_name = '{{ this.name }}';
            {% endset %}
            
            {% set results = run_query(table_exists_query) %}
            
            {% if results.columns[0].values() | length > 0 %}
                truncate table {{ this }};
            {% endif %}
        {% endif %}
        """
    )
}}

select customer_id,
       {{ mask_pii('name') }} as name,
       gender,
       sector,
       date_of_birth,
       address,
       city,
       country,
       {{ mask_pii('phone_number') }} as phone_number,
       {{ mask_pii('email') }} as email,
       income,
       employment_status,
       years_of_employment,
       cb_person_default_on_file,
       cb_preson_cred_hist_length,
       education_level,
       branch_code,
       reg_date
from {{ source('demo', 'customers') }}