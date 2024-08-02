{% macro mask_pii(column_name) %}
    {% if column_name == 'email' %}
        regexp_replace({{column_name}}, '^(.)(.*)(.@.*)$', '\2****\3')
    {% else %}
        SUBSTRING({{ column_name }}, 1,3) || '******' || SUBSTRING({{ column_name }}, length({{ column_name }}) -3, length({{ column_name }}))
    {% endif %}
{% endmacro %}