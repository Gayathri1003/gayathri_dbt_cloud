{% macro validate_phone(column_name) %}
CASE 
    WHEN {{ column_name }} IS NULL THEN 'NULL'
    WHEN REGEXP_LIKE(phone_number, '^[0-9]{10}$') THEN {{ column_name }}
    ELSE 'Invalid'
END
{% endmacro %}