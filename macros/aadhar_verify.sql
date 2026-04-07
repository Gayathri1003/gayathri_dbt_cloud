{% macro validate_aadhar(column_name) %}
CASE 
    WHEN {{ column_name }} IS NULL THEN 'NULL'
    WHEN REGEXP_LIKE(aadhar_number, '^[0-9]{12}$') THEN {{ column_name }}
    ELSE 'Invalid'
END
{% endmacro %}