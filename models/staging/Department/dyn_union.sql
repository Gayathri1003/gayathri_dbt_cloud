{{ config(
    materialized='table',
    schema='DBT_CLOUD'
) }}

-- Get all tables from RAW schema
{% set tables_query %}
SELECT table_name
FROM GAYATHRI_DBT_CLOUD.INFORMATION_SCHEMA.TABLES
WHERE table_schema = 'GAYATHRI_RAW_SCHEMA'
{% endset %}

{% set tables_result = run_query(tables_query) %}
{% set tables = tables_result.columns[0].values() %}


-- Get all columns from all tables
{% set columns_query %}
SELECT DISTINCT column_name
FROM GAYATHRI_DBT_CLOUD.INFORMATION_SCHEMA.COLUMNS
WHERE table_schema = 'GAYATHRI_RAW_SCHEMA'
{% endset %}

{% set columns_result = run_query(columns_query) %}
{% set all_columns = columns_result.columns[0].values() %}


-- Start dynamic union
{% for table in tables %}

SELECT

{% for col in all_columns %}

-- Check if column exists in current table
{% set col_check_query %}
SELECT COUNT(*)
FROM GAYATHRI_DBT_CLOUD.INFORMATION_SCHEMA.COLUMNS
WHERE table_schema = 'GAYATHRI_RAW_SCHEMA'
AND table_name = '{{ table }}'
AND column_name = '{{ col }}'
{% endset %}

{% set col_result = run_query(col_check_query) %}
{% set exists = col_result.columns[0].values()[0] %}

{% if exists > 0 %}
{{ col }}
{% else %}
NULL AS {{ col }}
{% endif %}

{% if not loop.last %},{% endif %}

{% endfor %}

FROM GAYATHRI_DBT_CLOUD.GAYATHRI_RAW_SCHEMA.{{ table }}

{% if not loop.last %}
UNION ALL
{% endif %}

{% endfor %}