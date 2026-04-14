{{ config(
    materialized='table'
) }}

SELECT * FROM {{ ref('stg_dept_HR') }}

UNION ALL

SELECT * FROM {{ ref('stg_dept_Finance') }}

UNION ALL

SELECT * FROM {{ ref('stg_dept_IT') }}