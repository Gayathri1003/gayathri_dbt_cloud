{{ config(
    materialized='incremental',
    unique_key='EMP_ID',
    incremental_strategy='merge'
) }}

WITH source_data AS (

    SELECT 
        EMP_ID,
        EMP_NAME,
        SALARY,
        DEPARTMENT
    FROM {{ source('shop_src', 'HR') }}

    UNION ALL

    SELECT 
        EMP_ID,
        EMP_NAME,
        SALARY,
        DEPARTMENT
    FROM {{ source('shop_src', 'IT') }}

    UNION ALL

    SELECT 
        EMP_ID,
        EMP_NAME,
        SALARY,
        DEPARTMENT
    FROM {{ source('shop_src', 'Finance') }}

),

hashed_source AS (

    SELECT
        *,
        MD5(CONCAT_WS('|', EMP_ID, EMP_NAME, SALARY, DEPARTMENT)) AS ROW_HASH
    FROM source_data

)

{% if is_incremental() %}

SELECT
    s.*,
    
    CASE
        WHEN t.EMP_ID IS NULL THEN 'INSERT'
        ELSE 'UPDATE'
    END AS LOAD_TYPE,

    COALESCE(t.created_at, CURRENT_TIMESTAMP) AS created_at,
    CURRENT_TIMESTAMP AS updated_at

FROM hashed_source s
LEFT JOIN {{ this }} t
ON s.EMP_ID = t.EMP_ID

WHERE
    t.EMP_ID IS NULL
    OR s.ROW_HASH <> t.ROW_HASH

{% else %}

SELECT
    *,
    'INSERT' AS LOAD_TYPE,
    CURRENT_TIMESTAMP AS created_at,
    CURRENT_TIMESTAMP AS updated_at
FROM hashed_source

{% endif %}