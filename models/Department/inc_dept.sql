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
        DEPARTMENT,
        MD5(CONCAT_WS('|', EMP_ID, EMP_NAME, SALARY, DEPARTMENT)) AS ROW_HASH
    FROM {{ source('shop_src', 'HR') }}

    UNION ALL

    SELECT 
        EMP_ID,
        EMP_NAME,
        SALARY,
        DEPARTMENT,
        MD5(CONCAT_WS('|', EMP_ID, EMP_NAME, SALARY, DEPARTMENT)) AS ROW_HASH
    FROM {{ source('shop_src', 'IT') }}

    UNION ALL

    SELECT 
        EMP_ID,
        EMP_NAME,
        SALARY,
        DEPARTMENT,
        MD5(CONCAT_WS('|', EMP_ID, EMP_NAME, SALARY, DEPARTMENT)) AS ROW_HASH
    FROM {{ source('shop_src', 'Finance') }}

)

{% if is_incremental() %}

SELECT
    s.*,

    CASE
        WHEN t.EMP_ID IS NULL THEN 'INSERT'
        WHEN s.ROW_HASH <> t.ROW_HASH THEN 'UPDATE'
        ELSE 'NO_CHANGE'
    END AS LOAD_TYPE,

    CASE
        WHEN t.EMP_ID IS NULL THEN CURRENT_TIMESTAMP
        ELSE t.created_at
    END AS created_at,

    CASE
        WHEN t.EMP_ID IS NULL THEN CURRENT_TIMESTAMP
        WHEN s.ROW_HASH <> t.ROW_HASH THEN CURRENT_TIMESTAMP
        ELSE t.updated_at
    END AS updated_at

FROM source_data s
LEFT JOIN {{ this }} t
ON s.EMP_ID = t.EMP_ID

{% else %}

SELECT
    *,
    'INSERT' AS LOAD_TYPE,
    CURRENT_TIMESTAMP AS created_at,
    CURRENT_TIMESTAMP AS updated_at
FROM source_data

{% endif %}