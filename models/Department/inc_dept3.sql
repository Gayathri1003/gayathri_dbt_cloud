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
    s.EMP_ID,
    s.EMP_NAME,
    s.SALARY,
    s.DEPARTMENT,
    s.ROW_HASH,

    COALESCE(t.created_at, CURRENT_TIMESTAMP) AS created_at,

    CASE
        WHEN t.ROW_HASH IS NULL THEN CURRENT_TIMESTAMP
        WHEN s.ROW_HASH <> t.ROW_HASH THEN CURRENT_TIMESTAMP
        ELSE t.updated_at
    END AS updated_at

FROM hashed_source s
LEFT JOIN {{ this }} t
ON s.EMP_ID = t.EMP_ID

WHERE
    t.EMP_ID IS NULL
    OR s.ROW_HASH <> t.ROW_HASH

{% else %}

SELECT
    EMP_ID,
    EMP_NAME,
    SALARY,
    DEPARTMENT,
    ROW_HASH,
    CURRENT_TIMESTAMP AS created_at,
    CURRENT_TIMESTAMP AS updated_at
FROM hashed_source

{% endif %}