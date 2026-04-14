{{ 
    config(
        materialized = 'view'
    ) 
}}

WITH source AS (
    SELECT 
        customer_id,
        name,
        email,
        phone_number,
        aadhar_number
    FROM {{ source('shop_src', 'customers') }}
),

cleaned AS (
    SELECT
        customer_id,
        TRIM(name) AS name,
        CASE 
            WHEN email is NULL then 'NULL'
            WHEN email LIKE '%@email.com' THEN LOWER(email)
            ELSE 'Invalid'
        END AS email,
        {{ validate_phone('phone_number') }} AS Phone_Number,
        {{ validate_aadhar('aadhar_number') }} AS Aadhar_Number
    FROM source
)

SELECT * FROM cleaned