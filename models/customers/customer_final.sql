{{ 
    config(
        materialized = 'table'
    ) 
}}

SELECT
    customer_id,
    name,
    email,
    case 
        when phone_number NOT IN ('Invalid','NULL')  then
            CONCAT('XXXXXX', RIGHT(phone_number, 4))
        else phone_number
    END AS phone_number,
    case 
        when aadhar_number NOT IN ('Invalid','NULL') then 
            CONCAT('XXXXXXXX', RIGHT(aadhar_number, 4))
        else aadhar_number
    end as aadhar_number
FROM {{ ref('stg_customers') }}