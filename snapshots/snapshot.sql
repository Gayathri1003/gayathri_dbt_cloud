{% snapshot snapshot_customer %}

{{
    config(
        target_schema='snapshots',
        unique_key='customer_id',
        strategy='check',
        check_cols=['email', 'phone_number']
    )
}}

SELECT
    customer_id,
    name,
    email,
    phone_number,
    aadhar_number
FROM {{ source('shop_src', 'customers') }}

{% endsnapshot %}
