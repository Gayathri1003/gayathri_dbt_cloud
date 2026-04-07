SELECT * from {{ source('shop_src', 'HR') }}

union all

SELECT * from {{ source('shop_src', 'IT') }}

UNION ALL

SELECT * from {{ source('shop_src', 'Finance') }}
