{% set tables = ['HR', 'IT', 'Finance'] %}

{% for table in tables %}

SELECT *
FROM {{ source('shop_src', table) }}

{% if not loop.last %}
UNION ALL
{% endif %}

{% endfor %}