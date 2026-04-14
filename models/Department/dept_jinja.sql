{% set tables = ['HR', 'Finance', 'IT'] %}

{% for table in tables %}

SELECT *
FROM {{ ref('stg_dept_' ~ table) }}

{% if not loop.last %}
UNION ALL
{% endif %}

{% endfor %}