{{ config(materialized='table') }}

{% set dept_models = [
    'stg_dept_IT',
    'stg_dept_HR',
    'stg_dept_Finance'
] %}

{% set all_columns = [] %}

-- Collect all columns across models
{% for model in dept_models %}
    {% set cols = adapter.get_columns_in_relation(ref(model)) %}
    {% for col in cols %}
        {% if col.name not in all_columns %}
            {% do all_columns.append(col.name) %}
        {% endif %}
    {% endfor %}
{% endfor %}

{% for model in dept_models %}

    {% set cols = adapter.get_columns_in_relation(ref(model)) %}
    {% set model_cols = cols | map(attribute='name') | list %}

SELECT
    {% for col in all_columns %}
        {% if col in model_cols %}
            {{ col }}
        {% else %}
            NULL AS {{ col }}
        {% endif %}
        {% if not loop.last %}, {% endif %}
    {% endfor %}
FROM {{ ref(model) }}

{% if not loop.last %}
UNION ALL
{% endif %}

{% endfor %}