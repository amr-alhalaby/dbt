{%- macro latest_staging_model(staging_ref) -%}

SELECT *
FROM {{ ref(staging_ref) }}
WHERE row_is_active = 1

{%- endmacro -%}
