{%- macro staging_model(
    source_name,
    table_name,
    materialization='view',
    pk_column=none,
    source_pk_column=none
) -%}

{{
    config(
        materialized=materialization
    )
}}

{%- set config_query -%}
    SELECT
        raw_column_name,
        target_column_name,
        target_data_type,
        target_order_num
    FROM {{ ref('staging_config') }}
    WHERE UPPER(raw_table_name) = UPPER('{{ table_name }}')
    ORDER BY target_order_num
{%- endset -%}

{%- if execute -%}
    {%- set config_results = run_query(config_query) -%}
    {%- set column_configs = config_results.rows -%}
{%- else -%}
    {%- set column_configs = [] -%}
{%- endif -%}

WITH source AS (
    SELECT *
    FROM {{ source(source_name, table_name) }}
    {%- if source_pk_column %}
    WHERE {{ source_pk_column }} IS NOT NULL  -- Filter out records with null primary key
    {%- endif %}
),

transformed AS (
    SELECT
        {%- for col in column_configs %}
        {{ transform_column(col[0], col[1], col[2]) }}
        {%- if not loop.last %},{% endif %}
        {%- endfor %}
    FROM source
),

with_validity AS (
    SELECT
        *,
        updated_at AS row_valid_from,
        {{ generate_row_valid('updated_at', pk_column) }} AS row_valid_to
    FROM transformed
),

final AS (
    SELECT
        *,
        CASE
            WHEN row_valid_to IS NULL THEN 1
            ELSE 0
        END AS row_is_active
    FROM with_validity
)

SELECT * FROM final

{%- endmacro -%}
