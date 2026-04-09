{%- macro generate_row_valid(timestamp_col, pk_column) -%}
    LEAD({{ timestamp_col }}) OVER (
        PARTITION BY {{ pk_column }}
        ORDER BY {{ timestamp_col }}
    )
{%- endmacro -%}
