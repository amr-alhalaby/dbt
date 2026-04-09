{%- macro transform_column(raw_column, target_column, data_type) -%}
    {{ raw_column }}::{{ data_type }} AS {{ target_column }}
{%- endmacro -%}
