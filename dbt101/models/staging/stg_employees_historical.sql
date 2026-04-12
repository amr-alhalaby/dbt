-- Example: Historical employees with incremental materialization

{{ staging_model(
    source_name='raw',
    table_name='employees',
    materialization='view',
    pk_column='employee_id',
    source_pk_column='employee_id'
) }}
