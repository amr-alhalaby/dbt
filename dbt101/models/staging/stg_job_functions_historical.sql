{{ staging_model(
    source_name='raw',
    table_name='job_functions',
    materialization='view',
    pk_column='job_function_id'
) }}
