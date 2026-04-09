{{ staging_model(
    source_name='raw',
    table_name='skills',
    materialization='view',
    pk_column='skill_id'
) }}
