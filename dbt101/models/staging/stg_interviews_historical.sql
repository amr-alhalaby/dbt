{{ staging_model(
    source_name='raw',
    table_name='interviews',
    materialization='view',
    pk_column='interview_id',
    source_pk_column='id'
) }}
