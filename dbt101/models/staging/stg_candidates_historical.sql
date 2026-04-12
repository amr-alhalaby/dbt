{{ staging_model(
    source_name='raw',
    table_name='candidates',
    materialization='view',
    pk_column='candidate_id',
    source_pk_column='candidate_id'
) }}
