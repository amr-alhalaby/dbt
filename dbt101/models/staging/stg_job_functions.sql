{{
    config(
        materialized='view'
    )
}}

WITH source AS (
  SELECT * FROM {{ source('raw', 'job_functions') }}
),

renamed AS (
  SELECT
    -- Metadata
    _offset AS offset_value,

    -- Primary keys
    job_function_id,

    -- Attributes
    base_name,
    category,

    -- Flags
    is_active::boolean AS is_active,

    -- Attributes
    level::int AS level,
    track,
    seniority_level,
    seniority_index::int AS seniority_index,

    -- Timestamps
    TO_TIMESTAMP(_created_micros::bigint / 1000000) AS created_at,
    TO_TIMESTAMP(_updated_micros::bigint / 1000000) AS updated_at

  FROM source
)

SELECT * FROM renamed
