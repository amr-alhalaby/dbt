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
    -- Primary keys
    job_function_id,

    -- Attributes
    base_name,
    category,
    level::int AS level,
    track,
    seniority_level,
    seniority_index::int AS seniority_index,

    -- Flags
    is_active::boolean AS is_active,

    -- Metadata
    _offset,
    to_timestamp(_created_micros::bigint / 1000000) AS created_at,
    to_timestamp(_updated_micros::bigint / 1000000) AS updated_at

  FROM source
)

SELECT * FROM renamed
