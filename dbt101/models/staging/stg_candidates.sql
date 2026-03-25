{{
    config(
        materialized='view'
    )
}}

WITH source AS (
  SELECT * FROM {{ source('raw', 'candidates') }}
),

renamed AS (
  SELECT
    -- Metadata
    _offset AS offset_value,

    -- Primary keys
    candidate_id,

    -- Foreign keys
    primary_skill_id,
    job_function_id,

    -- Attributes
    staffing_status,
    english_level,

    -- Timestamps
    TO_TIMESTAMP(_created_micros::bigint / 1000000) AS created_at,
    TO_TIMESTAMP(_updated_micros::bigint / 1000000) AS updated_at

  FROM source
)

SELECT * FROM renamed
