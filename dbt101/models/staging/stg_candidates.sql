{{
    config(
        materialized='incremental',
        unique_key='candidate_id'
    )
}}

WITH source AS (
  SELECT * FROM {{ source('raw', 'candidates') }}
  {% if is_incremental() %}
    WHERE to_timestamp(_updated_micros::bigint / 1000000) > (SELECT max(updated_at) FROM {{ this }})
  {% endif %}
),

renamed AS (
  SELECT
    -- Primary keys
    candidate_id,

    -- Foreign keys
    primary_skill_id,
    job_function_id,

    -- Attributes
    staffing_status,
    english_level,

    -- Metadata
    _offset,
    -- Convert microseconds to timestamp (cast VARCHAR to BIGINT first)
    to_timestamp(_created_micros::bigint / 1000000) AS created_at,
    to_timestamp(_updated_micros::bigint / 1000000) AS updated_at

  FROM source
)

SELECT * FROM renamed
