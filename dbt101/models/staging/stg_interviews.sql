{{
    config(
        materialized='view'
    )
}}

WITH source AS (
  SELECT * FROM {{ source('raw', 'interviews') }}
),

renamed AS (
  SELECT
    -- Primary keys
    id AS interview_id,

    -- Foreign keys
    candidate_id,
    interviewer_id,

    -- Attributes
    candidate_type,
    status,
    location,
    run_type,
    type AS interview_type,
    media_status,
    invite_answer_status,

    -- Flags
    logged::boolean AS is_logged,
    media_available::boolean AS is_media_available,

    -- Metadata
    _offset,
    to_timestamp(_created_micros::bigint / 1000000) AS created_at,
    to_timestamp(_updated_micros::bigint / 1000000) AS updated_at

  FROM source
)

SELECT * FROM renamed
