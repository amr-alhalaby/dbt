{{
    config(
        materialized='incremental',
        unique_key='interview_id',
        transient=true
    )
}}

WITH source AS (
  SELECT * FROM {{ source('raw', 'interviews') }}
  {% if is_incremental() %}
    WHERE TO_TIMESTAMP(_updated_micros::bigint / 1000000) > (
      SELECT MAX(tbl.updated_at)
      FROM {{ this }} AS tbl
    )
  {% endif %}
),

renamed AS (
  SELECT
    -- Metadata
    _offset AS offset_value,

    -- Primary keys
    id AS interview_id,

    -- Attributes
    candidate_type,

    -- Foreign keys
    candidate_id,
    status,
    interviewer_id,
    location,

    -- Flags
    logged::boolean AS is_logged,
    media_available::boolean AS is_media_available,

    -- Attributes
    run_type,
    type,
    media_status,
    invite_answer_status,

    -- Timestamps
    TO_TIMESTAMP(_created_micros::bigint / 1000000) AS created_at,
    TO_TIMESTAMP(_updated_micros::bigint / 1000000) AS updated_at

  FROM source
)

SELECT * FROM renamed
