{{
    config(
        materialized='view'
    )
}}

WITH source AS (
  SELECT * FROM {{ source('raw', 'skills') }}
),

renamed AS (
  SELECT
    -- Metadata
    _offset AS offset_value,

    -- Primary keys
    id AS skill_id,

    -- Flags
    is_active::boolean AS is_active,
    is_primary::boolean AS is_primary,
    is_key::boolean AS is_key,
    is_key_reason::boolean AS is_key_reason,

    -- Attributes
    type,
    name,
    url,

    -- Foreign keys
    parent_id,

    -- Timestamps
    TO_TIMESTAMP(_created_micros::bigint / 1000000) AS created_at,
    TO_TIMESTAMP(_updated_micros::bigint / 1000000) AS updated_at

  FROM source
)

SELECT * FROM renamed
