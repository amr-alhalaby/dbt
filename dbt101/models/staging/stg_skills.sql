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
    -- Primary keys
    id AS skill_id,

    -- Foreign keys
    parent_id,

    -- Attributes
    type AS skill_type,
    name AS skill_name,
    url,
    is_key_reason,

    -- Flags
    is_active::boolean AS is_active,
    is_primary::boolean AS is_primary,
    is_key::boolean AS is_key,

    -- Metadata
    _offset,
    to_timestamp(_created_micros::bigint / 1000000) AS created_at,
    to_timestamp(_updated_micros::bigint / 1000000) AS updated_at

  FROM source
)

SELECT * FROM renamed
