{{
    config(
        materialized='view'
    )
}}

WITH source AS (
  SELECT * FROM {{ source('raw', 'employees') }}
),

renamed AS (
  SELECT
    -- Primary keys
    employee_id,

    -- Foreign keys
    job_function_id,
    primary_skill_id,

    -- Attributes
    production_category,
    employment_status,
    org_category,
    org_category_type,

    -- Dates
    is_active::boolean AS is_active,
    _offset,

    -- Flags
    to_timestamp(work_start_micros::bigint / 1000000) AS work_start_date,

    -- Metadata
    CASE
      WHEN work_end_micros = '' THEN NULL
      ELSE to_timestamp(work_end_micros::bigint / 1000000)
    END AS work_end_date,
    to_timestamp(_created_micros::bigint / 1000000) AS created_at,
    to_timestamp(_updated_micros::bigint / 1000000) AS updated_at

  FROM source
)

SELECT * FROM renamed
