{{
    config(
        materialized='table'
    )
}}

WITH source AS (
  SELECT * FROM {{ source('raw', 'employees') }}
),

renamed AS (
  SELECT
    -- Metadata
    _offset AS offset_value,

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

    -- Flags
    is_active::boolean AS is_active,

    -- Dates
    DATE(TO_TIMESTAMP(work_start_micros::bigint / 1000000)) AS work_start_date,
    CASE
      WHEN work_end_micros = '' THEN NULL
      ELSE DATE(TO_TIMESTAMP(work_end_micros::bigint / 1000000))
    END AS work_end_date,

    -- Timestamps
    TO_TIMESTAMP(_created_micros::bigint / 1000000) AS created_at,
    TO_TIMESTAMP(_updated_micros::bigint / 1000000) AS updated_at

  FROM source
)

SELECT * FROM renamed
