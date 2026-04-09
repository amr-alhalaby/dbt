{{
    config(
        materialized='table'
    )
}}

SELECT
  offset::BIGINT AS _offset,  -- noqa
  employee_id AS id,
  job_function_id,
  primary_skill_id,
  production_category,
  employment_status,
  org_category,
  org_category_type,
  work_start_date,
  work_end_date,
  is_active,
  row_valid_from AS valid_from_datetime,
  row_valid_to AS valid_to_datetime
FROM {{ ref('stg_employees_historical') }}
