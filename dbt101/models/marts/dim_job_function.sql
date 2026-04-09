SELECT
  job_function_id AS id,
  base_name,
  category,
  is_active,
  level::INT AS level,
  track,
  seniority_level,
  seniority_index
FROM {{ ref('stg_job_functions_latest') }}
