SELECT
  offset::BIGINT AS _offset,  -- noqa
  candidate_id AS id,
  primary_skill_id,
  staffing_status,
  english_level,
  job_function_id,
  row_valid_from AS valid_from_datetime,
  row_valid_to AS valid_to_datetime
FROM {{ ref('stg_candidates_historical') }}
