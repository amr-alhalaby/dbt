{{
    config(
        materialized='table'
    )
}}

SELECT
  skill_id AS id,
  is_active,
  skill_type AS type,  -- noqa: RF04
  skill_name AS name,  -- noqa: RF04
  url,
  parent_skill_id AS parent_id
FROM {{ ref('stg_skills_latest') }}
