{{
    config(
        materialized='table'
    )
}}

{{ latest_staging_model('stg_skills_historical') }}
