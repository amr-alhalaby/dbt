{{
    config(
        materialized='table'
    )
}}

{{ latest_staging_model('stg_candidates_historical') }}
