{{
    config(
        materialized='table'
    )
}}

{{ latest_staging_model('stg_interviews_historical') }}
