{{
    config(
        materialized='table'
    )
}}

{{ latest_staging_model('stg_job_functions_historical') }}
