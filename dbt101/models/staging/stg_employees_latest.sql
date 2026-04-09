{{
    config(
        materialized='table'
    )
}}

{{ latest_staging_model('stg_employees_historical') }}
