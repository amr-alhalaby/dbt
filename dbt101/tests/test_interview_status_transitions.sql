-- Custom test: Ensure interview status transitions are logical
-- An interview should not go from COMPLETED back to IN_PROGRESS
-- Severity: warn - flags data quality issues without failing the pipeline

{{ config(severity='warn') }}

WITH status_changes AS (
    SELECT
        interview_id,
        status,
        updated_at,
        LAG(status) OVER (PARTITION BY interview_id ORDER BY updated_at) AS previous_status
    FROM {{ ref('stg_interviews_historical') }}
),

invalid_transitions AS (
    SELECT
        interview_id,
        previous_status,
        status,
        updated_at
    FROM status_changes
    WHERE
        (previous_status = 'COMPLETED' AND status IN ('DRAFT', 'REQUESTED', 'SCHEDULED', 'IN_PROGRESS'))
        OR (previous_status = 'CANCELLED' AND status != 'CANCELLED')
)

SELECT * FROM invalid_transitions
