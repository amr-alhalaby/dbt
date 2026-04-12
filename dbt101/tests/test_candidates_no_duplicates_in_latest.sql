-- Custom test: Check for duplicate candidate records in latest table
-- Should have exactly one active record per candidate_id

WITH duplicate_check AS (
    SELECT
        candidate_id,
        COUNT(*) AS record_count
    FROM {{ ref('stg_candidates_latest') }}
    GROUP BY candidate_id
    HAVING COUNT(*) > 1
)

SELECT * FROM duplicate_check
