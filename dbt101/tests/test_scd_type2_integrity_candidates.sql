-- Custom test: Verify SCD Type 2 logic for candidates
-- Each candidate should have non-overlapping validity windows
-- Current records (row_valid_to IS NULL) should have row_is_active = 1

WITH overlapping_records AS (
    SELECT
        a.candidate_id,
        a.row_valid_from AS a_valid_from,
        a.row_valid_to AS a_valid_to,
        b.row_valid_from AS b_valid_from,
        b.row_valid_to AS b_valid_to
    FROM {{ ref('stg_candidates_historical') }} a
    INNER JOIN {{ ref('stg_candidates_historical') }} b
        ON a.candidate_id = b.candidate_id
        AND a.offset != b.offset
    WHERE
        -- Check for overlapping date ranges
        a.row_valid_from < COALESCE(b.row_valid_to, '9999-12-31'::TIMESTAMP_NTZ)
        AND COALESCE(a.row_valid_to, '9999-12-31'::TIMESTAMP_NTZ) > b.row_valid_from
),

invalid_active_flags AS (
    SELECT
        candidate_id,
        row_valid_to,
        row_is_active
    FROM {{ ref('stg_candidates_historical') }}
    WHERE
        (row_valid_to IS NULL AND row_is_active != 1)
        OR (row_valid_to IS NOT NULL AND row_is_active != 0)
)

SELECT 'overlapping' AS error_type, candidate_id FROM overlapping_records
UNION ALL
SELECT 'invalid_flag' AS error_type, candidate_id FROM invalid_active_flags
