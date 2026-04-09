WITH interviews_historical AS (
  SELECT * FROM {{ ref('stg_interviews_historical') }}
),

-- Pivot status changes into separate datetime columns
interview_statuses AS (
  SELECT
    interview_id,
    MIN(created_at) AS created_datetime,
    MAX(CASE WHEN UPPER(status) = 'DRAFT' THEN updated_at END) AS draft_datetime,
    MAX(
      CASE WHEN UPPER(status) = 'REQUESTED' THEN updated_at END
    ) AS requested_datetime,
    MAX(
      CASE WHEN UPPER(status) = 'SCHEDULED' THEN updated_at END
    ) AS scheduled_datetime,
    MAX(
      CASE WHEN UPPER(status) = 'IN_PROGRESS' THEN updated_at END
    ) AS started_datetime,
    MAX(
      CASE WHEN UPPER(status) = 'PENDING_FEEDBACK' THEN updated_at END
    ) AS finished_datetime,
    MAX(
      CASE WHEN UPPER(status) = 'COMPLETED' THEN updated_at END
    ) AS feedback_provided_datetime,
    MAX(
      CASE WHEN UPPER(status) = 'CANCELLED' THEN updated_at END
    ) AS cancelled_datetime
  FROM interviews_historical
  GROUP BY interview_id
),

-- Get interview attributes from earliest record (at creation time)
interview_at_creation AS (
  SELECT *
  FROM interviews_historical
  QUALIFY
    ROW_NUMBER() OVER (
      PARTITION BY interview_id ORDER BY updated_at
    ) = 1
),

-- Point-in-time: candidate offset at interview creation
candidates_pit AS (
  SELECT
    i.interview_id,
    c.offset::BIGINT AS candidate_offset
  FROM interview_at_creation AS i
  LEFT JOIN {{ ref('stg_candidates_historical') }} AS c
    ON
      i.candidate_id = c.candidate_id
      AND i.created_at >= c.row_valid_from
      AND (i.created_at < c.row_valid_to OR c.row_valid_to IS NULL)
),

-- Point-in-time: interviewer offset at interview creation
employees_pit AS (
  SELECT
    i.interview_id,
    e.offset::BIGINT AS interviewer_offset
  FROM interview_at_creation AS i
  LEFT JOIN {{ ref('stg_employees_historical') }} AS e
    ON
      i.interviewer_id = e.employee_id
      AND i.created_at >= e.row_valid_from
      AND (i.created_at < e.row_valid_to OR e.row_valid_to IS NULL)
),

final AS (
  SELECT
    -- Primary key
    i.interview_id AS id,

    -- Attributes
    i.candidate_type,
    c.candidate_offset,
    i.status,
    e.interviewer_offset,
    i.location,
    i.logged::BOOLEAN AS is_logged,
    i.media_available::BOOLEAN AS is_media_available,
    i.run_type,
    i.interview_type AS type,  -- noqa: RF04
    i.media_status,
    i.invite_answer_status,

    -- Date key (for dim_date join)
    s.created_datetime::DATE AS created_date,

    -- Status datetime columns
    s.created_datetime,
    s.draft_datetime,
    s.requested_datetime,
    s.scheduled_datetime,
    s.started_datetime,
    s.finished_datetime,
    s.feedback_provided_datetime,
    s.cancelled_datetime,

    -- interview_duration: in_progress -> pending_feedback
    CASE  -- noqa: LT02
      WHEN s.started_datetime IS NOT NULL
        AND s.finished_datetime IS NOT NULL
        THEN
          DATEDIFF(
            'minute',
            s.started_datetime,
            s.finished_datetime
          )
    END AS interview_duration,

    -- feedback_delay: pending_feedback -> completed
    CASE  -- noqa: LT02
      WHEN s.finished_datetime IS NOT NULL
        AND s.feedback_provided_datetime IS NOT NULL
        THEN
          DATEDIFF(
            'minute',
            s.finished_datetime,
            s.feedback_provided_datetime
          )
    END AS feedback_delay

  FROM interview_at_creation AS i
  LEFT JOIN interview_statuses AS s
    ON i.interview_id = s.interview_id
  LEFT JOIN candidates_pit AS c
    ON i.interview_id = c.interview_id
  LEFT JOIN employees_pit AS e
    ON i.interview_id = e.interview_id
)

SELECT * FROM final
