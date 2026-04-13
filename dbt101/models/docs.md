{% docs weekend_logic %}

## Weekend Calculation Logic

The `is_weekend` column identifies whether a date falls on a weekend using Snowflake's `DAYOFWEEK()` function.

**Important:** Snowflake's `DAYOFWEEK()` returns:
- `0` = Sunday
- `1` = Monday
- `2` = Tuesday
- `3` = Wednesday
- `4` = Thursday
- `5` = Friday
- `6` = Saturday

Therefore, weekends are identified as `DAYOFWEEK(date) IN (0, 6)`.

This differs from some other database systems where Sunday might be 1 or 7, so it's important to verify the behavior in your environment before relying on this logic.

{% enddocs %}

{% docs historical_scd_type2_pattern %}

## Historical Staging Models - SCD Type 2

Historical staging models implement **Slowly Changing Dimension Type 2** tracking, which preserves the complete history of changes for each record.

### Key Columns

- **`_loaded_at`**: Timestamp when the record was loaded into the warehouse
- **`_valid_from`**: Start of the period when this version of the record was valid
- **`_valid_to`**: End of the period when this version was valid (NULL for current record)
- **`row_is_active`**: Boolean flag indicating if this is the current/active version (1 = active, 0 = historical)

### Usage Pattern

These models use the custom `staging_model()` macro which:
1. Extracts data from source tables
2. Adds technical columns for SCD Type 2 tracking
3. Maintains full history of all changes
4. Enables point-in-time analysis

### Querying Historical Data

```sql
-- Get current records only
SELECT * FROM stg_candidates_historical WHERE row_is_active = 1

-- Get historical snapshot at specific date
SELECT * FROM stg_candidates_historical
WHERE '2024-01-15' BETWEEN _valid_from AND COALESCE(_valid_to, '9999-12-31')
```

{% enddocs %}

{% docs point_in_time_joins %}

## Point-in-Time (PIT) Joins

Point-in-time joins retrieve the version of a record that was valid at a specific moment in history, enabling accurate historical analysis.

### Pattern

In `fct_interview`, when joining candidate data to an interview, we need the candidate's attributes **as they were** when the interview was created, not as they are today.

```sql
SELECT
    i.interview_id,
    c.offset AS candidate_offset,
    c.primary_skill_id,
    c.staffing_status
FROM interview_at_creation AS i
LEFT JOIN stg_candidates_historical AS c
    ON i.candidate_id = c.candidate_id
    AND i.created_at BETWEEN c._valid_from AND COALESCE(c._valid_to, '9999-12-31')
```

### Why This Matters

Without PIT joins, joining to SCD Type 2 tables would return:
- Multiple rows per key (one for each historical version)
- Current values instead of historical values
- Inaccurate historical reporting

### Best Practices

- Always include the temporal join condition: `AND event_time BETWEEN _valid_from AND COALESCE(_valid_to, '9999-12-31')`
- Use the earliest/creation timestamp for the fact event
- Document which point in time you're joining to (creation, completion, etc.)

{% enddocs %}

{% docs offset_column %}

## Offset Column

The `offset` column is a **technical surrogate key** generated during data ingestion from source systems.

### Characteristics

- **Type**: `BIGINT` (not incrementing integer)
- **Source**: System-generated during initial load
- **Uniqueness**: Unique within a table but not meaningful across tables
- **Use Case**: Row-level identification and deduplication

### When to Use

✅ **Do use** for:
- Deduplication within a single table
- Technical tracking and debugging
- Joining historical snapshots when business keys + timestamps aren't sufficient

❌ **Don't use** for:
- Business logic or joins across tables
- User-facing reports or analytics
- Assuming sequential ordering

### Example

```sql
-- Correct: Use offset for technical deduplication
SELECT DISTINCT ON (offset) *
FROM raw.candidates

-- Incorrect: Don't use offset for business joins
-- Use business keys (candidate_id, employee_id) instead
```

{% enddocs %}

{% docs interview_status_timeline %}

## Interview Status Timeline

The `fct_interview` model pivots interview status changes into a timeline with discrete datetime columns for each major milestone.

### Status Progression

Interviews typically flow through these statuses:

1. **DRAFT** → Interview created but not finalized
2. **REQUESTED** → Interview formally requested
3. **SCHEDULED** → Time and date confirmed
4. **IN_PROGRESS** → Interview is happening
5. **PENDING_FEEDBACK** → Interview complete, awaiting feedback
6. **COMPLETED** → Feedback provided
7. **CANCELLED** → Interview was cancelled (terminal state)

### Timeline Columns

Each status has a corresponding `_datetime` column capturing when that status was reached:
- `created_datetime` - When the interview record was first created
- `draft_datetime` - When status = 'DRAFT'
- `requested_datetime` - When status = 'REQUESTED'
- `scheduled_datetime` - When status = 'SCHEDULED'
- `started_datetime` - When status = 'IN_PROGRESS'
- `finished_datetime` - When status = 'PENDING_FEEDBACK'
- `feedback_provided_datetime` - When status = 'COMPLETED'
- `cancelled_datetime` - When status = 'CANCELLED'

This structure enables easy calculation of duration metrics:
```sql
-- Time from creation to scheduling
DATEDIFF('hour', created_datetime, scheduled_datetime) AS hours_to_schedule

-- Time from finish to feedback
DATEDIFF('day', finished_datetime, feedback_provided_datetime) AS days_to_feedback
```

{% enddocs %}
